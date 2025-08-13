/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.flink.lake.split.LakeSnapshotSplit;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.InnerTableScan;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.flink.utils.DataLakeUtils.extractLakeCatalogProperties;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** A generator for lake splits. */
public class LakeSplitGenerator {

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;

    private final LakeSource<LakeSplit> lakeSource;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
    }

    public List<SourceSplitBase> generateHybridLakeSplits() throws Exception {
        // get the file store
        LakeSnapshot lakeSnapshotInfo =
                flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();
        FileStoreTable fileStoreTable =
                getTable(
                        lakeSnapshotInfo.getSnapshotId(),
                        extractLakeCatalogProperties(tableInfo.getProperties()));

        boolean isLogTable = !tableInfo.hasPrimaryKey();
        boolean isPartitioned = tableInfo.isPartitioned();

        Map<String, Map<Integer, List<LakeSplit>>> lakeSplits =
                groupLakeSplits(
                        lakeSource
                                .createPlanner(
                                        (LakeSource.PlannerContext) lakeSnapshotInfo::getSnapshotId)
                                .plan());
        if (isPartitioned) {
            List<PartitionInfo> partitionInfos =
                    flussAdmin.listPartitionInfos(tableInfo.getTablePath()).get();
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));
            return generatePartitionTableSplit(
                    lakeSplits,
                    isLogTable,
                    lakeSnapshotInfo.getTableBucketsOffset(),
                    partitionNameById,
                    fileStoreTable);
        } else {
            Map<Integer, List<LakeSplit>> nonPartitionLakeSplits =
                    lakeSplits.values().iterator().next();
            // non-partitioned table
            return generateNoPartitionedTableSplit(
                    nonPartitionLakeSplits,
                    isLogTable,
                    lakeSnapshotInfo.getTableBucketsOffset(),
                    fileStoreTable);
        }
    }

    private Map<String, Map<Integer, List<LakeSplit>>> groupLakeSplits(List<LakeSplit> lakeSplits) {
        Map<String, Map<Integer, List<LakeSplit>>> result = new HashMap<>();
        for (LakeSplit split : lakeSplits) {
            String partition = String.join("$", split.partition());
            int bucket = split.bucket();
            // Get or create the partition group
            Map<Integer, List<LakeSplit>> bucketMap =
                    result.computeIfAbsent(partition, k -> new HashMap<>());
            List<LakeSplit> splitList = bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>());
            splitList.add(split);
        }
        return result;
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById,
            @Nullable FileStoreTable fileStoreTable)
            throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>();
        Map<String, Long> flussPartitionIdByName =
                partitionNameById.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getValue,
                                        Map.Entry::getKey,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));
        long lakeSplitPartitionId = -1L;

        // iterate lake splits
        for (Map.Entry<String, Map<Integer, List<LakeSplit>>> lakeSplitEntry :
                lakeSplits.entrySet()) {
            String partitionName = lakeSplitEntry.getKey();
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition = lakeSplitEntry.getValue();
            Long partitionId = flussPartitionIdByName.remove(partitionName);
            if (partitionId != null) {
                // mean the partition also exist in fluss partition
                Map<Integer, Long> bucketEndOffset =
                        stoppingOffsetInitializer.getBucketOffsets(
                                partitionName,
                                IntStream.range(0, bucketCount)
                                        .boxed()
                                        .collect(Collectors.toList()),
                                bucketOffsetsRetriever);
                splits.addAll(
                        generateSplit(
                                lakeSplitsOfPartition,
                                partitionId,
                                partitionName,
                                isLogTable,
                                tableBucketSnapshotLogOffset,
                                bucketEndOffset,
                                fileStoreTable));

            } else {
                // only lake data
                splits.addAll(
                        toLakeSnapshotSplits(
                                lakeSplitsOfPartition,
                                partitionName,
                                // now, we can't get partition id for the partition only
                                // in lake, set them to a arbitrary partition id, but
                                // make sure different partition have different partition id
                                // to enable different partition can be distributed to different
                                // tasks
                                lakeSplitPartitionId--));
            }
        }

        // iterate remain fluss splits
        for (Map.Entry<String, Long> partitionIdByNameEntry : flussPartitionIdByName.entrySet()) {
            String partitionName = partitionIdByNameEntry.getKey();
            long partitionId = partitionIdByNameEntry.getValue();
            Map<Integer, Long> bucketEndOffset =
                    stoppingOffsetInitializer.getBucketOffsets(
                            partitionName,
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
            splits.addAll(
                    generateSplit(
                            null,
                            partitionId,
                            partitionName,
                            isLogTable,
                            // pass empty map since we won't read lake splits
                            Collections.emptyMap(),
                            bucketEndOffset,
                            fileStoreTable));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Integer, Long> bucketEndOffset,
            @Nullable FileStoreTable fileStoreTable) {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            if (lakeSplits != null) {
                splits.addAll(toLakeSnapshotSplits(lakeSplits, partitionName, partitionId));
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                long stoppingOffset = bucketEndOffset.get(bucket);
                if (snapshotLogOffset == null) {
                    // no any data commit to this bucket, scan from fluss log
                    splits.add(
                            new LogSplit(
                                    tableBucket, partitionName, EARLIEST_OFFSET, stoppingOffset));
                } else {
                    // need to read remain fluss log
                    if (snapshotLogOffset < stoppingOffset) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        snapshotLogOffset,
                                        stoppingOffset));
                    }
                }
            }
        } else {
            // it's primary key table
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                long stoppingOffset = bucketEndOffset.get(bucket);
                FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();

                splits.add(
                        generateSplitForPrimaryKeyTableBucket(
                                fileStoreTable,
                                splitGenerator,
                                tableBucket,
                                partitionName,
                                snapshotLogOffset,
                                stoppingOffset));
            }
        }

        return splits;
    }

    private List<SourceSplitBase> toLakeSnapshotSplits(
            Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable String partitionName,
            @Nullable Long partitionId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (LakeSplit lakeSplit :
                lakeSplits.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, lakeSplit.bucket());
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit));
        }
        return splits;
    }

    private SourceSplitBase generateSplitForPrimaryKeyTableBucket(
            FileStoreTable fileStoreTable,
            FileStoreSourceSplitGenerator splitGenerator,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset) {

        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return new PaimonSnapshotAndFlussLogSplit(
                    tableBucket, partitionName, null, EARLIEST_OFFSET, stoppingOffset);
        }

        // then, generate a split contains
        // snapshot and change log so that we can merge change log and snapshot
        // to get the full data
        fileStoreTable =
                fileStoreTable.copy(
                        Collections.singletonMap(
                                CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
                                // we set a max size to make sure only one splits
                                MemorySize.MAX_VALUE.toString()));
        InnerTableScan tableScan =
                fileStoreTable.newScan().withBucketFilter((b) -> b == tableBucket.getBucket());

        if (partitionName != null) {
            tableScan =
                    tableScan.withPartitionFilter(getPartitionSpec(fileStoreTable, partitionName));
        }

        List<FileStoreSourceSplit> fileStoreSourceSplits =
                splitGenerator.createSplits(tableScan.plan());

        checkState(fileStoreSourceSplits.size() == 1, "Splits for primary key table must be 1.");
        FileStoreSourceSplit fileStoreSourceSplit = fileStoreSourceSplits.get(0);
        return new PaimonSnapshotAndFlussLogSplit(
                tableBucket,
                partitionName,
                fileStoreSourceSplit,
                snapshotLogOffset,
                stoppingOffset);
    }

    private Map<String, String> getPartitionSpec(
            FileStoreTable fileStoreTable, String partitionName) {
        List<String> partitionKeys = fileStoreTable.partitionKeys();
        checkState(
                partitionKeys.size() == 1,
                "Must only one partition key for paimon table %, but got %s, the partition keys are: ",
                tableInfo.getTablePath(),
                partitionKeys.size(),
                partitionKeys);
        return Collections.singletonMap(partitionKeys.get(0), partitionName);
    }

    private FileStoreTable getTable(long snapshotId, Map<String, String> catalogProperties)
            throws Exception {
        try (Catalog catalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(catalogProperties))) {
            return (FileStoreTable)
                    catalog.getTable(
                                    Identifier.create(
                                            tableInfo.getTablePath().getDatabaseName(),
                                            tableInfo.getTablePath().getTableName()))
                            .copy(
                                    Collections.singletonMap(
                                            CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                            String.valueOf(snapshotId)));
        }
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            Map<Integer, List<LakeSplit>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            FileStoreTable fileStoreTable) {
        // iterate all bucket
        // assume bucket is from 0 to bucket count
        Map<Integer, Long> bucketEndOffset =
                stoppingOffsetInitializer.getBucketOffsets(
                        null,
                        IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                        bucketOffsetsRetriever);
        return generateSplit(
                lakeSplits,
                null,
                null,
                isLogTable,
                tableBucketSnapshotLogOffset,
                bucketEndOffset,
                fileStoreTable);
    }
}
