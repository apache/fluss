/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.lakehouse;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.split.PaimonSnapshotSplit;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.connector.flink.source.split.LogSplit;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.connector.flink.utils.DataLakeUtils.extractLakeCatalogProperties;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * A generator for lake splits.
 *
 * <p>todo: current is always assume is paimon, make it pluggable.
 */
public class LakeSplitGenerator {

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
    }

    public List<SourceSplitBase> generateLakeSplits() throws Exception {
        // get the file store
        LakeSnapshot lakeSnapshotInfo =
                flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();
        FileStoreTable fileStoreTable =
                getTable(
                        lakeSnapshotInfo.getSnapshotId(),
                        extractLakeCatalogProperties(tableInfo.getProperties()));
        boolean isLogTable = fileStoreTable.schema().primaryKeys().isEmpty();
        boolean isPartitioned = !fileStoreTable.schema().partitionKeys().isEmpty();

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
                    isLogTable,
                    fileStoreTable,
                    lakeSnapshotInfo.getTableBucketsOffset(),
                    partitionNameById);
        } else {
            // non-partitioned table
            return generateNoPartitionedTableSplit(
                    isLogTable, fileStoreTable, lakeSnapshotInfo.getTableBucketsOffset());
        }
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            boolean isLogTable,
            FileStoreTable fileStoreTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Map.Entry<Long, String> partitionNameByIdEntry : partitionNameById.entrySet()) {
            long partitionId = partitionNameByIdEntry.getKey();
            String partitionName = partitionNameByIdEntry.getValue();
            Map<Integer, Long> bucketEndOffset =
                    stoppingOffsetInitializer.getBucketOffsets(
                            partitionName,
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
            splits.addAll(
                    generateSplit(
                            fileStoreTable,
                            partitionId,
                            partitionName,
                            isLogTable,
                            tableBucketSnapshotLogOffset,
                            bucketEndOffset));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            FileStoreTable fileStoreTable,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Integer, Long> bucketEndOffset) {
        List<SourceSplitBase> splits = new ArrayList<>();
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
        if (isLogTable) {
            // it's log table, we don't care about bucket, and we can't get bucket in paimon's
            // dynamic bucket; so first generate split for the whole paimon snapshot,
            // then generate log split for each bucket paimon snapshot + fluss log
            splits.addAll(
                    generateSplitForLogSnapshot(
                            fileStoreTable, splitGenerator, partitionId, partitionName));
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

    private List<SourceSplitBase> generateSplitForLogSnapshot(
            FileStoreTable fileStoreTable,
            FileStoreSourceSplitGenerator splitGenerator,
            @Nullable Long partitionId,
            @Nullable String partitionName) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // paimon snapshot
        InnerTableScan scan = fileStoreTable.newScan();
        if (partitionName != null) {
            scan = scan.withPartitionFilter(getPartitionSpec(fileStoreTable, partitionName));
        }
        // for snapshot splits, we always use bucket = -1 ad the bucket since we can't get bucket in
        // paimon's log table
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, -1);
        // snapshot splits + one log split
        for (FileStoreSourceSplit fileStoreSourceSplit : splitGenerator.createSplits(scan.plan())) {
            splits.add(new PaimonSnapshotSplit(tableBucket, partitionName, fileStoreSourceSplit));
        }
        return splits;
    }

    private Map<String, String> getPartitionSpec(
            FileStoreTable fileStoreTable, String partitionName) {
        List<String> partitionKeys = fileStoreTable.partitionKeys();
        checkState(
                partitionKeys.size() == 1,
                "Must only one partition key for paimon table %, but got %s, the partition keys are: ",
                tableInfo.getTablePath(),
                partitionKeys.size(),
                partitionKeys.size());
        return Collections.singletonMap(partitionKeys.get(0), partitionName);
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

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            boolean isLogTable,
            FileStoreTable fileStoreTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset) {
        // iterate all bucket
        // assume bucket is from 0 to bucket count
        Map<Integer, Long> bucketEndOffset =
                stoppingOffsetInitializer.getBucketOffsets(
                        null,
                        IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                        bucketOffsetsRetriever);
        return generateSplit(
                fileStoreTable,
                null,
                null,
                isLogTable,
                tableBucketSnapshotLogOffset,
                bucketEndOffset);
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
}
