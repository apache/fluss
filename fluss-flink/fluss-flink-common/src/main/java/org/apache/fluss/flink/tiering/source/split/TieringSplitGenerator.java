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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.source.enumerator.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.utils.Preconditions.checkState;

/** A generator for lake splits. */
public class TieringSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSplitGenerator.class);

    private final Admin flussAdmin;

    public TieringSplitGenerator(Admin flussAdmin) {
        this.flussAdmin = flussAdmin;
    }

    public List<TieringSplit> generateTableSplits(TablePath tablePath) throws Exception {

        final TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
        final BucketOffsetsRetriever bucketOffsetsRetriever =
                new BucketOffsetsRetrieverImpl(flussAdmin, tablePath);

        // Get table lake snapshot info of the given table.
        LakeSnapshot lakeSnapshotInfo;
        try {
            lakeSnapshotInfo = flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();
            LOG.info("Last committed lake table snapshot info is:{}", lakeSnapshotInfo);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (t instanceof LakeTableSnapshotNotExistException) {
                lakeSnapshotInfo = null;
            } else {
                throw new FlinkRuntimeException(
                        String.format(
                                "Failed to get table snapshot for table %s",
                                tableInfo.getTablePath()),
                        ExceptionUtils.stripCompletionException(e));
            }
        }
        // partitioned table
        if (tableInfo.isPartitioned()) {
            List<PartitionInfo> partitionInfos =
                    flussAdmin.listPartitionInfos(tableInfo.getTablePath()).get();
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));

            return generatePartitionTableSplit(
                    tableInfo, partitionNameById, bucketOffsetsRetriever, lakeSnapshotInfo);
        } else {
            // non-partitioned table
            return generateNonPartitionedTableSplit(
                    tableInfo, bucketOffsetsRetriever, lakeSnapshotInfo);
        }
    }

    /** Generates all splits for partitioned table. */
    private List<TieringSplit> generatePartitionTableSplit(
            TableInfo tableInfo,
            Map<Long, String> partitionNameById,
            BucketOffsetsRetriever bucketOffsetsRetriever,
            @Nullable LakeSnapshot lakeSnapshotInfo) {
        List<TieringSplit> splits = new ArrayList<>();
        for (Map.Entry<Long, String> partitionNameByIdEntry : partitionNameById.entrySet()) {
            long partitionId = partitionNameByIdEntry.getKey();
            String partitionName = partitionNameByIdEntry.getValue();
            Map<Integer, Long> latestBucketsOffset =
                    bucketOffsetsRetriever.latestOffsets(
                            partitionName,
                            IntStream.range(0, tableInfo.getNumBuckets())
                                    .boxed()
                                    .collect(Collectors.toList()));
            KvSnapshots latestKvSnapshots = null;
            if (tableInfo.hasPrimaryKey()) {
                // get the table partition latest kv snapshot info
                try {
                    latestKvSnapshots =
                            flussAdmin
                                    .getLatestKvSnapshots(tableInfo.getTablePath(), partitionName)
                                    .get();
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Failed to get table snapshot for table %s and partition %s",
                                    tableInfo.getTablePath(), partitionName),
                            ExceptionUtils.stripCompletionException(e));
                }
            }

            splits.addAll(
                    generateTableSplit(
                            tableInfo,
                            partitionId,
                            partitionName,
                            lakeSnapshotInfo,
                            latestKvSnapshots,
                            latestBucketsOffset));
        }
        return splits;
    }

    /** Generates all splits for Non-partitioned table. */
    private List<TieringSplit> generateNonPartitionedTableSplit(
            TableInfo tableInfo,
            BucketOffsetsRetriever bucketOffsetsRetriever,
            @Nullable LakeSnapshot lakeSnapshotInfo) {
        Map<Integer, Long> latestBucketsOffset =
                bucketOffsetsRetriever.latestOffsets(
                        null,
                        IntStream.range(0, tableInfo.getNumBuckets())
                                .boxed()
                                .collect(Collectors.toList()));
        KvSnapshots latestKvSnapshots = null;
        if (tableInfo.hasPrimaryKey()) {
            try {
                latestKvSnapshots = flussAdmin.getLatestKvSnapshots(tableInfo.getTablePath()).get();
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Failed to get table snapshot for table %s",
                                tableInfo.getTablePath()),
                        ExceptionUtils.stripCompletionException(e));
            }
        }

        return generateTableSplit(
                tableInfo, null, null, lakeSnapshotInfo, latestKvSnapshots, latestBucketsOffset);
    }

    private List<TieringSplit> generateTableSplit(
            TableInfo tableInfo,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            @Nullable LakeSnapshot lakeSnapshotInfo,
            @Nullable KvSnapshots latestKvSnapshots,
            Map<Integer, Long> latestBucketsOffset) {
        List<TieringSplit> splits = new ArrayList<>();

        if (tableInfo.hasPrimaryKey()) {
            // it's primary key table
            checkState(latestKvSnapshots != null);
            for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long lastCommittedBucketOffset =
                        lakeSnapshotInfo != null
                                ? lakeSnapshotInfo.getTableBucketsOffset().get(tableBucket)
                                : null;
                Long latestSnapshotId =
                        latestKvSnapshots.getSnapshotId(bucket).isPresent()
                                ? latestKvSnapshots.getSnapshotId(bucket).getAsLong()
                                : null;
                Long offsetOfLatestSnapshotId =
                        latestKvSnapshots.getSnapshotId(bucket).isPresent()
                                ? latestKvSnapshots.getLogOffset(bucket).getAsLong()
                                : null;
                Long latestBucketOffset = latestBucketsOffset.get(bucket);

                generateSplitForPrimaryKeyTableBucket(
                                tableInfo.getTablePath(),
                                tableBucket,
                                partitionName,
                                latestSnapshotId,
                                offsetOfLatestSnapshotId,
                                lastCommittedBucketOffset,
                                latestBucketOffset)
                        .ifPresent(splits::add);
            }

        } else {
            // it's log table
            for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long lastCommittedOffset =
                        lakeSnapshotInfo != null
                                ? lakeSnapshotInfo.getTableBucketsOffset().get(tableBucket)
                                : null;
                long latestBucketOffset = latestBucketsOffset.get(bucket);
                generateSplitForLogTableBucket(
                                tableInfo.getTablePath(),
                                tableBucket,
                                partitionName,
                                lastCommittedOffset,
                                latestBucketOffset)
                        .ifPresent(splits::add);
            }
        }

        return splits;
    }

    private Optional<TieringSplit> generateSplitForPrimaryKeyTableBucket(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long latestSnapshotId,
            @Nullable Long latestOffsetOfSnapshot,
            @Nullable Long lastCommittedBucketOffset,
            long latestBucketOffset) {
        if (latestBucketOffset <= 0) {
            LOG.debug(
                    "The latestBucketOffset {} is equals or less than 0, skip generating split for bucket {}",
                    latestBucketOffset,
                    tableBucket);
            return Optional.empty();
        }

        // the bucket is never been tiered, read kv snapshot is more efficient
        if (lastCommittedBucketOffset == null) {
            if (latestSnapshotId == null) {
                // bucket with non snapshot, scan log from earliest to latest offset
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                EARLIEST_OFFSET,
                                latestBucketOffset,
                                0));
            } else {
                // bucket with snapshot, read kv to latest snapshotId + latestOffsetOfSnapshot
                checkState(latestOffsetOfSnapshot != null);
                return Optional.of(
                        new TieringSnapshotSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                latestSnapshotId,
                                latestOffsetOfSnapshot,
                                0));
            }
        } else {
            // the bucket has been tiered, read bounded log
            if (lastCommittedBucketOffset < latestBucketOffset) {
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                lastCommittedBucketOffset,
                                latestBucketOffset,
                                0));
            } else {
                LOG.info(
                        "The lastCommittedBucketOffset {} is equals or bigger than latestBucketOffset {}, skip generating split for bucket {}",
                        lastCommittedBucketOffset,
                        latestBucketOffset,
                        tableBucket);
                return Optional.empty();
            }
        }
    }

    private Optional<TieringSplit> generateSplitForLogTableBucket(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long lastCommittedBucketOffset,
            long latestBucketOffset) {
        if (latestBucketOffset <= 0) {
            LOG.debug(
                    "The latestBucketOffset {} is equals or less than 0, skip generating split for bucket {}",
                    latestBucketOffset,
                    tableBucket);
            return Optional.empty();
        }

        // the bucket is never been tiered
        if (lastCommittedBucketOffset == null) {
            // the bucket is never been tiered, scan fluss log from the earliest offset
            return Optional.of(
                    new TieringLogSplit(
                            tablePath,
                            tableBucket,
                            partitionName,
                            EARLIEST_OFFSET,
                            latestBucketOffset,
                            0));
        } else {
            // the bucket has been tiered, scan remain fluss log
            if (lastCommittedBucketOffset < latestBucketOffset) {
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                lastCommittedBucketOffset,
                                latestBucketOffset,
                                0));
            }
        }
        LOG.info(
                "The lastCommittedBucketOffset {} is equals or bigger than latestBucketOffset {}, skip generating split for bucket {}",
                lastCommittedBucketOffset,
                latestBucketOffset,
                tableBucket);
        return Optional.empty();
    }
}
