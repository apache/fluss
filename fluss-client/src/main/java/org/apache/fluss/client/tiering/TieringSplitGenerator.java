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

package org.apache.fluss.client.tiering;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.Preconditions.checkState;

/** A generator for lake splits. */
public class TieringSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSplitGenerator.class);

    private final Admin flussAdmin;

    public TieringSplitGenerator(Admin flussAdmin) {
        this.flussAdmin = flussAdmin;
    }

    public List<TieringSplit> generateTableSplits(TableInfo tableInfo) throws Exception {
        List<PartitionInfo> partitionInfos =
                tableInfo.isPartitioned()
                        ? flussAdmin.listPartitionInfos(tableInfo.getTablePath(), true).get()
                        : Collections.emptyList();
        return generateTableSplits(tableInfo, partitionInfos);
    }

    /**
     * Generates splits using the supplied partition metadata, including historical partitions.
     * Callers can reuse this metadata to initialize writers for the same tiering round. For
     * non-partitioned tables, pass an empty list.
     */
    public List<TieringSplit> generateTableSplits(
            TableInfo tableInfo, List<PartitionInfo> partitionInfos) throws Exception {
        TablePath tablePath = tableInfo.getTablePath();
        // fetches real earliest offsets, see withLogStartOffsets
        final BucketOffsetsRetriever bucketOffsetsRetriever =
                new BucketOffsetsRetrieverImpl(flussAdmin, tablePath, true);

        // Get table lake snapshot info of the given table.
        LakeSnapshot lakeSnapshotInfo;
        try {
            lakeSnapshotInfo = flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();
            LOG.info(
                    "Last committed lake table snapshot: snapshotId={}, numBucketOffsets={}",
                    lakeSnapshotInfo.getSnapshotId(),
                    lakeSnapshotInfo.getTableBucketsOffset().size());
            LOG.debug("Last committed lake table snapshot detail: {}", lakeSnapshotInfo);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (t instanceof LakeTableSnapshotNotExistException) {
                lakeSnapshotInfo = null;
            } else {
                throw new FlussRuntimeException(
                        String.format(
                                "Failed to get table snapshot for table %s",
                                tableInfo.getTablePath()),
                        ExceptionUtils.stripCompletionException(e));
            }
        }
        // partitioned table
        if (tableInfo.isPartitioned()) {
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));
            Map<Long, Integer> bucketCountById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getBucketCount));

            return generatePartitionTableSplit(
                    tableInfo,
                    partitionNameById,
                    bucketCountById,
                    bucketOffsetsRetriever,
                    lakeSnapshotInfo);
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
            Map<Long, Integer> bucketCountById,
            BucketOffsetsRetriever bucketOffsetsRetriever,
            @Nullable LakeSnapshot lakeSnapshotInfo) {
        List<TieringSplit> splits = new ArrayList<>();
        for (Map.Entry<Long, String> partitionNameByIdEntry : partitionNameById.entrySet()) {
            long partitionId = partitionNameByIdEntry.getKey();
            String partitionName = partitionNameByIdEntry.getValue();
            int partitionBucketCount = bucketCountById.get(partitionId);
            Map<Integer, Long> latestBucketsOffset =
                    bucketOffsetsRetriever.latestOffsets(
                            partitionName,
                            IntStream.range(0, partitionBucketCount)
                                    .boxed()
                                    .collect(Collectors.toList()));
            splits.addAll(
                    withLogStartOffsets(
                            generateTableSplit(
                                    tableInfo,
                                    partitionId,
                                    partitionName,
                                    partitionBucketCount,
                                    lakeSnapshotInfo,
                                    latestBucketsOffset),
                            partitionName,
                            bucketOffsetsRetriever));
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
        return withLogStartOffsets(
                generateTableSplit(
                        tableInfo,
                        null,
                        null,
                        tableInfo.getNumBuckets(),
                        lakeSnapshotInfo,
                        latestBucketsOffset),
                null,
                bucketOffsetsRetriever);
    }

    /**
     * Sets the bucket's log start offset on the splits that read from the earliest offset, as an
     * old bucket's log no longer starts at 0. Best effort: only the backlog estimate depends on it.
     */
    private List<TieringSplit> withLogStartOffsets(
            List<TieringSplit> splits,
            @Nullable String partitionName,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        List<Integer> earliestBuckets = new ArrayList<>();
        for (TieringSplit split : splits) {
            if (readsFromEarliest(split)) {
                earliestBuckets.add(split.getTableBucket().getBucket());
            }
        }
        if (earliestBuckets.isEmpty()) {
            return splits;
        }

        Map<Integer, Long> logStartOffsets;
        try {
            logStartOffsets =
                    bucketOffsetsRetriever.earliestOffsets(partitionName, earliestBuckets);
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to get the log start offsets of buckets {} of partition {}, "
                            + "their backlog is estimated from offset 0.",
                    earliestBuckets,
                    partitionName,
                    e);
            return splits;
        }

        List<TieringSplit> result = new ArrayList<>(splits.size());
        for (TieringSplit split : splits) {
            Long logStartOffset = logStartOffsets.get(split.getTableBucket().getBucket());
            if (readsFromEarliest(split) && logStartOffset != null && logStartOffset >= 0) {
                result.add(split.asTieringLogSplit().withLogStartOffset(logStartOffset));
            } else {
                result.add(split);
            }
        }
        return result;
    }

    private static boolean readsFromEarliest(TieringSplit split) {
        return split.isTieringLogSplit()
                && split.asTieringLogSplit().getStartingOffset() == EARLIEST_OFFSET;
    }

    private List<TieringSplit> generateTableSplit(
            TableInfo tableInfo,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            int numBuckets,
            @Nullable LakeSnapshot lakeSnapshotInfo,
            Map<Integer, Long> latestBucketsOffset) {
        List<TieringSplit> splits = new ArrayList<>();

        if (tableInfo.hasPrimaryKey()) {
            // it's primary key table
            // Historical partitions have no local KV snapshots and only tier retained WAL.
            boolean historicalPartition = HISTORICAL_PARTITION_VALUE.equals(partitionName);
            KvSnapshots latestKvSnapshots =
                    new KvSnapshots(
                            tableInfo.getTableId(),
                            partitionId,
                            Collections.emptyMap(),
                            Collections.emptyMap());
            boolean kvSnapshotsFetched = false;
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long lastCommittedBucketOffset =
                        lakeSnapshotInfo != null
                                ? lakeSnapshotInfo.getTableBucketsOffset().get(tableBucket)
                                : null;
                long latestBucketOffset = latestBucketsOffset.get(bucket);
                boolean needsKvSnapshot =
                        !historicalPartition
                                && latestBucketOffset > 0
                                && lastCommittedBucketOffset == null;
                if (needsKvSnapshot && !kvSnapshotsFetched) {
                    latestKvSnapshots =
                            fetchLatestKvSnapshots(tableInfo.getTablePath(), partitionName);
                    checkState(latestKvSnapshots != null);
                    kvSnapshotsFetched = true;
                }

                generateSplitForPrimaryKeyTableBucket(
                                tableInfo.getTablePath(),
                                tableBucket,
                                partitionName,
                                latestKvSnapshots,
                                lastCommittedBucketOffset,
                                latestBucketOffset)
                        .ifPresent(splits::add);
            }

        } else {
            // it's log table
            for (int bucket = 0; bucket < numBuckets; bucket++) {
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

    private KvSnapshots fetchLatestKvSnapshots(
            TablePath tablePath, @Nullable String partitionName) {
        try {
            return (partitionName == null
                            ? flussAdmin.getLatestKvSnapshots(tablePath)
                            : flussAdmin.getLatestKvSnapshots(tablePath, partitionName))
                    .get();
        } catch (Exception e) {
            String errorMessage =
                    partitionName == null
                            ? String.format(
                                    "Failed to get KV snapshot metadata for table %s", tablePath)
                            : String.format(
                                    "Failed to get KV snapshot metadata for table %s and partition %s",
                                    tablePath, partitionName);
            throw new FlussRuntimeException(
                    errorMessage, ExceptionUtils.stripCompletionException(e));
        }
    }

    private Optional<TieringSplit> generateSplitForPrimaryKeyTableBucket(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            KvSnapshots latestKvSnapshots,
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
            int bucket = tableBucket.getBucket();
            if (!latestKvSnapshots.getSnapshotId(bucket).isPresent()) {
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
                checkState(latestKvSnapshots.getLogOffset(bucket).isPresent());
                return Optional.of(
                        new TieringSnapshotSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                latestKvSnapshots.getSnapshotId(bucket).getAsLong(),
                                latestKvSnapshots.getLogOffset(bucket).getAsLong(),
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
                LOG.debug(
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
                            latestBucketOffset));
        } else {
            // the bucket has been tiered, scan remain fluss log
            if (lastCommittedBucketOffset < latestBucketOffset) {
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                lastCommittedBucketOffset,
                                latestBucketOffset));
            }
        }
        LOG.debug(
                "The lastCommittedBucketOffset {} is equals or bigger than latestBucketOffset {}, skip generating split for bucket {}",
                lastCommittedBucketOffset,
                latestBucketOffset,
                tableBucket);
        return Optional.empty();
    }
}
