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

package org.apache.fluss.flink.source.enumerator;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Generates bounded Fluss-only splits for batch scans. */
final class FlussOnlyBatchSplitGenerator {

    private final TableInfo tableInfo;
    private final boolean hasPrimaryKey;
    private final boolean isPartitioned;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    private final BucketOffsetsRetriever bucketOffsetsRetriever;
    private final Supplier<Set<PartitionInfo>> partitionSupplier;
    private final KvSnapshotsRetriever kvSnapshotsRetriever;
    private final Predicate<TableBucket> tableBucketSkipper;

    FlussOnlyBatchSplitGenerator(
            TableInfo tableInfo,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetsInitializer,
            BucketOffsetsRetriever bucketOffsetsRetriever,
            Supplier<Set<PartitionInfo>> partitionSupplier,
            KvSnapshotsRetriever kvSnapshotsRetriever,
            Predicate<TableBucket> tableBucketSkipper) {
        this.tableInfo = tableInfo;
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.partitionSupplier = partitionSupplier;
        this.kvSnapshotsRetriever = kvSnapshotsRetriever;
        this.tableBucketSkipper = tableBucketSkipper;
    }

    List<SourceSplitBase> generate() {
        if (isPartitioned) {
            Set<PartitionInfo> partitionInfos = partitionSupplier.get();
            return hasPrimaryKey
                    ? generatePrimaryKeyTableSplits(partitionInfos)
                    : generateLogTableSplits(partitionInfos);
        } else {
            return hasPrimaryKey
                    ? getBatchSnapshotAndLogSplits(kvSnapshotsRetriever.get(null), null)
                    : getLogSplits(null, null);
        }
    }

    private List<SourceSplitBase> generatePrimaryKeyTableSplits(
            Collection<PartitionInfo> partitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (PartitionInfo partition : partitions) {
            String partitionName = partition.getPartitionName();
            splits.addAll(
                    getBatchSnapshotAndLogSplits(
                            kvSnapshotsRetriever.get(partitionName), partitionName));
        }
        return splits;
    }

    private List<SourceSplitBase> generateLogTableSplits(Collection<PartitionInfo> partitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (PartitionInfo partition : partitions) {
            splits.addAll(getLogSplits(partition.getPartitionId(), partition.getPartitionName()));
        }
        return splits;
    }

    private List<SourceSplitBase> getBatchSnapshotAndLogSplits(
            KvSnapshots snapshots, @Nullable String partitionName) {
        long tableId = snapshots.getTableId();
        Long partitionId = snapshots.getPartitionId();
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (Integer bucketId : snapshots.getBucketIds()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            if (!tableBucketSkipper.test(tableBucket)) {
                bucketsNeedInitOffset.add(bucketId);
            }
        }

        Map<Integer, Long> stoppingOffsets =
                bucketsNeedInitOffset.isEmpty()
                        ? Collections.emptyMap()
                        : stoppingOffsetsInitializer.getBucketOffsets(
                                partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever);

        for (Integer bucketId : bucketsNeedInitOffset) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
            long batchSnapshotId =
                    snapshotId.isPresent()
                            ? snapshotId.getAsLong()
                            : HybridSnapshotLogSplit.NO_SNAPSHOT_ID;
            long logStartingOffset;
            if (snapshotId.isPresent()) {
                OptionalLong logOffset = snapshots.getLogOffset(bucketId);
                checkState(
                        logOffset.isPresent(),
                        "Log offset should be present if snapshot id is present.");
                logStartingOffset = logOffset.getAsLong();
            } else {
                logStartingOffset = LogScanner.EARLIEST_OFFSET;
            }

            Long logStoppingOffset = stoppingOffsets.get(bucketId);
            checkState(
                    logStoppingOffset != null,
                    "Stopping offset should be present for bucket %s.",
                    bucketId);
            splits.add(
                    new HybridSnapshotLogSplit(
                            tableBucket,
                            partitionName,
                            batchSnapshotId,
                            0,
                            false,
                            logStartingOffset,
                            logStoppingOffset,
                            true));
        }
        return splits;
    }

    private List<SourceSplitBase> getLogSplits(
            @Nullable Long partitionId, @Nullable String partitionName) {
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
            if (!tableBucketSkipper.test(tableBucket)) {
                bucketsNeedInitOffset.add(bucketId);
            }
        }

        if (!bucketsNeedInitOffset.isEmpty()) {
            Map<Integer, Long> startingOffsets =
                    startingOffsetsInitializer.getBucketOffsets(
                            partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever);
            Map<Integer, Long> stoppingOffsets =
                    stoppingOffsetsInitializer.getBucketOffsets(
                            partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever);
            for (Integer bucketId : bucketsNeedInitOffset) {
                Long startingOffset = startingOffsets.get(bucketId);
                Long stoppingOffset = stoppingOffsets.get(bucketId);
                checkState(
                        startingOffset != null,
                        "Starting offset should be present for bucket %s.",
                        bucketId);
                checkState(
                        stoppingOffset != null,
                        "Stopping offset should be present for bucket %s.",
                        bucketId);
                splits.add(
                        new LogSplit(
                                new TableBucket(tableInfo.getTableId(), partitionId, bucketId),
                                partitionName,
                                startingOffset,
                                stoppingOffset));
            }
        }
        return splits;
    }

    /** Retrieves latest KV snapshots and keeps any required snapshot leases alive. */
    @FunctionalInterface
    interface KvSnapshotsRetriever {
        KvSnapshots get(@Nullable String partitionName);
    }
}
