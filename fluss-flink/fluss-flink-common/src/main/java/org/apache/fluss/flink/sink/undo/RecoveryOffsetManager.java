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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.admin.ProducerOffsetsResult;
import org.apache.fluss.client.admin.RegisterResult;
import org.apache.fluss.flink.sink.ChannelComputer;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.undo.UndoRecoveryManager.UndoOffsets;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Manages recovery offset determination for undo recovery in aggregation tables.
 *
 * <p>State semantics:
 *
 * <ul>
 *   <li>{@code null} means that no Flink checkpoint is being restored, so the producer offset
 *       snapshot provides the initial baseline.
 *   <li>V2 state is a complete sparse baseline; an assigned live bucket absent from it has baseline
 *       zero.
 *   <li>Legacy or empty restored state is rejected because it cannot prove the same completeness
 *       guarantee.
 * </ul>
 *
 * <p>The complete baseline is preserved independently from the Undo work set. Unchanged buckets
 * must remain in the next checkpoint even though only buckets whose current offsets exceed their
 * baselines require Undo.
 *
 * <p>Recovery flow:
 *
 * <ol>
 *   <li>Classify Flink state and reject legacy V1 state before external reads.
 *   <li>Load the current partition metadata through the existing Admin API.
 *   <li>Merge state fragments without guessing and enumerate every assigned live bucket.
 *   <li>Resolve each bucket's baseline and fetch its strict current log end offset.
 *   <li>Return the complete non-zero baseline separately from the bounded Undo work set.
 * </ol>
 */
public class RecoveryOffsetManager {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryOffsetManager.class);

    public static final long DEFAULT_PRODUCER_OFFSETS_POLL_INTERVAL_MS = 100;

    /** Default maximum total time to poll for producer offsets before giving up (5 minutes). */
    public static final long DEFAULT_MAX_POLL_TIMEOUT_MS = 5 * 60 * 1000;

    private final Admin admin;
    private final String producerId;
    private final int subtaskIndex;
    private final int parallelism;
    private final long pollIntervalMs;
    private final long maxPollTimeoutMs;
    private final TablePath tablePath;
    private final long tableId;

    private final boolean isPartitioned;
    private final int numBuckets;

    /** Cached partition info to avoid multiple RPC calls. */
    @Nullable private List<PartitionInfo> cachedPartitionInfos;

    /** Recovery strategy types. */
    public enum RecoveryStrategy {
        FRESH_START,
        CHECKPOINT_RECOVERY,
        PRODUCER_OFFSET_RECOVERY
    }

    private enum RecoveryStateKind {
        NO_FLINK_STATE,
        V1_LEGACY,
        V2_COMPLETE
    }

    /** Result of recovery strategy determination. */
    public static class RecoveryDecision {
        private final RecoveryStrategy strategy;
        private final Map<TableBucket, Long> recoveryOffsets;
        private final Map<TableBucket, UndoOffsets> undoOffsets;

        private RecoveryDecision(
                RecoveryStrategy strategy,
                Map<TableBucket, Long> recoveryOffsets,
                Map<TableBucket, UndoOffsets> undoOffsets) {
            this.strategy = strategy;
            this.recoveryOffsets = recoveryOffsets;
            this.undoOffsets = undoOffsets;
        }

        public RecoveryStrategy getStrategy() {
            return strategy;
        }

        /** Returns all non-zero recovery offsets that the next checkpoint must preserve. */
        public Map<TableBucket, Long> getRecoveryOffsets() {
            return recoveryOffsets;
        }

        /**
         * Returns the UndoOffsets map containing both checkpoint offset and log end offset.
         *
         * <p>This is used by UndoRecoveryManager to perform undo recovery without needing to call
         * listOffset again.
         *
         * @return map of bucket to UndoOffsets, empty if no recovery is needed
         */
        public Map<TableBucket, UndoOffsets> getUndoOffsets() {
            return undoOffsets;
        }

        public boolean needsUndoRecovery() {
            return !undoOffsets.isEmpty();
        }

        static RecoveryDecision of(
                RecoveryStrategy strategy,
                Map<TableBucket, Long> recoveryOffsets,
                Map<TableBucket, UndoOffsets> undoOffsets) {
            return new RecoveryDecision(strategy, recoveryOffsets, undoOffsets);
        }

        @Override
        public String toString() {
            return String.format(
                    "RecoveryDecision{strategy=%s, recoveryBuckets=%d, undoBuckets=%d}",
                    strategy, recoveryOffsets.size(), undoOffsets.size());
        }
    }

    public RecoveryOffsetManager(
            Admin admin,
            String producerId,
            int subtaskIndex,
            int parallelism,
            long pollIntervalMs,
            long maxPollTimeoutMs,
            TablePath tablePath,
            TableInfo tableInfo) {
        this(
                admin,
                producerId,
                subtaskIndex,
                parallelism,
                pollIntervalMs,
                maxPollTimeoutMs,
                tablePath,
                tableInfo.getTableId(),
                tableInfo.getNumBuckets(),
                tableInfo.isPartitioned());
    }

    /** Package-private constructor for testing without TableInfo dependency. */
    RecoveryOffsetManager(
            Admin admin,
            String producerId,
            int subtaskIndex,
            int parallelism,
            long pollIntervalMs,
            long maxPollTimeoutMs,
            TablePath tablePath,
            long tableId,
            int numBuckets,
            boolean isPartitioned) {
        this.admin = admin;
        this.producerId = producerId;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.pollIntervalMs = pollIntervalMs;
        this.maxPollTimeoutMs = maxPollTimeoutMs;
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.isPartitioned = isPartitioned;
        this.numBuckets = numBuckets;
    }

    // ==================== Public API ====================

    /** Determines recovery strategy and returns filtered recovery offsets. */
    public RecoveryDecision determineRecoveryStrategy(
            @Nullable Collection<WriterState> recoveredState) throws Exception {
        LOG.info(
                "Determining recovery for subtask {}/{}, producerId={}",
                subtaskIndex,
                parallelism,
                producerId);

        RecoveryStateKind stateKind = classifyRecoveredState(recoveredState);
        Map<Long, String> partitionNames = getPartitionNameMap();
        Map<TableBucket, Long> recoveryOffsets =
                stateKind == RecoveryStateKind.NO_FLINK_STATE
                        ? getProducerOffsets()
                        : mergeCheckpointState(recoveredState, partitionNames);

        LOG.info(
                "Recovery offsets for subtask {} (source={}): {}",
                subtaskIndex,
                stateKind,
                recoveryOffsets);

        Set<TableBucket> allBuckets = getAllBuckets();
        Set<TableBucket> filteredBuckets = filterBucketsBySharding(allBuckets, partitionNames);

        LOG.info(
                "Subtask {}: filteredBuckets={}, recoveryOffsets={}",
                subtaskIndex,
                filteredBuckets,
                recoveryOffsets);

        Map<TableBucket, Long> currentOffsets =
                fetchCurrentOffsets(filteredBuckets, partitionNames);

        LOG.info("Subtask {}: currentOffsets={}", subtaskIndex, currentOffsets);

        Map<TableBucket, Long> retainedRecoveryOffsets = new HashMap<>();
        Map<TableBucket, UndoOffsets> undoOffsets = new HashMap<>();
        List<TableBucket> legacyStateGaps = new ArrayList<>();

        for (TableBucket bucket : filteredBuckets) {
            Long sourceOffset = recoveryOffsets.get(bucket);
            long baseline = sourceOffset == null ? 0L : sourceOffset;
            Long currentOffset = currentOffsets.get(bucket);
            if (currentOffset == null) {
                throw new IllegalStateException("missing latest offset for live bucket " + bucket);
            }
            long current = currentOffset;

            if (stateKind == RecoveryStateKind.V1_LEGACY && sourceOffset == null && current > 0) {
                legacyStateGaps.add(bucket);
            }

            LOG.info(
                    "Subtask {}: bucket={}, baseline={} (explicit={}), current={}",
                    subtaskIndex,
                    bucket,
                    baseline,
                    sourceOffset != null,
                    current);

            if (baseline > current) {
                throw new IllegalStateException(
                        String.format(
                                "Data inconsistency: bucket %s baseline=%d > current=%d",
                                bucket, baseline, current));
            }
            if (baseline > 0) {
                retainedRecoveryOffsets.put(bucket, baseline);
            }
            if (baseline < current) {
                undoOffsets.put(bucket, new UndoOffsets(baseline, current));
            }
        }

        if (!legacyStateGaps.isEmpty()) {
            LOG.warn(
                    "Restoring legacy V1 Undo Recovery state with {} assigned live buckets "
                            + "missing from the checkpoint state. V1 cannot distinguish buckets "
                            + "that had offset zero at checkpoint time from buckets whose state "
                            + "was previously lost. Missing buckets use recovery offset zero, "
                            + "so Undo Recovery may scan excessive history.",
                    legacyStateGaps.size());
            LOG.debug("Legacy V1 state gaps for subtask {}: {}", subtaskIndex, legacyStateGaps);
        }

        RecoveryStrategy strategy =
                undoOffsets.isEmpty()
                        ? RecoveryStrategy.FRESH_START
                        : stateKind == RecoveryStateKind.NO_FLINK_STATE
                                ? RecoveryStrategy.PRODUCER_OFFSET_RECOVERY
                                : RecoveryStrategy.CHECKPOINT_RECOVERY;
        LOG.info(
                "{}: {} buckets need recovery for subtask {}",
                strategy,
                undoOffsets.size(),
                subtaskIndex);
        return RecoveryDecision.of(strategy, retainedRecoveryOffsets, undoOffsets);
    }

    /** Cleans up registered producer offsets. Should only be called by Task0. */
    public void cleanupOffsets() {
        if (subtaskIndex != 0) {
            return;
        }
        try {
            LOG.info("Cleaning up producer offsets for {}", producerId);
            admin.deleteProducerOffsets(producerId).get();
        } catch (Exception e) {
            LOG.warn("Failed to cleanup producer offsets: {}", e.getMessage());
        }
    }

    // ==================== Step 1: Get Recovery Offsets ====================

    private RecoveryStateKind classifyRecoveredState(
            @Nullable Collection<WriterState> recoveredState) {
        if (recoveredState == null) {
            return RecoveryStateKind.NO_FLINK_STATE;
        }
        if (recoveredState.isEmpty()) {
            // A checkpoint produced by V2 always contains at least one state element, even when
            // its baseline is empty. An empty restored collection therefore cannot prove a
            // complete baseline.
            throw new IllegalStateException(
                    "The job was restored but Undo Recovery has no state fragments. "
                            + "Cannot distinguish a legacy empty state from a topology that did "
                            + "not contain Undo Recovery; perform a controlled stateless restart.");
        }

        WriterState.StateFormat stateFormat = null;
        for (WriterState state : recoveredState) {
            if (state == null) {
                throw new IllegalStateException("Undo Recovery state contains a null fragment.");
            }
            if (stateFormat != null && state.getStateFormat() != stateFormat) {
                throw new IllegalStateException(
                        "Undo Recovery state contains mixed V1 and V2 fragments.");
            }
            stateFormat = state.getStateFormat();
        }
        if (stateFormat == WriterState.StateFormat.V1_LEGACY) {
            return RecoveryStateKind.V1_LEGACY;
        }
        return RecoveryStateKind.V2_COMPLETE;
    }

    private Map<TableBucket, Long> mergeCheckpointState(
            Collection<WriterState> states, Map<Long, String> partitionNames) {
        Map<TableBucket, Long> merged = new HashMap<>();
        for (WriterState state : states) {
            if (state.getStateFormat() == WriterState.StateFormat.V2_COMPLETE
                    && state.getTableId() != tableId) {
                throw new IllegalStateException(
                        String.format(
                                "V2 state table ID %d does not match current table ID %d for %s.",
                                state.getTableId(), tableId, tablePath));
            }
            for (Map.Entry<TableBucket, Long> entry : state.getBucketOffsets().entrySet()) {
                TableBucket bucket = entry.getKey();
                validateTableId(bucket);
                validateBaselineOffset(bucket, entry.getValue());
                if (!isLiveStateBucket(bucket, partitionNames)) {
                    continue;
                }
                putMergedOffset(merged, bucket, entry.getValue());
            }
        }
        return merged;
    }

    private void putMergedOffset(Map<TableBucket, Long> merged, TableBucket bucket, Long offset) {
        Long previous = merged.putIfAbsent(bucket, offset);
        if (previous != null && !previous.equals(offset)) {
            throw new IllegalStateException(
                    String.format(
                            "Conflicting checkpoint offsets for %s: %d and %d.",
                            bucket, previous, offset));
        }
    }

    private void validateTableId(TableBucket bucket) {
        if (bucket.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "Table '%s' has been re-created (state tableId=%d, current tableId=%d). "
                                    + "Cannot restore from checkpoint/savepoint after table re-creation.",
                            tablePath, bucket.getTableId(), tableId));
        }
    }

    private boolean isLiveStateBucket(TableBucket bucket, Map<Long, String> partitionNames) {
        Long partitionId = bucket.getPartitionId();
        if (isPartitioned) {
            if (partitionId == null) {
                throw new IllegalStateException(
                        "State bucket " + bucket + " has no partition ID for a partitioned table.");
            }
            return partitionNames.containsKey(partitionId);
        }
        if (partitionId != null) {
            throw new IllegalStateException(
                    "State bucket " + bucket + " has a partition ID for a non-partitioned table.");
        }
        return true;
    }

    private void validateBaselineOffset(TableBucket bucket, Long offset) {
        if (offset == null || offset < 0) {
            throw new IllegalStateException(
                    "Invalid checkpoint baseline offset for " + bucket + ": " + offset);
        }
    }

    private Map<TableBucket, Long> getProducerOffsets() throws Exception {
        if (subtaskIndex == 0) {
            registerCurrentOffsets();
        }
        ProducerOffsetsResult result = pollForOffsets();
        Map<TableBucket, Long> offsets = result.getTableOffsets().get(tableId);
        return offsets != null ? offsets : new HashMap<>();
    }

    private void registerCurrentOffsets() throws Exception {
        LOG.info("Task0 registering offsets for {}", producerId);
        Map<TableBucket, Long> offsets = fetchAllBucketOffsets();
        LOG.info("Task0 registering offsets: {}", offsets);
        RegisterResult result = admin.registerProducerOffsets(producerId, offsets).get();
        LOG.info("Registration result: {} ({} offsets)", result, offsets.size());
    }

    private ProducerOffsetsResult pollForOffsets() throws Exception {
        int attempt = 0;
        long startTime = System.currentTimeMillis();
        Exception lastException = null;
        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= maxPollTimeoutMs) {
                throw new RuntimeException(
                        String.format(
                                "Timed out polling for producer offsets after %d ms (%d attempts). "
                                        + "producerId=%s, subtask=%d/%d. Last error: %s",
                                elapsed,
                                attempt,
                                producerId,
                                subtaskIndex,
                                parallelism,
                                lastException != null
                                        ? lastException.getMessage()
                                        : "no valid result"),
                        lastException);
            }
            try {
                ProducerOffsetsResult result = admin.getProducerOffsets(producerId).get();
                if (result != null && result.getExpirationTime() > System.currentTimeMillis()) {
                    return result;
                }
            } catch (Exception e) {
                lastException = e;
                LOG.warn(
                        "Failed to get producer offsets (attempt {}), retrying: {}",
                        attempt + 1,
                        e.getMessage());
            }
            attempt++;
            Thread.sleep(pollIntervalMs);
        }
    }

    // ==================== Step 2: Get All Buckets ====================

    /** Get all buckets for the table (all partitions for partitioned tables). */
    private Set<TableBucket> getAllBuckets() throws Exception {
        Set<TableBucket> buckets = new HashSet<>();
        if (isPartitioned) {
            for (PartitionInfo partition : getPartitionInfos()) {
                for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                    buckets.add(new TableBucket(tableId, partition.getPartitionId(), bucketId));
                }
            }
        } else {
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                buckets.add(new TableBucket(tableId, bucketId));
            }
        }
        return buckets;
    }

    // ==================== Step 3: Filter by Sharding ====================

    private Set<TableBucket> filterBucketsBySharding(
            Set<TableBucket> buckets, Map<Long, String> partitionNames) {
        Set<TableBucket> filtered = new HashSet<>();
        for (TableBucket bucket : buckets) {
            if (isAssignedToSubtask(bucket, partitionNames)) {
                filtered.add(bucket);
            }
        }
        return filtered;
    }

    /**
     * Determines if a bucket is assigned to the current subtask.
     *
     * <p>Uses {@link ChannelComputer#shouldCombinePartitionInSharding} and {@link
     * ChannelComputer#select} to ensure consistent sharding logic with {@link
     * org.apache.fluss.flink.sink.FlinkRowDataChannelComputer}.
     *
     * <p>For partitioned tables, if the partition has been deleted (partitionName not found in
     * partitionNames map), the bucket is considered not assigned to any subtask and will be
     * skipped.
     *
     * @param bucket the bucket to check
     * @param partitionNames map of partition ID to partition name
     * @return true if the bucket is assigned to this subtask, false if not assigned or partition
     *     deleted
     */
    private boolean isAssignedToSubtask(TableBucket bucket, Map<Long, String> partitionNames) {
        // For partitioned table bucket, get partition name first
        String partitionName = null;
        if (bucket.getPartitionId() != null) {
            partitionName = partitionNames.get(bucket.getPartitionId());
            if (partitionName == null) {
                // Partition has been deleted, skip this bucket
                LOG.debug(
                        "Partition {} not found (deleted?), skipping bucket {}",
                        bucket.getPartitionId(),
                        bucket);
                return false;
            }
        }

        // Use shared logic to determine sharding strategy and compute channel
        int channel;
        if (ChannelComputer.shouldCombinePartitionInSharding(
                isPartitioned, numBuckets, parallelism)) {
            // When shouldCombinePartitionInSharding is true, partitionName is guaranteed non-null
            // because: 1) isPartitioned=true means bucket has partitionId
            //          2) deleted partitions already returned false above
            channel = ChannelComputer.select(partitionName, bucket.getBucket(), parallelism);
        } else {
            channel = ChannelComputer.select(bucket.getBucket(), parallelism);
        }
        return channel == subtaskIndex;
    }

    // ==================== Step 4: Fetch Current Offsets ====================

    private Map<TableBucket, Long> fetchCurrentOffsets(
            Set<TableBucket> buckets, Map<Long, String> partitionNames) throws Exception {
        Map<TableBucket, Long> offsets = new HashMap<>();

        // Group buckets by partition
        Map<Long, List<TableBucket>> byPartition = new HashMap<>();
        List<TableBucket> nonPartitioned = new ArrayList<>();
        for (TableBucket bucket : buckets) {
            if (bucket.getPartitionId() != null) {
                byPartition
                        .computeIfAbsent(bucket.getPartitionId(), k -> new ArrayList<>())
                        .add(bucket);
            } else {
                nonPartitioned.add(bucket);
            }
        }

        // Fetch non-partitioned buckets
        if (!nonPartitioned.isEmpty()) {
            fetchBucketOffsets(null, nonPartitioned, offsets);
        }

        // Fetch partitioned buckets
        for (Map.Entry<Long, List<TableBucket>> entry : byPartition.entrySet()) {
            Long partitionId = entry.getKey();
            String partitionName = partitionNames.get(partitionId);
            if (partitionName == null) {
                throw new IllegalStateException(
                        "Partition " + partitionId + " not found in partition info cache");
            }
            fetchBucketOffsets(partitionName, entry.getValue(), offsets);
        }

        return offsets;
    }

    // ==================== Partition Info Cache ====================

    private List<PartitionInfo> getPartitionInfos() throws Exception {
        if (cachedPartitionInfos == null) {
            cachedPartitionInfos = admin.listPartitionInfos(tablePath).get();
            LOG.debug("Fetched {} partition infos for {}", cachedPartitionInfos.size(), tablePath);
        }
        return cachedPartitionInfos;
    }

    private Map<Long, String> getPartitionNameMap() throws Exception {
        if (!isPartitioned) {
            return new HashMap<>();
        }
        Map<Long, String> nameMap = new HashMap<>();
        for (PartitionInfo partition : getPartitionInfos()) {
            nameMap.put(partition.getPartitionId(), partition.getPartitionName());
        }
        return nameMap;
    }

    // ==================== Offset Fetching Helpers ====================

    private Map<TableBucket, Long> fetchAllBucketOffsets() throws Exception {
        Map<TableBucket, Long> offsets = new HashMap<>();
        if (isPartitioned) {
            for (PartitionInfo partition : getPartitionInfos()) {
                fetchPartitionOffsets(
                        partition.getPartitionName(), partition.getPartitionId(), offsets);
            }
        } else {
            fetchPartitionOffsets(null, null, offsets);
        }
        return offsets;
    }

    private void fetchPartitionOffsets(
            @Nullable String partitionName,
            @Nullable Long partitionId,
            Map<TableBucket, Long> offsets)
            throws Exception {
        List<Integer> bucketIds = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            bucketIds.add(i);
        }
        ListOffsetsResult result = listOffsets(partitionName, bucketIds);

        for (int bucketId : bucketIds) {
            TableBucket bucket =
                    partitionId != null
                            ? new TableBucket(tableId, partitionId, bucketId)
                            : new TableBucket(tableId, bucketId);
            offsets.put(bucket, getOffset(result, bucketId));
        }
    }

    private void fetchBucketOffsets(
            @Nullable String partitionName,
            List<TableBucket> buckets,
            Map<TableBucket, Long> offsets)
            throws Exception {
        List<Integer> bucketIds =
                buckets.stream().map(TableBucket::getBucket).collect(Collectors.toList());
        ListOffsetsResult result = listOffsets(partitionName, bucketIds);

        for (TableBucket bucket : buckets) {
            offsets.put(bucket, getOffset(result, bucket.getBucket()));
        }
    }

    private ListOffsetsResult listOffsets(@Nullable String partitionName, List<Integer> bucketIds)
            throws Exception {
        return partitionName != null
                ? admin.listOffsets(
                        tablePath, partitionName, bucketIds, new OffsetSpec.LatestSpec())
                : admin.listOffsets(tablePath, bucketIds, new OffsetSpec.LatestSpec());
    }

    private long getOffset(ListOffsetsResult result, int bucketId) throws Exception {
        if (result == null) {
            throw new IllegalStateException("null latest offset result for bucket ID " + bucketId);
        }
        CompletableFuture<Long> bucketResult = result.bucketResult(bucketId);
        if (bucketResult == null) {
            throw new IllegalStateException("missing latest offset for bucket ID " + bucketId);
        }
        Long offset = bucketResult.get();
        if (offset == null) {
            throw new IllegalStateException("null latest offset for bucket ID " + bucketId);
        }
        if (offset < 0) {
            throw new IllegalStateException(
                    "negative latest offset for bucket ID " + bucketId + ": " + offset);
        }
        return offset;
    }
}
