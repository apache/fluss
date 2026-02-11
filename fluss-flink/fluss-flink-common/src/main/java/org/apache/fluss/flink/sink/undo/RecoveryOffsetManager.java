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
import java.util.stream.Collectors;

/**
 * Manages recovery offset determination for undo recovery in aggregation tables.
 *
 * <p>Recovery flow:
 *
 * <ol>
 *   <li>Get recovery offsets (from checkpoint or producer offsets)
 *   <li>Fetch partition info once and cache it (partitioned tables only), create TableBuckets for
 *       partitions not in recovery offsets
 *   <li>Filter buckets by sharding to current subtask
 *   <li>Fetch current offsets for filtered buckets (one RPC per partition due to Admin API
 *       limitation)
 *   <li>Filter buckets with changed offsets (recovery < current, new partition buckets use 0 as
 *       original offset)
 *   <li>Return recovery decision
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

    /** Result of recovery strategy determination. */
    public static class RecoveryDecision {
        private final RecoveryStrategy strategy;
        @Nullable private final Map<TableBucket, Long> recoveryOffsets;
        @Nullable private final Map<TableBucket, UndoOffsets> undoOffsets;

        private RecoveryDecision(
                RecoveryStrategy strategy,
                @Nullable Map<TableBucket, Long> recoveryOffsets,
                @Nullable Map<TableBucket, UndoOffsets> undoOffsets) {
            this.strategy = strategy;
            this.recoveryOffsets = recoveryOffsets;
            this.undoOffsets = undoOffsets;
        }

        public RecoveryStrategy getStrategy() {
            return strategy;
        }

        @Nullable
        public Map<TableBucket, Long> getRecoveryOffsets() {
            return recoveryOffsets;
        }

        /**
         * Returns the UndoOffsets map containing both checkpoint offset and log end offset.
         *
         * <p>This is used by UndoRecoveryManager to perform undo recovery without needing to call
         * listOffset again.
         *
         * @return map of bucket to UndoOffsets, or null if no recovery needed
         */
        @Nullable
        public Map<TableBucket, UndoOffsets> getUndoOffsets() {
            return undoOffsets;
        }

        public boolean needsUndoRecovery() {
            return strategy != RecoveryStrategy.FRESH_START
                    && undoOffsets != null
                    && !undoOffsets.isEmpty();
        }

        static RecoveryDecision of(
                RecoveryStrategy strategy,
                @Nullable Map<TableBucket, Long> recoveryOffsets,
                @Nullable Map<TableBucket, UndoOffsets> undoOffsets) {
            return new RecoveryDecision(strategy, recoveryOffsets, undoOffsets);
        }

        @Override
        public String toString() {
            int size = undoOffsets != null ? undoOffsets.size() : 0;
            return String.format("RecoveryDecision{strategy=%s, buckets=%d}", strategy, size);
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

        // Step 1: Get recovery offsets (checkpoint or producer offsets)
        // Note: recoveredState == null means no checkpoint exists (fresh start or pre-checkpoint
        // failure)
        //       recoveredState != null but empty means checkpoint exists but no data was written
        //       OR the checkpoint was taken before UndoRecoveryOperator was added to the topology
        //       In both cases, we should use producer offsets for recovery decision
        boolean hasCheckpoint = recoveredState != null && !recoveredState.isEmpty();
        Map<TableBucket, Long> recoveryOffsets =
                hasCheckpoint ? mergeCheckpointState(recoveredState) : getProducerOffsets();

        // Validate that checkpoint state refers to the same table (detect table re-creation)
        if (hasCheckpoint) {
            validateTableId(recoveryOffsets);
        }

        LOG.info(
                "Recovery offsets for subtask {} (source={}): {}",
                subtaskIndex,
                hasCheckpoint ? "checkpoint" : "producer",
                recoveryOffsets);

        // Step 2: Get all buckets (to ensure no bucket is missed during listOffset)
        Set<TableBucket> allBuckets = getAllBuckets();

        // Step 3: Filter by sharding (both allBuckets and recoveryOffsets)
        Map<Long, String> partitionNames = getPartitionNameMap();
        Set<TableBucket> filteredBuckets = filterBucketsBySharding(allBuckets, partitionNames);
        Map<TableBucket, Long> filteredRecoveryOffsets =
                filterRecoveryOffsetsBySharding(recoveryOffsets, partitionNames);

        LOG.info(
                "Subtask {}: filteredBuckets={}, filteredRecoveryOffsets={}",
                subtaskIndex,
                filteredBuckets,
                filteredRecoveryOffsets);

        if (filteredBuckets.isEmpty()) {
            LOG.info("No buckets assigned to subtask {} after filtering", subtaskIndex);
            return RecoveryDecision.of(RecoveryStrategy.FRESH_START, null, null);
        }

        // Step 4: Fetch current offsets for all filtered buckets
        Map<TableBucket, Long> currentOffsets =
                fetchCurrentOffsets(filteredBuckets, partitionNames);

        LOG.info("Subtask {}: currentOffsets={}", subtaskIndex, currentOffsets);

        // Step 5: Filter changed buckets and build UndoOffsets in one pass
        // For buckets not in filteredRecoveryOffsets, use 0 as recovery offset
        Map<TableBucket, Long> changedOffsets = new HashMap<>();
        Map<TableBucket, UndoOffsets> undoOffsets = new HashMap<>();

        for (TableBucket bucket : filteredBuckets) {
            long recovery = filteredRecoveryOffsets.getOrDefault(bucket, 0L);
            long current = currentOffsets.getOrDefault(bucket, 0L);
            boolean inRecoveryOffsets = filteredRecoveryOffsets.containsKey(bucket);

            LOG.info(
                    "Subtask {}: bucket={}, recovery={} (inCheckpoint={}), current={}",
                    subtaskIndex,
                    bucket,
                    recovery,
                    inRecoveryOffsets,
                    current);

            if (recovery > current) {
                throw new IllegalStateException(
                        String.format(
                                "Data inconsistency: bucket %s recovery=%d > current=%d",
                                bucket, recovery, current));
            }
            if (recovery < current) {
                changedOffsets.put(bucket, recovery);
                // Build UndoOffsets with checkpointOffset and logEndOffset (current offset)
                undoOffsets.put(bucket, new UndoOffsets(recovery, current));
            }
            // recovery == current: no change, skip
        }

        // Only return FRESH_START when changedOffsets is empty (after all checks)
        if (changedOffsets.isEmpty()) {
            LOG.info(
                    "No buckets with changed offsets, fresh start (hasCheckpointState={})",
                    hasCheckpoint);
            return RecoveryDecision.of(RecoveryStrategy.FRESH_START, null, null);
        }

        // Step 6: Return decision with both recoveryOffsets and undoOffsets
        RecoveryStrategy strategy =
                hasCheckpoint
                        ? RecoveryStrategy.CHECKPOINT_RECOVERY
                        : RecoveryStrategy.PRODUCER_OFFSET_RECOVERY;
        LOG.info(
                "{}: {} buckets need recovery for subtask {}",
                strategy,
                changedOffsets.size(),
                subtaskIndex);
        return RecoveryDecision.of(strategy, changedOffsets, undoOffsets);
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

    private Map<TableBucket, Long> mergeCheckpointState(Collection<WriterState> states) {
        Map<TableBucket, Long> merged = new HashMap<>();
        for (WriterState state : states) {
            state.getBucketOffsets()
                    .forEach((bucket, offset) -> merged.merge(bucket, offset, Math::max));
        }
        return merged;
    }

    /**
     * Validates that all buckets in the recovery offsets belong to the current table.
     *
     * <p>If the table was dropped and re-created, the checkpoint state will contain buckets with
     * the old table ID, which won't match the current table ID. In this case, restoring from the
     * checkpoint is not safe and should fail explicitly.
     *
     * @param recoveryOffsets the merged recovery offsets from checkpoint state
     * @throws IllegalStateException if any bucket has a mismatched table ID
     */
    private void validateTableId(Map<TableBucket, Long> recoveryOffsets) {
        for (TableBucket bucket : recoveryOffsets.keySet()) {
            if (bucket.getTableId() != tableId) {
                throw new IllegalStateException(
                        String.format(
                                "Table '%s' has been re-created (state tableId=%d, current tableId=%d). "
                                        + "Cannot restore from checkpoint/savepoint after table re-creation.",
                                tablePath, bucket.getTableId(), tableId));
            }
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

    private Map<TableBucket, Long> filterRecoveryOffsetsBySharding(
            Map<TableBucket, Long> recoveryOffsets, Map<Long, String> partitionNames) {
        Map<TableBucket, Long> filtered = new HashMap<>();
        for (Map.Entry<TableBucket, Long> entry : recoveryOffsets.entrySet()) {
            if (isAssignedToSubtask(entry.getKey(), partitionNames)) {
                filtered.put(entry.getKey(), entry.getValue());
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
        Long offset = result.bucketResult(bucketId).get();
        return offset != null ? offset : 0L;
    }
}
