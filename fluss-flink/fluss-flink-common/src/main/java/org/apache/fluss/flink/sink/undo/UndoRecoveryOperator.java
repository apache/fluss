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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.state.WriterStateSerializer;
import org.apache.fluss.flink.sink.undo.UndoRecoveryManager.UndoOffsets;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A Flink stream operator that preserves aggregation failover-recovery topology and state identity.
 *
 * <p>With {@link RecoveryAction#UNDO}, the operator tracks bucket offsets in Union List State and
 * uses {@link RecoveryOffsetManager} and {@link UndoRecoveryManager} to restore the checkpointed
 * materialized state.
 *
 * <p>With {@link RecoveryAction#NO_OP}, the operator remains in the topology for checkpoint
 * compatibility and skips data undo and offset tracking. When a producer ID is configured, subtask
 * 0 uses a short-lived connection to delete its stale producer offsets once during initialization;
 * otherwise NO_OP does not connect. No table is opened. It still claims the existing undo state
 * descriptor so that the next checkpoint can discard that state. A separate constant-size marker
 * records which action produced a checkpoint; a checkpoint produced by NO_OP cannot later be
 * restored with UNDO because it has no undo baseline.
 *
 * <p>Input elements always pass through unchanged.
 *
 * @param <IN> The type of input elements
 * @see ProducerOffsetReporter
 * @see RecoveryOffsetManager
 */
@Internal
public class UndoRecoveryOperator<IN> extends AbstractStreamOperator<IN>
        implements OneInputStreamOperator<IN, IN>, BoundedOneInput, ProducerOffsetReporter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryOperator.class);

    /** State descriptor name for the Union List State. */
    private static final String UNDO_RECOVERY_STATE_NAME = "undo_recovery_state";

    /** State descriptor name for the recovery action that produced a checkpoint. */
    private static final String RECOVERY_ACTION_STATE_NAME = "recovery_action_state";

    // ==================== Configuration Fields ====================

    /** The table path for the Fluss table. */
    private final TablePath tablePath;

    /** The Fluss configuration. */
    private final Configuration flussConfig;

    /** The row type of the table. */
    private final RowType tableRowType;

    /** Target column indexes for partial update (null for full row). */
    @Nullable private final int[] targetColumnIndexes;

    /** The number of buckets in the table. */
    private final int numBuckets;

    /** Whether the table is partitioned. */
    private final boolean isPartitioned;

    /**
     * The producer ID used for producer offset snapshot management.
     *
     * <p>This is used by {@link RecoveryOffsetManager} to register and retrieve producer offsets
     * for pre-checkpoint failure recovery. If null, UNDO resolves it to the Flink job ID while
     * NO_OP performs no producer-offset operation.
     */
    @Nullable private final String configuredProducerId;

    private final RecoveryAction recoveryAction;

    /**
     * The resolved producer ID (either configured or defaulted to Flink job ID).
     *
     * <p>This is set during {@link #initializeState} for UNDO subtasks and for NO_OP subtask 0 when
     * a producer ID is configured, and used for producer offset operations.
     */
    private transient String resolvedProducerId;

    /** The polling interval in milliseconds for producer offsets synchronization. */
    private final long producerOffsetsPollIntervalMs;

    /** The maximum total time in milliseconds to poll for producer offsets before giving up. */
    private final long maxPollTimeoutMs;

    /**
     * The registry ID used to register/remove this operator in the static delegate registry.
     *
     * <p>This ID is passed from {@link UndoRecoveryOperatorFactory} and used by {@link #close()} to
     * remove this operator from the registry by ID (O(1)) instead of by reference (O(n)).
     */
    private final String offsetReporterRegistryId;

    // ==================== State Fields ====================

    /** Union List State used by UNDO and claimed for cleanup by the NO_OP compatibility shell. */
    private transient ListState<WriterState> undoStateList;

    /** Union List State containing the action that produced a checkpoint. */
    private transient ListState<String> recoveryActionState;

    /**
     * Map from TableBucket to the latest written offset.
     *
     * <p>For UNDO, this map is updated by the downstream SinkWriter via {@link
     * #reportOffset(TableBucket, long)} and is used to create WriterState during checkpoint
     * snapshotting. It remains empty for NO_OP.
     *
     * <p>Uses ConcurrentHashMap for thread-safe updates from async write callbacks. The
     * ConcurrentHashMap's native thread-safety is sufficient since {@code merge()} is atomic and
     * {@code snapshotState()} runs on the mailbox thread (single-threaded context).
     */
    private transient ConcurrentHashMap<TableBucket, Long> bucketOffsets;

    /** Flag indicating whether the producer offsets have been deleted after first checkpoint. */
    private transient boolean producerOffsetsDeleted;

    /**
     * Flag indicating whether this operator was restored from a checkpoint.
     *
     * <p>This is used to determine whether to delete producer offsets after the first checkpoint.
     * Producer offsets should only be deleted when we're recovering from a checkpoint, not when
     * starting fresh. This ensures that if the job fails before the first checkpoint, the producer
     * offsets are still available for recovery on the next restart.
     */
    private transient boolean restoredFromCheckpoint;

    /** The subtask index of this operator instance. */
    private transient int subtaskIndex;

    /** The total parallelism of this operator. */
    private transient int parallelism;

    // ==================== Fluss Connection Fields ====================

    /** Fluss connection, lazily initialized only when undo recovery is needed. */
    @Nullable private transient Connection connection;

    /** Fluss table, lazily initialized only when undo recovery is needed. */
    @Nullable private transient Table table;

    // ==================== Constructor ====================

    UndoRecoveryOperator(
            StreamOperatorParameters<IN> parameters,
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            int numBuckets,
            boolean isPartitioned,
            @Nullable String producerId,
            RecoveryAction recoveryAction,
            long producerOffsetsPollIntervalMs,
            long maxPollTimeoutMs,
            String offsetReporterRegistryId) {
        super();
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableRowType = tableRowType;
        this.targetColumnIndexes = targetColumnIndexes;
        this.numBuckets = numBuckets;
        this.isPartitioned = isPartitioned;
        this.configuredProducerId = producerId;
        this.recoveryAction = checkNotNull(recoveryAction, "recoveryAction must not be null.");
        this.producerOffsetsPollIntervalMs = producerOffsetsPollIntervalMs;
        this.maxPollTimeoutMs = maxPollTimeoutMs;
        this.offsetReporterRegistryId = offsetReporterRegistryId;

        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
    }

    // ==================== State Initialization ====================

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        parallelism = RuntimeContextAdapter.getNumberOfParallelSubtasks(runtimeContext);
        subtaskIndex = RuntimeContextAdapter.getIndexOfThisSubtask(runtimeContext);
        producerOffsetsDeleted = false;
        restoredFromCheckpoint = context.isRestored();

        LOG.info(
                "Initializing UndoRecoveryOperator for table {} with action {} (subtask {}/{})",
                tablePath,
                recoveryAction,
                subtaskIndex,
                parallelism);

        recoveryActionState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        RECOVERY_ACTION_STATE_NAME, StringSerializer.INSTANCE));
        validateRecoveryActionTransition(context.isRestored());

        undoStateList =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        UNDO_RECOVERY_STATE_NAME, new WriterStateSerializer()));

        if (recoveryAction == RecoveryAction.NO_OP) {
            initializeNoOpState();
            LOG.info("UndoRecoveryOperator initialized with NO_OP for subtask {}", subtaskIndex);
            return;
        }

        Collection<WriterState> recoveredState = null;
        if (context.isRestored()) {
            recoveredState = new ArrayList<>();
            for (WriterState state : undoStateList.get()) {
                recoveredState.add(state);
            }
        }
        initializeUndoRecovery(recoveredState);
    }

    private void validateRecoveryActionTransition(boolean restored) throws Exception {
        if (!restored) {
            return;
        }

        RecoveryAction checkpointAction =
                resolveCheckpointRecoveryAction(recoveryActionState.get());
        if (checkpointAction != null) {
            validateRecoveryActionTransition(checkpointAction, recoveryAction);
        }
    }

    @Nullable
    private static RecoveryAction resolveCheckpointRecoveryAction(Iterable<String> actionNames) {
        RecoveryAction checkpointAction = null;
        for (String actionName : actionNames) {
            RecoveryAction action;
            try {
                action = RecoveryAction.valueOf(actionName);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(
                        "Unknown recovery action marker in checkpoint: " + actionName, e);
            }
            if (checkpointAction != null && checkpointAction != action) {
                throw new IllegalStateException(
                        "Found conflicting recovery action markers in checkpoint: "
                                + checkpointAction
                                + " and "
                                + action
                                + '.');
            }
            checkpointAction = action;
        }
        return checkpointAction;
    }

    private static void validateRecoveryActionTransition(
            RecoveryAction checkpointAction, RecoveryAction currentAction) {
        if (checkpointAction == RecoveryAction.NO_OP && currentAction == RecoveryAction.UNDO) {
            throw new IllegalStateException(
                    "Cannot restore recovery action UNDO from a checkpoint created with NO_OP. "
                            + "NO_OP checkpoints do not retain offsets required by undo recovery.");
        }
    }

    private void initializeNoOpState() throws Exception {
        initializeBucketOffsets(Collections.emptyMap());
        if (subtaskIndex != 0 || configuredProducerId == null) {
            return;
        }

        resolvedProducerId = configuredProducerId;
        LOG.info("Task0 deleting stale producer offsets for producerId {}", resolvedProducerId);
        try (Connection cleanupConnection = ConnectionFactory.createConnection(flussConfig)) {
            cleanupConnection.getAdmin().deleteProducerOffsets(resolvedProducerId).get();
        }
        LOG.info("Task0 deleted stale producer offsets for producerId {}", resolvedProducerId);
    }

    private void initializeUndoRecovery(@Nullable Collection<WriterState> recoveredState)
            throws Exception {
        resolvedProducerId = resolveProducerId();

        initializeFlussConnection();
        if (table == null) {
            table = connection.getTable(tablePath);
        }

        RecoveryOffsetManager offsetManager =
                new RecoveryOffsetManager(
                        connection.getAdmin(),
                        resolvedProducerId,
                        subtaskIndex,
                        parallelism,
                        producerOffsetsPollIntervalMs,
                        maxPollTimeoutMs,
                        tablePath,
                        table.getTableInfo());
        RecoveryOffsetManager.RecoveryDecision decision =
                offsetManager.determineRecoveryStrategy(recoveredState);
        LOG.info("Recovery decision for subtask {}: {}", subtaskIndex, decision);
        applyRecoveryDecision(decision);
        LOG.info(
                "UndoRecoveryOperator initialized for subtask {} with {} bucket offsets",
                subtaskIndex,
                bucketOffsets.size());
    }

    private String resolveProducerId() {
        if (configuredProducerId != null) {
            return configuredProducerId;
        }
        String producerId = RuntimeContextAdapter.getJobId(getRuntimeContext()).toString();
        LOG.info("Using Flink job ID as producerId: {}", producerId);
        return producerId;
    }

    private void applyRecoveryDecision(RecoveryOffsetManager.RecoveryDecision decision)
            throws Exception {
        if (decision.needsUndoRecovery()) {
            Map<TableBucket, UndoOffsets> undoOffsets = decision.getUndoOffsets();
            LOG.info(
                    "Executing undo recovery for subtask {}: {} buckets",
                    subtaskIndex,
                    undoOffsets.size());
            performUndoRecovery(undoOffsets);

            Map<TableBucket, Long> recoveryOffsets = decision.getRecoveryOffsets();
            LOG.info(
                    "Subtask {} initializing bucketOffsets from recovery: {} buckets",
                    subtaskIndex,
                    recoveryOffsets.size());
            initializeBucketOffsets(recoveryOffsets);
        } else {
            LOG.info("No undo recovery needed for subtask {}", subtaskIndex);
            initializeBucketOffsets(Collections.emptyMap());
        }
    }

    /**
     * Initializes the Fluss connection lazily.
     *
     * <p>The connection is only created when needed (e.g., for fetching partition info or
     * performing undo recovery).
     */
    private void initializeFlussConnection() {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(flussConfig);
            LOG.debug("Created Fluss connection for table {}", tablePath);
        }
    }

    // ==================== Undo Recovery Execution ====================

    /**
     * Performs undo recovery for the given bucket offsets.
     *
     * <p>This method reuses {@link UndoRecoveryManager} for the actual recovery logic. The recovery
     * process reads changelog records from the checkpoint offset to the log end offset and applies
     * inverse operations to restore the bucket state.
     *
     * @param undoOffsets the bucket undo offsets containing checkpoint offset and log end offset
     * @throws Exception if recovery fails
     */
    private void performUndoRecovery(Map<TableBucket, UndoOffsets> undoOffsets) throws Exception {
        LOG.info(
                "Performing undo recovery for {} buckets on subtask {}/{}",
                undoOffsets.size(),
                subtaskIndex,
                parallelism);

        // Reuse UndoRecoveryManager for actual recovery
        // UndoOffsets already contains both checkpointOffset and logEndOffset,
        // so no additional listOffset call is needed
        UndoRecoveryManager recoveryManager = new UndoRecoveryManager(table, targetColumnIndexes);
        recoveryManager.performUndoRecovery(undoOffsets, subtaskIndex, parallelism);

        LOG.info(
                "Completed undo recovery for {} buckets on subtask {}/{}",
                undoOffsets.size(),
                subtaskIndex,
                parallelism);
    }

    // ==================== State Snapshotting ====================

    /**
     * Snapshots the current state during checkpoint.
     *
     * <p>The action marker is stored once in Union List State. UNDO also stores the current bucket
     * offsets. NO_OP clears any restored undo state and writes no replacement.
     *
     * <p>For UNDO, producer offset cleanup is not done here. It is done after checkpoint completion
     * to prevent data loss between snapshot and completion. NO_OP cleans up an explicitly
     * configured producer ID during initialization.
     *
     * @param context the state snapshot context containing checkpoint information
     * @throws Exception if snapshotting fails
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        recoveryActionState.clear();
        if (subtaskIndex == 0) {
            recoveryActionState.add(recoveryAction.name());
        }

        // Clear existing state
        undoStateList.clear();

        if (recoveryAction == RecoveryAction.NO_OP) {
            LOG.debug(
                    "Subtask {} cleared undo state at NO_OP checkpoint {}",
                    subtaskIndex,
                    context.getCheckpointId());
            return;
        }

        // Add new state if bucket offsets is not empty
        if (bucketOffsets != null) {
            if (!bucketOffsets.isEmpty()) {
                WriterState state = new WriterState(new HashMap<>(bucketOffsets));
                undoStateList.add(state);
                LOG.info(
                        "Subtask {} snapshot state at checkpoint {}: {} buckets",
                        subtaskIndex,
                        context.getCheckpointId(),
                        bucketOffsets.size());
                LOG.debug(
                        "Subtask {} checkpoint {} bucketOffsets details: {}",
                        subtaskIndex,
                        context.getCheckpointId(),
                        bucketOffsets);
            } else {
                LOG.debug(
                        "Subtask {} snapshot state at checkpoint {}: bucketOffsets is EMPTY",
                        subtaskIndex,
                        context.getCheckpointId());
            }
        }
    }

    /**
     * Called when an UNDO checkpoint is completed successfully.
     *
     * <p>This method triggers producer offset cleanup after the first successful checkpoint, but
     * ONLY when the operator was restored from a checkpoint. This is critical for the producer
     * offset recovery scenario:
     *
     * <ul>
     *   <li>If starting fresh (no checkpoint): Do NOT delete producer offsets. They are needed for
     *       recovery if the job fails before the next checkpoint.
     *   <li>If restored from checkpoint: Delete producer offsets after the first successful
     *       checkpoint. The checkpoint state now contains the recovery offsets, so producer offsets
     *       are no longer needed.
     * </ul>
     *
     * @param checkpointId the ID of the completed checkpoint
     * @throws Exception if cleanup fails
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        LOG.info(
                "Checkpoint {} completed for subtask {} (restoredFromCheckpoint={}, bucketOffsets={})",
                checkpointId,
                subtaskIndex,
                restoredFromCheckpoint,
                bucketOffsets);

        // Only delete producer offsets if we were restored from a checkpoint.
        // If starting fresh, keep producer offsets for potential recovery on failure.
        if (recoveryAction == RecoveryAction.UNDO
                && restoredFromCheckpoint
                && bucketOffsets != null
                && !bucketOffsets.isEmpty()) {
            deleteProducerOffsetsIfNeeded();
        }
    }

    /**
     * Deletes producer offsets after first checkpoint (Task0 only).
     *
     * <p>This cleanup is necessary to prevent stale producer offsets from being used in subsequent
     * recovery scenarios. Only Task0 performs the deletion to avoid concurrent cleanup attempts.
     */
    private void deleteProducerOffsetsIfNeeded() {
        if (recoveryAction == RecoveryAction.NO_OP || producerOffsetsDeleted) {
            return;
        }
        producerOffsetsDeleted = true;

        // Only Task0 should delete the offsets
        if (subtaskIndex != 0) {
            return;
        }

        LOG.info("Task0 deleting producer offsets for producerId {}", resolvedProducerId);
        try {
            initializeFlussConnection();
            connection.getAdmin().deleteProducerOffsets(resolvedProducerId).get();
            LOG.info("Successfully deleted producer offsets for producerId {}", resolvedProducerId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to delete producer offsets for {}: {}",
                    resolvedProducerId,
                    e.getMessage());
        }
    }

    // ==================== OneInputStreamOperator Methods ====================

    /**
     * Processes an input element by passing it through to the output unchanged.
     *
     * <p>This operator does not modify, filter, or buffer any input elements. All elements are
     * emitted to the output immediately in the same order they are received.
     *
     * @param element the input element to process
     * @throws Exception if processing fails
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // Pass through unchanged
        output.collect(element);
    }

    // ==================== BoundedOneInput Methods ====================

    /**
     * Called when the input is exhausted (for bounded streams).
     *
     * <p>For UNDO, cleans up producer offsets to prevent stale entries from lingering. This is
     * important for bounded jobs where {@link #notifyCheckpointComplete} may never be called.
     *
     * <p>The cleanup is idempotent — {@link #deleteProducerOffsetsIfNeeded()} uses the {@code
     * producerOffsetsDeleted} flag and Task0-only guard, so it's safe to call from both here and
     * {@link #notifyCheckpointComplete}.
     *
     * @throws Exception if end input processing fails
     */
    @Override
    public void endInput() throws Exception {
        if (recoveryAction == RecoveryAction.UNDO) {
            deleteProducerOffsetsIfNeeded();
        }
    }

    // ==================== ProducerOffsetReporter Methods ====================

    /**
     * Reports a written offset for a bucket.
     *
     * <p>NO_OP ignores reports defensively. For UNDO, this method is called from async write
     * callbacks on multiple threads. Thread-safety is provided by the ConcurrentHashMap's atomic
     * {@code merge()} operation.
     *
     * <p>The method updates the offset only if the new offset is greater than the existing one,
     * ensuring monotonically increasing offsets for each bucket.
     *
     * @param bucket the bucket that was written to
     * @param offset the offset of the written record
     */
    @Override
    public void reportOffset(TableBucket bucket, long offset) {
        if (recoveryAction == RecoveryAction.NO_OP) {
            return;
        }
        if (bucketOffsets != null) {
            bucketOffsets.merge(bucket, offset, Math::max);
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Reported offset {} for bucket {} (current max: {})",
                        offset,
                        bucket,
                        bucketOffsets.get(bucket));
            }
        } else {
            LOG.warn(
                    "Received offset report for bucket {} before bucketOffsets was initialized, "
                            + "offset {} will be ignored",
                    bucket,
                    offset);
        }
    }

    // ==================== Lifecycle Methods ====================

    /**
     * Closes the operator and releases all resources.
     *
     * <p>This method performs cleanup in the following order:
     *
     * <ol>
     *   <li>Close the Fluss Table instance (if created)
     *   <li>Close the Fluss Connection instance (if created)
     *   <li>Call super.close() to complete operator cleanup
     * </ol>
     *
     * @throws Exception if super.close() fails
     */
    @Override
    public void close() throws Exception {
        // Remove this operator from the static DELEGATE_REGISTRY to prevent memory leaks.
        // Each job submission registers entries in the registry via ProducerOffsetReporterHolder,
        // and without this cleanup, entries accumulate indefinitely in long-running clusters.
        UndoRecoveryOperatorFactory.removeDelegate(offsetReporterRegistryId);

        // Close Table instance first (if created)
        if (table != null) {
            try {
                table.close();
                LOG.debug("Closed Fluss table for {}", tablePath);
            } catch (Exception e) {
                LOG.warn("Failed to close Fluss table for {}", tablePath, e);
            } finally {
                table = null;
            }
        }

        // Close Connection instance second (if created)
        if (connection != null) {
            try {
                connection.close();
                LOG.debug("Closed Fluss connection for {}", tablePath);
            } catch (Exception e) {
                LOG.warn("Failed to close Fluss connection for {}", tablePath, e);
            } finally {
                connection = null;
            }
        }

        // Call super.close() at the end
        super.close();
    }

    // ==================== Getters for Testing ====================

    public TablePath getTablePath() {
        return tablePath;
    }

    public Configuration getFlussConfig() {
        return flussConfig;
    }

    public RowType getTableRowType() {
        return tableRowType;
    }

    @Nullable
    public int[] getTargetColumnIndexes() {
        return targetColumnIndexes;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    @Nullable
    public String getProducerId() {
        return resolvedProducerId != null ? resolvedProducerId : configuredProducerId;
    }

    @Nullable
    public String getConfiguredProducerId() {
        return configuredProducerId;
    }

    public long getProducerOffsetsPollIntervalMs() {
        return producerOffsetsPollIntervalMs;
    }

    public long getMaxPollTimeoutMs() {
        return maxPollTimeoutMs;
    }

    @Nullable
    public Map<TableBucket, Long> getBucketOffsets() {
        return bucketOffsets;
    }

    /**
     * Initializes the bucket offsets map and its associated lock.
     *
     * @param initialOffsets the initial bucket offsets
     */
    protected void initializeBucketOffsets(Map<TableBucket, Long> initialOffsets) {
        this.bucketOffsets = new ConcurrentHashMap<>();
        this.bucketOffsets.putAll(initialOffsets);
    }
}
