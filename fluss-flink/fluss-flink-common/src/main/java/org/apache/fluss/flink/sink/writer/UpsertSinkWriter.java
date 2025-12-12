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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.sink.ChannelComputer;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.state.WriterStateSerializer;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** An upsert sink writer for fluss primary key table. */
public class UpsertSinkWriter<InputT> extends FlinkSinkWriter<InputT>
        implements CheckpointedFunction {

    private static final String WRITER_STATE_NAME = "fluss-writer-state";
    private static final Duration LOG_POLL_TIMEOUT = Duration.ofSeconds(1);

    private transient UpsertWriter upsertWriter;
    private transient ListState<WriterState> writerStateListState;

    /**
     * In-memory state tracking bucket offsets.
     *
     * <p>This map stores the last committed log offset for each TableBucket that this writer is
     * responsible for. It is used for:
     *
     * <ul>
     *   <li>Checkpoint: snapshot current offsets to Flink state backend
     *   <li>Recovery: restore offsets from previous checkpoint
     *   <li>Scale up/down: merge offsets from all previous parallel instances
     * </ul>
     *
     * <p>With state filtering enabled, this map only contains buckets that are routed to this
     * subtask, reducing memory footprint in large-scale scenarios.
     */
    private transient Map<TableBucket, Long> bucketOffsets;

    /**
     * The index of this subtask (0-based). Used for filtering buckets that should be handled by
     * this subtask.
     */
    private final int subtaskIndex;

    /**
     * The total parallelism of the sink operator. Used together with subtaskIndex to determine
     * bucket ownership.
     */
    private final int parallelism;

    /**
     * The indexes of primary key columns in the row. Used to extract primary key from a row for
     * undo operation deduplication.
     */
    private transient int[] primaryKeyIndexes;

    /**
     * Field getters for primary key columns. Used to extract primary key values efficiently from
     * InternalRow without switch-case on data types.
     */
    private transient FieldGetter[] primaryKeyFieldGetters;

    /**
     * Temporary storage for merged bucket offsets before filtering. This is populated in
     * initializeState() and filtered in initialize().
     */
    private transient Map<TableBucket, Long> unfilteredBucketOffsets;

    public UpsertSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> flussSerializationSchema,
            int subtaskIndex,
            int parallelism,
            String lockOwnerId) {
        super(
                tablePath,
                configureWithLockOwnerId(flussConfig, lockOwnerId),
                tableRowType,
                targetColumnIndexes,
                mailboxExecutor,
                flussSerializationSchema);
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    /**
     * Configure the fluss config with lock owner ID.
     *
     * <p>The lock owner ID is used for column lock coordination during undo recovery. All subtasks
     * of the same job share the same lock owner ID (typically the Flink job ID).
     *
     * @param flussConfig the base fluss configuration
     * @param lockOwnerId the lock owner ID (typically Flink job ID)
     * @return a new Configuration with lock owner ID set
     */
    private static Configuration configureWithLockOwnerId(
            Configuration flussConfig, String lockOwnerId) {
        Configuration configWithLockOwnerId = new Configuration(flussConfig);
        configWithLockOwnerId.setString(
                org.apache.fluss.config.ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, lockOwnerId);
        return configWithLockOwnerId;
    }

    @Override
    public void initialize(SinkWriterMetricGroup metricGroup) {
        super.initialize(metricGroup);
        Upsert upsert = table.newUpsert();
        if (targetColumnIndexes != null) {
            upsert = upsert.partialUpdate(targetColumnIndexes);
        }
        upsertWriter = upsert.createWriter();

        // Initialize bucket offsets map if not already done (in case initialize is called before
        // initializeState)
        if (bucketOffsets == null) {
            bucketOffsets = new HashMap<>();
        }

        // Perform state filtering if we have unfiltered offsets from recovery
        if (unfilteredBucketOffsets != null && !unfilteredBucketOffsets.isEmpty()) {
            filterBucketOffsets();
        }

        // Initialize primary key indexes and field getters for undo operation optimization
        TableInfo tableInfo = table.getTableInfo();
        List<String> pkNames = tableInfo.getPrimaryKeys();
        primaryKeyIndexes = new int[pkNames.size()];
        primaryKeyFieldGetters = new FieldGetter[pkNames.size()];

        org.apache.fluss.types.RowType flussRowType = tableInfo.getRowType();
        for (int i = 0; i < pkNames.size(); i++) {
            int fieldIndex = tableRowType.getFieldIndex(pkNames.get(i));
            primaryKeyIndexes[i] = fieldIndex;
            // Create field getter for efficient value extraction
            primaryKeyFieldGetters[i] =
                    InternalRow.createFieldGetter(flussRowType.getTypeAt(fieldIndex), fieldIndex);
        }

        // Perform undo recovery if there are checkpoint offsets to recover from
        if (!bucketOffsets.isEmpty()) {
            try {
                performUndoRecovery();
            } catch (Exception e) {
                LOG.error("Failed to perform undo recovery", e);
                throw new RuntimeException("Failed to perform undo recovery", e);
            }
        }

        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow) {
        if (opType == OperationType.UPSERT) {
            return upsertWriter.upsert(internalRow);
        } else if (opType == OperationType.DELETE) {
            return upsertWriter.delete(internalRow);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + opType);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        upsertWriter.flush();
        checkAsyncException();
    }

    @Override
    TableWriter getTableWriter() {
        return upsertWriter;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // First, flush to ensure all writes are acknowledged and offsets are up-to-date
        upsertWriter.flush();

        // Update in-memory bucket offsets from UpsertWriter's tracker
        // This ensures we capture the latest acknowledged offsets
        Map<TableBucket, Long> currentOffsets = upsertWriter.bucketsAckStatus();
        bucketOffsets.putAll(currentOffsets);

        // Clear previous state in UnionListState
        writerStateListState.clear();

        // Create a new WriterState with current checkpoint ID and bucket offsets
        WriterState currentState = new WriterState(context.getCheckpointId(), bucketOffsets);

        // Add current state to UnionListState
        // When using UnionListState, each parallel instance stores its own state,
        // and during recovery, all states from all instances will be broadcast to
        // all new instances
        writerStateListState.add(currentState);

        LOG.info(
                "Snapshot state for checkpoint {}: tracked {} buckets with offsets",
                context.getCheckpointId(),
                bucketOffsets.size());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Create UnionListState to store writer state
        // UnionListState ensures that:
        // 1. Each parallel instance stores its own WriterState during checkpoint
        // 2. During recovery, all WriterStates from all previous parallel instances
        //    are broadcast to all new parallel instances
        // 3. Each new instance can reconstruct the full picture and filter its own buckets
        ListStateDescriptor<WriterState> stateDescriptor =
                new ListStateDescriptor<>(WRITER_STATE_NAME, new WriterStateSerializer());

        writerStateListState = context.getOperatorStateStore().getUnionListState(stateDescriptor);

        // Initialize in-memory bucket offsets map
        bucketOffsets = new HashMap<>();

        // During recovery, merge all WriterStates from all previous parallel instances
        if (context.isRestored()) {
            // Merge all bucket offsets from all previous instances
            // State filtering will be performed later in initialize() after table is initialized
            unfilteredBucketOffsets = new HashMap<>();
            long maxCheckpointId = -1;
            int stateCount = 0;

            for (WriterState state : writerStateListState.get()) {
                maxCheckpointId = Math.max(maxCheckpointId, state.getCheckpointId());
                stateCount++;

                // Merge bucket offsets from all instances
                // If the same bucket appears in multiple states (which shouldn't happen
                // in normal cases since each bucket is routed to only one instance),
                // we take the maximum offset to ensure consistency
                for (Map.Entry<TableBucket, Long> entry : state.getBucketOffsets().entrySet()) {
                    unfilteredBucketOffsets.merge(entry.getKey(), entry.getValue(), Math::max);
                }
            }

            LOG.info(
                    "Restored writer state from checkpoint {}: "
                            + "merged {} total bucket offsets from {} previous instances "
                            + "(filtering will be performed in initialize())",
                    maxCheckpointId,
                    unfilteredBucketOffsets.size(),
                    stateCount);

            // Note on deferred filtering:
            // State filtering requires table metadata (partitioning strategy, bucket count)
            // which is only available after initialize() is called. Therefore, we defer
            // filtering to initialize() method.
            //
            // Note on scale up/down handling:
            // - Scale up: Each new instance receives all states from previous instances via
            //   UnionListState broadcast. After merging, we filter to keep only buckets that
            //   route to this subtask (based on routing logic). Memory is optimized
            //   as each instance only keeps its own buckets.
            //
            // - Scale down: Similar to scale up, each remaining instance gets all previous
            //   states, merges them, and filters to keep only its assigned buckets. Some
            //   instances will now handle more buckets than before, but they have the correct
            //   offset information from the merged state.
            //
            // Key benefits of filtering:
            // - Memory optimization: Each instance only keeps 1/parallelism of the buckets
            // - Correct routing: Uses same logic as FlinkStreamPartitioner/ChannelComputer
            // - State consistency: No bucket offsets are lost during scale up/down
            //
            // Partitioned table handling:
            // - For partitioned tables where numBuckets % parallelism != 0, we use both
            //   partition name and bucket ID to compute the target channel. This matches
            //   the data routing logic in FlinkRowDataChannelComputer and helps avoid
            //   data skew across partitions.
        } else {
            LOG.info(
                    "No previous state to restore, starting fresh with empty bucket offsets for subtask {}/{}",
                    subtaskIndex,
                    parallelism);
        }
    }

    /**
     * Filter bucket offsets to only keep buckets that should be routed to this subtask.
     *
     * <p>This method is called during initialize() after table metadata is available. It filters
     * the merged bucket offsets from all previous instances to only keep the buckets that should be
     * handled by this subtask based on the routing logic.
     */
    private void filterBucketOffsets() {
        // Get table info to determine partitioning strategy
        TableInfo tableInfo = table.getTableInfo();
        boolean isPartitioned = tableInfo.isPartitioned();
        int numBuckets = tableInfo.getNumBuckets();

        // Determine if we need to combine shuffle with partition name
        // This matches the logic in FlinkRowDataChannelComputer
        boolean combineShuffleWithPartitionName = isPartitioned && numBuckets % parallelism != 0;

        // Prepare partition metadata if needed for partitioned tables
        Cluster cluster = null;
        if (isPartitioned) {
            // Get metadata updater from connection to access partition information
            if (!(connection instanceof FlussConnection)) {
                throw new IllegalStateException(
                        "Expected FlussConnection but got: " + connection.getClass().getName());
            }
            FlussConnection flussConnection = (FlussConnection) connection;
            MetadataUpdater metadataUpdater = flussConnection.getMetadataUpdater();

            // Collect all partition IDs from merged offsets and update partition metadata
            // to ensure we have all partition names in the cluster cache
            Set<Long> partitionIds = new HashSet<>();
            for (TableBucket tableBucket : unfilteredBucketOffsets.keySet()) {
                if (tableBucket.getPartitionId() != null) {
                    partitionIds.add(tableBucket.getPartitionId());
                }
            }
            if (!partitionIds.isEmpty()) {
                metadataUpdater.checkAndUpdatePartitionMetadata(tablePath, partitionIds);
            }

            cluster = metadataUpdater.getCluster();
        }

        // Now filter: only keep buckets that should be routed to this subtask
        int filteredOutCount = 0;
        for (Map.Entry<TableBucket, Long> entry : unfilteredBucketOffsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            int targetChannel;

            if (combineShuffleWithPartitionName) {
                // For partitioned tables with bucket/parallelism mismatch,
                // use partition name + bucket to compute target channel
                Long partitionId = bucket.getPartitionId();
                if (partitionId == null) {
                    LOG.warn(
                            "Expected partition ID for partitioned table but found null in bucket: {}",
                            bucket);
                    targetChannel = ChannelComputer.select(bucket.getBucket(), parallelism);
                } else {
                    String partitionName = cluster.getPartitionNameOrElseThrow(partitionId);
                    targetChannel =
                            ChannelComputer.select(partitionName, bucket.getBucket(), parallelism);
                }
            } else {
                // For non-partitioned tables or partitioned tables where bucket count
                // is divisible by parallelism, only use bucket ID
                targetChannel = ChannelComputer.select(bucket.getBucket(), parallelism);
            }

            if (targetChannel == subtaskIndex) {
                // This bucket belongs to current subtask
                bucketOffsets.put(bucket, entry.getValue());
            } else {
                // This bucket belongs to another subtask, filter it out
                filteredOutCount++;
            }
        }

        LOG.info(
                "Filtered bucket offsets: "
                        + "total {} buckets from merged state, "
                        + "kept {} buckets for subtask {}/{}, filtered out {} buckets "
                        + "[partitioned={}, combineShuffleWithPartitionName={}]",
                unfilteredBucketOffsets.size(),
                bucketOffsets.size(),
                subtaskIndex,
                parallelism,
                filteredOutCount,
                isPartitioned,
                combineShuffleWithPartitionName);

        // Clear unfiltered offsets to free memory
        unfilteredBucketOffsets = null;
    }

    /**
     * Perform undo recovery to restore bucket states to checkpoint positions.
     *
     * <p>This method ensures exactly-once semantics by undoing any writes that occurred after the
     * last checkpoint but before the failure. The recovery process:
     *
     * <ol>
     *   <li>For each bucket: read log records from checkpoint offset to current latest offset
     *   <li>Generate merged undo operations to reverse the effect of those records
     *   <li>Apply undo operations in overwrite mode to reset bucket state
     * </ol>
     *
     * <p>Note: Column lock management is handled transparently by the fluss-client layer.
     */
    private void performUndoRecovery() {
        LOG.info(
                "Starting undo recovery for {} buckets on subtask {}/{}",
                bucketOffsets.size(),
                subtaskIndex,
                parallelism);

        // Process each bucket
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            long checkpointOffset = entry.getValue();

            LOG.info(
                    "Performing undo recovery for bucket {} from checkpoint offset {}",
                    bucket,
                    checkpointOffset);

            try {
                // Read logs from checkpoint to latest
                Map<GenericRow, UndoOperation> undoOperations =
                        readAndComputeUndoOperations(bucket, checkpointOffset);

                if (undoOperations.isEmpty()) {
                    LOG.info("No operations to undo for bucket {}, skipping undo writes", bucket);
                    continue;
                }

                // Apply undo operations in overwrite mode
                applyUndoOperations(bucket, undoOperations);

                LOG.info(
                        "Successfully completed undo recovery for bucket {} with {} operations",
                        bucket,
                        undoOperations.size());
            } catch (Exception e) {
                LOG.error("Failed to perform undo recovery for bucket {}", bucket, e);
                throw new RuntimeException(
                        "Failed to perform undo recovery for bucket " + bucket, e);
            }
        }

        LOG.info("Completed undo recovery for all buckets");
    }

    /** Represents an undo operation with its type and optional row data. */
    private static class UndoOperation {
        final ChangeType type;
        final InternalRow row; // Only set for UPSERT operations

        UndoOperation(ChangeType type) {
            this(type, null);
        }

        UndoOperation(ChangeType type, InternalRow row) {
            this.type = type;
            this.row = row;
        }
    }

    /**
     * Read log records from checkpoint offset to latest and compute undo operations.
     *
     * <p>This method:
     *
     * <ol>
     *   <li>Creates a log scanner for the bucket
     *   <li>Reads all records from checkpoint offset to latest
     *   <li>Computes undo operations by primary key, only tracking the first change per key
     * </ol>
     *
     * <p>Optimization: For the same primary key, we only care about the first change since
     * checkpoint. This is because we only need to restore the state to what it was at checkpoint
     * time. Subsequent changes to the same key can be ignored as they all happened after checkpoint
     * and don't affect the final undo operation.
     *
     * <p>The undo logic for the first change of each primary key:
     *
     * <ul>
     *   <li>INSERT: adds to undo map as DELETE (key didn't exist at checkpoint)
     *   <li>UPDATE_BEFORE/UPDATE_AFTER: adds to undo map as UPSERT with UPDATE_BEFORE row (restore
     *       old value)
     *   <li>DELETE: adds to undo map as UPSERT with DELETE row (key existed at checkpoint, needs
     *       restoration)
     * </ul>
     *
     * @param bucket the bucket to read from
     * @param checkpointOffset the checkpoint offset to start reading from
     * @return a map from primary key to the undo operation
     * @throws Exception if reading fails
     */
    private Map<GenericRow, UndoOperation> readAndComputeUndoOperations(
            TableBucket bucket, long checkpointOffset) throws Exception {
        // LinkedHashMap to preserve insertion order for deterministic undo operations
        Map<GenericRow, UndoOperation> undoOperations = new LinkedHashMap<>();

        try (LogScanner scanner = table.newScan().createLogScanner()) {
            // Subscribe to the bucket starting from the checkpoint offset
            if (bucket.getPartitionId() != null) {
                scanner.subscribe(bucket.getPartitionId(), bucket.getBucket(), checkpointOffset);
            } else {
                scanner.subscribe(bucket.getBucket(), checkpointOffset);
            }

            // Poll and process all available records
            boolean hasMoreRecords = true;
            int totalRecordsProcessed = 0;

            while (hasMoreRecords) {
                ScanRecords records = scanner.poll(LOG_POLL_TIMEOUT);

                if (records.isEmpty()) {
                    // No more records available
                    hasMoreRecords = false;
                    break;
                }

                List<ScanRecord> bucketRecords = records.records(bucket);
                for (ScanRecord record : bucketRecords) {
                    processRecordForUndo(record, undoOperations);
                    totalRecordsProcessed++;
                }

                // If we got fewer records than expected, we might have reached the end
                if (bucketRecords.size() < 1000) { // Assuming batch size threshold
                    hasMoreRecords = false;
                }
            }

            LOG.info(
                    "Read {} log records from bucket {} (from offset {}), computed {} undo operations",
                    totalRecordsProcessed,
                    bucket,
                    checkpointOffset,
                    undoOperations.size());
        }

        return undoOperations;
    }

    /**
     * Process a single log record to update the undo operations map.
     *
     * <p>Optimization: We only track the first change for each primary key since checkpoint. Once a
     * primary key is recorded in the undo map, all subsequent changes to the same key are ignored.
     * This is because we only need to restore the state to what it was at checkpoint time.
     *
     * <p>The logic for the first occurrence of each primary key:
     *
     * <ul>
     *   <li>INSERT: Record needs to be deleted (add DELETE to undo map)
     *   <li>UPDATE_BEFORE: Record the old value to restore (add UPSERT to undo map)
     *   <li>UPDATE_AFTER: Skip if UPDATE_BEFORE already processed
     *   <li>DELETE: Record needs to be re-inserted with the row data (add UPSERT to undo map)
     * </ul>
     *
     * @param record the log record to process
     * @param undoOperations the map of undo operations to update
     */
    private void processRecordForUndo(
            ScanRecord record, Map<GenericRow, UndoOperation> undoOperations) {
        ChangeType changeType = record.getChangeType();
        InternalRow row = record.getRow();

        // Extract primary key from the row
        GenericRow primaryKey = extractPrimaryKey(row);

        // Optimization: Only process the first change for each primary key
        // If this primary key already has an undo operation, skip it
        if (undoOperations.containsKey(primaryKey)) {
            // This key was already processed, ignore subsequent changes
            return;
        }

        // Record the undo operation for the first change of this primary key
        switch (changeType) {
            case INSERT:
                // New row inserted after checkpoint, needs to be deleted
                undoOperations.put(primaryKey, new UndoOperation(ChangeType.DELETE));
                break;

            case UPDATE_BEFORE:
                // Row was updated after checkpoint, need to restore the old value
                // Convert the full row to GenericRow for storage
                GenericRow oldRow = convertToGenericRow(row);
                undoOperations.put(primaryKey, new UndoOperation(ChangeType.INSERT, oldRow));
                break;

            case UPDATE_AFTER:
                // UPDATE_AFTER is paired with UPDATE_BEFORE in the changelog
                // If we see UPDATE_AFTER first (without seeing UPDATE_BEFORE), it means
                // the UPDATE_BEFORE was at or before the checkpoint offset, so we only
                // saw the "after" part. In this case, we need to delete the new value
                // to restore to the checkpoint state.
                //
                // If UPDATE_BEFORE was already processed, this UPDATE_AFTER should be
                // ignored (already handled by UPDATE_BEFORE).
                if (!undoOperations.containsKey(primaryKey)) {
                    undoOperations.put(primaryKey, new UndoOperation(ChangeType.DELETE));
                }
                // Otherwise, skip - the UPDATE_BEFORE already recorded the undo operation
                break;

            case DELETE:
                // Row was deleted after checkpoint, need to re-insert it
                // The DELETE record contains the full row data
                GenericRow deletedRow = convertToGenericRow(row);
                undoOperations.put(primaryKey, new UndoOperation(ChangeType.INSERT, deletedRow));
                break;

            default:
                LOG.warn("Unexpected change type for undo: {}", changeType);
        }
    }

    /**
     * Convert InternalRow to GenericRow for storage in undo operations map.
     *
     * @param row the InternalRow to convert
     * @return a GenericRow with all fields copied
     */
    private GenericRow convertToGenericRow(InternalRow row) {
        int fieldCount = tableRowType.getFieldCount();
        GenericRow genericRow = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(
                            table.getTableInfo().getRowType().getTypeAt(i), i);
            genericRow.setField(i, fieldGetter.getFieldOrNull(row));
        }
        return genericRow;
    }

    /**
     * Apply undo operations to the bucket using overwrite mode.
     *
     * <p>This method:
     *
     * <ol>
     *   <li>Creates a temporary overwrite connection for undo recovery
     *   <li>Applies all undo operations (typically DELETEs)
     *   <li>Flushes to ensure all operations are persisted
     *   <li>Closes the overwrite connection
     * </ol>
     *
     * <p>The main writer connection remains in normal mode throughout this process. The temporary
     * overwrite connection is used only for the undo recovery phase to ensure data consistency.
     *
     * @param bucket the bucket to apply undo operations to
     * @param undoOperations the map of undo operations to apply (keyed by primary key)
     * @throws Exception if applying operations fails
     */
    private void applyUndoOperations(
            TableBucket bucket, Map<GenericRow, UndoOperation> undoOperations) throws Exception {
        LOG.info(
                "Applying {} undo operations to bucket {} using overwrite connection",
                undoOperations.size(),
                bucket);

        // Create a temporary recovery connection for undo recovery
        // This connection bypasses the merge engine for all writes (overwrite mode)
        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(flussConfig)) {
            Table recoveryTable = recoveryConn.getTable(tablePath);

            // Create a temporary UpsertWriter from the recovery connection
            Upsert recoveryUpsert = recoveryTable.newUpsert();
            if (targetColumnIndexes != null) {
                recoveryUpsert = recoveryUpsert.partialUpdate(targetColumnIndexes);
            }
            UpsertWriter recoveryWriter = recoveryUpsert.createWriter();

            try {
                List<CompletableFuture<?>> futures = new ArrayList<>();

                for (Map.Entry<GenericRow, UndoOperation> entry : undoOperations.entrySet()) {
                    GenericRow primaryKey = entry.getKey();
                    UndoOperation undoOp = entry.getValue();

                    CompletableFuture<?> future;
                    if (undoOp.type == ChangeType.DELETE) {
                        // Delete by primary key - need to construct a full row with nulls for
                        // non-PK fields
                        // The delete operation only requires primary key values, other fields can
                        // be null
                        GenericRow fullRow = expandPrimaryKeyToFullRow(primaryKey);
                        future = recoveryWriter.delete(fullRow);
                    } else if (undoOp.type == ChangeType.INSERT) {
                        // Re-insert the row with the data from the undo operation
                        future = recoveryWriter.upsert(undoOp.row);
                    } else {
                        LOG.warn("Unexpected undo operation type: {}", undoOp.type);
                        continue;
                    }

                    futures.add(future);
                }

                // Wait for all operations to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

                // Flush to ensure all undo operations are persisted
                recoveryWriter.flush();

                LOG.info("Successfully applied all undo operations to bucket {}", bucket);
            } finally {
                recoveryWriter.close();
                recoveryTable.close();
            }
        }
    }

    /**
     * Extract primary key from a full row and return it as a GenericRow.
     *
     * <p>This method creates a new GenericRow containing only the primary key fields, which can be
     * used as a key in Map structures for deduplication purposes.
     *
     * <p>Uses pre-created FieldGetters for efficient value extraction without type switching.
     *
     * @param row the full row
     * @return a GenericRow containing only the primary key fields
     */
    private GenericRow extractPrimaryKey(InternalRow row) {
        GenericRow pkRow = new GenericRow(primaryKeyFieldGetters.length);
        for (int i = 0; i < primaryKeyFieldGetters.length; i++) {
            // Use FieldGetter to extract value efficiently
            // FieldGetter already handles null checking internally
            pkRow.setField(i, primaryKeyFieldGetters[i].getFieldOrNull(row));
        }
        return pkRow;
    }

    /**
     * Expand a primary key row to a full row with nulls for non-primary key fields.
     *
     * <p>This method creates a new GenericRow with the same field count as the table schema,
     * populating the primary key fields from the input and setting all other fields to null.
     *
     * @param primaryKey the primary key row
     * @return a full row with primary key values and nulls for other fields
     */
    private GenericRow expandPrimaryKeyToFullRow(GenericRow primaryKey) {
        int totalFieldCount = tableRowType.getFieldCount();
        GenericRow fullRow = new GenericRow(totalFieldCount);

        // Initialize all fields to null
        for (int i = 0; i < totalFieldCount; i++) {
            fullRow.setField(i, null);
        }

        // Set primary key fields
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            int fieldIndex = primaryKeyIndexes[i];
            fullRow.setField(fieldIndex, primaryKey.getField(i));
        }

        return fullRow;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
