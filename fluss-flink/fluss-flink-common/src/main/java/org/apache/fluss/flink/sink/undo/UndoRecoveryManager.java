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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.protocol.MergeMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Manages undo recovery operations during Flink sink writer initialization.
 *
 * <p>This manager ensures exactly-once semantics by reversing any writes that occurred after the
 * last successful checkpoint but before a failure. The recovery process involves:
 *
 * <ol>
 *   <li>Reading changelog records from checkpoint offset to current latest offset
 *   <li>Computing inverse operations for each affected primary key
 *   <li>Applying undo operations using OVERWRITE mode to restore bucket state
 * </ol>
 *
 * <p><b>Undo Logic:</b> For each primary key, only the first change after checkpoint determines the
 * undo action:
 *
 * <ul>
 *   <li>{@code INSERT} → Delete the row (it didn't exist at checkpoint)
 *   <li>{@code UPDATE_BEFORE} → Restore the old value (this was the state at checkpoint)
 *   <li>{@code UPDATE_AFTER} → Ignored (UPDATE_BEFORE already handled the undo)
 *   <li>{@code DELETE} → Re-insert the deleted row (it existed at checkpoint)
 * </ul>
 *
 * <p>This class delegates to specialized components:
 *
 * <ul>
 *   <li>{@link UndoComputer} - Undo operation computation using {@link KeyEncoder} for primary key
 *       encoding
 *   <li>{@link UndoRecoveryExecutor} - Changelog reading and write execution
 * </ul>
 *
 * @see MergeMode#OVERWRITE
 */
public class UndoRecoveryManager {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryManager.class);

    private final Table table;
    @Nullable private final int[] targetColumnIndexes;

    /**
     * Creates a new UndoRecoveryManager.
     *
     * @param table the Fluss table to perform recovery on
     * @param targetColumnIndexes optional target columns for partial update (null for full row)
     */
    public UndoRecoveryManager(Table table, @Nullable int[] targetColumnIndexes) {
        this.table = table;
        this.targetColumnIndexes = targetColumnIndexes;

        logPartialUpdateConfig(targetColumnIndexes);
    }

    private void logPartialUpdateConfig(@Nullable int[] targetColumnIndexes) {
        if (targetColumnIndexes != null) {
            LOG.info(
                    "Undo recovery configured with partial update columns: {}",
                    Arrays.toString(targetColumnIndexes));
        }
    }

    // ==================== Public API ====================

    /**
     * Performs undo recovery for buckets with known undo offsets.
     *
     * <p>The caller is responsible for providing both the checkpoint offset (start) and the log end
     * offset (end) for each bucket. This avoids redundant listOffset calls since the caller
     * typically already has this information from determining which buckets need recovery.
     *
     * @param bucketUndoOffsets map of bucket to its undo offsets (checkpointOffset, logEndOffset)
     * @param subtaskIndex the Flink subtask index (for logging)
     * @param parallelism the total parallelism (for logging)
     * @throws Exception if recovery fails
     */
    public void performUndoRecovery(
            Map<TableBucket, UndoOffsets> bucketUndoOffsets, int subtaskIndex, int parallelism)
            throws Exception {

        if (bucketUndoOffsets.isEmpty()) {
            LOG.debug("No buckets to recover on subtask {}/{}", subtaskIndex, parallelism);
            return;
        }

        LOG.info(
                "Starting undo recovery for {} bucket(s) on subtask {}/{}",
                bucketUndoOffsets.size(),
                subtaskIndex,
                parallelism);

        List<BucketRecoveryContext> contexts = buildRecoveryContexts(bucketUndoOffsets);

        try (LogScanner scanner = createLogScanner()) {
            Upsert recoveryUpsert = table.newUpsert().mergeMode(MergeMode.OVERWRITE);
            if (targetColumnIndexes != null) {
                recoveryUpsert = recoveryUpsert.partialUpdate(targetColumnIndexes);
            }
            UpsertWriter writer = recoveryUpsert.createWriter();

            // Create UndoComputer with writer for streaming execution
            Schema schema = table.getTableInfo().getSchema();
            // Use CompactedKeyEncoder directly instead of the deprecated KeyEncoder.of(),
            // which requires a lake format parameter that is not applicable here.
            KeyEncoder keyEncoder =
                    CompactedKeyEncoder.createKeyEncoder(
                            schema.getRowType(), schema.getPrimaryKey().get().getColumnNames());
            UndoComputer undoComputer = new UndoComputer(keyEncoder, writer);

            UndoRecoveryExecutor executor = new UndoRecoveryExecutor(scanner, writer, undoComputer);
            // This is a blocking operation that reads changelog and executes undo operations.
            // It may take a significant amount of time depending on the volume of records to
            // process.
            executor.execute(contexts);
        }

        // Calculate total undo operations for summary
        int totalUndoOps = 0;
        for (BucketRecoveryContext ctx : contexts) {
            totalUndoOps += ctx.getProcessedKeys().size();
        }

        LOG.info(
                "Completed undo recovery for {} bucket(s) with {} undo operation(s) on subtask {}/{}",
                bucketUndoOffsets.size(),
                totalUndoOps,
                subtaskIndex,
                parallelism);
    }

    /**
     * Encapsulates the offset information needed for undo recovery of a bucket.
     *
     * <p>This class holds the checkpoint offset (recovery start point) and log end offset (recovery
     * end point) for a bucket's undo recovery operation.
     */
    public static class UndoOffsets {
        private final long checkpointOffset;
        private final long logEndOffset;

        /**
         * Creates a new UndoOffsets.
         *
         * @param checkpointOffset the checkpoint offset (start point, inclusive)
         * @param logEndOffset the log end offset (end point, exclusive)
         * @throws IllegalArgumentException if offsets are negative or checkpointOffset >
         *     logEndOffset
         */
        public UndoOffsets(long checkpointOffset, long logEndOffset) {
            if (checkpointOffset < 0) {
                throw new IllegalArgumentException(
                        "checkpointOffset must be non-negative: " + checkpointOffset);
            }
            if (logEndOffset < 0) {
                throw new IllegalArgumentException(
                        "logEndOffset must be non-negative: " + logEndOffset);
            }
            if (checkpointOffset > logEndOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "checkpointOffset (%d) must not be greater than logEndOffset (%d)",
                                checkpointOffset, logEndOffset));
            }
            // Note: checkpointOffset == logEndOffset is allowed, it simply means no recovery needed
            this.checkpointOffset = checkpointOffset;
            this.logEndOffset = logEndOffset;
        }

        public long getCheckpointOffset() {
            return checkpointOffset;
        }

        public long getLogEndOffset() {
            return logEndOffset;
        }
    }

    // ==================== Scanner Factory ====================

    /**
     * Creates a LogScanner for reading changelog records.
     *
     * <p>This method is protected to allow test subclasses to inject custom scanners for fault
     * injection testing.
     *
     * @return a new LogScanner instance
     */
    protected LogScanner createLogScanner() {
        return table.newScan().createLogScanner();
    }

    // ==================== Recovery Context Building ====================

    private List<BucketRecoveryContext> buildRecoveryContexts(
            Map<TableBucket, UndoOffsets> bucketUndoOffsets) {

        List<BucketRecoveryContext> contexts = new ArrayList<>();

        for (Map.Entry<TableBucket, UndoOffsets> entry : bucketUndoOffsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            UndoOffsets offsets = entry.getValue();

            BucketRecoveryContext ctx =
                    new BucketRecoveryContext(
                            bucket, offsets.getCheckpointOffset(), offsets.getLogEndOffset());
            contexts.add(ctx);
        }

        return contexts;
    }
}
