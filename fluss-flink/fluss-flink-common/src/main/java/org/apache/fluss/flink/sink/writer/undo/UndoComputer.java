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

package org.apache.fluss.flink.sink.writer.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.ByteArrayWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Computes and executes undo operations from changelog records using streaming execution.
 *
 * <p>This class uses a {@link KeyEncoder} to encode primary keys as byte arrays for efficient
 * deduplication, and executes undo operations immediately via {@link UpsertWriter}. The undo logic
 * is:
 *
 * <ul>
 *   <li>{@code INSERT} → Delete the row (it didn't exist at checkpoint)
 *   <li>{@code UPDATE_BEFORE} → Restore the old value (this was the state at checkpoint)
 *   <li>{@code UPDATE_AFTER} → Ignored (UPDATE_BEFORE already handled the undo)
 *   <li>{@code DELETE} → Re-insert the deleted row (it existed at checkpoint)
 * </ul>
 *
 * <p>For partial update mode, two writers are used:
 *
 * <ul>
 *   <li>{@code partialWriter} - for UPDATE_BEFORE and DELETE undo (restore/re-insert partial
 *       columns)
 *   <li>{@code deleteWriter} - for INSERT undo (delete entire row, not just partial columns)
 * </ul>
 *
 * <p>For each primary key, only the first change after checkpoint determines the undo action. The
 * original row from {@link ScanRecord} is directly used without copying, as each ScanRecord
 * contains an independent row instance.
 */
public class UndoComputer {

    private static final Logger LOG = LoggerFactory.getLogger(UndoComputer.class);

    private final KeyEncoder keyEncoder;
    private final UpsertWriter writer;
    @Nullable private final UpsertWriter deleteWriter;

    // Track which writer was last used to know when to flush.
    // This state should be reset before processing each new bucket via resetWriterState().
    private boolean lastUsedDeleteWriter = false;

    /**
     * Creates an UndoComputer for full row mode.
     *
     * @param keyEncoder the key encoder for primary key deduplication
     * @param writer the writer for all undo operations
     */
    public UndoComputer(KeyEncoder keyEncoder, UpsertWriter writer) {
        this(keyEncoder, writer, null);
    }

    /**
     * Creates an UndoComputer with optional separate delete writer for partial update mode.
     *
     * <p>In partial update mode, INSERT undo requires deleting the entire row (not just partial
     * columns), so a separate full-row delete writer is needed.
     *
     * @param keyEncoder the key encoder for primary key deduplication
     * @param writer the writer for UPDATE_BEFORE and DELETE undo operations
     * @param deleteWriter optional separate writer for INSERT undo (delete entire row), null for
     *     full row mode
     */
    public UndoComputer(
            KeyEncoder keyEncoder, UpsertWriter writer, @Nullable UpsertWriter deleteWriter) {
        this.keyEncoder = keyEncoder;
        this.writer = writer;
        this.deleteWriter = deleteWriter;
    }

    /**
     * Resets the writer state before processing a new bucket.
     *
     * <p>This ensures that the lastUsedDeleteWriter flag is reset, preventing incorrect flush
     * behavior when switching between buckets.
     */
    public void resetWriterState() {
        this.lastUsedDeleteWriter = false;
    }

    /**
     * Processes a changelog record and executes the undo operation immediately.
     *
     * <p>Only the first change for each primary key triggers an undo operation.
     *
     * @param record the changelog record
     * @param processedKeys set of already processed primary keys for deduplication
     * @return CompletableFuture for the async write, or null if skipped
     */
    @Nullable
    public CompletableFuture<?> processRecord(
            ScanRecord record, Set<ByteArrayWrapper> processedKeys) {
        byte[] encodedKey = keyEncoder.encodeKey(record.getRow());
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(encodedKey);

        // Skip if we already processed this key
        if (processedKeys.contains(keyWrapper)) {
            return null;
        }

        CompletableFuture<?> future = computeAndExecuteUndo(record);
        if (future != null) {
            processedKeys.add(keyWrapper);
        }
        return future;
    }

    /**
     * Computes and executes the undo operation for a changelog record.
     *
     * <p>UPDATE_AFTER is ignored because UPDATE_BEFORE and UPDATE_AFTER always come in pairs, with
     * UPDATE_BEFORE appearing first. The undo logic is determined by UPDATE_BEFORE which contains
     * the old value needed for restoration.
     *
     * <p>The original row is directly used without copying because:
     *
     * <ul>
     *   <li>Each ScanRecord contains an independent GenericRow instance (created in
     *       CompletedFetch.toScanRecord)
     *   <li>For DELETE operations, UpsertWriter.delete() only needs the primary key fields
     *   <li>For UPSERT operations, the original row contains the exact data to restore
     * </ul>
     *
     * <p>For partial update mode, INSERT undo uses the separate deleteWriter (if provided) to
     * delete the entire row, not just the partial columns. Before switching between writers, the
     * previous writer is flushed to avoid mixing records with different target columns in the same
     * batch.
     *
     * @param record the changelog record
     * @return CompletableFuture for the async write, or null if no action needed (e.g.,
     *     UPDATE_AFTER)
     */
    @Nullable
    private CompletableFuture<?> computeAndExecuteUndo(ScanRecord record) {
        ChangeType changeType = record.getChangeType();
        InternalRow row = record.getRow();

        switch (changeType) {
            case INSERT:
                // Row was inserted after checkpoint → delete it
                // Use deleteWriter if available (for partial update mode to delete entire row)
                if (deleteWriter != null) {
                    // Flush partial writer before using delete writer to avoid mixing
                    // records with different target columns in the same batch
                    if (!lastUsedDeleteWriter) {
                        writer.flush();
                        lastUsedDeleteWriter = true;
                    }
                    return deleteWriter.delete(row);
                }
                return writer.delete(row);

            case UPDATE_BEFORE:
                // Row was updated after checkpoint → restore old value
                // Flush delete writer before using partial writer
                if (deleteWriter != null && lastUsedDeleteWriter) {
                    deleteWriter.flush();
                    lastUsedDeleteWriter = false;
                }
                return writer.upsert(row);

            case UPDATE_AFTER:
                // Ignored: UPDATE_BEFORE already handled the undo logic for this key.
                return null;

            case DELETE:
                // Row was deleted after checkpoint → re-insert it
                // Flush delete writer before using partial writer
                if (deleteWriter != null && lastUsedDeleteWriter) {
                    deleteWriter.flush();
                    lastUsedDeleteWriter = false;
                }
                return writer.upsert(row);

            default:
                LOG.warn("Unexpected change type for undo: {}", changeType);
                return null;
        }
    }
}
