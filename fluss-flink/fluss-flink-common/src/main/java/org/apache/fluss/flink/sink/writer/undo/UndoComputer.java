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
 * <p>For each primary key, only the first change after checkpoint determines the undo action. The
 * original row from {@link ScanRecord} is directly used without copying, as each ScanRecord
 * contains an independent row instance.
 */
public class UndoComputer {

    private static final Logger LOG = LoggerFactory.getLogger(UndoComputer.class);

    private final KeyEncoder keyEncoder;
    private final UpsertWriter writer;

    /**
     * Creates an UndoComputer.
     *
     * @param keyEncoder the key encoder for primary key deduplication
     * @param writer the writer for all undo operations
     */
    public UndoComputer(KeyEncoder keyEncoder, UpsertWriter writer) {
        this.keyEncoder = keyEncoder;
        this.writer = writer;
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
        // Skip UPDATE_AFTER before key encoding — UPDATE_BEFORE already handled the undo
        if (record.getChangeType() == ChangeType.UPDATE_AFTER) {
            return null;
        }

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
                return writer.delete(row);

            case UPDATE_BEFORE:
                // Row was updated after checkpoint → restore old value
                return writer.upsert(row);

            case UPDATE_AFTER:
                // Ignored: UPDATE_BEFORE already handled the undo logic for this key.
                return null;

            case DELETE:
                // Row was deleted after checkpoint → re-insert it
                return writer.upsert(row);

            default:
                LOG.warn("Unexpected change type for undo: {}", changeType);
                return null;
        }
    }
}
