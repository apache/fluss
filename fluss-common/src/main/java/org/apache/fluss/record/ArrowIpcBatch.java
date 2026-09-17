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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Serialized Arrow data and the metadata needed to read a range of log records.
 *
 * <p>The schema and record batch are encapsulated Arrow IPC messages, without the Fluss log header
 * or change-type sidecar. The payload retains its physical schema and all its rows. Consumers must
 * apply {@link #getColumnMapping()}, {@link #getRowOffset()} and {@link #getRecordCount()} to
 * produce the requested logical result. The optional {@code fluss-client-arrow} module performs
 * these operations for Arrow Java users.
 *
 * <p>This immutable object owns heap-backed bytes. Every buffer returned is read-only, has an
 * independent position, and remains valid after subsequent polls and after the scanner closes. No
 * close or Arrow Java dependency is required.
 */
@PublicEvolving
public final class ArrowIpcBatch {
    private final byte[] schema;
    private final byte[] recordBatch;
    private final byte[] outputSchema;
    @Nullable private final int[] columnMapping;
    private final long baseLogOffset;
    private final long timestamp;
    private final int schemaId;
    private final int rowOffset;
    private final int recordCount;
    @Nullable private final byte[] changeTypes;

    // Takes ownership of arrays created by the read context; slices share these immutable arrays.
    ArrowIpcBatch(
            byte[] schema,
            byte[] recordBatch,
            byte[] outputSchema,
            @Nullable int[] columnMapping,
            long baseLogOffset,
            long timestamp,
            int schemaId,
            int rowOffset,
            int recordCount,
            @Nullable byte[] changeTypes) {
        this.schema = schema;
        this.recordBatch = recordBatch;
        this.outputSchema = outputSchema;
        this.columnMapping = columnMapping;
        this.baseLogOffset = baseLogOffset;
        this.timestamp = timestamp;
        this.schemaId = schemaId;
        this.rowOffset = rowOffset;
        this.recordCount = recordCount;
        this.changeTypes = changeTypes;
    }

    /** Returns the IPC schema message describing the physical record-batch payload. */
    public ByteBuffer getSchema() {
        return ByteBuffer.wrap(schema).asReadOnlyBuffer();
    }

    /**
     * Returns the IPC record-batch message, including its metadata and possibly compressed body.
     * This is not a complete IPC stream: its schema is supplied separately by {@link #getSchema()}.
     */
    public ByteBuffer getRecordBatch() {
        return ByteBuffer.wrap(recordBatch).asReadOnlyBuffer();
    }

    /** Returns the IPC schema message for the requested output after projection and evolution. */
    public ByteBuffer getOutputSchema() {
        return ByteBuffer.wrap(outputSchema).asReadOnlyBuffer();
    }

    /**
     * Returns the mapping from output columns to physical columns, or null for identity. A value of
     * -1 means an output column must be filled with nulls. The returned array is a copy.
     */
    @Nullable
    public int[] getColumnMapping() {
        return columnMapping == null ? null : columnMapping.clone();
    }

    /** Returns the first logical row's index in the physical Arrow payload. */
    public int getRowOffset() {
        return rowOffset;
    }

    /** Returns the number of logical rows to read starting at {@link #getRowOffset()}. */
    public int getRecordCount() {
        return recordCount;
    }

    /** Returns the write-time schema id; the physical schema also reflects server projection. */
    public int getSchemaId() {
        return schemaId;
    }

    /** Returns the log offset of the first logical row in this batch. */
    public long getBaseLogOffset() {
        return baseLogOffset + rowOffset;
    }

    /** Returns the commit timestamp of this batch. */
    public long getTimestamp() {
        return timestamp;
    }

    /** Returns whether this batch contains append-only records. */
    public boolean isAppendOnly() {
        return changeTypes == null;
    }

    /** Returns the change type of a logical row, or APPEND_ONLY for append-only tables. */
    public ChangeType getChangeType(int rowId) {
        checkArgument(
                rowId >= 0 && rowId < recordCount,
                "rowId must be in [0, %s), but is %s",
                recordCount,
                rowId);
        return changeTypes == null
                ? ChangeType.APPEND_ONLY
                : ChangeType.fromByteValue(changeTypes[rowOffset + rowId]);
    }

    /** Returns one encoded change-type byte per logical row, absent for append-only batches. */
    public Optional<ByteBuffer> getChangeTypes() {
        return changeTypes == null
                ? Optional.empty()
                : Optional.of(
                        ByteBuffer.wrap(changeTypes, rowOffset, recordCount)
                                .slice()
                                .asReadOnlyBuffer());
    }

    /**
     * Returns the retained serialized payload and sidecar size, including rows outside the slice.
     */
    public long getSizeInBytes() {
        return (long) recordBatch.length + (changeTypes == null ? 0 : changeTypes.length);
    }

    /** Returns an immutable view of a logical row range, sharing this batch's serialized bytes. */
    public ArrowIpcBatch slice(int fromIndex, int length) {
        checkArgument(
                fromIndex >= 0 && length >= 0 && fromIndex <= recordCount - length,
                "Invalid row range [%s, %s) for %s records",
                fromIndex,
                (long) fromIndex + length,
                recordCount);
        return new ArrowIpcBatch(
                schema,
                recordBatch,
                outputSchema,
                columnMapping,
                baseLogOffset,
                timestamp,
                schemaId,
                rowOffset + fromIndex,
                length,
                changeTypes);
    }
}
