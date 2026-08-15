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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Holds a scanned Arrow batch together with the log metadata of the batch.
 *
 * <p>This class only supports append-only log tables. CDC tables are not supported.
 *
 * <p>The caller must close this object after use in order to release the underlying Arrow memory.
 */
@PublicEvolving
public class ArrowBatchData implements AutoCloseable {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final long baseLogOffset;
    private final long timestamp;
    private final int schemaId;
    @Nullable private final byte[] changeTypes;
    private boolean closed;

    public ArrowBatchData(
            VectorSchemaRoot vectorSchemaRoot, long baseLogOffset, long timestamp, int schemaId) {
        this(vectorSchemaRoot, baseLogOffset, timestamp, schemaId, null);
    }

    ArrowBatchData(
            VectorSchemaRoot vectorSchemaRoot,
            long baseLogOffset,
            long timestamp,
            int schemaId,
            @Nullable byte[] changeTypes) {
        this.vectorSchemaRoot = checkNotNull(vectorSchemaRoot, "vectorSchemaRoot must not be null");
        this.baseLogOffset = baseLogOffset;
        this.timestamp = timestamp;
        this.schemaId = schemaId;
        checkArgument(
                changeTypes == null || changeTypes.length == vectorSchemaRoot.getRowCount(),
                "changeTypes length must match row count %s, but is %s",
                vectorSchemaRoot.getRowCount(),
                changeTypes == null ? 0 : changeTypes.length);
        this.changeTypes = changeTypes;
    }

    /** Returns the Arrow vectors of this batch. */
    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }

    /** Returns the schema id of this batch. */
    public int getSchemaId() {
        return schemaId;
    }

    /** Returns the base log offset of this batch. */
    public long getBaseLogOffset() {
        return baseLogOffset;
    }

    /** Returns the commit timestamp of this batch. */
    public long getTimestamp() {
        return timestamp;
    }

    /** Returns the number of rows in this batch. */
    public int getRecordCount() {
        return vectorSchemaRoot.getRowCount();
    }

    /** Returns whether every row in this batch is append-only. */
    public boolean isAppendOnly() {
        return changeTypes == null;
    }

    /**
     * Returns the stored change type for the given row.
     *
     * <p>Append-only batches do not materialize a change-type vector and return {@link
     * ChangeType#APPEND_ONLY} for every row.
     */
    public ChangeType getChangeType(int rowId) {
        checkArgument(
                rowId >= 0 && rowId < getRecordCount(),
                "rowId must be in [0, %s), but is %s",
                getRecordCount(),
                rowId);
        return changeTypes == null
                ? ChangeType.APPEND_ONLY
                : ChangeType.fromByteValue(changeTypes[rowId]);
    }

    /**
     * Returns the stored per-row change-type vector as a read-only buffer.
     *
     * <p>The buffer contains one {@link ChangeType#toByteValue() encoded byte} for every row and is
     * absent for append-only batches. The returned buffer remains valid for the lifetime of this
     * object.
     */
    public Optional<ByteBuffer> getChangeTypes() {
        return changeTypes == null
                ? Optional.empty()
                : Optional.of(ByteBuffer.wrap(changeTypes).asReadOnlyBuffer());
    }

    /** Returns the total size in bytes of the underlying Arrow buffers. */
    public long getSizeInBytes() {
        long size = 0;
        for (FieldVector vector : vectorSchemaRoot.getFieldVectors()) {
            for (ArrowBuf buf : vector.getBuffers(false)) {
                size += buf.readableBytes();
            }
        }
        if (changeTypes != null) {
            size += changeTypes.length;
        }
        return size;
    }

    /**
     * Creates a new {@link ArrowBatchData} containing a contiguous slice of this batch's rows and
     * releases the original vector data.
     *
     * <p>After this method returns, the original {@link ArrowBatchData} instance MUST NOT be used
     * or closed. The caller is responsible for closing the returned instance.
     *
     * @param skipRows the number of leading rows to skip
     * @return a new {@link ArrowBatchData} containing the remaining rows after skipping
     */
    public ArrowBatchData sliceAndTransferOwnership(int skipRows) {
        checkArgument(skipRows >= 0, "skipRows must be >= 0, but is %s", skipRows);
        checkArgument(
                skipRows < getRecordCount(),
                "skipRows(%s) must be < recordCount(%s)",
                skipRows,
                getRecordCount());
        int remainingRows = getRecordCount() - skipRows;
        VectorSchemaRoot slicedRoot = vectorSchemaRoot.slice(skipRows, remainingRows);
        byte[] slicedChangeTypes =
                changeTypes == null
                        ? null
                        : Arrays.copyOfRange(changeTypes, skipRows, changeTypes.length);
        // release original vector buffers; sliced vectors hold independent copies
        close();
        return new ArrowBatchData(
                slicedRoot, baseLogOffset + skipRows, timestamp, schemaId, slicedChangeTypes);
    }

    /**
     * Creates a new {@link ArrowBatchData} containing only the first {@code rowCount} rows and
     * releases the original vector data.
     *
     * <p>After this method returns, the original {@link ArrowBatchData} instance MUST NOT be used
     * or closed. The caller is responsible for closing the returned instance.
     *
     * @param rowCount the number of leading rows to keep
     * @return a new {@link ArrowBatchData} containing the first {@code rowCount} rows
     */
    public ArrowBatchData truncateAndTransferOwnership(int rowCount) {
        checkArgument(rowCount > 0, "rowCount must be > 0, but is %s", rowCount);
        checkArgument(
                rowCount <= getRecordCount(),
                "rowCount(%s) must be <= recordCount(%s)",
                rowCount,
                getRecordCount());
        VectorSchemaRoot slicedRoot = vectorSchemaRoot.slice(0, rowCount);
        byte[] slicedChangeTypes =
                changeTypes == null ? null : Arrays.copyOfRange(changeTypes, 0, rowCount);
        close();
        return new ArrowBatchData(
                slicedRoot, baseLogOffset, timestamp, schemaId, slicedChangeTypes);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            vectorSchemaRoot.close();
        }
    }
}
