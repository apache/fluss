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

import org.apache.fluss.annotation.Internal;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Holds a scanned Arrow batch together with the log metadata of the batch.
 *
 * <p>This class only supports append-only log tables. CDC tables are not supported.
 *
 * <p>The caller must close this object after use in order to release the underlying Arrow memory.
 */
@Internal
public class ArrowBatchData implements AutoCloseable {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final long baseLogOffset;
    private final long timestamp;
    private final int schemaId;
    private boolean closed;

    public ArrowBatchData(
            VectorSchemaRoot vectorSchemaRoot, long baseLogOffset, long timestamp, int schemaId) {
        this.vectorSchemaRoot = checkNotNull(vectorSchemaRoot, "vectorSchemaRoot must not be null");
        this.baseLogOffset = baseLogOffset;
        this.timestamp = timestamp;
        this.schemaId = schemaId;
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

    /** Returns the total size in bytes of the underlying Arrow buffers. */
    public long getSizeInBytes() {
        long size = 0;
        for (FieldVector vector : vectorSchemaRoot.getFieldVectors()) {
            for (ArrowBuf buf : vector.getBuffers(false)) {
                size += buf.readableBytes();
            }
        }
        return size;
    }

    /**
     * Returns the maximum non-null timestamp value of the given field in epoch milliseconds, or
     * {@code null} if the field is not a timestamp field or all values are null.
     */
    @Nullable
    public Long getMaxTimestampMillis(int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= vectorSchemaRoot.getFieldVectors().size()) {
            return null;
        }
        FieldVector vector = vectorSchemaRoot.getVector(fieldIndex);
        if (!(vector instanceof TimeStampVector)) {
            return null;
        }

        Long maxTimestampMillis = null;
        int rowCount = getRecordCount();
        for (int rowId = 0; rowId < rowCount; rowId++) {
            if (vector.isNull(rowId)) {
                continue;
            }
            Long timestampMillis = getTimestampMillis((TimeStampVector) vector, rowId);
            if (timestampMillis == null) {
                return null;
            }
            maxTimestampMillis =
                    maxTimestampMillis == null
                            ? timestampMillis
                            : Math.max(maxTimestampMillis, timestampMillis);
        }
        return maxTimestampMillis;
    }

    @Nullable
    private static Long getTimestampMillis(TimeStampVector vector, int rowId) {
        if (vector instanceof TimeStampSecVector) {
            return vector.get(rowId) * 1000;
        } else if (vector instanceof TimeStampMilliVector) {
            return vector.get(rowId);
        } else if (vector instanceof TimeStampMicroVector) {
            return vector.get(rowId) / 1000;
        } else if (vector instanceof TimeStampNanoVector) {
            return vector.get(rowId) / 1_000_000;
        } else {
            return null;
        }
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
        // release original vector buffers; sliced vectors hold independent copies
        close();
        return new ArrowBatchData(slicedRoot, baseLogOffset + skipRows, timestamp, schemaId);
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
        close();
        return new ArrowBatchData(slicedRoot, baseLogOffset, timestamp, schemaId);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            vectorSchemaRoot.close();
        }
    }
}
