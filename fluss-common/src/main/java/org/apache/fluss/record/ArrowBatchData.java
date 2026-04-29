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

import org.apache.arrow.vector.VectorSchemaRoot;

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
        vectorSchemaRoot.close();
        return new ArrowBatchData(slicedRoot, baseLogOffset + skipRows, timestamp, schemaId);
    }

    @Override
    public void close() {
        vectorSchemaRoot.close();
    }
}
