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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Holds a scanned Arrow batch together with the log metadata of the batch.
 *
 * <p>The caller must close this object after use in order to release the underlying Arrow memory.
 *
 * @since 0.10
 */
@PublicEvolving
public class ArrowBatchData implements AutoCloseable {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final BufferAllocator allocator;
    @Nullable private final ChangeType[] changeTypes;
    private final long baseLogOffset;
    private final long timestamp;
    private final int schemaId;

    public ArrowBatchData(
            VectorSchemaRoot vectorSchemaRoot,
            BufferAllocator allocator,
            @Nullable ChangeType[] changeTypes,
            long baseLogOffset,
            long timestamp,
            int schemaId) {
        this.vectorSchemaRoot = checkNotNull(vectorSchemaRoot, "vectorSchemaRoot must not be null");
        this.allocator = checkNotNull(allocator, "allocator must not be null");
        this.changeTypes = changeTypes;
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

    /** Returns whether this batch is append-only. */
    public boolean isAppendOnly() {
        return changeTypes == null;
    }

    /** Returns the change type of the given row. */
    public ChangeType getChangeType(int rowId) {
        validateRowId(rowId);
        if (changeTypes == null) {
            return ChangeType.APPEND_ONLY;
        }
        return changeTypes[rowId];
    }

    /** Returns the log offset of the given row. */
    public long getLogOffset(int rowId) {
        validateRowId(rowId);
        return baseLogOffset + rowId;
    }

    private void validateRowId(int rowId) {
        checkArgument(
                rowId >= 0 && rowId < getRecordCount(),
                "rowId must be in [0, %s), but is %s",
                getRecordCount(),
                rowId);
    }

    @Override
    public void close() {
        try {
            vectorSchemaRoot.close();
        } finally {
            allocator.close();
        }
    }
}
