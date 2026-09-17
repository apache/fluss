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

package org.apache.fluss.client.arrow;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.record.ArrowIpcBatch;
import org.apache.fluss.record.ChangeType;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Decoded Arrow vectors and their Fluss log metadata.
 *
 * <p>Close this batch before closing the allocator passed to {@link ArrowBatchReader#read}. Closing
 * the scanner or the reader's input does not invalidate these vectors. Closing this batch releases
 * its vectors and never closes the caller's allocator.
 */
@PublicEvolving
public final class ArrowBatch implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final ArrowIpcBatch metadata;
    private boolean closed;

    ArrowBatch(VectorSchemaRoot root, ArrowIpcBatch metadata) {
        this.root = root;
        this.metadata = metadata;
    }

    /** Returns the vectors in the requested output column order and logical row range. */
    public VectorSchemaRoot getVectorSchemaRoot() {
        return root;
    }

    /** Returns the batch's write-time schema id. */
    public int getSchemaId() {
        return metadata.getSchemaId();
    }

    /** Returns the first returned row's log offset. */
    public long getBaseLogOffset() {
        return metadata.getBaseLogOffset();
    }

    /** Returns the batch's commit timestamp. */
    public long getTimestamp() {
        return metadata.getTimestamp();
    }

    /** Returns the number of rows. */
    public int getRecordCount() {
        return metadata.getRecordCount();
    }

    /** Returns whether this batch contains append-only records. */
    public boolean isAppendOnly() {
        return metadata.isAppendOnly();
    }

    /** Returns the change type of the given row in the decoded vectors. */
    public ChangeType getChangeType(int rowId) {
        return metadata.getChangeType(rowId);
    }

    /** Returns the read-only change-type sidecar, absent for append-only tables. */
    public Optional<ByteBuffer> getChangeTypes() {
        return metadata.getChangeTypes();
    }

    /** Releases the decoded vectors. Repeated calls have no effect. */
    @Override
    public void close() {
        if (!closed) {
            root.close();
            closed = true;
        }
    }
}
