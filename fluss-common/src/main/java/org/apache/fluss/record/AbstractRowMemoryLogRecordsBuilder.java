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

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.utils.crc.Crc32C;

import java.io.IOException;

import static org.apache.fluss.record.DefaultLogRecordBatch.BASE_OFFSET_LENGTH;
import static org.apache.fluss.record.DefaultLogRecordBatch.CRC_OFFSET;
import static org.apache.fluss.record.DefaultLogRecordBatch.LAST_OFFSET_DELTA_OFFSET;
import static org.apache.fluss.record.DefaultLogRecordBatch.LENGTH_LENGTH;
import static org.apache.fluss.record.DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.DefaultLogRecordBatch.SCHEMA_ID_OFFSET;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Abstract base builder for row-based MemoryLogRecords builders sharing common logic. */
abstract class AbstractRowMemoryLogRecordsBuilder<T> implements AutoCloseable {
    protected static final int BUILDER_DEFAULT_OFFSET = 0;

    protected final long baseLogOffset;
    protected final int schemaId;
    protected final int writeLimit;
    protected final byte magic;
    protected final AbstractPagedOutputView pagedOutputView;
    protected final MemorySegment firstSegment;
    protected final boolean appendOnly;

    private BytesView builtBuffer = null;
    private long writerId;
    private int batchSequence;
    private int currentRecordNumber;
    private int sizeInBytes;
    private volatile boolean isClosed;
    private boolean aborted = false;

    protected AbstractRowMemoryLogRecordsBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly) {
        this.appendOnly = appendOnly;
        checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.baseLogOffset = baseLogOffset;
        this.schemaId = schemaId;
        this.writeLimit = writeLimit;
        this.magic = magic;
        this.pagedOutputView = pagedOutputView;
        this.firstSegment = pagedOutputView.getCurrentSegment();
        this.writerId = NO_WRITER_ID;
        this.batchSequence = NO_BATCH_SEQUENCE;
        this.currentRecordNumber = 0;
        this.isClosed = false;

        // Skip header initially; will be written in build()
        this.pagedOutputView.setPosition(RECORD_BATCH_HEADER_SIZE);
        this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
    }

    protected AbstractRowMemoryLogRecordsBuilder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView, boolean appendOnly) {
        this(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                writeLimit,
                CURRENT_LOG_MAGIC_VALUE,
                outputView,
                appendOnly);
    }

    /** Implement to return size of the record (including length field). */
    protected abstract int sizeOf(T row);

    /** Implement to write the record and return total written bytes including length field. */
    protected abstract int writeRecord(ChangeType changeType, T row) throws IOException;

    public boolean hasRoomFor(T row) {
        return sizeInBytes + sizeOf(row) <= writeLimit;
    }

    public void append(ChangeType changeType, T row) throws Exception {
        appendRecord(changeType, row);
    }

    private void appendRecord(ChangeType changeType, T row) throws IOException {
        if (aborted) {
            throw new IllegalStateException(
                    "Tried to append a record, but "
                            + getClass().getSimpleName()
                            + " has already been aborted");
        }
        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsBuilder is closed for record appends");
        }
        if (appendOnly && changeType != ChangeType.APPEND_ONLY) {
            throw new IllegalArgumentException(
                    "Only append-only change type is allowed for append-only row log builder, but got "
                            + changeType);
        }

        int recordByteSizes = writeRecord(changeType, row);
        currentRecordNumber++;
        sizeInBytes += recordByteSizes;
    }

    public BytesView build() throws IOException {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        if (builtBuffer != null) {
            return builtBuffer;
        }
        writeBatchHeader();
        builtBuffer =
                MultiBytesView.builder()
                        .addMemorySegmentByteViewList(pagedOutputView.getWrittenSegments())
                        .build();
        return builtBuffer;
    }

    public void setWriterState(long writerId, int batchBaseSequence) {
        this.writerId = writerId;
        this.batchSequence = batchBaseSequence;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        // trigger to rewrite batch header
        this.builtBuffer = null;
        this.writerId = writerId;
        this.batchSequence = batchSequence;
    }

    public long writerId() {
        return writerId;
    }

    public int batchSequence() {
        return batchSequence;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void abort() {
        aborted = true;
    }

    @Override
    public void close() throws IOException {
        if (aborted) {
            throw new IllegalStateException(
                    "Cannot close "
                            + getClass().getSimpleName()
                            + " as it has already been aborted");
        }
        isClosed = true;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    // ----------------------- internal methods -------------------------------
    private void writeBatchHeader() throws IOException {
        // pagedOutputView doesn't support seek to previous segment,
        // so we create a new output view on the first segment
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(firstSegment);
        outputView.setPosition(0);
        // update header.
        outputView.writeLong(baseLogOffset);
        outputView.writeInt(sizeInBytes - BASE_OFFSET_LENGTH - LENGTH_LENGTH);
        outputView.writeByte(magic);

        // write empty timestamp which will be overridden on server side
        outputView.writeLong(0);
        // write empty crc first.
        outputView.writeUnsignedInt(0);

        outputView.writeShort((short) schemaId);
        // write attributes (currently only appendOnly flag)
        outputView.writeBoolean(appendOnly);
        // skip write attribute byte for now.
        outputView.setPosition(LAST_OFFSET_DELTA_OFFSET);
        if (currentRecordNumber > 0) {
            outputView.writeInt(currentRecordNumber - 1);
        } else {
            // If there is no record, we write 0 for filed lastOffsetDelta
            outputView.writeInt(0);
        }
        outputView.writeLong(writerId);
        outputView.writeInt(batchSequence);
        outputView.writeInt(currentRecordNumber);

        // Update crc.
        long crc = Crc32C.compute(pagedOutputView.getWrittenSegments(), SCHEMA_ID_OFFSET);
        outputView.setPosition(CRC_OFFSET);
        outputView.writeUnsignedInt(crc);
    }
}
