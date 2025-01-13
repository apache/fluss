/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.Preconditions;
import com.alibaba.fluss.utils.crc.Crc32C;

import java.io.IOException;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.BASE_OFFSET_LENGTH;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.CRC_OFFSET;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.LENGTH_LENGTH;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.SCHEMA_ID_OFFSET;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.WRITE_CLIENT_ID_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;

/**
 * Default builder for {@link MemoryLogRecords} of log records in {@link LogFormat#INDEXED} format.
 */
public class MemoryLogRecordsIndexedBuilder implements AutoCloseable {
    private static final int BUILDER_DEFAULT_OFFSET = 0;

    private final long baseLogOffset;
    private final int schemaId;
    // The max bytes can be appended.
    private final int writeLimit;
    private final byte magic;
    private final AbstractPagedOutputView pagedOutputView;
    private final MemorySegment firstSegment;

    private BytesView builtBuffer = null;
    private long writerId;
    private int batchSequence;
    private int currentRecordNumber;
    private int sizeInBytes;
    private boolean isClosed;

    private MemoryLogRecordsIndexedBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView pagedOutputView) {
        Preconditions.checkArgument(
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

        // We don't need to write header information while the builder creating,
        // we'll skip it first.
        this.pagedOutputView.setPosition(RECORD_BATCH_HEADER_SIZE);
        this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
    }

    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView) {
        return new MemoryLogRecordsIndexedBuilder(
                BUILDER_DEFAULT_OFFSET, schemaId, writeLimit, CURRENT_LOG_MAGIC_VALUE, outputView);
    }

    @VisibleForTesting
    public static MemoryLogRecordsIndexedBuilder builder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView outputView)
            throws IOException {
        return new MemoryLogRecordsIndexedBuilder(
                baseLogOffset, schemaId, writeLimit, magic, outputView);
    }

    /**
     * Check if we have room for a new record containing the given row. If no records have been
     * appended, then this returns true.
     */
    public boolean hasRoomFor(InternalRow row) {
        return sizeInBytes + DefaultLogRecord.sizeOf(row) <= writeLimit;
    }

    public void append(RowKind rowKind, InternalRow row) throws Exception {
        appendRecord(rowKind, row);
    }

    private void appendRecord(RowKind rowKind, InternalRow row) throws IOException {
        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsBuilder is closed for record appends");
        }

        int recordByteSizes = DefaultLogRecord.writeTo(pagedOutputView, rowKind, row);
        currentRecordNumber++;
        sizeInBytes += recordByteSizes;
    }

    public BytesView build() throws IOException {
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

    @Override
    public void close() throws IOException {
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
        // skip write attribute byte for now.
        outputView.setPosition(WRITE_CLIENT_ID_OFFSET);
        outputView.writeLong(writerId);
        outputView.writeInt(batchSequence);
        outputView.writeInt(currentRecordNumber);

        // Update crc.
        long crc = Crc32C.compute(pagedOutputView.getWrittenSegments(), SCHEMA_ID_OFFSET);
        outputView.setPosition(CRC_OFFSET);
        outputView.writeUnsignedInt(crc);
    }
}
