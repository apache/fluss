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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.Preconditions;
import com.alibaba.fluss.utils.crc.Crc32C;

import java.io.IOException;

import static com.alibaba.fluss.record.MemoryLogRecordBatch.BASE_OFFSET_LENGTH;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.CRC_OFFSET;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.LENGTH_LENGTH;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.SCHEMA_ID_OFFSET;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.WRITE_CLIENT_ID_OFFSET;
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
    private final MemorySegmentOutputView outputView;

    private long writerId;
    private int batchSequence;
    private int currentRecordNumber;
    private int sizeInBytes;
    private boolean isClosed;
    private MemoryLogRecords builtRecords;

    private MemoryLogRecordsIndexedBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            MemorySegmentOutputView outputView)
            throws IOException {
        Preconditions.checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.baseLogOffset = baseLogOffset;
        this.schemaId = schemaId;
        this.writeLimit = writeLimit;
        this.magic = magic;
        this.outputView = outputView;
        this.writerId = NO_WRITER_ID;
        this.batchSequence = NO_BATCH_SEQUENCE;
        this.currentRecordNumber = 0;
        this.isClosed = false;

        // We don't need to write header information while the builder creating,
        // we'll skip it first.
        outputView.setPosition(RECORD_BATCH_HEADER_SIZE);
        this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
    }

    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId, int writeLimit, MemorySegment segment) throws IOException {
        return new MemoryLogRecordsIndexedBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                writeLimit,
                CURRENT_LOG_MAGIC_VALUE,
                new MemorySegmentOutputView(segment));
    }

    public static MemoryLogRecordsIndexedBuilder builder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            MemorySegmentOutputView outputView)
            throws IOException {
        return new MemoryLogRecordsIndexedBuilder(
                baseLogOffset, schemaId, writeLimit, magic, outputView);
    }

    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId, MemorySegmentOutputView outputView) throws IOException {
        return new MemoryLogRecordsIndexedBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                Integer.MAX_VALUE,
                CURRENT_LOG_MAGIC_VALUE,
                outputView);
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

        int recordByteSizes = DefaultLogRecord.writeTo(outputView, rowKind, row);
        currentRecordNumber++;
        sizeInBytes += recordByteSizes;
    }

    public MemoryLogRecords build() throws IOException {
        if (builtRecords != null) {
            return builtRecords;
        }

        writeBatchHeader();
        MemorySegment segment = outputView.getMemorySegment();
        builtRecords = MemoryLogRecords.pointToMemory(segment, 0, sizeInBytes);
        return builtRecords;
    }

    public void setWriterState(long writerId, int batchBaseSequence) {
        this.writerId = writerId;
        this.batchSequence = batchBaseSequence;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        // trigger to rewrite batch header
        this.builtRecords = null;
        this.writerId = writerId;
        this.batchSequence = batchSequence;
    }

    public MemorySegment getMemorySegment() {
        return outputView.getMemorySegment();
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
        int originPos = outputView.getPosition();
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
        outputView.setPosition(CRC_OFFSET);
        long crc =
                Crc32C.compute(
                        outputView.getSharedBuffer(),
                        SCHEMA_ID_OFFSET,
                        sizeInBytes - SCHEMA_ID_OFFSET);
        outputView.writeUnsignedInt(crc);

        // reset the position to origin position.
        outputView.setPosition(originPos);
    }
}
