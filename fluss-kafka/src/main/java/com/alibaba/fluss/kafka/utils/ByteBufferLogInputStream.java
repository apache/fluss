/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.kafka.utils;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.record.DefaultLogRecordBatch;
import com.alibaba.fluss.record.LogInputStream;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.utils.crc.Crc32C;

import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.BASE_OFFSET_LENGTH;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.CRC_OFFSET;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.LENGTH_LENGTH;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.SCHEMA_ID_OFFSET;
import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class ByteBufferLogInputStream implements LogInputStream<LogRecordBatch> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public LogRecordBatch nextBatch() throws IOException {
        int remaining = buffer.remaining();
        Integer batchSize = nextBatchSize();
        if (batchSize == null || remaining < batchSize) {
            return null;
        }
        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        ByteBuffer batchSlice = buffer.slice();
        batchSlice.limit(batchSize);
        buffer.position(buffer.position() + batchSize);

        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return createDefaultLogRecordBatch(batchSlice);
        } else {
            throw new UnsupportedOperationException("Only support Kafka V2 records");
        }
    }

    /**
     * Validates the header of the next batch and returns batch size.
     *
     * @return next batch size including LOG_OVERHEAD if buffer contains header up to magic byte,
     *     null otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    Integer nextBatchSize() throws CorruptRecordException {
        int remaining = buffer.remaining();
        if (remaining < LOG_OVERHEAD) {
            return null;
        }
        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);
        // V0 has the smallest overhead, stricter checking is done later
        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0) {
            throw new CorruptRecordException(
                    String.format(
                            "Record size %d is less than the minimum record overhead (%d)",
                            recordSize, LegacyRecord.RECORD_OVERHEAD_V0));
        }

        if (recordSize > maxMessageSize) {
            throw new CorruptRecordException(
                    String.format(
                            "Record size %d exceeds the largest allowable message size (%d).",
                            recordSize, maxMessageSize));
        }

        if (remaining < HEADER_SIZE_UP_TO_MAGIC) {
            return null;
        }

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE) {
            throw new CorruptRecordException("Invalid magic found in record: " + magic);
        }

        return recordSize + LOG_OVERHEAD;
    }

    private LogRecordBatch createDefaultLogRecordBatch(ByteBuffer batchSlice) throws IOException {
        ByteBuffer bodySlice = MemoryRecordsParser.extractBody(batchSlice);
        MemoryRecordsParser.Header header = MemoryRecordsParser.extractHeader(batchSlice);

        if (bodySlice == null || header == null) {
            throw new IllegalStateException("Extracted components cannot be null");
        }
        // create a new buffer to hold the batch header and body
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_BATCH_HEADER_SIZE + bodySlice.remaining());

        // write batch body
        buffer.position(RECORD_BATCH_HEADER_SIZE);
        buffer.put(bodySlice);

        // write batch header
        writeBatchHeader(buffer, buffer.position(), header);

        // flip the buffer.
        buffer.flip();
        return instanceDefaultLogRecordBatch(buffer);
    }

    private static DefaultLogRecordBatch instanceDefaultLogRecordBatch(ByteBuffer buffer) {
        DefaultLogRecordBatch batch = new DefaultLogRecordBatch();
        batch.pointTo(MemorySegment.wrap(buffer.array()), 0);
        return batch;
    }

    /**
     * Write batch header to buffer.
     *
     * @param buffer the ByteBuffer to write the header to
     * @param sizeInBytes the size of the batch in bytes
     * @param header the header of the batch
     * @return a MemorySegmentOutputView representing the written header
     * @throws IOException if an I/O error occurs during writing
     */
    private static MemorySegmentOutputView writeBatchHeader(
            ByteBuffer buffer, int sizeInBytes, MemoryRecordsParser.Header header)
            throws IOException {
        MemorySegment memorySegment = MemorySegment.wrap(buffer.array());
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(memorySegment);

        outputView.setPosition(0);
        // BaseOffset => Int64
        outputView.writeLong(header.getBaseOffset());

        // Length => Int32
        outputView.writeInt(sizeInBytes - BASE_OFFSET_LENGTH - LENGTH_LENGTH);

        // Magic => Int8
        outputView.writeByte(header.getMagic());

        // CommitTimestamp => Int64,  write empty timestamp which will be overridden on server side
        outputView.writeLong(header.getBaseTimestamp());

        // CRC => Uint32
        outputView.writeUnsignedInt(0);

        // SchemaId => Int16;
        // If the Fluss client needs to read data written by Kafka Client, then the schemaId should
        // be set.
        outputView.writeShort((short) -1);

        // Attributes => Int8 (currently only appendOnly flag)
        // If fluss client needs to read data written by Kafka Client, then the appendOnly flag
        // should be set.
        byte flussAttributes =
                (byte) MemoryRecordsParser.computeFlussAttributes(header.getAttributes(), true);
        outputView.writeByte(flussAttributes);

        // LastOffsetDelta => Int32
        outputView.writeInt(header.getLastOffsetDelta());

        // WriterID => Int64
        outputView.writeLong(header.getProducerId());

        // SequenceID => Int32
        outputView.writeInt(header.getBaseSequence());

        // RecordCount => Int32
        outputView.writeInt(header.getRecordsCount());

        // Update crc.
        ByteBuffer crcBuffer = memorySegment.wrap(0, sizeInBytes);
        long crc = Crc32C.compute(crcBuffer, SCHEMA_ID_OFFSET, sizeInBytes - SCHEMA_ID_OFFSET);
        outputView.setPosition(CRC_OFFSET);
        outputView.writeUnsignedInt(crc);
        return outputView;
    }
}
