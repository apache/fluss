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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.record.DefaultLogRecordBatch;
import com.alibaba.fluss.record.LogRecordBatch;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Crc32C;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.ATTRIBUTES_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.BASE_OFFSET_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.BASE_SEQUENCE_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.BASE_TIMESTAMP_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.CRC_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.LAST_OFFSET_DELTA_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.MAGIC_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.MAX_TIMESTAMP_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.PARTITION_LEADER_EPOCH_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.PRODUCER_EPOCH_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.PRODUCER_ID_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.RECORDS_COUNT_OFFSET;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.LENGTH_OFFSET;
import static org.apache.kafka.common.record.DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/** Utils for converting between Kafka MemoryRecords and Fluss LogRecordBatch. */
public class RecordsUtils {

    /**
     * Convert Kafka MemoryRecords to Fluss LogRecordBatch.
     *
     * @param records the Kafka MemoryRecords to convert
     * @return the converted Fluss LogRecordBatch
     * @throws IOException if an I/O error occurs during conversion
     */
    public static LogRecordBatch convert(MemoryRecords records) throws IOException {
        if (records == null) {
            throw new IllegalArgumentException("Records cannot be null");
        }
        return new ByteBufferLogInputStream(records.buffer().duplicate(), Integer.MAX_VALUE)
                .nextBatch();
    }

    /**
     * Convert Fluss LogRecordBatch to Kafka MemoryRecords.
     *
     * @param batch the Fluss LogRecordBatch to convert
     * @return the converted Kafka MemoryRecords
     * @throws IOException if an I/O error occurs during conversion
     */
    public static MemoryRecords convert(LogRecordBatch batch) throws IOException {
        if (batch == null) {
            throw new IllegalArgumentException("LogRecordBatch cannot be null");
        }
        DefaultLogRecordBatch recordBatch = (DefaultLogRecordBatch) batch;
        int flussHeaderSize = DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE;

        // Extract the body buffer from Fluss LogRecordBatch.
        ByteBuffer originalBuffer = ByteBuffer.wrap(recordBatch.getSegment().getArray());
        originalBuffer.position(flussHeaderSize);
        ByteBuffer bodyBuffer = originalBuffer.slice();

        // Build the kafka buffer.
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_BATCH_OVERHEAD + bodyBuffer.remaining());

        // Write the body buffer.
        buffer.position(RECORD_BATCH_OVERHEAD);
        buffer.put(bodyBuffer);
        int position = buffer.position();

        // Write the header.
        writeMemoryRecordsHeader(buffer, position, recordBatch);

        buffer.position(position);
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    @VisibleForTesting
    public static void writeMemoryRecordsHeader(
            ByteBuffer buffer, int sizeInBytes, DefaultLogRecordBatch batch) {

        int initialPosition = 0;
        int size = buffer.position();
        buffer.position(initialPosition);

        buffer.putLong(BASE_OFFSET_OFFSET, batch.baseLogOffset());
        buffer.putInt(LENGTH_OFFSET, size - LOG_OVERHEAD);
        buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, 0);
        buffer.put(MAGIC_OFFSET, batch.magic());

        byte attributes = (byte) MemoryRecordsParser.recoverKafkaAttributes(batch.attributes());
        buffer.putShort(ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(BASE_TIMESTAMP_OFFSET, batch.commitTimestamp());
        buffer.putLong(MAX_TIMESTAMP_OFFSET, batch.commitTimestamp());
        buffer.putInt(LAST_OFFSET_DELTA_OFFSET, batch.lastOffsetDelta());
        buffer.putLong(PRODUCER_ID_OFFSET, batch.writerId());
        buffer.putShort(PRODUCER_EPOCH_OFFSET, (short) 0);
        buffer.putInt(BASE_SEQUENCE_OFFSET, batch.batchSequence());
        buffer.putInt(RECORDS_COUNT_OFFSET, batch.getRecordCount());
        long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
        buffer.putInt(CRC_OFFSET, (int) crc);
        buffer.position(initialPosition + RECORD_BATCH_OVERHEAD);
    }
}
