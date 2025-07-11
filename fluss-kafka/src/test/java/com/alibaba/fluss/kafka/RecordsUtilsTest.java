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

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.kafka.utils.MemoryRecordsParser;
import com.alibaba.fluss.kafka.utils.RecordsUtils;
import com.alibaba.fluss.record.DefaultLogRecordBatch;
import com.alibaba.fluss.record.LogRecordBatch;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.ATTRIBUTES_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.BASE_OFFSET_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.BASE_SEQUENCE_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.BASE_TIMESTAMP_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.LAST_OFFSET_DELTA_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.MAGIC_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.PRODUCER_ID_OFFSET;
import static com.alibaba.fluss.kafka.utils.MemoryRecordsParser.RECORDS_COUNT_OFFSET;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link RecordsUtils}. */
public class RecordsUtilsTest {

    private MemoryRecords memoryRecords;
    private DefaultLogRecordBatch flussBatch;

    @BeforeEach
    void setup() throws IOException {
        memoryRecords = createTestKafkaRecords();
        flussBatch = (DefaultLogRecordBatch) RecordsUtils.convert(memoryRecords);
    }

    @Test
    void testConvertKafkaToFluss() {
        assertThat(flussBatch).isNotNull();
        assertThat(flussBatch.getRecordCount()).isEqualTo(10);
        assertThat(RecordBatch.CURRENT_MAGIC_VALUE).isEqualTo(flussBatch.magic());
    }

    @Test
    void testConvertFlussToKafka() throws IOException {
        MemoryRecords convertedRecords = RecordsUtils.convert(flussBatch);
        assertThat(convertedRecords).isNotNull();
        Iterator<Record> original = memoryRecords.records().iterator();
        assertThat(original.hasNext()).isTrue();
        Iterator<Record> converted = convertedRecords.records().iterator();
        assertThat(converted.hasNext()).isTrue();

        while (original.hasNext() && converted.hasNext()) {
            Record origRecord = original.next();
            Record convRecord = converted.next();

            assertThat(origRecord.key()).isEqualTo(convRecord.key());
            assertThat(origRecord.value()).isEqualTo(convRecord.value());
            assertThat(origRecord.timestamp()).isEqualTo(convRecord.timestamp());
        }

        ByteBuffer buffer = ByteBuffer.allocate(128);
        convertedRecords.buffer().get(buffer.array(), 0, 128);
        assertThat(RecordBatch.CURRENT_MAGIC_VALUE).isEqualTo(buffer.get(MAGIC_OFFSET));
    }

    @Test
    void testNullInputHandling() {

        Throwable thrown = catchThrowable(() -> RecordsUtils.convert((MemoryRecords) null));
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class);

        Throwable thrown2 = catchThrowable(() -> RecordsUtils.convert((LogRecordBatch) null));
        assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testHeaderConversion() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        int sizeInBytes = buffer.remaining();
        RecordsUtils.writeMemoryRecordsHeader(buffer, sizeInBytes, flussBatch);
        buffer.position(0);
        // baseOffset
        assertThat(flussBatch.baseLogOffset()).isEqualTo(buffer.getLong(BASE_OFFSET_OFFSET));
        // magic
        assertThat(flussBatch.magic()).isEqualTo(buffer.get(MAGIC_OFFSET));
        // magic
        assertThat(flussBatch.commitTimestamp())
                .isEqualTo(buffer.getLong(BASE_TIMESTAMP_OFFSET)); // commitTimestamp
        // magic
        byte flussAttributes = flussBatch.attributes();
        boolean isAppendOnly = (flussAttributes & APPEND_ONLY_FLAG_MASK) > 0;
        assertThat(isAppendOnly).isTrue();

        // attributes
        short attributes = MemoryRecordsParser.recoverKafkaAttributes(flussAttributes);
        assertThat(attributes).isEqualTo(buffer.get(ATTRIBUTES_OFFSET));

        // lastOffsetDelta
        assertThat(flussBatch.lastOffsetDelta()).isEqualTo(buffer.getInt(LAST_OFFSET_DELTA_OFFSET));

        // writerId
        assertThat(flussBatch.writerId()).isEqualTo(buffer.getLong(PRODUCER_ID_OFFSET));

        // batchSequence
        assertThat(flussBatch.batchSequence()).isEqualTo(buffer.getInt(BASE_SEQUENCE_OFFSET));

        // recordCount
        assertThat(flussBatch.getRecordCount()).isEqualTo(buffer.getInt(RECORDS_COUNT_OFFSET));
    }

    @Test
    void testEmptyRecordsConversion() throws IOException {
        MemoryRecords emptyRecords = MemoryRecords.EMPTY;
        LogRecordBatch emptyBatch = RecordsUtils.convert(emptyRecords);
        assertThat(emptyBatch).isNull();
    }

    private MemoryRecords createTestKafkaRecords() {
        try (MemoryRecordsBuilder builder =
                MemoryRecords.builder(
                        ByteBuffer.allocate(1024),
                        Compression.NONE,
                        TimestampType.CREATE_TIME,
                        0L)) {
            for (int i = 0; i < 10; i++) {
                builder.append(
                        System.currentTimeMillis(),
                        ("key-" + i).getBytes(),
                        ("value-" + i).getBytes());
            }
            return builder.build();
        }
    }
}
