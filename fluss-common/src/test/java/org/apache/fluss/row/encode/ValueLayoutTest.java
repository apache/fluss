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

package org.apache.fluss.row.encode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for versioned raw value layout. */
class ValueLayoutTest {

    @Test
    void testVersion3ValueLayoutStoresBigEndianValueTimestamp() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long valueTimestampMs = 1234567890123L;
        ValueLayout valueLayout = ValueLayout.forVersion(KV_FORMAT_VERSION_3);

        BinaryValue value =
                ValueEncoder.forVersion(KV_FORMAT_VERSION_3, ignored -> valueTimestampMs)
                        .createValue(DEFAULT_SCHEMA_ID, row);
        byte[] encoded = value.encodeValue();
        MemorySegment segment = MemorySegment.wrap(encoded);

        assertThat(encoded).hasSize(valueLayout.rowOffset() + row.getSizeInBytes());
        assertThat(valueLayout.readSchemaId(segment)).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(valueLayout.readValueTimestamp(segment)).isEqualTo(valueTimestampMs);
        assertThat(value.hasValueTimestamp()).isTrue();
        assertThat(value.getValueTimestampMs()).isEqualTo(valueTimestampMs);

        byte[] expectedRowBytes = new byte[row.getSizeInBytes()];
        row.copyTo(expectedRowBytes, 0);
        assertThat(Arrays.copyOfRange(encoded, valueLayout.rowOffset(), encoded.length))
                .isEqualTo(expectedRowBytes);

        BinaryValue decoded =
                new ValueDecoder(
                                new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                                KvFormat.COMPACTED,
                                KV_FORMAT_VERSION_3)
                        .decodeValue(encoded);
        assertThat(decoded.schemaId).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(decoded.getValueTimestampMs()).isEqualTo(valueTimestampMs);
        assertThat(decoded.row.getInt(0)).isEqualTo(1);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("a");
    }

    @Test
    void testVersion3EncoderCanOverrideValueTimestampProvider() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        ValueEncoder writeEncoder = ValueEncoder.forVersion(KV_FORMAT_VERSION_3, ignored -> 100L);

        BinaryValue recoveredValue =
                writeEncoder
                        .withTimestampProvider(ignored -> 200L)
                        .createValue(DEFAULT_SCHEMA_ID, row);

        assertThat(recoveredValue.getValueTimestampMs()).isEqualTo(200L);
        assertThat(writeEncoder.createValue(DEFAULT_SCHEMA_ID, row).getValueTimestampMs())
                .isEqualTo(100L);
    }

    @Test
    void testVersion3ValueRecordBatchDecodesThroughValueLayout() throws Exception {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] encodedValue = ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, 100L, row);
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(encodedValue);
        DefaultValueRecordBatch recordBatch = builder.build();

        ValueRecord valueRecord =
                recordBatch
                        .records(
                                ValueRecordReadContext.createReadContext(
                                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                                        KvFormat.COMPACTED,
                                        ValueLayout.forVersion(KV_FORMAT_VERSION_3)))
                        .iterator()
                        .next();

        assertThat(valueRecord.schemaId()).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(valueRecord.getRow().getInt(0)).isEqualTo(1);
        assertThat(valueRecord.getRow().getString(1).toString()).isEqualTo("a");
    }

    @Test
    void testFixedSchemaDecoderDecodesVersion3ValueThroughValueLayout() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] encodedValue = ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, 100L, row);
        FixedSchemaDecoder decoder =
                new FixedSchemaDecoder(
                        KvFormat.COMPACTED,
                        DATA1_SCHEMA,
                        ValueLayout.forVersion(KV_FORMAT_VERSION_3));

        InternalRow decoded = decoder.decode(MemorySegment.wrap(encodedValue));

        assertThat(decoded.getInt(0)).isEqualTo(1);
        assertThat(decoded.getString(1).toString()).isEqualTo("a");
    }
}
