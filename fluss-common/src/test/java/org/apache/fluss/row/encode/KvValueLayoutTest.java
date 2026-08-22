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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the versioned KV value layout. */
class KvValueLayoutTest {

    @Test
    void testVersion3KvValueLayoutStoresBigEndianValueTag() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long valueTag = 1234567890123L;
        KvValueLayout kvValueLayout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);
        AtomicInteger providerCalls = new AtomicInteger();

        ValueEncoder encoder =
                ValueEncoder.forKvFormatVersion(
                        KV_FORMAT_VERSION_3,
                        ignored -> {
                            providerCalls.incrementAndGet();
                            return valueTag;
                        });
        byte[] encoded = encoder.encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
        MemorySegment segment = MemorySegment.wrap(encoded);

        assertThat(encoded).hasSize(kvValueLayout.rowPayloadOffset() + row.getSizeInBytes());
        assertThat(kvValueLayout.readSchemaId(segment)).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(kvValueLayout.readValueTag(segment)).isEqualTo(valueTag);
        assertThat(providerCalls).hasValue(1);

        byte[] expectedRowBytes = new byte[row.getSizeInBytes()];
        row.copyTo(expectedRowBytes, 0);
        assertThat(Arrays.copyOfRange(encoded, kvValueLayout.rowPayloadOffset(), encoded.length))
                .isEqualTo(expectedRowBytes);

        BinaryValue decoded =
                new ValueDecoder(
                                new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                                KvFormat.COMPACTED,
                                KV_FORMAT_VERSION_3)
                        .decodeValue(encoded);
        assertThat(decoded.schemaId).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(decoded.row.getInt(0)).isEqualTo(1);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("a");
    }

    @Test
    void testVersion3EncoderCanOverrideValueTagProvider() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        ValueEncoder writeEncoder =
                ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, ignored -> 100L);

        byte[] recoveredValue =
                writeEncoder
                        .withValueTagProvider(ignored -> 200L)
                        .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
        byte[] writtenValue = writeEncoder.encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);

        assertThat(layout.readValueTag(MemorySegment.wrap(recoveredValue))).isEqualTo(200L);
        assertThat(layout.readValueTag(MemorySegment.wrap(writtenValue))).isEqualTo(100L);
    }

    @Test
    void testValueTagProviderMustMatchKvValueLayout() {
        assertThatThrownBy(
                        () -> ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_2, ignored -> 100L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be null");
        assertThatThrownBy(() -> ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be non-null");
    }

    @Test
    void testValueTagWriteRejectsOutOfBoundsTarget() {
        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);

        assertThatThrownBy(() -> layout.writeValueTag(new byte[layout.rowPayloadOffset() - 1], 1L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> layout.writeValueTag(new byte[layout.rowPayloadOffset()], -3, 1L))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testVersion3ValueRecordBatchDecodesThroughKvValueLayout() throws Exception {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] encodedValue = encodeVersion3Value(row, 100L);
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(encodedValue);
        DefaultValueRecordBatch recordBatch = builder.build();

        ValueRecord valueRecord =
                recordBatch
                        .records(
                                ValueRecordReadContext.createReadContext(
                                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                                        KvFormat.COMPACTED,
                                        KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3)))
                        .iterator()
                        .next();

        assertThat(valueRecord.schemaId()).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(valueRecord.getRow().getInt(0)).isEqualTo(1);
        assertThat(valueRecord.getRow().getString(1).toString()).isEqualTo("a");
    }

    @Test
    void testFixedSchemaDecoderDecodesVersion3ValueThroughKvValueLayout() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] encodedValue = encodeVersion3Value(row, 100L);
        FixedSchemaDecoder decoder =
                new FixedSchemaDecoder(
                        KvFormat.COMPACTED,
                        DATA1_SCHEMA,
                        KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3));

        InternalRow decoded = decoder.decode(MemorySegment.wrap(encodedValue));

        assertThat(decoded.getInt(0)).isEqualTo(1);
        assertThat(decoded.getString(1).toString()).isEqualTo("a");
    }

    private static byte[] encodeVersion3Value(BinaryRow row, long valueTag) {
        return ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, ignored -> valueTag)
                .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
    }
}
