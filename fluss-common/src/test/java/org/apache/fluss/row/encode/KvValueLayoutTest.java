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
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.ByteArraySlice;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for KV value layouts. */
class KvValueLayoutTest {

    @Test
    void testLayoutVersionMapping() {
        assertThat(KvValueLayout.fromVersion(1)).isSameAs(KvValueLayout.PLAIN);
        assertThat(KvValueLayout.fromVersion(2)).isSameAs(KvValueLayout.TAGGED);
        assertThatThrownBy(() -> KvValueLayout.fromVersion(3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported KV value layout version 3");
    }

    @Test
    void testLongTagKeepsRpcValueBodyAsSuffix() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] value =
                ValueEncoder.forLayout(KvValueLayout.TAGGED, ignored -> 123L)
                        .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));

        assertThat(KvValueLayout.TAGGED.valueBodyOffset()).isEqualTo(8);
        assertThat(KvValueLayout.TAGGED.schemaIdOffset()).isEqualTo(8);
        assertThat(KvValueLayout.TAGGED.rowPayloadOffset()).isEqualTo(10);
        assertThat(KvValueLayout.TAGGED.readValueTag(MemorySegment.wrap(value))).isEqualTo(123L);
        assertThat(KvValueLayout.TAGGED.readSchemaId(MemorySegment.wrap(value)))
                .isEqualTo(DEFAULT_SCHEMA_ID);

        ByteArraySlice rpcValue = KvValueLayout.TAGGED.toValueBodySlice(value);
        assertThat(rpcValue.array()).isSameAs(value);
        assertThat(rpcValue.offset()).isEqualTo(Long.BYTES);
        assertThat(rpcValue.length()).isEqualTo(value.length - Long.BYTES);
        assertThat(rpcValue.toByteArray())
                .containsExactly(ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, row));
        assertThat(KvValueLayout.TAGGED.toValueBodySlice(null)).isNull();
    }

    @Test
    void testTwoArgumentValueDecoderUsesPlainLayout() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] value = ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, row);
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);

        BinaryValue implicitPlain =
                new ValueDecoder(schemaGetter, KvFormat.COMPACTED).decodeValue(value);
        BinaryValue explicitPlain =
                new ValueDecoder(schemaGetter, KvFormat.COMPACTED, KvValueLayout.PLAIN)
                        .decodeValue(value);

        assertThat(implicitPlain).isEqualTo(explicitPlain);
    }

    @Test
    void testPersistedHeaderByteOrder() {
        byte[] plainValue = new byte[KvValueLayout.PLAIN.rowPayloadOffset()];
        KvValueLayout.PLAIN.writeSchemaId(plainValue, (short) 0x1234);
        assertThat(plainValue).containsExactly((byte) 0x34, (byte) 0x12);

        byte[] taggedValue = new byte[KvValueLayout.TAGGED.rowPayloadOffset()];
        KvValueLayout.TAGGED.writeValueTag(taggedValue, 0x0102030405060708L);
        KvValueLayout.TAGGED.writeSchemaId(taggedValue, (short) 0x1234);
        assertThat(taggedValue)
                .containsExactly(
                        (byte) 0x01,
                        (byte) 0x02,
                        (byte) 0x03,
                        (byte) 0x04,
                        (byte) 0x05,
                        (byte) 0x06,
                        (byte) 0x07,
                        (byte) 0x08,
                        (byte) 0x34,
                        (byte) 0x12);
    }
}
