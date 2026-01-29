/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.decode;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedKeyDecoder}. */
class CompactedKeyDecoderTest {

    @Test
    void testDecodeKey() {
        // All fields as keys
        verifyDecode(
                RowType.of(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.INT()),
                new int[] {0, 1, 2},
                row(1, 3L, 2),
                (decoded, original) -> {
                    assertThat(decoded.getFieldCount()).isEqualTo(3);
                    assertThat(decoded.getInt(0)).isEqualTo(1);
                    assertThat(decoded.getLong(1)).isEqualTo(3L);
                    assertThat(decoded.getInt(2)).isEqualTo(2);
                });

        // Partial key - single INT key
        RowType type1 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()},
                        new String[] {"id", "name", "value"});
        verifyDecode(
                type1,
                Collections.singletonList("id"),
                row(100, "Alice", 999L),
                (decoded, original) -> {
                    assertThat(decoded.getFieldCount()).isEqualTo(1);
                    assertThat(decoded.getInt(0)).isEqualTo(100);
                });

        // Single STRING key at non-first position
        RowType type2 =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"partition", "f1", "f2"});
        verifyDecode(
                type2,
                Collections.singletonList("f2"),
                row("p1", 1L, "a2"),
                (decoded, original) -> {
                    assertThat(decoded.getFieldCount()).isEqualTo(1);
                    assertThat(decoded.getString(0).toString()).isEqualTo("a2");
                });

        // Multiple keys (INT, STRING)
        RowType type3 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT(),
                            DataTypes.DOUBLE(),
                            DataTypes.BOOLEAN()
                        },
                        new String[] {"id", "name", "age", "score", "active"});
        verifyDecode(
                type3,
                Arrays.asList("id", "name"),
                row(100, "Alice", 25L, 95.5, true),
                (decoded, original) -> {
                    assertThat(decoded.getFieldCount()).isEqualTo(2);
                    assertThat(decoded.getInt(0)).isEqualTo(100);
                    assertThat(decoded.getString(1).toString()).isEqualTo("Alice");
                });

        // Non-sequential key positions
        RowType type4 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING()
                        },
                        new String[] {"a", "b", "c", "d", "e"});
        verifyDecode(
                type4,
                Arrays.asList("b", "d"),
                row("v0", 42, "v2", 999L, "v4"),
                (decoded, original) -> {
                    assertThat(decoded.getFieldCount()).isEqualTo(2);
                    assertThat(decoded.getInt(0)).isEqualTo(42);
                    assertThat(decoded.getLong(1)).isEqualTo(999L);
                });

        // Various data types at non-sequential positions
        RowType type5 =
                RowType.of(
                        new DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.STRING()
                        },
                        new String[] {"a", "b", "c", "d", "e", "f", "g", "h"});
        verifyDecode(
                type5,
                Arrays.asList("b", "d", "g"),
                row(true, (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, BinaryString.fromString("test")),
                (decoded, original) -> {
                    assertThat(decoded.getFieldCount()).isEqualTo(3);
                    assertThat(decoded.getByte(0)).isEqualTo((byte) 1);
                    assertThat(decoded.getInt(1)).isEqualTo(3);
                    assertThat(decoded.getDouble(2)).isEqualTo(6.0);
                });
    }

    @Test
    void testEncodeDecodeRoundTrip() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()},
                        new String[] {"id", "name", "value"});
        List<String> pk = Arrays.asList("id", "name");
        CompactedKeyEncoder encoder = CompactedKeyEncoder.createKeyEncoder(rowType, pk);
        CompactedKeyDecoder decoder = CompactedKeyDecoder.createKeyDecoder(rowType, pk);

        for (InternalRow original :
                new InternalRow[] {
                    row(1, "alice", 100L),
                    row(2, "bob", 200L),
                    row(999, "test", 999L),
                    row(0, "", 0L)
                }) {
            InternalRow decoded = decoder.decodeKey(encoder.encodeKey(original));
            assertThat(decoded.getInt(0)).isEqualTo(original.getInt(0));
            assertThat(decoded.getString(1).toString()).isEqualTo(original.getString(1).toString());
        }
    }

    @Test
    void testDecodeInvalidKey() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        assertThatThrownBy(
                        () ->
                                CompactedKeyDecoder.createKeyDecoder(
                                        rowType, Collections.singletonList("invalidField")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalidField not found in row type");
    }

    private void verifyDecode(
            RowType rowType,
            int[] keyPos,
            InternalRow original,
            BiConsumer<InternalRow, InternalRow> assertions) {
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(rowType, keyPos);
        CompactedKeyDecoder decoder = new CompactedKeyDecoder(rowType, keyPos);
        assertions.accept(decoder.decodeKey(encoder.encodeKey(original)), original);
    }

    private void verifyDecode(
            RowType rowType,
            List<String> keys,
            InternalRow original,
            BiConsumer<InternalRow, InternalRow> assertions) {
        CompactedKeyEncoder encoder = CompactedKeyEncoder.createKeyEncoder(rowType, keys);
        CompactedKeyDecoder decoder = CompactedKeyDecoder.createKeyDecoder(rowType, keys);
        assertions.accept(decoder.decodeKey(encoder.encodeKey(original)), original);
    }
}
