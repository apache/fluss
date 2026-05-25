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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowReader;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowTest;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.guava32.com.google.common.io.BaseEncoding;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedKeyEncoder}. */
class CompactedKeyEncoderTest {

    @Test
    void testEncodeKey() {
        // test int, long as primary key
        final RowType rowType = RowType.of(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.INT());
        InternalRow row = row(1, 3L, 2);
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(rowType);

        byte[] bytes = encoder.encodeKey(row);
        assertThat(bytes).isEqualTo(new byte[] {1, 3, 2});

        row = row(2, 5L, 6);
        bytes = encoder.encodeKey(row);
        assertThat(bytes).isEqualTo(new byte[] {2, 5, 6});
    }

    @Test
    void testEncodeKeyWithKeyNames() {
        final DataType[] dataTypes =
                new DataType[] {DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()};
        final String[] fieldNames = new String[] {"partition", "f1", "f2"};
        final RowType rowType = RowType.of(dataTypes, fieldNames);

        InternalRow row = row("p1", 1L, "a2");
        List<String> pk = Collections.singletonList("f2");

        CompactedKeyEncoder keyEncoder = CompactedKeyEncoder.createKeyEncoder(rowType, pk);
        byte[] encodedBytes = keyEncoder.encodeKey(row);

        //  2 (start of text), 97 (the letter a), 50 (the number 2)
        assertThat(encodedBytes).isEqualTo(new byte[] {2, 97, 50});

        // decode it, should only get "a2"
        InternalRow encodedKey =
                decodeRow(
                        new DataType[] {
                            DataTypes.STRING().copy(false),
                        },
                        encodedBytes);
        assertThat(encodedKey.getFieldCount()).isEqualTo(1);
        assertThat(encodedKey.getString(0).toString()).isEqualTo("a2");
    }

    @Test
    void testGetKey() {
        // test int, long as primary key
        final RowType rowType =
                RowType.of(
                        DataTypes.INT(), DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING());
        int[] pkIndexes = new int[] {0, 1, 2};
        final CompactedKeyEncoder compactedKeyEncoder = new CompactedKeyEncoder(rowType, pkIndexes);

        InternalRow row = row(1, 3L, 2, "a1");

        byte[] keyBytes = compactedKeyEncoder.encodeKey(row);
        assertThat(keyBytes).isEqualTo(new byte[] {1, 3, 2});

        // should throw exception when the column is null
        assertThatThrownBy(
                        () -> {
                            InternalRow nullRow = row(1, 2L, null, "a2");
                            compactedKeyEncoder.encodeKey(nullRow);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Null value is not allowed for compacted key encoder in position 2 with type INT.");

        // test int, string as primary key
        RowType rowType1 =
                RowType.of(
                        DataTypes.STRING(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.STRING());
        pkIndexes = new int[] {1, 2};
        final CompactedKeyEncoder keyEncoder1 = new CompactedKeyEncoder(rowType1, pkIndexes);
        row =
                row(
                        BinaryString.fromString("a1"),
                        1,
                        BinaryString.fromString("a2"),
                        BinaryString.fromString("a3"));
        keyBytes = keyEncoder1.encodeKey(row);

        // 1, 2 (start of text), 97 (the letter a), 50 (the number 2)
        assertThat(keyBytes).isEqualTo(new byte[] {1, 2, 97, 50});

        InternalRow keyRow =
                decodeRow(
                        new DataType[] {
                            DataTypes.INT().copy(false), DataTypes.STRING().copy(false),
                        },
                        keyBytes);
        assertThat(keyRow.getInt(0)).isEqualTo(1);
        assertThat(keyRow.getString(1).toString()).isEqualTo("a2");
    }

    @Test
    void testGetKeyForAllTypes() throws Exception {
        // just test the InternalRowKeyGetter can handle all datatypes as primary key
        RowType rowType = createAllRowType();
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        try (IndexedRowWriter writer = IndexedRowTest.genRecordForAllTypes(dataTypes)) {
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            // the last column will be null, we exclude the last column as primary key
            int[] pkIndexes = IntStream.range(0, rowType.getFieldCount() - 1).toArray();
            DataType[] keyDataTypes = new DataType[pkIndexes.length];
            for (int i = 0; i < pkIndexes.length; i++) {
                keyDataTypes[i] = dataTypes[pkIndexes[i]].copy(false);
            }

            final CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, pkIndexes);
            byte[] keyBytes = keyEncoder.encodeKey(row);

            InternalRow keyRow = decodeRow(keyDataTypes, keyBytes);

            // Expected encoding of all types, ensuring consistency across Java and Rust
            // clients. Hex bytes are grouped by type for readability.
            StringBuilder expectedHexString = new StringBuilder();
            // BOOLEAN: true
            expectedHexString.append("01");
            // TINYINT: 2
            expectedHexString.append("02");
            // SMALLINT: 10
            expectedHexString.append("0A");
            // INT: 100
            expectedHexString.append("00 64");
            // BIGINT: -6101065172474983726
            expectedHexString.append("D2 95 FC D8 CE B1 AA AA AB 01");
            // FLOAT: 13.2
            expectedHexString.append("33 33 53 41");
            // DOUBLE: 15.21
            expectedHexString.append("EC 51 B8 1E 85 6B 2E 40");
            // DATE: "2023-10-25"
            expectedHexString.append("C7 99 01");
            // TIME(0): "09:30:00.0"
            expectedHexString.append("C0 B3 A7 10");
            // BINARY(20): "1234567890"
            expectedHexString.append("0A 31 32 33 34 35 36 37 38 39 30");
            // BYTES: "20".getBytes()
            expectedHexString.append("02 32 30");
            // CHAR(2): "1"
            expectedHexString.append("01 31");
            // STRING: "hello"
            expectedHexString.append("05 68 65 6C 6C 6F");
            // DECIMAL(5,2): fromUnscaledLong(9, 5, 2)
            expectedHexString.append("09");
            // DECIMAL(20,0): fromBigDecimal(new BigDecimal(10), 20, 0)
            expectedHexString.append("01 0A");
            // TIMESTAMP(1): TimestampNtz.fromMillis(1698235273182L)
            expectedHexString.append("DE 9F D7 B5 B6 31");
            // TIMESTAMP(5): TimestampNtz.fromMillis(1698235273182L)
            expectedHexString.append("DE 9F D7 B5 B6 31 00");
            // TIMESTAMP_LTZ(1): TimestampLtz.fromEpochMillis(1698235273182L)
            expectedHexString.append("DE 9F D7 B5 B6 31");
            // TIMESTAMP_LTZ(5): TimestampLtz.fromEpochMillis(1698235273182L)
            expectedHexString.append("DE 9F D7 B5 B6 31 00");
            // ARRAY(INT): GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234)
            expectedHexString.append("30 09 00 00 00 40 00 00 00 01 ");
            expectedHexString.append("00 00 00 02 00 00 00 03 00 00 ");
            expectedHexString.append("00 04 00 00 00 05 00 00 00 F5 ");
            expectedHexString.append("FF FF FF 00 00 00 00 BC 01 00 ");
            expectedHexString.append("00 5A 8F 01 00 00 00 00 00");
            // ARRAY<FLOAT NOT NULL>: GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, MAX, MIN)
            expectedHexString.append("20 06 00 00 00 00 00 00 00 CD ");
            expectedHexString.append("CC CC 3D CD CC 8C 3F 00 00 00 ");
            expectedHexString.append("BF 33 33 D3 40 FF FF 7F 7F 01 ");
            expectedHexString.append("00 00 00");
            // ARRAY<ARRAY<STRING>>
            expectedHexString.append("58 03 00 00 00 02 00 00 00 20 ");
            expectedHexString.append("00 00 00 20 00 00 00 00 00 00 ");
            expectedHexString.append("00 00 00 00 00 18 00 00 00 40 ");
            expectedHexString.append("00 00 00 03 00 00 00 02 00 00 ");
            expectedHexString.append("00 61 00 00 00 00 00 00 81 00 ");
            expectedHexString.append("00 00 00 00 00 00 00 63 00 00 ");
            expectedHexString.append("00 00 00 00 81 02 00 00 00 00 ");
            expectedHexString.append("00 00 00 68 65 6C 6C 6F 00 00 ");
            expectedHexString.append("85 77 6F 72 6C 64 00 00 85");
            // MAP<INT NOT NULL, STRING>
            expectedHexString.append("3C 18 00 00 00 03 00 00 00 00 ");
            expectedHexString.append("00 00 00 00 00 00 00 01 00 00 ");
            expectedHexString.append("00 02 00 00 00 00 00 00 00 03 ");
            expectedHexString.append("00 00 00 01 00 00 00 00 00 00 ");
            expectedHexString.append("00 00 00 00 00 31 00 00 00 00 ");
            expectedHexString.append("00 00 81 32 00 00 00 00 00 00 ");
            expectedHexString.append("81");
            byte[] expected =
                    BaseEncoding.base16().decode(expectedHexString.toString().replace(" ", ""));

            assertThat(keyBytes).isEqualTo(expected);

            // get the field getter for the key field
            InternalRow.FieldGetter[] fieldGetters =
                    new InternalRow.FieldGetter[keyDataTypes.length];
            for (int i = 0; i < keyDataTypes.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(keyDataTypes[i], i);
            }
            // get the field from key row and origin row, and then check each field
            for (int i = 0; i < keyDataTypes.length; i++) {
                assertThat(fieldGetters[i].getFieldOrNull(keyRow))
                        .as("Field " + i + " of type " + keyDataTypes[i])
                        .isEqualTo(fieldGetters[i].getFieldOrNull(row));
            }
        }
    }

    private InternalRow decodeRow(DataType[] dataTypes, byte[] values) {
        // use 0 as field count, then the null bits will be 0
        CompactedRowReader compactedRowReader = new CompactedRowReader(0);
        compactedRowReader.pointTo(MemorySegment.wrap(values), 0, values.length);

        CompactedRowDeserializer compactedRowDeserializer = new CompactedRowDeserializer(dataTypes);
        GenericRow genericRow = new GenericRow(dataTypes.length);
        compactedRowDeserializer.deserialize(compactedRowReader, genericRow);
        return genericRow;
    }
}
