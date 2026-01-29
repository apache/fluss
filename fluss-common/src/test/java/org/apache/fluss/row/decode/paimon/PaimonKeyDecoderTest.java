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

package org.apache.fluss.row.decode.paimon;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PaimonKeyDecoder} to verify decoding correctness and round-trip. */
class PaimonKeyDecoderTest {

    @Test
    void testDecodeAllTypes() {
        // Create a row type with all supported types as primary key fields
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DATE(),
                            DataTypes.TIME(),
                            DataTypes.CHAR(5),
                            DataTypes.STRING(),
                            DataTypes.BINARY(10),
                            DataTypes.BYTES(),
                            DataTypes.DECIMAL(5, 2),
                            DataTypes.DECIMAL(20, 0),
                            DataTypes.TIMESTAMP(1),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.TIMESTAMP_LTZ(1),
                            DataTypes.TIMESTAMP_LTZ(6)
                        },
                        new String[] {
                            "bool_col",
                            "byte_col",
                            "short_col",
                            "int_col",
                            "long_col",
                            "float_col",
                            "double_col",
                            "date_col",
                            "time_col",
                            "char_col",
                            "string_col",
                            "binary_col",
                            "bytes_col",
                            "compact_decimal_col",
                            "non_compact_decimal_col",
                            "compact_ts_col",
                            "non_compact_ts_col",
                            "compact_ltz_col",
                            "non_compact_ltz_col"
                        });

        List<String> keys = rowType.getFieldNames();
        PaimonKeyEncoder encoder = new PaimonKeyEncoder(rowType, keys);
        PaimonKeyDecoder decoder = new PaimonKeyDecoder(rowType, keys);

        long millis = 1698235273182L;
        int nanos = 456789;

        // Create test row with all types
        InternalRow original = createAllTypesRow(millis, nanos);

        // Encode and decode
        byte[] encoded = encoder.encodeKey(original);
        InternalRow decoded = decoder.decodeKey(encoded);

        // Verify all fields
        assertThat(decoded.getFieldCount()).isEqualTo(19);
        assertThat(decoded.getBoolean(0)).isTrue();
        assertThat(decoded.getByte(1)).isEqualTo((byte) 127);
        assertThat(decoded.getShort(2)).isEqualTo((short) 32767);
        assertThat(decoded.getInt(3)).isEqualTo(Integer.MAX_VALUE);
        assertThat(decoded.getLong(4)).isEqualTo(Long.MAX_VALUE);
        assertThat(decoded.getFloat(5))
                .isCloseTo(3.14f, org.assertj.core.data.Offset.offset(0.01f));
        assertThat(decoded.getDouble(6))
                .isCloseTo(2.718, org.assertj.core.data.Offset.offset(0.001));
        assertThat(decoded.getInt(7)).isEqualTo(19655);
        assertThat(decoded.getInt(8)).isEqualTo(34200000);
        assertThat(decoded.getString(9).toString()).isEqualTo("hello");
        assertThat(decoded.getString(10).toString()).isEqualTo("world");
        assertThat(decoded.getBytes(11)).isEqualTo("test".getBytes());
        assertThat(decoded.getBytes(12)).isEqualTo("data".getBytes());
        assertThat(decoded.getDecimal(13, 5, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        assertThat(decoded.getDecimal(14, 20, 0).toBigDecimal())
                .isEqualTo(new BigDecimal("12345678901234567890"));
        assertThat(decoded.getTimestampNtz(15, 1).getMillisecond()).isEqualTo(millis);
        TimestampNtz decodedTs = decoded.getTimestampNtz(16, 6);
        assertThat(decodedTs.getMillisecond()).isEqualTo(millis);
        assertThat(decodedTs.getNanoOfMillisecond()).isEqualTo(nanos);
        assertThat(decoded.getTimestampLtz(17, 1).getEpochMillisecond()).isEqualTo(millis);
        TimestampLtz decodedLtz = decoded.getTimestampLtz(18, 6);
        assertThat(decodedLtz.getEpochMillisecond()).isEqualTo(millis);
        assertThat(decodedLtz.getNanoOfMillisecond()).isEqualTo(nanos);
    }

    @Test
    void testDecodeStringVariants() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"s"});
        PaimonKeyEncoder encoder = new PaimonKeyEncoder(rowType, rowType.getFieldNames());
        PaimonKeyDecoder decoder = new PaimonKeyDecoder(rowType, rowType.getFieldNames());

        // Test short strings (≤7 bytes with 0x80 marker) and long strings (>7 bytes)
        for (String testStr : Arrays.asList("", "a", "1234567", "12345678", "Hello, Paimon!")) {
            InternalRow original = createStringRow(testStr);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getString(0).toString()).isEqualTo(testStr);
        }
    }

    @Test
    void testDecodePartialKeys() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()},
                        new String[] {"id", "name", "value"});

        List<String> keys = Collections.singletonList("id");
        PaimonKeyEncoder encoder = new PaimonKeyEncoder(rowType, keys);
        PaimonKeyDecoder decoder = new PaimonKeyDecoder(rowType, keys);

        InternalRow original = createPartialKeyRow();
        byte[] encoded = encoder.encodeKey(original);
        InternalRow decoded = decoder.decodeKey(encoded);

        assertThat(decoded.getFieldCount()).isEqualTo(1);
        assertThat(decoded.getInt(0)).isEqualTo(100);
    }

    private InternalRow createAllTypesRow(long millis, int nanos) {
        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.CHAR(5),
                    DataTypes.STRING(),
                    DataTypes.BINARY(10),
                    DataTypes.BYTES(),
                    DataTypes.DECIMAL(5, 2),
                    DataTypes.DECIMAL(20, 0),
                    DataTypes.TIMESTAMP(1),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP_LTZ(1),
                    DataTypes.TIMESTAMP_LTZ(6)
                };
        IndexedRow indexedRow = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeBoolean(true);
        writer.writeByte((byte) 127);
        writer.writeShort((short) 32767);
        writer.writeInt(Integer.MAX_VALUE);
        writer.writeLong(Long.MAX_VALUE);
        writer.writeFloat(3.14f);
        writer.writeDouble(2.718);
        writer.writeInt(19655);
        writer.writeInt(34200000);
        writer.writeChar(BinaryString.fromString("hello"), 5);
        writer.writeString(BinaryString.fromString("world"));
        writer.writeBinary("test".getBytes(), 10);
        writer.writeBytes("data".getBytes());
        writer.writeDecimal(Decimal.fromBigDecimal(new BigDecimal("123.45"), 5, 2), 5);
        writer.writeDecimal(
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567890"), 20, 0), 20);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(millis), 1);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(millis, nanos), 6);
        writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(millis), 1);
        writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(millis, nanos), 6);
        indexedRow.pointTo(writer.segment(), 0, writer.position());
        return indexedRow;
    }

    private InternalRow createStringRow(String value) {
        return row((Object) BinaryString.fromString(value));
    }

    private InternalRow createPartialKeyRow() {
        return row(100, BinaryString.fromString("Alice"), 999L);
    }
}
