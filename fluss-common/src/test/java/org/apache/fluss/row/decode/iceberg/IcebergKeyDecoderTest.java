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

package org.apache.fluss.row.decode.iceberg;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.iceberg.IcebergKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IcebergKeyDecoder} to verify decoding correctness and round-trip with
 * encoder.
 */
class IcebergKeyDecoderTest {

    @Test
    void testSingleKeyFieldRequirement() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        // Should succeed with single key
        IcebergKeyDecoder decoder = new IcebergKeyDecoder(rowType, Collections.singletonList("id"));
        assertThat(decoder).isNotNull();

        // Should fail with multiple keys
        assertThatThrownBy(() -> new IcebergKeyDecoder(rowType, Arrays.asList("id", "name")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Key fields must have exactly one field");
    }

    @Test
    void testDecodeInteger() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));
        IcebergKeyDecoder decoder = new IcebergKeyDecoder(rowType, Collections.singletonList("id"));

        for (int value : new int[] {0, -42, 42, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            InternalRow original = row(value);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getInt(0)).isEqualTo(value);
        }
    }

    @Test
    void testDecodeLong() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"});
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));
        IcebergKeyDecoder decoder = new IcebergKeyDecoder(rowType, Collections.singletonList("id"));

        for (long value : new long[] {0L, -999L, 1234567890123456789L, Long.MAX_VALUE}) {
            InternalRow original = row(value);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getLong(0)).isEqualTo(value);
        }
    }

    @Test
    void testDecodeDate() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.DATE()}, new String[] {"date"});
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("date"));
        IcebergKeyDecoder decoder =
                new IcebergKeyDecoder(rowType, Collections.singletonList("date"));

        for (int value : new int[] {0, 19655}) {
            InternalRow original = row(value);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getInt(0)).isEqualTo(value);
        }
    }

    @Test
    void testDecodeTime() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.TIME()}, new String[] {"time"});
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("time"));
        IcebergKeyDecoder decoder =
                new IcebergKeyDecoder(rowType, Collections.singletonList("time"));

        for (int value : new int[] {0, 34200000, 86399999}) {
            InternalRow original = row(value);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getInt(0)).isEqualTo(value);
        }
    }

    @Test
    void testDecodeTimestamp() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.TIMESTAMP(6)}, new String[] {"ts"});
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("ts"));
        IcebergKeyDecoder decoder = new IcebergKeyDecoder(rowType, Collections.singletonList("ts"));

        // Iceberg uses microsecond precision, so only test values that are multiples of 1000 nanos
        long[] millisValues = {0L, 1000L, 1698235273182L};
        int[] nanosValues = {
            0, 0, 123000, 999000
        }; // Must be multiples of 1000 for microsecond precision

        for (int i = 0; i < millisValues.length; i++) {
            InternalRow original =
                    row((Object) TimestampNtz.fromMillis(millisValues[i], nanosValues[i]));
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            TimestampNtz ts = decoded.getTimestampNtz(0, 6);
            assertThat(ts.getMillisecond()).isEqualTo(millisValues[i]);
            assertThat(ts.getNanoOfMillisecond()).isEqualTo(nanosValues[i]);
        }
    }

    @Test
    void testDecodeString() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"name"});
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("name"));
        IcebergKeyDecoder decoder =
                new IcebergKeyDecoder(rowType, Collections.singletonList("name"));

        for (String value : new String[] {"", "a", "Hello", "Hello Iceberg!", "UTF-8: 你好世界"}) {
            InternalRow original = row((Object) BinaryString.fromString(value));
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getString(0).toString()).isEqualTo(value);
        }
    }

    @Test
    void testDecodeBinary() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"data"});
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("data"));
        IcebergKeyDecoder decoder =
                new IcebergKeyDecoder(rowType, Collections.singletonList("data"));

        for (byte[] value :
                new byte[][] {new byte[0], "test".getBytes(), new byte[] {0, 1, 127, -128, -1}}) {
            InternalRow original = row((Object) value);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getBytes(0)).isEqualTo(value);
        }
    }

    @Test
    void testDecodeDecimal() {
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.DECIMAL(10, 2)}, new String[] {"amount"});
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("amount"));
        IcebergKeyDecoder decoder =
                new IcebergKeyDecoder(rowType, Collections.singletonList("amount"));

        for (String decimalStr : new String[] {"0.00", "123.45", "-999.99", "12345678.90"}) {
            BigDecimal value = new BigDecimal(decimalStr);
            Decimal decimal = Decimal.fromBigDecimal(value, 10, 2);
            InternalRow original = row((Object) decimal);
            byte[] encoded = encoder.encodeKey(original);
            InternalRow decoded = decoder.decodeKey(encoded);
            assertThat(decoded.getDecimal(0, 10, 2).toBigDecimal()).isEqualTo(value);
        }

        // Test high precision decimal
        RowType highPrecisionRowType =
                RowType.of(new DataType[] {DataTypes.DECIMAL(38, 10)}, new String[] {"big_amount"});
        IcebergKeyEncoder highPrecisionEncoder =
                new IcebergKeyEncoder(
                        highPrecisionRowType, Collections.singletonList("big_amount"));
        IcebergKeyDecoder highPrecisionDecoder =
                new IcebergKeyDecoder(
                        highPrecisionRowType, Collections.singletonList("big_amount"));
        BigDecimal highValue = new BigDecimal("1234567890123456789012345678.1234567890");
        Decimal highDecimal = Decimal.fromBigDecimal(highValue, 38, 10);
        InternalRow highOriginal = row((Object) highDecimal);
        byte[] encoded = highPrecisionEncoder.encodeKey(highOriginal);
        InternalRow decoded = highPrecisionDecoder.decodeKey(encoded);
        assertThat(decoded.getDecimal(0, 38, 10).toBigDecimal()).isEqualTo(highValue);
    }
}
