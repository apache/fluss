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

package org.apache.fluss.cli.util;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataTypeConverterTest {

    @Test
    void testConvertFromStringBoolean() {
        assertThat(DataTypeConverter.convertFromString("true", DataTypes.BOOLEAN()))
                .isEqualTo(true);
        assertThat(DataTypeConverter.convertFromString("false", DataTypes.BOOLEAN()))
                .isEqualTo(false);
        assertThat(DataTypeConverter.convertFromString("TRUE", DataTypes.BOOLEAN()))
                .isEqualTo(true);
    }

    @Test
    void testConvertFromStringTinyInt() {
        assertThat(DataTypeConverter.convertFromString("127", DataTypes.TINYINT()))
                .isEqualTo((byte) 127);
        assertThat(DataTypeConverter.convertFromString("-128", DataTypes.TINYINT()))
                .isEqualTo((byte) -128);
    }

    @Test
    void testConvertFromStringSmallInt() {
        assertThat(DataTypeConverter.convertFromString("32767", DataTypes.SMALLINT()))
                .isEqualTo((short) 32767);
        assertThat(DataTypeConverter.convertFromString("-32768", DataTypes.SMALLINT()))
                .isEqualTo((short) -32768);
    }

    @Test
    void testConvertFromStringInteger() {
        assertThat(DataTypeConverter.convertFromString("123", DataTypes.INT())).isEqualTo(123);
        assertThat(DataTypeConverter.convertFromString("-456", DataTypes.INT())).isEqualTo(-456);
    }

    @Test
    void testConvertFromStringBigInt() {
        assertThat(DataTypeConverter.convertFromString("9223372036854775807", DataTypes.BIGINT()))
                .isEqualTo(9223372036854775807L);
        assertThat(DataTypeConverter.convertFromString("-9223372036854775808", DataTypes.BIGINT()))
                .isEqualTo(-9223372036854775808L);
    }

    @Test
    void testConvertFromStringFloat() {
        assertThat(DataTypeConverter.convertFromString("3.14", DataTypes.FLOAT())).isEqualTo(3.14f);
        assertThat(DataTypeConverter.convertFromString("-2.5", DataTypes.FLOAT())).isEqualTo(-2.5f);
    }

    @Test
    void testConvertFromStringDouble() {
        assertThat(DataTypeConverter.convertFromString("3.141592653589793", DataTypes.DOUBLE()))
                .isEqualTo(3.141592653589793);
        assertThat(DataTypeConverter.convertFromString("-2.718281828", DataTypes.DOUBLE()))
                .isEqualTo(-2.718281828);
    }

    @Test
    void testConvertFromStringDecimal() {
        DataType decimalType = DataTypes.DECIMAL(10, 2);
        Decimal result = (Decimal) DataTypeConverter.convertFromString("123.45", decimalType);

        assertThat(result.toBigDecimal()).isEqualByComparingTo(new BigDecimal("123.45"));
    }

    @Test
    void testConvertFromStringChar() {
        BinaryString result =
                (BinaryString) DataTypeConverter.convertFromString("'A'", DataTypes.CHAR(1));
        assertThat(result.toString()).isEqualTo("A");
    }

    @Test
    void testConvertFromStringStringWithQuotes() {
        BinaryString result =
                (BinaryString) DataTypeConverter.convertFromString("'Alice'", DataTypes.STRING());
        assertThat(result.toString()).isEqualTo("Alice");
    }

    @Test
    void testConvertFromStringStringWithoutQuotes() {
        BinaryString result =
                (BinaryString) DataTypeConverter.convertFromString("Alice", DataTypes.STRING());
        assertThat(result.toString()).isEqualTo("Alice");
    }

    @Test
    void testConvertFromStringDate() {
        int days = (Integer) DataTypeConverter.convertFromString("2024-01-15", DataTypes.DATE());
        LocalDate date = LocalDate.ofEpochDay(days);
        assertThat(date).isEqualTo(LocalDate.of(2024, 1, 15));
    }

    @Test
    void testConvertFromStringTime() {
        int millis = (Integer) DataTypeConverter.convertFromString("10:30:45", DataTypes.TIME(0));
        LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000L);
        assertThat(time).isEqualTo(LocalTime.of(10, 30, 45));

        int millisWithFraction =
                (Integer) DataTypeConverter.convertFromString("10:30:45.123", DataTypes.TIME(3));
        LocalTime timeWithFraction = LocalTime.ofNanoOfDay(millisWithFraction * 1_000_000L);
        assertThat(timeWithFraction).isEqualTo(LocalTime.of(10, 30, 45, 123_000_000));
    }

    @Test
    void testConvertFromStringTimestamp() {
        TimestampNtz result =
                (TimestampNtz)
                        DataTypeConverter.convertFromString(
                                "2024-01-15T10:30:45", DataTypes.TIMESTAMP(0));
        assertThat(result.toLocalDateTime()).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45));

        TimestampNtz spacedResult =
                (TimestampNtz)
                        DataTypeConverter.convertFromString(
                                "2024-01-15 10:30:45", DataTypes.TIMESTAMP(0));
        assertThat(spacedResult.toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45));
    }

    @Test
    void testConvertFromStringTimestampLtz() {
        TimestampLtz result =
                (TimestampLtz)
                        DataTypeConverter.convertFromString(
                                "2024-01-15T10:30:45Z", DataTypes.TIMESTAMP_LTZ(0));
        assertThat(result.toInstant()).isEqualTo(Instant.parse("2024-01-15T10:30:45Z"));

        TimestampLtz preciseResult =
                (TimestampLtz)
                        DataTypeConverter.convertFromString(
                                "2024-01-15T10:30:45Z", DataTypes.TIMESTAMP_LTZ(3));
        assertThat(preciseResult.toInstant()).isEqualTo(Instant.parse("2024-01-15T10:30:45Z"));
    }

    @Test
    void testConvertFromStringNull() {
        assertThat(DataTypeConverter.convertFromString("NULL", DataTypes.INT())).isNull();
        assertThat(DataTypeConverter.convertFromString("null", DataTypes.INT())).isNull();
        assertThat(DataTypeConverter.convertFromString(null, DataTypes.INT())).isNull();
    }

    @Test
    void testConvertFromStringInvalidInteger() {
        assertThatThrownBy(() -> DataTypeConverter.convertFromString("abc", DataTypes.INT()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to parse value 'abc' for type INTEGER");
    }

    @Test
    void testConvertFromStringInvalidDate() {
        assertThatThrownBy(
                        () -> DataTypeConverter.convertFromString("invalid-date", DataTypes.DATE()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to parse value 'invalid-date' for type DATE");
    }

    @Test
    void testConvertToStringBoolean() {
        assertThat(DataTypeConverter.convertToString(true, DataTypes.BOOLEAN())).isEqualTo("true");
        assertThat(DataTypeConverter.convertToString(false, DataTypes.BOOLEAN()))
                .isEqualTo("false");
    }

    @Test
    void testConvertToStringInteger() {
        assertThat(DataTypeConverter.convertToString(123, DataTypes.INT())).isEqualTo("123");
        assertThat(DataTypeConverter.convertToString(-456, DataTypes.INT())).isEqualTo("-456");
    }

    @Test
    void testConvertToStringBigInt() {
        assertThat(DataTypeConverter.convertToString(9223372036854775807L, DataTypes.BIGINT()))
                .isEqualTo("9223372036854775807");
    }

    @Test
    void testConvertToStringDouble() {
        assertThat(DataTypeConverter.convertToString(3.14, DataTypes.DOUBLE())).isEqualTo("3.14");
    }

    @Test
    void testConvertToStringDecimal() {
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2);
        assertThat(DataTypeConverter.convertToString(decimal, DataTypes.DECIMAL(10, 2)))
                .isEqualTo("123.45");
    }

    @Test
    void testConvertToStringBinaryString() {
        BinaryString str = BinaryString.fromString("Alice");
        assertThat(DataTypeConverter.convertToString(str, DataTypes.STRING())).isEqualTo("Alice");
    }

    @Test
    void testConvertToStringDate() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        int days = (int) date.toEpochDay();
        assertThat(DataTypeConverter.convertToString(days, DataTypes.DATE()))
                .isEqualTo("2024-01-15");
    }

    @Test
    void testConvertToStringTime() {
        LocalTime time = LocalTime.of(10, 30, 45);
        int millis = (int) (time.toNanoOfDay() / 1_000_000L);
        assertThat(DataTypeConverter.convertToString(millis, DataTypes.TIME(0)))
                .isEqualTo("10:30:45");
    }

    @Test
    void testConvertToStringTimestamp() {
        TimestampNtz timestamp =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 1, 15, 10, 30, 45));
        assertThat(DataTypeConverter.convertToString(timestamp, DataTypes.TIMESTAMP(0)))
                .isEqualTo("2024-01-15 10:30:45");

        TimestampLtz ltz = TimestampLtz.fromInstant(Instant.parse("2024-01-15T10:30:45Z"));
        assertThat(DataTypeConverter.convertToString(ltz, DataTypes.TIMESTAMP_LTZ(0)))
                .isEqualTo("2024-01-15T10:30:45Z");
    }

    @Test
    void testConvertToStringNull() {
        assertThat(DataTypeConverter.convertToString(null, DataTypes.INT())).isEqualTo("NULL");
    }

    @Test
    void testGetFieldAsStringFromRow() {
        GenericRow row = new GenericRow(3);
        row.setField(0, 123);
        row.setField(1, BinaryString.fromString("Alice"));
        row.setField(2, 3.14);

        assertThat(DataTypeConverter.getFieldAsString(row, 0, DataTypes.INT())).isEqualTo("123");
        assertThat(DataTypeConverter.getFieldAsString(row, 1, DataTypes.STRING()))
                .isEqualTo("Alice");
        assertThat(DataTypeConverter.getFieldAsString(row, 2, DataTypes.DOUBLE()))
                .isEqualTo("3.14");
    }

    @Test
    void testGetFieldAsStringComplexTypes() {
        GenericRow row = new GenericRow(3);
        row.setField(0, new GenericArray(new Object[] {1, 2}));

        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("k1"), 1);
        map.put(BinaryString.fromString("k2"), 2);
        row.setField(1, new GenericMap(map));

        GenericRow nested = new GenericRow(2);
        nested.setField(0, BinaryString.fromString("Bob"));
        nested.setField(1, 30);
        row.setField(2, nested);

        assertThat(DataTypeConverter.getFieldAsString(row, 0, DataTypes.ARRAY(DataTypes.INT())))
                .isEqualTo("ARRAY[1, 2]");
        assertThat(
                        DataTypeConverter.getFieldAsString(
                                row, 1, DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))
                .isEqualTo("MAP[k1, 1, k2, 2]");
        assertThat(
                        DataTypeConverter.getFieldAsString(
                                row,
                                2,
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT()))))
                .isEqualTo("ROW(Bob, 30)");
    }

    @Test
    void testConvertFromStringComplexLiterals() {
        Object array =
                DataTypeConverter.convertFromString(
                        "ARRAY[1, 2, 3]", DataTypes.ARRAY(DataTypes.INT()));
        assertThat(array).isInstanceOf(GenericArray.class);

        Object map =
                DataTypeConverter.convertFromString(
                        "MAP['k1', 1, 'k2', 2]",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThat(map).isInstanceOf(GenericMap.class);

        Object row =
                DataTypeConverter.convertFromString(
                        "ROW('Alice', 30)",
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("age", DataTypes.INT())));
        assertThat(row).isInstanceOf(GenericRow.class);
    }

    @Test
    void testGetFieldAsStringNullValue() {
        GenericRow row = new GenericRow(1);
        row.setField(0, null);

        assertThat(DataTypeConverter.getFieldAsString(row, 0, DataTypes.INT())).isEqualTo("NULL");
    }

    @Test
    void testGetFieldValueFromRow() {
        GenericRow row = new GenericRow(5);
        row.setField(0, 123);
        row.setField(1, BinaryString.fromString("Alice"));
        row.setField(2, 3.14);
        row.setField(3, Decimal.fromBigDecimal(new BigDecimal("12.34"), 4, 2));
        row.setField(4, "bytes".getBytes());

        assertThat(DataTypeConverter.getFieldValue(row, 0, DataTypes.INT())).isEqualTo(123);
        assertThat(DataTypeConverter.getFieldValue(row, 1, DataTypes.STRING()).toString())
                .isEqualTo("Alice");
        assertThat(DataTypeConverter.getFieldValue(row, 2, DataTypes.DOUBLE())).isEqualTo(3.14);
        assertThat(DataTypeConverter.getFieldValue(row, 3, DataTypes.DECIMAL(4, 2)))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("12.34"), 4, 2));
        assertThat(DataTypeConverter.getFieldValue(row, 4, DataTypes.BYTES()))
                .isEqualTo("bytes".getBytes());
    }

    @Test
    void testGetFieldValueNullValue() {
        GenericRow row = new GenericRow(1);
        row.setField(0, null);

        assertThat(DataTypeConverter.getFieldValue(row, 0, DataTypes.INT())).isNull();
    }

    @Test
    void testConvertFromStringBytesHexFormat() {
        byte[] result =
                (byte[]) DataTypeConverter.convertFromString("0x48656c6c6f", DataTypes.BYTES());
        assertThat(new String(result)).isEqualTo("Hello");

        byte[] base64 = (byte[]) DataTypeConverter.convertFromString("SGVsbG8=", DataTypes.BYTES());
        assertThat(new String(base64)).isEqualTo("Hello");
    }

    @Test
    void testConvertToStringBytes() {
        byte[] bytes = "Hello".getBytes();
        String result = DataTypeConverter.convertToString(bytes, DataTypes.BYTES());
        assertThat(result).isEqualTo("0x48656c6c6f");
    }

    @Test
    void testConvertFromStringBytesInvalidOddLengthHex() {
        assertThatThrownBy(() -> DataTypeConverter.convertFromString("0x123", DataTypes.BYTES()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Hex string must have even length");
    }

    @Test
    void testConvertFromStringBytesInvalidHexCharacters() {
        assertThatThrownBy(() -> DataTypeConverter.convertFromString("0xZZZZ", DataTypes.BYTES()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid hex string");
    }

    @Test
    void testConvertFromStringBytesEmptyHex() {
        byte[] result = (byte[]) DataTypeConverter.convertFromString("0x", DataTypes.BYTES());
        assertThat(result).isEmpty();
    }

    @Test
    void testRoundTripConversion() {
        String originalValue = "123";
        DataType dataType = DataTypes.INT();

        Object converted = DataTypeConverter.convertFromString(originalValue, dataType);
        String backToString = DataTypeConverter.convertToString(converted, dataType);

        assertThat(backToString).isEqualTo(originalValue);
    }

    @Test
    void testRoundTripConversionString() {
        String originalValue = "'Alice'";
        DataType dataType = DataTypes.STRING();

        Object converted = DataTypeConverter.convertFromString(originalValue, dataType);
        String backToString = DataTypeConverter.convertToString(converted, dataType);

        assertThat(backToString).isEqualTo("Alice");
    }
}
