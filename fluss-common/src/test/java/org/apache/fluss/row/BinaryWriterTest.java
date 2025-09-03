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

package org.apache.fluss.row;

import org.apache.fluss.record.TestData;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link BinaryWriter} interface methods. */
class BinaryWriterTest {

    private BinaryRowData row;
    private BinaryRowWriter writer;

    @BeforeEach
    void setUp() {
        // Create a row with enough fields to test all data types
        row = new BinaryRowData(15);
        writer = new BinaryRowWriter(row, 100);
    }

    @Test
    void testReset() {
        // Write some data first
        writer.writeInt(0, 42);
        writer.writeString(1, BinaryString.fromString("test"));
        writer.complete();

        // Verify data is written
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("test");

        // Reset and write new data
        writer.reset();
        writer.writeInt(0, 100);
        writer.writeString(1, BinaryString.fromString("reset"));
        writer.complete();

        // Verify new data is written
        assertThat(row.getInt(0)).isEqualTo(100);
        assertThat(row.getString(1).toString()).isEqualTo("reset");
    }

    @Test
    void testSetNullAt() {
        writer.setNullAt(0);
        writer.writeInt(1, 42);
        writer.setNullAt(2);
        writer.complete();

        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(42);
        assertThat(row.isNullAt(2)).isTrue();
    }

    @Test
    void testWriteBoolean() {
        writer.writeBoolean(0, true);
        writer.writeBoolean(1, false);
        writer.complete();

        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getBoolean(1)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(bytes = {Byte.MIN_VALUE, -1, 0, 1, 127, Byte.MAX_VALUE})
    void testWriteByte(byte value) {
        writer.writeByte(0, value);
        writer.complete();

        assertThat(row.getByte(0)).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(shorts = {Short.MIN_VALUE, -1, 0, 1, 32767, Short.MAX_VALUE})
    void testWriteShort(short value) {
        writer.writeShort(0, value);
        writer.complete();

        assertThat(row.getShort(0)).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, 42, Integer.MAX_VALUE})
    void testWriteInt(int value) {
        writer.writeInt(0, value);
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, -1L, 0L, 1L, 42L, Long.MAX_VALUE})
    void testWriteLong(long value) {
        writer.writeLong(0, value);
        writer.complete();

        assertThat(row.getLong(0)).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(floats = {Float.MIN_VALUE, -1.5f, 0.0f, 1.5f, 42.5f, Float.MAX_VALUE})
    void testWriteFloat(float value) {
        writer.writeFloat(0, value);
        writer.complete();

        assertThat(row.getFloat(0)).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.MIN_VALUE, -1.5, 0.0, 1.5, 42.5, Double.MAX_VALUE})
    void testWriteDouble(double value) {
        writer.writeDouble(0, value);
        writer.complete();

        assertThat(row.getDouble(0)).isEqualTo(value);
    }

    @Test
    void testWriteString() {
        // Test empty string
        writer.writeString(0, BinaryString.fromString(""));
        // Test short string (fits in fixed part)
        writer.writeString(1, BinaryString.fromString("hello"));
        // Test long string (goes to variable part)
        writer.writeString(
                2,
                BinaryString.fromString(
                        "This is a very long string that should go to variable part"));
        // Test Unicode string
        writer.writeString(3, BinaryString.fromString("测试中文字符串"));
        // Test string with special characters
        writer.writeString(4, BinaryString.fromString("Hello\nWorld\t!"));
        writer.complete();

        assertThat(row.getString(0).toString()).isEmpty();
        assertThat(row.getString(1).toString()).isEqualTo("hello");
        assertThat(row.getString(2).toString())
                .isEqualTo("This is a very long string that should go to variable part");
        assertThat(row.getString(3).toString()).isEqualTo("测试中文字符串");
        assertThat(row.getString(4).toString()).isEqualTo("Hello\nWorld\t!");
    }

    @Test
    void testWriteBinary() {
        // Test empty byte array
        writer.writeBinary(0, new byte[0]);
        // Test small byte array (fits in fixed part)
        writer.writeBinary(1, new byte[] {1, 2, 3});
        // Test large byte array (goes to variable part)
        writer.writeBinary(2, new byte[] {1, -1, 5, 10, -10, 127, -128, 0, 50, -50});
        writer.complete();

        assertThat(row.getBinary(0, 0)).isEmpty();
        assertThat(row.getBinary(1, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(row.getBinary(2, 10))
                .isEqualTo(new byte[] {1, -1, 5, 10, -10, 127, -128, 0, 50, -50});
    }

    @Test
    void testWriteDecimal() {
        // Test compact decimal (precision <= 18) - cannot be null
        int precision1 = 4;
        int scale1 = 2;
        Decimal decimal1 = Decimal.fromUnscaledLong(555, precision1, scale1);
        writer.writeDecimal(0, decimal1, precision1);

        // Test non-compact decimal (precision > 18)
        int precision2 = 25;
        int scale2 = 5;
        Decimal decimal2 =
                Decimal.fromBigDecimal(BigDecimal.valueOf(123.456789), precision2, scale2);
        writer.writeDecimal(1, decimal2, precision2);

        // Test null decimal with non-compact precision (> 18)
        writer.writeDecimal(2, null, precision2);

        writer.complete();

        assertThat(row.getDecimal(0, precision1, scale1).toString()).isEqualTo("5.55");
        assertThat(row.getDecimal(1, precision2, scale2).toString()).isEqualTo("123.45679");
        assertThat(row.isNullAt(2)).isTrue();
    }

    @Test
    void testWriteTimestampLtz() {
        // Test compact timestamp (precision <= 3) - cannot be null
        int precision1 = 3;
        TimestampLtz timestamp1 = TimestampLtz.fromEpochMillis(123456789L);
        writer.writeTimestampLtz(0, timestamp1, precision1);

        // Test non-compact timestamp (precision > 3)
        int precision2 = 9;
        TimestampLtz timestamp2 =
                TimestampLtz.fromLocalDateTime(
                        LocalDateTime.of(2023, 12, 25, 10, 30, 45, 123456789));
        writer.writeTimestampLtz(1, timestamp2, precision2);

        // Test null timestamp with non-compact precision (> 3)
        writer.writeTimestampLtz(2, null, precision2);

        writer.complete();

        assertThat(row.getTimestampLtz(0, precision1)).isEqualTo(timestamp1);
        assertThat(row.getTimestampLtz(1, precision2)).isEqualTo(timestamp2);
        assertThat(row.isNullAt(2)).isTrue();
    }

    @Test
    void testWriteTimestampNtz() {
        // Test compact timestamp (precision <= 3) - cannot be null
        int precision1 = 3;
        TimestampNtz timestamp1 = TimestampNtz.fromMillis(123456789L);
        writer.writeTimestampNtz(0, timestamp1, precision1);

        // Test non-compact timestamp (precision > 3)
        int precision2 = 9;
        TimestampNtz timestamp2 =
                TimestampNtz.fromLocalDateTime(
                        LocalDateTime.of(2023, 12, 25, 10, 30, 45, 123456789));
        writer.writeTimestampNtz(1, timestamp2, precision2);

        // Test null timestamp with non-compact precision (> 3)
        writer.writeTimestampNtz(2, null, precision2);

        writer.complete();

        assertThat(row.getTimestampNtz(0, precision1)).isEqualTo(timestamp1);
        assertThat(row.getTimestampNtz(1, precision2)).isEqualTo(timestamp2);
        assertThat(row.isNullAt(2)).isTrue();
    }

    @Test
    void testComplete() {
        writer.writeInt(0, 42);
        writer.writeString(1, BinaryString.fromString("test"));

        // Before complete, the data might not be fully accessible
        writer.complete();

        // After complete, all data should be accessible
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("test");
    }

    @Test
    void testMultipleWriteOperations() {
        // Test writing multiple different types in sequence
        writer.writeBoolean(0, true);
        writer.writeByte(1, (byte) 42);
        writer.writeShort(2, (short) 1000);
        writer.writeInt(3, 50000);
        writer.writeLong(4, 1234567890L);
        writer.writeFloat(5, 3.14f);
        writer.writeDouble(6, 2.718281828);
        writer.writeString(7, BinaryString.fromString("hello"));
        writer.writeBinary(8, new byte[] {1, 2, 3, 4, 5});
        writer.setNullAt(9);
        writer.complete();

        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 42);
        assertThat(row.getShort(2)).isEqualTo((short) 1000);
        assertThat(row.getInt(3)).isEqualTo(50000);
        assertThat(row.getLong(4)).isEqualTo(1234567890L);
        assertThat(row.getFloat(5)).isEqualTo(3.14f);
        assertThat(row.getDouble(6)).isEqualTo(2.718281828);
        assertThat(row.getString(7).toString()).isEqualTo("hello");
        assertThat(row.getBinary(8, 5)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(row.isNullAt(9)).isTrue();
    }

    @Test
    void testReuseWriterWithDifferentData() {
        // First round of writing
        writer.writeInt(0, 42);
        writer.writeString(1, BinaryString.fromString("first"));
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("first");

        // Reset and write different data
        writer.reset();
        writer.writeInt(0, 100);
        writer.writeString(1, BinaryString.fromString("second"));
        writer.writeBoolean(2, true);
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(100);
        assertThat(row.getString(1).toString()).isEqualTo("second");
        assertThat(row.getBoolean(2)).isTrue();
    }

    @Test
    @SuppressWarnings("deprecation")
    void testDeprecatedWriteMethod() {
        // Test deprecated static write method with various data types
        BinaryRowData testRow = new BinaryRowData(10);
        BinaryRowWriter testWriter = new BinaryRowWriter(testRow);

        // Test Boolean
        BinaryWriter.write(testWriter, 0, true, DataTypes.BOOLEAN());
        // Test Integer
        BinaryWriter.write(testWriter, 1, 42, DataTypes.INT());
        // Test String
        BinaryWriter.write(testWriter, 2, BinaryString.fromString("test"), DataTypes.STRING());
        // Test Decimal
        DecimalType decimalType = DataTypes.DECIMAL(10, 2);
        Decimal decimal = Decimal.fromBigDecimal(BigDecimal.valueOf(123.45), 10, 2);
        BinaryWriter.write(testWriter, 3, decimal, decimalType);
        // Test Timestamp with local time zone
        LocalZonedTimestampType lzTimestampType = DataTypes.TIMESTAMP_LTZ(6);
        TimestampLtz timestampLtz = TimestampLtz.fromEpochMillis(123456789L);
        BinaryWriter.write(testWriter, 4, timestampLtz, lzTimestampType);
        // Test Timestamp without time zone
        TimestampType timestampType = DataTypes.TIMESTAMP(6);
        TimestampNtz timestampNtz = TimestampNtz.fromMillis(123456789L);
        BinaryWriter.write(testWriter, 5, timestampNtz, timestampType);

        testWriter.complete();

        assertThat(testRow.getBoolean(0)).isTrue();
        assertThat(testRow.getInt(1)).isEqualTo(42);
        assertThat(testRow.getString(2).toString()).isEqualTo("test");
        assertThat(testRow.getDecimal(3, 10, 2).toString()).isEqualTo("123.45");
        assertThat(testRow.getTimestampLtz(4, 6)).isEqualTo(timestampLtz);
        assertThat(testRow.getTimestampNtz(5, 6)).isEqualTo(timestampNtz);
    }

    @Test
    @SuppressWarnings("deprecation")
    void testDeprecatedWriteMethodUnsupportedType() {
        // Test that unsupported type throws exception
        BinaryRowData testRow = new BinaryRowData(1);
        BinaryRowWriter testWriter = new BinaryRowWriter(testRow);

        // Create an unsupported data type (using ARRAY as example)
        DataType arrayType = DataTypes.ARRAY(DataTypes.INT());

        assertThatThrownBy(() -> BinaryWriter.write(testWriter, 0, null, arrayType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Not support type");
    }

    @Test
    void testWriterLifecycle() {
        // Test proper lifecycle: reset -> write -> complete
        writer.reset();
        writer.writeInt(0, 42);
        writer.writeString(1, BinaryString.fromString("lifecycle"));
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("lifecycle");

        // Test another cycle
        writer.reset();
        writer.writeBoolean(0, false);
        writer.writeLong(2, 9876543210L);
        writer.complete();

        assertThat(row.getBoolean(0)).isFalse();
        assertThat(row.getLong(2)).isEqualTo(9876543210L);
    }

    @Test
    void testWriteWithTestDataValues() {
        // Use some values from TestData to ensure compatibility
        Object[] data = TestData.DATA1.get(0); // {1, "a"}

        writer.writeInt(0, (Integer) data[0]);
        writer.writeString(1, BinaryString.fromString((String) data[1]));
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(1);
        assertThat(row.getString(1).toString()).isEqualTo("a");
    }

    @Test
    void testBoundaryValues() {
        // Test boundary values for different data types
        writer.writeByte(0, Byte.MIN_VALUE);
        writer.writeByte(1, Byte.MAX_VALUE);
        writer.writeShort(2, Short.MIN_VALUE);
        writer.writeShort(3, Short.MAX_VALUE);
        writer.writeInt(4, Integer.MIN_VALUE);
        writer.writeInt(5, Integer.MAX_VALUE);
        writer.writeLong(6, Long.MIN_VALUE);
        writer.writeLong(7, Long.MAX_VALUE);
        writer.writeFloat(8, Float.MIN_VALUE);
        writer.writeFloat(9, Float.MAX_VALUE);
        writer.writeDouble(10, Double.MIN_VALUE);
        writer.writeDouble(11, Double.MAX_VALUE);
        writer.complete();

        assertThat(row.getByte(0)).isEqualTo(Byte.MIN_VALUE);
        assertThat(row.getByte(1)).isEqualTo(Byte.MAX_VALUE);
        assertThat(row.getShort(2)).isEqualTo(Short.MIN_VALUE);
        assertThat(row.getShort(3)).isEqualTo(Short.MAX_VALUE);
        assertThat(row.getInt(4)).isEqualTo(Integer.MIN_VALUE);
        assertThat(row.getInt(5)).isEqualTo(Integer.MAX_VALUE);
        assertThat(row.getLong(6)).isEqualTo(Long.MIN_VALUE);
        assertThat(row.getLong(7)).isEqualTo(Long.MAX_VALUE);
        assertThat(row.getFloat(8)).isEqualTo(Float.MIN_VALUE);
        assertThat(row.getFloat(9)).isEqualTo(Float.MAX_VALUE);
        assertThat(row.getDouble(10)).isEqualTo(Double.MIN_VALUE);
        assertThat(row.getDouble(11)).isEqualTo(Double.MAX_VALUE);
    }
}
