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

package com.alibaba.fluss.row.encode.iceberg;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IcebergKeyEncoder} to verify the encoding matches Iceberg's format.
 *
 * <p>This test uses Iceberg's actual Conversions class to ensure our encoding is byte-for-byte
 * compatible with Iceberg's implementation.
 */
class IcebergKeyEncoderTest {

    @Test
    void testSingleKeyFieldRequirement() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        // Should succeed with single key
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));
        assertThat(encoder).isNotNull();

        // Should fail with multiple keys
        assertThatThrownBy(() -> new IcebergKeyEncoder(rowType, Arrays.asList("id", "name")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Key fields must have exactly one field");
    }

    @Test
    void testIntegerEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});

        int testValue = 42;
        IndexedRow row = createRowWithInt(testValue);
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.IntegerType.get(), testValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testLongEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"});

        long testValue = 1234567890123456789L;
        IndexedRow row = createRowWithLong(testValue);
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.LongType.get(), testValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testStringEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"name"});

        String testValue = "Hello Iceberg, Fluss this side!";
        IndexedRow row = createRowWithString(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("name"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.StringType.get(), testValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testBooleanEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BOOLEAN()}, new String[] {"flag"});

        // Test true
        IndexedRow rowTrue = createRowWithBoolean(true);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("flag"));

        byte[] ourEncodedTrue = encoder.encodeKey(rowTrue);
        ByteBuffer icebergBufferTrue = Conversions.toByteBuffer(Types.BooleanType.get(), true);
        byte[] icebergEncodedTrue = toByteArray(icebergBufferTrue);

        assertThat(ourEncodedTrue).isEqualTo(icebergEncodedTrue);

        // Test false
        IndexedRow rowFalse = createRowWithBoolean(false);
        byte[] ourEncodedFalse = encoder.encodeKey(rowFalse);
        ByteBuffer icebergBufferFalse = Conversions.toByteBuffer(Types.BooleanType.get(), false);
        byte[] icebergEncodedFalse = toByteArray(icebergBufferFalse);

        assertThat(ourEncodedFalse).isEqualTo(icebergEncodedFalse);
    }

    @Test
    void testFloatDoubleEncoding() {
        // Test float
        RowType floatRowType =
                RowType.of(new DataType[] {DataTypes.FLOAT()}, new String[] {"value"});

        float floatVal = 3.14f;
        IndexedRow floatRow = createRowWithFloat(floatVal);
        IcebergKeyEncoder floatEncoder =
                new IcebergKeyEncoder(floatRowType, Collections.singletonList("value"));

        byte[] ourEncodedFloat = floatEncoder.encodeKey(floatRow);
        ByteBuffer icebergBufferFloat = Conversions.toByteBuffer(Types.FloatType.get(), floatVal);
        byte[] icebergEncodedFloat = toByteArray(icebergBufferFloat);

        assertThat(ourEncodedFloat).isEqualTo(icebergEncodedFloat);

        // Test double
        RowType doubleRowType =
                RowType.of(new DataType[] {DataTypes.DOUBLE()}, new String[] {"value"});

        double doubleVal = 3.14159265359;
        IndexedRow doubleRow = createRowWithDouble(doubleVal);
        IcebergKeyEncoder doubleEncoder =
                new IcebergKeyEncoder(doubleRowType, Collections.singletonList("value"));

        byte[] ourEncodedDouble = doubleEncoder.encodeKey(doubleRow);
        ByteBuffer icebergBufferDouble =
                Conversions.toByteBuffer(Types.DoubleType.get(), doubleVal);
        byte[] icebergEncodedDouble = toByteArray(icebergBufferDouble);

        assertThat(ourEncodedDouble).isEqualTo(icebergEncodedDouble);
    }

    @Test
    void testDecimalEncoding() {
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.DECIMAL(10, 2)}, new String[] {"amount"});

        BigDecimal testValue = new BigDecimal("123.45");
        IndexedRow row = createRowWithDecimal(testValue, 10, 2);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("amount"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        Type.PrimitiveType decimalType = Types.DecimalType.of(10, 2);
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(decimalType, testValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testTimestampEncoding() {
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.TIMESTAMP(6)}, new String[] {"event_time"});

        // Iceberg expects microseconds for TIMESTAMP type
        long millis = 1698235273182L;
        int nanos = 123456;
        long micros = millis * 1000 + (nanos / 1000);

        IndexedRow row = createRowWithTimestampNtz(millis, nanos);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("event_time"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer =
                Conversions.toByteBuffer(Types.TimestampType.withoutZone(), micros);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testTimestampWithTimezoneEncoding() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.TIMESTAMP_LTZ(6)}, new String[] {"event_time"});

        // Iceberg expects microseconds for TIMESTAMP type
        long millis = 1698235273182L;
        int nanos = 45678;
        long micros = millis * 1000 + (nanos / 1000);

        IndexedRow row = createRowWithTimestampLtz(millis, nanos);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("event_time"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.TimestampType.withZone(), micros);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testDateEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.DATE()}, new String[] {"date"});

        // Date value as days since epoch
        int dateValue = 19655; // 2023-10-25
        IndexedRow row = createRowWithDate(dateValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("date"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.DateType.get(), dateValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testTimeEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.TIME()}, new String[] {"time"});

        // Fluss stores time as int (milliseconds since midnight)
        int timeMillis = 34200000;
        long timeMicros = timeMillis * 1000L; // Convert to microseconds for Iceberg

        IndexedRow row = createRowWithTime(timeMillis);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("time"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation (expects microseconds as long)
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.TimeType.get(), timeMicros);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    @Test
    void testBinaryEncoding() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"data"});

        byte[] testValue = "Hello i only understand binary data".getBytes();
        IndexedRow row = createRowWithBytes(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("data"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Encode with Iceberg's implementation
        // Iceberg expects ByteBuffer for BINARY type
        ByteBuffer icebergBuffer =
                Conversions.toByteBuffer(Types.BinaryType.get(), ByteBuffer.wrap(testValue));
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        assertThat(ourEncoded).isEqualTo(icebergEncoded);
    }

    // Helper method to convert ByteBuffer to byte array
    private byte[] toByteArray(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }

    // ---- Helper methods to create IndexedRow instances ----

    private IndexedRow createRowWithInt(int value) {
        DataType[] dataTypes = {DataTypes.INT()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(value);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithLong(long value) {
        DataType[] dataTypes = {DataTypes.BIGINT()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeLong(value);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithString(String value) {
        DataType[] dataTypes = {DataTypes.STRING()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeString(BinaryString.fromString(value));
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithBoolean(boolean value) {
        DataType[] dataTypes = {DataTypes.BOOLEAN()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeBoolean(value);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithFloat(float value) {
        DataType[] dataTypes = {DataTypes.FLOAT()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeFloat(value);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithDouble(double value) {
        DataType[] dataTypes = {DataTypes.DOUBLE()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeDouble(value);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithDecimal(BigDecimal value, int precision, int scale) {
        DataType[] dataTypes = {DataTypes.DECIMAL(precision, scale)};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeDecimal(Decimal.fromBigDecimal(value, precision, scale), precision);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithTimestampNtz(long millis, int nanos) {
        DataType[] dataTypes = {DataTypes.TIMESTAMP(6)};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(millis, nanos), 6);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithTimestampLtz(long millis, int nanos) {
        DataType[] dataTypes = {DataTypes.TIMESTAMP_LTZ(6)};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(millis, nanos), 6);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithDate(int days) {
        DataType[] dataTypes = {DataTypes.DATE()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(days);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithTime(int millis) {
        DataType[] dataTypes = {DataTypes.TIME()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(millis); // Fluss stores TIME as int (milliseconds)
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private IndexedRow createRowWithBytes(byte[] value) {
        DataType[] dataTypes = {DataTypes.BYTES()};
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeBytes(value);
        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }
}
