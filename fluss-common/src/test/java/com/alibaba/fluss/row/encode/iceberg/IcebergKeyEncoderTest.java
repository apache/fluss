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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;

import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
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
    void testIntegerEncoding() throws IOException {
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
    void testLongEncoding() throws IOException {
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
    void testStringEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"name"});

        String testValue = "Hello Iceberg, Fluss this side!";
        IndexedRow row = createRowWithString(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("name"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Decode length prefix
        int length = ByteBuffer.wrap(ourEncoded, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] actualContent = Arrays.copyOfRange(ourEncoded, 4, 4 + length);

        // Encode with Iceberg's Conversions
        byte[] expectedContent =
                toByteArray(Conversions.toByteBuffer(Types.StringType.get(), testValue));

        // Validate length and content
        assertThat(length).isEqualTo(expectedContent.length);
        assertThat(actualContent).isEqualTo(expectedContent);
    }

    @Test
    void testDecimalEncoding() throws IOException {
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.DECIMAL(10, 2)}, new String[] {"amount"});

        BigDecimal testValue = new BigDecimal("123.45");
        IndexedRow row = createRowWithDecimal(testValue, 10, 2);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("amount"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Extract the decimal length prefix and bytes from ourEncoded
        int length = ByteBuffer.wrap(ourEncoded, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] actualDecimal = Arrays.copyOfRange(ourEncoded, 4, 4 + length);

        // Encode the same value with Iceberg's implementation (no prefix)
        Type.PrimitiveType decimalType = Types.DecimalType.of(10, 2);
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(decimalType, testValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        // Validate that our content (excluding the prefix) matches Iceberg's encoding
        assertThat(length).isEqualTo(icebergEncoded.length);
        assertThat(actualDecimal).isEqualTo(icebergEncoded);
    }

    @Test
    void testTimestampEncoding() throws IOException {
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
    void testDateEncoding() throws IOException {
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
    void testTimeEncoding() throws IOException {
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
    void testBinaryEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"data"});

        byte[] testValue = "Hello i only understand binary data".getBytes();
        IndexedRow row = createRowWithBytes(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("data"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // Decode length prefix
        int length = ByteBuffer.wrap(ourEncoded, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] actualContent = Arrays.copyOfRange(ourEncoded, 4, 4 + length);

        // Encode using Iceberg's Conversions (input should be ByteBuffer)
        ByteBuffer icebergBuffer =
                Conversions.toByteBuffer(Types.BinaryType.get(), ByteBuffer.wrap(testValue));
        byte[] expectedContent = toByteArray(icebergBuffer);

        // Validate length and content
        assertThat(length).isEqualTo(expectedContent.length);
        assertThat(actualContent).isEqualTo(expectedContent);
    }

    // Helper method to convert ByteBuffer to byte array
    private byte[] toByteArray(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }

    // ---- Helper methods to create IndexedRow instances ----

    private IndexedRow createRowWithInt(int value) throws IOException {
        DataType[] dataTypes = {DataTypes.INT()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeInt(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithLong(long value) throws IOException {
        DataType[] dataTypes = {DataTypes.BIGINT()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeLong(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithString(String value) throws IOException {
        DataType[] dataTypes = {DataTypes.STRING()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeString(BinaryString.fromString(value));
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithBoolean(boolean value) throws IOException {
        DataType[] dataTypes = {DataTypes.BOOLEAN()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeBoolean(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithFloat(float value) throws IOException {
        DataType[] dataTypes = {DataTypes.FLOAT()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeFloat(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithDouble(double value) throws IOException {
        DataType[] dataTypes = {DataTypes.DOUBLE()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeDouble(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithDecimal(BigDecimal value, int precision, int scale)
            throws IOException {
        DataType[] dataTypes = {DataTypes.DECIMAL(precision, scale)};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeDecimal(Decimal.fromBigDecimal(value, precision, scale), precision);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithTimestampNtz(long millis, int nanos) throws IOException {
        DataType[] dataTypes = {DataTypes.TIMESTAMP(6)};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeTimestampNtz(TimestampNtz.fromMillis(millis, nanos), 6);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithTimestampLtz(long millis, int nanos) throws IOException {
        DataType[] dataTypes = {DataTypes.TIMESTAMP_LTZ(6)};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(millis, nanos), 6);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithDate(int days) throws IOException {
        DataType[] dataTypes = {DataTypes.DATE()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeInt(days);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithTime(int millis) throws IOException {
        DataType[] dataTypes = {DataTypes.TIME()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeInt(millis); // Fluss stores TIME as int (milliseconds)
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            writer.close();
            return row;
        }
    }

    private IndexedRow createRowWithBytes(byte[] value) throws IOException {
        DataType[] dataTypes = {DataTypes.BYTES()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeBytes(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    private IndexedRow createRowWithTimestampNtz(long millis, int nanos, DataType type)
            throws IOException {
        DataType[] dataTypes = {DataTypes.BYTES()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeTimestampNtz(TimestampNtz.fromMillis(millis, nanos), getPrecision(type));
            IndexedRow row = new IndexedRow(new DataType[] {type});
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }
}
