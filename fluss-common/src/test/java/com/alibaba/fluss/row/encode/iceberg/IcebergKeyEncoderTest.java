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

import com.alibaba.fluss.row.indexed.IndexedRow;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static com.alibaba.fluss.testutils.RowUtils.createRowWithBytes;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithDate;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithDecimal;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithInt;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithLong;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithString;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithTime;
import static com.alibaba.fluss.testutils.RowUtils.createRowWithTimestampNtz;
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

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(testValue);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.IntegerType.get(), testValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        // In Iceberg `Conversions` class, it treats the 'testValue' as an integer, the output bytes
        // array is 4 bytes length. (e.g. icebergEncoded = [42, 0, 0, 0])
        // But in key encoding, iceberg treats an integer as a long value, the output is a 8 bytes
        // array. (e.g. ourEncoded = [42, 0, 0, 0, 0, 0, 0, 0])
        // Thus we need to extract the first 4 bytes first from key encoded bytes array, and then
        // check the values.
        byte[] actualContent = Arrays.copyOfRange(ourEncoded, 0, 4);

        assertThat(actualContent).isEqualTo(icebergEncoded);
    }

    @Test
    void testLongEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"});

        long testValue = 1234567890123456789L;
        IndexedRow row = createRowWithLong(testValue);
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(testValue);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);

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

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = testValue.getBytes(StandardCharsets.UTF_8);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);

        // Decode length prefix
        int length = equivalentBytes.length;
        byte[] actualContent = Arrays.copyOfRange(ourEncoded, 0, length);

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

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = testValue.unscaledValue().toByteArray();
        assertThat(ourEncoded).isEqualTo(equivalentBytes);

        // Extract the decimal length prefix and bytes from ourEncoded
        int length = equivalentBytes.length;
        byte[] actualDecimal = Arrays.copyOfRange(ourEncoded, 0, length);

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
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(micros);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);

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

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(dateValue);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);

        // Encode with Iceberg's implementation
        ByteBuffer icebergBuffer = Conversions.toByteBuffer(Types.DateType.get(), dateValue);
        byte[] icebergEncoded = toByteArray(icebergBuffer);

        // In Iceberg `Conversions` class, it treats the 'testValue' as an integer, the output bytes
        // array is 4 bytes length. (e.g. icebergEncoded = [42, 0, 0, 0])
        // But in key encoding, iceberg treats an integer as a long value, the output is a 8 bytes
        // array. (e.g. ourEncoded = [42, 0, 0, 0, 0, 0, 0, 0])
        // Thus we need to extract the first 4 bytes first from key encoded bytes array, and then
        // check the values.
        byte[] actualContent = Arrays.copyOfRange(ourEncoded, 0, 4);

        assertThat(actualContent).isEqualTo(icebergEncoded);
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
        byte[] equivalentBytes = toBytes(timeMicros);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
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
        int length = testValue.length;
        byte[] actualContent = Arrays.copyOfRange(ourEncoded, 0, length);

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

    private byte[] toBytes(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }
}
