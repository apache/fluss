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

package org.apache.fluss.row.encode.iceberg;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link IcebergKeyEncoder} to verify the encoding matches Iceberg's format. */
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
        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(testValue);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
    }

    @Test
    void testLongEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"});

        long testValue = 1234567890123456789L;
        GenericRow row = GenericRow.of(testValue);
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
        GenericRow row = GenericRow.of(BinaryString.fromString(testValue));
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("name"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = testValue.getBytes(StandardCharsets.UTF_8);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
    }

    @Test
    void testDecimalEncoding() throws IOException {
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.DECIMAL(10, 2)}, new String[] {"amount"});

        BigDecimal testValue = new BigDecimal("123.45");
        Decimal decimal = Decimal.fromBigDecimal(testValue, 10, 2);
        GenericRow row = GenericRow.of(decimal);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("amount"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = testValue.unscaledValue().toByteArray();
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
    }

    @Test
    void testTimestampEncoding() throws IOException {
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.TIMESTAMP(6)}, new String[] {"event_time"});

        // Iceberg expects microseconds for TIMESTAMP type
        long millis = 1698235273182L;
        int nanos = 123456;
        long micros = millis * 1000 + (nanos / 1000);

        TimestampNtz testValue = TimestampNtz.fromMillis(millis, nanos);
        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("event_time"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(micros);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
    }

    @Test
    void testDateEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.DATE()}, new String[] {"date"});

        // Date value as days since epoch
        int dateValue = 19655; // 2023-10-25
        GenericRow row = GenericRow.of(dateValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("date"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);

        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(dateValue);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
    }

    @Test
    void testTimeEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.TIME()}, new String[] {"time"});

        // Fluss stores time as int (milliseconds since midnight)
        int timeMillis = 34200000;
        long timeMicros = timeMillis * 1000L; // Convert to microseconds for Iceberg

        GenericRow row = GenericRow.of(timeMillis);

        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("time"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);
        byte[] equivalentBytes = toBytes(timeMicros);
        assertThat(ourEncoded).isEqualTo(equivalentBytes);
    }

    @Test
    void testBinaryEncoding() throws IOException {
        RowType rowType = RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"data"});

        byte[] testValue = "Hello i only understand binary data".getBytes();
        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("data"));

        // Encode with our implementation
        byte[] ourEncoded = encoder.encodeKey(row);
        assertThat(ourEncoded).isEqualTo(testValue);
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
