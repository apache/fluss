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

package com.alibaba.fluss.bucketing;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.encode.iceberg.IcebergKeyEncoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link IcebergBucketingFunction}. */
class IcebergBucketingFunctionTest {

    @Test
    void testIntegerHash() throws IOException {
        int testValue = 42;
        int bucketNum = 10;

        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});

        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(testValue);
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.IntegerType.get()).apply(testValue);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testLongHash() throws IOException {
        long testValue = 1234567890123456789L;
        int bucketNum = 10;

        RowType rowType = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"});

        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(rowType, Collections.singletonList("id"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(testValue);
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.LongType.get()).apply(testValue);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testStringHash() throws IOException {
        String testValue = "Hello Iceberg, Fluss this side!";
        int bucketNum = 10;

        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"name"});

        GenericRow row = GenericRow.of(BinaryString.fromString(testValue));
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("name"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = testValue.getBytes(StandardCharsets.UTF_8);
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.StringType.get()).apply(testValue);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testDecimalHash() throws IOException {
        BigDecimal testValue = new BigDecimal("123.45");
        Decimal decimal = Decimal.fromBigDecimal(testValue, 10, 2);
        int bucketNum = 10;

        RowType rowType =
                RowType.of(new DataType[] {DataTypes.DECIMAL(10, 2)}, new String[] {"amount"});

        GenericRow row = GenericRow.of(decimal);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("amount"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = testValue.unscaledValue().toByteArray();
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.DecimalType.of(10, 2)).apply(testValue);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testTimestampEncodingHash() throws IOException {
        // Iceberg expects microseconds for TIMESTAMP type
        long millis = 1698235273182L;
        int nanos = 123456;
        long micros = millis * 1000 + (nanos / 1000);
        TimestampNtz testValue = TimestampNtz.fromMillis(millis, nanos);
        int bucketNum = 10;

        RowType rowType =
                RowType.of(new DataType[] {DataTypes.TIMESTAMP(6)}, new String[] {"event_time"});

        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("event_time"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(micros);
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.TimestampType.withoutZone()).apply(micros);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testDateHash() throws IOException {
        int dateValue = 19655;
        int bucketNum = 10;

        RowType rowType = RowType.of(new DataType[] {DataTypes.DATE()}, new String[] {"date"});
        GenericRow row = GenericRow.of(dateValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("date"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(dateValue);
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.DateType.get()).apply(dateValue);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testTimeHashing() throws IOException {
        // Fluss stores time as int (milliseconds since midnight)
        int timeMillis = 34200000;
        long timeMicros = timeMillis * 1000L; // Convert to microseconds for Iceberg
        int bucketNum = 10;

        RowType rowType = RowType.of(new DataType[] {DataTypes.TIME()}, new String[] {"time"});

        GenericRow row = GenericRow.of(timeMillis);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("time"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        byte[] equivalentBytes = toBytes(timeMicros);
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        int icebergBucket = transform.bind(Types.TimeType.get()).apply(timeMicros);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    @Test
    void testBinaryEncoding() throws IOException {
        byte[] testValue = "Hello i only understand binary data".getBytes();
        int bucketNum = 10;

        RowType rowType = RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"data"});

        GenericRow row = GenericRow.of(testValue);
        IcebergKeyEncoder encoder =
                new IcebergKeyEncoder(rowType, Collections.singletonList("data"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        assertThat(ourEncodedKey).isEqualTo(testValue);

        Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
        ByteBuffer byteBuffer = ByteBuffer.wrap(testValue);
        int icebergBucket = transform.bind(Types.BinaryType.get()).apply(byteBuffer);

        IcebergBucketingFunction icebergBucketingFunction = new IcebergBucketingFunction();
        int ourBucket = icebergBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(icebergBucket);
    }

    private byte[] toBytes(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }
}
