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

package org.apache.fluss.bucketing;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.hudi.HudiKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.hudi.index.bucket.BucketIdentifier;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.row.encode.hudi.HudiKeyEncoder.NULL_RECORDKEY_PLACEHOLDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link HudiBucketingFunction}. */
class HudiBucketingFunctionTest {

    @Test
    void testIntegerHash() {
        int testValue = 42;
        int bucketNum = 10;
        String key = "id";

        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {key});

        GenericRow row = GenericRow.of(testValue);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        String recordKey = String.valueOf(testValue);

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(new String[] {recordKey});
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(recordKey, key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testLongHash() {
        long testValue = 1234567890123456789L;
        int bucketNum = 10;
        String key = "id";

        RowType rowType = RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {key});

        GenericRow row = GenericRow.of(testValue);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        String recordKey = String.valueOf(testValue);

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(new String[] {recordKey});
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(recordKey, key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testStringHash() {
        String testValue = "Hello Hudi, Fluss this side!";
        int bucketNum = 10;
        String key = "name";

        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {key});

        GenericRow row = GenericRow.of(BinaryString.fromString(testValue));
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(new String[] {testValue});
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(testValue, key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testDecimalHash() {
        BigDecimal testValue = new BigDecimal("123.45");
        Decimal decimal = Decimal.fromBigDecimal(testValue, 10, 2);
        int bucketNum = 10;
        String key = "amount";

        RowType rowType = RowType.of(new DataType[] {DataTypes.DECIMAL(10, 2)}, new String[] {key});

        GenericRow row = GenericRow.of(decimal);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(new String[] {testValue.toPlainString()});
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(testValue.toPlainString(), key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testTimestampEncodingHash() throws IOException {
        long millis = 1698235273182L;
        int nanos = 123456;
        long micros = millis * 1000 + (nanos / 1000);
        TimestampNtz testValue = TimestampNtz.fromMillis(millis, nanos);
        int bucketNum = 10;
        String key = "event_time";

        RowType rowType = RowType.of(new DataType[] {DataTypes.TIMESTAMP(6)}, new String[] {key});

        GenericRow row = GenericRow.of(testValue);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(new String[] {testValue.toString()});
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(testValue.toString(), key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testDateHash() {
        int dateValue = 19655;
        int bucketNum = 10;
        String key = "date";

        RowType rowType = RowType.of(new DataType[] {DataTypes.DATE()}, new String[] {key});
        GenericRow row = GenericRow.of(dateValue);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes = toBytes(new String[] {String.valueOf(dateValue)});
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(String.valueOf(dateValue), key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testMultiKeysHashing() {
        int testIntValue = 42;
        long testLongValue = 1234567890123456789L;
        String testStringValue = "Hello Hudi, Fluss this side!";
        BigDecimal testValue = new BigDecimal("123.45");
        Decimal decimal = Decimal.fromBigDecimal(testValue, 10, 2);
        int bucketNum = 10;
        String key = "age,id,name,amount";
        String recordKey =
                "age:"
                        + testIntValue
                        + ",id:"
                        + testLongValue
                        + ",name:"
                        + testStringValue
                        + ",amount:"
                        + testValue;

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.DECIMAL(10, 2)
                        },
                        new String[] {"age", "id", "name", "amount"});
        GenericRow row =
                GenericRow.of(
                        testIntValue,
                        testLongValue,
                        BinaryString.fromString(testStringValue),
                        decimal);
        HudiKeyEncoder encoder =
                new HudiKeyEncoder(rowType, Arrays.asList("age", "id", "name", "amount"));

        // Encode with our implementation
        byte[] ourEncodedKey = encoder.encodeKey(row);
        // This is the equivalent bytes array which the key should be encoded to.
        byte[] equivalentBytes =
                toBytes(
                        new String[] {
                            String.valueOf(testIntValue),
                            String.valueOf(testLongValue),
                            testStringValue,
                            String.valueOf(decimal)
                        });
        assertThat(ourEncodedKey).isEqualTo(equivalentBytes);

        int hudiBucket = BucketIdentifier.getBucketId(recordKey, key, bucketNum);

        HudiBucketingFunction hudiBucketingFunction = new HudiBucketingFunction();
        int ourBucket = hudiBucketingFunction.bucketing(ourEncodedKey, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testNullFieldUsesPlaceholder() {
        int bucketNum = 10;
        String key = "name";

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING().copy(true)}, new String[] {key});

        // a row with an explicit null on the bucket key column
        GenericRow row = GenericRow.of((Object) null);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        // Encoded bytes must hash the placeholder, NOT the literal "null" string.
        byte[] ourEncodedKey = encoder.encodeKey(row);
        byte[] placeholderBytes = toBytes(new String[] {NULL_RECORDKEY_PLACEHOLDER});
        byte[] javaNullLiteralBytes = toBytes(new String[] {"null"});

        assertThat(ourEncodedKey).isEqualTo(placeholderBytes);
        assertThat(ourEncodedKey).isNotEqualTo(javaNullLiteralBytes);

        int hudiBucket =
                BucketIdentifier.getBucketId(NULL_RECORDKEY_PLACEHOLDER, key, bucketNum);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);
        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testNullFieldDoesNotCollideWithLiteralNullString() {
        // The literal string "null" must not produce the same encoded bytes as a real
        // null value — a regression test for the previous String.valueOf(null) behavior.
        String key = "name";
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING().copy(true)}, new String[] {key});

        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        byte[] encodedNull = encoder.encodeKey(GenericRow.of((Object) null));
        byte[] encodedLiteralNull =
                encoder.encodeKey(GenericRow.of(BinaryString.fromString("null")));

        assertThat(encodedNull).isNotEqualTo(encodedLiteralNull);
    }

    @Test
    void testBucketingRejectsInvalidBucketKey() {
        HudiBucketingFunction function = new HudiBucketingFunction();

        assertThatThrownBy(() -> function.bucketing(null, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null");

        // length 0 — wrong length
        assertThatThrownBy(() -> function.bucketing(new byte[0], 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly 4 bytes");

        // length < 4 — used to throw BufferUnderflowException, now must be IAE
        assertThatThrownBy(() -> function.bucketing(new byte[] {1, 2, 3}, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly 4 bytes");

        // length > 4 — used to silently truncate, now must be IAE
        assertThatThrownBy(() -> function.bucketing(new byte[] {1, 2, 3, 4, 5}, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly 4 bytes");
    }

    @Test
    void testBucketingRejectsNonPositiveNumBuckets() {
        HudiBucketingFunction function = new HudiBucketingFunction();
        byte[] anyKey = new byte[] {0, 0, 0, 1};

        assertThatThrownBy(() -> function.bucketing(anyKey, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
        assertThatThrownBy(() -> function.bucketing(anyKey, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    private byte[] toBytes(String[] value) {
        List<String> values = Arrays.asList(value);
        return ByteBuffer.allocate(4).putInt(values.hashCode()).array();
    }
}
