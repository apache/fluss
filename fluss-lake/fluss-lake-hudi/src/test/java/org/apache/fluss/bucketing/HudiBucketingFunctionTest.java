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
import org.apache.fluss.row.TimestampLtz;
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
import java.time.Instant;
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
        // NOTE: ',' is Hudi's record-part separator and is rejected by the encoder — use a
        // delimiter-free message here. (See HudiKeyEncoder Javadoc for details.)
        String testValue = "Hello Hudi - Fluss this side!";
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
        String testStringValue = "Hello Hudi - Fluss this side!";
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
    void testSingleFieldNull_keepsPlaceholderString_matchesHudi() {
        // SINGLE-field mode: Hudi's KeyGenUtils.extractRecordKeysByFields takes the
        // fast path (no ',' AND no ':' in the recordKey) and returns the raw string
        // verbatim — i.e. the literal "__null__" — without round-tripping it back to
        // a real null. The strong-aligned encoder must mirror that and hash the
        // placeholder string, NOT a null element.
        int bucketNum = 10;
        String key = "name";

        RowType rowType =
                RowType.of(new DataType[] {DataTypes.STRING().copy(true)}, new String[] {key});

        GenericRow row = GenericRow.of((Object) null);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));

        byte[] ourEncodedKey = encoder.encodeKey(row);
        byte[] placeholderStringBytes = toBytes(new String[] {NULL_RECORDKEY_PLACEHOLDER});
        byte[] literalNullStringBytes = toBytes(new String[] {"null"});

        assertThat(ourEncodedKey).isEqualTo(placeholderStringBytes);
        assertThat(ourEncodedKey).isNotEqualTo(literalNullStringBytes);

        // Cross-validate against Hudi's recordKey-string overload (production path).
        int hudiBucket = BucketIdentifier.getBucketId(NULL_RECORDKEY_PLACEHOLDER, key, bucketNum);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);
        assertThat(ourBucket).isEqualTo(hudiBucket);
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

    @Test
    void testCompositeBucketKeyMatchesHudiRecordKeyOverload() {
        // Strong alignment: go through Hudi's production parse path
        // (BucketIdentifier#getBucketId(String recordKey, ...)) instead of the
        // pre-parsed List<String> overload, so any future divergence in parsing
        // (including ':'-handling for timestamps) is caught.
        int bucketNum = 16;
        String f1 = "user_id";
        String f2 = "region";
        int idValue = 12345;
        String regionValue = "cn-north";

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {f1, f2});
        GenericRow row = GenericRow.of(idValue, BinaryString.fromString(regionValue));
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Arrays.asList(f1, f2));

        byte[] ourEncodedKey = encoder.encodeKey(row);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);

        String recordKey = f1 + ":" + idValue + "," + f2 + ":" + regionValue;
        int hudiBucket = BucketIdentifier.getBucketId(recordKey, f1 + "," + f2, bucketNum);
        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testCompositeBucketKeyWithNullFieldMatchesHudiRecordKeyOverload() {
        // Hudi serializes a null bucket-key field as "__null__" in the record key, then
        // parses it back to a `null` element before hashing. The strong-aligned encoder
        // must produce a bucket id identical to Hudi's recordKey-string parse path.
        int bucketNum = 8;
        String f1 = "user_id";
        String f2 = "region";

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT().copy(true), DataTypes.STRING().copy(true)},
                        new String[] {f1, f2});
        // f2 is null on purpose
        GenericRow row = GenericRow.of(42, null);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Arrays.asList(f1, f2));

        byte[] ourEncodedKey = encoder.encodeKey(row);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);

        // The on-wire record key Hudi would have built for this row.
        String recordKey = f1 + ":42," + f2 + ":" + NULL_RECORDKEY_PLACEHOLDER;
        int hudiBucket = BucketIdentifier.getBucketId(recordKey, f1 + "," + f2, bucketNum);
        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testBooleanAndIntegralTypes() {
        int bucketNum = 10;
        // BOOLEAN
        assertSingleFieldRoundTrip(
                DataTypes.BOOLEAN(), true, String.valueOf(true), "flag", bucketNum);
        assertSingleFieldRoundTrip(
                DataTypes.BOOLEAN(), false, String.valueOf(false), "flag", bucketNum);
        // TINYINT / SMALLINT
        assertSingleFieldRoundTrip(DataTypes.TINYINT(), (byte) 7, "7", "b", bucketNum);
        assertSingleFieldRoundTrip(DataTypes.SMALLINT(), (short) 12345, "12345", "s", bucketNum);
        // FLOAT (use a value whose Float.toString is stable across JVMs)
        assertSingleFieldRoundTrip(DataTypes.FLOAT(), 3.5f, Float.toString(3.5f), "f", bucketNum);
    }

    @Test
    void testDateAndTimeTypes() {
        int bucketNum = 10;
        // DATE is stored as days-since-epoch int internally; record key is its String value.
        int dateDays = 19852;
        assertSingleFieldRoundTrip(
                DataTypes.DATE(), dateDays, String.valueOf(dateDays), "d", bucketNum);
        // TIME is stored as ms-of-day int.
        int timeMillis = 12345678;
        assertSingleFieldRoundTrip(
                DataTypes.TIME(), timeMillis, String.valueOf(timeMillis), "t", bucketNum);
    }

    @Test
    void testTimestampLtzType() throws IOException {
        int bucketNum = 10;
        TimestampLtz ts = TimestampLtz.fromInstant(Instant.ofEpochMilli(1700000000000L));
        RowType rowType =
                RowType.of(new DataType[] {DataTypes.TIMESTAMP_LTZ(6)}, new String[] {"ts"});
        GenericRow row = GenericRow.of(ts);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList("ts"));
        byte[] enc = encoder.encodeKey(row);
        byte[] expected = toBytes(new String[] {ts.toString()});
        assertThat(enc).isEqualTo(expected);

        int hudiBucket =
                BucketIdentifier.getBucketId(Collections.singletonList(ts.toString()), bucketNum);
        int ourBucket = new HudiBucketingFunction().bucketing(enc, bucketNum);
        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testBucketingNumBucketsBoundaryValues() {
        HudiBucketingFunction f = new HudiBucketingFunction();
        // numBuckets == 1 => bucket id always 0
        for (int sample : new int[] {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            byte[] key = intToBytes(sample);
            assertThat(f.bucketing(key, 1)).isEqualTo(0);
        }
        // numBuckets == Integer.MAX_VALUE: bucket id == hash & MAX_VALUE
        int hash = Integer.MIN_VALUE; // stress sign-bit handling
        byte[] key = intToBytes(hash);
        int expected = (hash & Integer.MAX_VALUE) % Integer.MAX_VALUE;
        assertThat(f.bucketing(key, Integer.MAX_VALUE)).isEqualTo(expected);
        // bucket id must always be in [0, numBuckets)
        assertThat(f.bucketing(key, 7)).isGreaterThanOrEqualTo(0).isLessThan(7);
    }

    private void assertSingleFieldRoundTrip(
            DataType dataType, Object value, String stringified, String key, int bucketNum) {
        RowType rowType = RowType.of(new DataType[] {dataType}, new String[] {key});
        GenericRow row = GenericRow.of(value);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Collections.singletonList(key));
        byte[] ourEncodedKey = encoder.encodeKey(row);
        byte[] expected = toBytes(new String[] {stringified});
        assertThat(ourEncodedKey).isEqualTo(expected);

        int hudiBucket = BucketIdentifier.getBucketId(stringified, key, bucketNum);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);
        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    private static byte[] intToBytes(int v) {
        return new byte[] {(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    private byte[] toBytes(String[] value) {
        List<String> values = Arrays.asList(value);
        return ByteBuffer.allocate(4).putInt(values.hashCode()).array();
    }

    // ------------------------------------------------------------------
    // End-to-end tests against Hudi's recordKey-string overloads — these
    // exercise the same parsing path Hudi takes in production (HoodieKey
    // -> getHashKeys via ':' / ',' split) and therefore catch divergence
    // that the List<String>-based overload would silently hide.
    // ------------------------------------------------------------------

    @Test
    void testCompositeBucketKeyMatchesHudiRecordKeyOverload_safeValues() {
        // Values intentionally free of ':' and ',' — the encoder must agree with Hudi's
        // record-key-parsing path under the documented contract.
        int bucketNum = 16;
        String f1 = "user_id";
        String f2 = "region";
        int idValue = 12345;
        String regionValue = "cn-north";

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {f1, f2});
        GenericRow row = GenericRow.of(idValue, BinaryString.fromString(regionValue));
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Arrays.asList(f1, f2));

        byte[] ourEncodedKey = encoder.encodeKey(row);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);

        // Build Hudi's wire-format record key: "f1:v1,f2:v2", then go through the
        // recordKey-string overload that splits on ':' / ',' before hashing.
        String recordKey = f1 + ":" + idValue + "," + f2 + ":" + regionValue;
        int hudiBucket = BucketIdentifier.getBucketId(recordKey, f1 + "," + f2, bucketNum);

        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testCompositeKeyWithColonInValue_matchesHudiRecordKeyOverload() {
        // Hudi's KeyGenUtils.extractRecordKeysByFields has a dedicated look-ahead loop
        // that correctly handles values containing ':' (e.g. timestamps like
        // "2023-10-25T10:01:13.182Z"). The strong-aligned encoder must match Hudi
        // bit-for-bit on this case — not merely "document the divergence".
        int bucketNum = 8;
        String f1 = "tenant";
        String f2 = "ts";
        String tenant = "acme";
        TimestampLtz ts = TimestampLtz.fromInstant(Instant.ofEpochMilli(1700000000000L));

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ(6)},
                        new String[] {f1, f2});
        GenericRow row = GenericRow.of(BinaryString.fromString(tenant), ts);
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Arrays.asList(f1, f2));

        byte[] ourEncodedKey = encoder.encodeKey(row);
        int ourBucket = new HudiBucketingFunction().bucketing(ourEncodedKey, bucketNum);

        String recordKey = f1 + ":" + tenant + "," + f2 + ":" + ts.toString();
        int hudiBucket = BucketIdentifier.getBucketId(recordKey, f1 + "," + f2, bucketNum);

        // Strong alignment: bucket ids MUST match.
        assertThat(ourBucket).isEqualTo(hudiBucket);
    }

    @Test
    void testRejectsValueContainingRecordSeparator() {
        // ',' is Hudi's record-part separator and is NOT escaped by Hudi's record-key
        // serialization. The encoder must refuse such values rather than silently
        // diverge from Hudi's bucket layout.
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"tenant", "name"});
        GenericRow row =
                GenericRow.of(
                        BinaryString.fromString("acme"), BinaryString.fromString("smith, john"));
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Arrays.asList("tenant", "name"));

        assertThatThrownBy(() -> encoder.encodeKey(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'name'")
                .hasMessageContaining("','");
    }

    @Test
    void testRejectsLiteralPlaceholderValueInCompositeKey() {
        // Composite-key mode: a user-supplied value equal to Hudi's reserved placeholder
        // would be round-tripped to null by Hudi's parser, silently changing the bucket id.
        // (In single-field mode the placeholder is kept verbatim by Hudi, so it's safe.)
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"tenant", "name"});
        GenericRow row =
                GenericRow.of(
                        BinaryString.fromString("acme"),
                        BinaryString.fromString(NULL_RECORDKEY_PLACEHOLDER));
        HudiKeyEncoder encoder = new HudiKeyEncoder(rowType, Arrays.asList("tenant", "name"));

        assertThatThrownBy(() -> encoder.encodeKey(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved record-key placeholder");
    }

    @Test
    void testRejectsUnsupportedBucketKeyType() {
        // BYTES / BINARY would otherwise fall through to Object#toString() and produce
        // instance-bound bucket ids (e.g. "[B@1a2b3c"). The encoder must refuse them up
        // front rather than silently corrupt the bucket layout.
        RowType rowType = RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"payload"});

        assertThatThrownBy(() -> new HudiKeyEncoder(rowType, Collections.singletonList("payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Hudi bucket key type")
                .hasMessageContaining("payload");
    }
}
