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

package org.apache.fluss.row.ordered;

import org.apache.fluss.predicate.CompareUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.decode.OrderedKeyDecoder;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.OrderedKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression coverage for the ordered key format and field-prefix seek boundaries. */
class OrderedKeyCodecTest {

    @ParameterizedTest
    @MethodSource("scalarValues")
    void testScalarOrderAndRoundTrip(DataType type, List<Object> values) {
        RowType schema = RowType.of(type);
        List<String> fields = schema.getFieldNames();
        OrderedKeyEncoder encoder = OrderedKeyEncoder.createKeyEncoder(schema, fields);
        OrderedKeyDecoder decoder = OrderedKeyDecoder.createKeyDecoder(schema, fields);
        InternalRow.FieldGetter getter = InternalRow.createFieldGetter(type, 0);
        List<byte[]> keys = new ArrayList<>();
        for (Object value : values) {
            byte[] key = encoder.encodeKey(GenericRow.of(value));
            Object decoded = getter.getFieldOrNull(decoder.decodeKey(key));
            assertThat(Objects.deepEquals(decoded, value)).as("Round trip of %s", value).isTrue();
            assertThat(encoder.encodeKey(GenericRow.of(decoded))).isEqualTo(key);
            keys.add(key);
        }
        for (int i = 0; i < values.size(); i++) {
            for (int j = 0; j < values.size(); j++) {
                assertThat(Integer.signum(compare(keys.get(i), keys.get(j))))
                        .as("%s: %s versus %s", type, values.get(i), values.get(j))
                        .isEqualTo(
                                Integer.signum(
                                        CompareUtils.compareLiteral(
                                                type, values.get(i), values.get(j))));
            }
        }
    }

    private static Stream<Arguments> scalarValues() {
        List<Arguments> cases = new ArrayList<>();
        cases.add(Arguments.of(DataTypes.BOOLEAN(), Arrays.asList(false, true)));
        cases.add(
                Arguments.of(
                        DataTypes.TINYINT(),
                        Arrays.asList(Byte.MIN_VALUE, (byte) -1, (byte) 0, Byte.MAX_VALUE)));
        cases.add(
                Arguments.of(
                        DataTypes.SMALLINT(),
                        Arrays.asList(
                                Short.MIN_VALUE,
                                (short) -1,
                                (short) 0,
                                (short) 255,
                                (short) 256,
                                Short.MAX_VALUE)));
        List<Object> ints =
                new ArrayList<>(
                        Arrays.asList(
                                Integer.MIN_VALUE,
                                -1,
                                0,
                                1,
                                127,
                                128,
                                255,
                                256,
                                16383,
                                16384,
                                Integer.MAX_VALUE));
        List<Object> longs =
                new ArrayList<>(
                        Arrays.asList(
                                Long.MIN_VALUE,
                                -1L,
                                0L,
                                1L,
                                127L,
                                128L,
                                255L,
                                256L,
                                Long.MAX_VALUE));
        Random random = new Random(81274);
        for (int i = 0; i < 100; i++) {
            ints.add(random.nextInt());
            longs.add(random.nextLong());
        }
        cases.add(Arguments.of(DataTypes.INT(), ints));
        cases.add(Arguments.of(DataTypes.DATE(), ints));
        cases.add(Arguments.of(DataTypes.TIME(), Arrays.asList(0, 1, 255, 256, 86_399_999)));
        cases.add(Arguments.of(DataTypes.BIGINT(), longs));
        List<Object> strings = new ArrayList<>();
        for (String string :
                Arrays.asList(
                        "",
                        "a",
                        "a\u0000",
                        "aa",
                        "b",
                        "1234567",
                        "12345678",
                        "123456789",
                        "1234567890123456",
                        "中",
                        "😀")) {
            strings.add(BinaryString.fromString(string));
        }
        cases.add(Arguments.of(DataTypes.STRING(), strings));
        cases.add(Arguments.of(DataTypes.CHAR(16), strings));
        List<Object> binaries = new ArrayList<>();
        binaries.add(new byte[0]);
        binaries.add(new byte[] {(byte) 0x80});
        binaries.add(new byte[] {(byte) 0xff});
        binaries.add(new byte[] {0});
        binaries.add(new byte[] {0, 0});
        binaries.add(new byte[] {127});
        for (int size : new int[] {7, 8, 9, 16, 17}) {
            byte[] bytes = new byte[size];
            random.nextBytes(bytes);
            binaries.add(bytes);
        }
        cases.add(Arguments.of(DataTypes.BYTES(), binaries));
        List<Object> fixedBinaries = new ArrayList<>();
        for (int firstByte : new int[] {-128, -1, 0, 127}) {
            byte[] bytes = new byte[16];
            bytes[0] = (byte) firstByte;
            fixedBinaries.add(bytes);
        }
        cases.add(Arguments.of(DataTypes.BINARY(16), fixedBinaries));
        for (int precision : new int[] {1, 2, 3, 9, 18, 19, 38}) {
            int scale = precision / 2;
            List<Object> decimals = new ArrayList<>();
            BigDecimal maximum =
                    new BigDecimal(BigInteger.TEN.pow(precision).subtract(BigInteger.ONE), scale);
            for (BigDecimal value :
                    Arrays.asList(
                            maximum.negate(),
                            BigDecimal.valueOf(-1, scale),
                            BigDecimal.ZERO,
                            BigDecimal.valueOf(1, scale),
                            maximum)) {
                decimals.add(Decimal.fromBigDecimal(value, precision, scale));
            }
            cases.add(Arguments.of(DataTypes.DECIMAL(precision, scale), decimals));
        }
        for (int precision = 0; precision <= 9; precision++) {
            List<Object> ntz = new ArrayList<>();
            List<Object> ltz = new ArrayList<>();
            for (long millis : new long[] {-1000, 0, 1000}) {
                ntz.add(TimestampNtz.fromMillis(millis));
                ltz.add(TimestampLtz.fromEpochMillis(millis));
            }
            if (precision > 3) {
                int nanos = (int) Math.pow(10, 9 - precision);
                ntz.add(TimestampNtz.fromMillis(-1, nanos));
                ntz.add(TimestampNtz.fromMillis(0, nanos));
                ltz.add(TimestampLtz.fromEpochMillis(-1, nanos));
                ltz.add(TimestampLtz.fromEpochMillis(0, nanos));
            }
            cases.add(Arguments.of(DataTypes.TIMESTAMP(precision), ntz));
            cases.add(Arguments.of(DataTypes.TIMESTAMP_LTZ(precision), ltz));
        }
        return cases.stream();
    }

    @Test
    void testStableByteVectors() {
        assertThat(encode(DataTypes.INT(), -1)).containsExactly(0x7f, 0xff, 0xff, 0xff);
        assertThat(encode(DataTypes.INT(), 256)).containsExactly(0x80, 0, 1, 0);
        assertThat(encode(DataTypes.BIGINT(), 0L)).containsExactly(0x80, 0, 0, 0, 0, 0, 0, 0);
        assertThat(encode(DataTypes.STRING(), BinaryString.fromString("a")))
                .containsExactly(0x61, 0, 0, 0, 0, 0, 0, 0, 0xf8);
        assertThat(encode(DataTypes.STRING(), BinaryString.fromString("12345678")))
                .containsExactly(
                        0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0xff, 0, 0, 0, 0, 0, 0, 0,
                        0, 0xf7);
        assertThat(encode(DataTypes.BYTES(), new byte[] {(byte) 0x80, 0}))
                .containsExactly(0, 0x80, 0, 0, 0, 0, 0, 0, 0xf9);
        assertThat(
                        encode(
                                DataTypes.DECIMAL(2, 1),
                                Decimal.fromBigDecimal(new BigDecimal("-0.1"), 2, 1)))
                .containsExactly(0x7f);
        assertThat(encode(DataTypes.TIMESTAMP(9), TimestampNtz.fromMillis(0, 1)))
                .containsExactly(0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
    }

    @Test
    void testCompactedIntegerRegression() {
        RowType schema = RowType.of(DataTypes.INT());
        CompactedKeyEncoder compacted = new CompactedKeyEncoder(schema);
        assertThat(
                        compare(
                                compacted.encodeKey(GenericRow.of(255)),
                                compacted.encodeKey(GenericRow.of(256))))
                .isPositive();
        assertThat(compare(encode(DataTypes.INT(), 255), encode(DataTypes.INT(), 256)))
                .isNegative();
    }

    @Test
    void testCompoundPrefixesAndAllRangeEndpoints() {
        RowType schema =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.INT()},
                        new String[] {"group", "time", "id"});
        OrderedKeyEncoder full = OrderedKeyEncoder.createKeyEncoder(schema, schema.getFieldNames());
        OrderedKeyEncoder prefix =
                OrderedKeyEncoder.createKeyEncoder(schema, Collections.singletonList("group"));
        OrderedKeyEncoder endpoint =
                OrderedKeyEncoder.createKeyEncoder(schema, Arrays.asList("group", "time"));
        BinaryString group = BinaryString.fromString("12345678");
        byte[] p = prefix.encodeKey(GenericRow.of(group, 0L, 0));
        byte[] lower = endpoint.encodeKey(GenericRow.of(group, 255L, 0));
        byte[] upper = endpoint.encodeKey(GenericRow.of(group, 256L, 0));
        List<byte[]> keys = new ArrayList<>();
        for (long time : new long[] {254, 255, 256, 257}) {
            for (int id : new int[] {-1, 0, 1}) {
                byte[] key = full.encodeKey(GenericRow.of(group, time, id));
                assertThat(key).startsWith(p);
                keys.add(key);
            }
        }
        keys.add(full.encodeKey(GenericRow.of(BinaryString.fromString("123456789"), 255L, 0)));
        keys.sort(OrderedKeyCodecTest::compare);
        assertThat(select(keys, p, OrderedKeyCodec.prefixUpperBound(p))).hasSize(12);
        assertThat(select(keys, lower, upper)).hasSize(3);
        assertThat(select(keys, lower, OrderedKeyCodec.prefixUpperBound(upper))).hasSize(6);
        assertThat(select(keys, OrderedKeyCodec.prefixUpperBound(lower), upper)).isEmpty();
        assertThat(
                        select(
                                keys,
                                OrderedKeyCodec.prefixUpperBound(lower),
                                OrderedKeyCodec.prefixUpperBound(upper)))
                .hasSize(3);
        assertThat(select(keys, lower, lower)).isEmpty();
        assertThat(select(keys, lower, OrderedKeyCodec.prefixUpperBound(lower))).hasSize(3);
        assertThat(select(keys, p, upper)).hasSize(6);
        assertThat(select(keys, lower, OrderedKeyCodec.prefixUpperBound(p))).hasSize(9);
    }

    @Test
    void testProjectionAndBufferIndependence() {
        RowType schema =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});
        OrderedKeyEncoder encoder =
                OrderedKeyEncoder.createKeyEncoder(schema, Arrays.asList("name", "id"));
        OrderedKeyDecoder decoder =
                OrderedKeyDecoder.createKeyDecoder(schema, Arrays.asList("name", "id"));
        byte[] first = encoder.encodeKey(GenericRow.of(1, BinaryString.fromString("a")));
        encoder.encodeKey(GenericRow.of(2, BinaryString.fromString("b")));
        InternalRow result = decoder.decodeKey(first);
        assertThat(result.getString(0)).isEqualTo(BinaryString.fromString("a"));
        assertThat(result.getInt(1)).isEqualTo(1);
    }

    @Test
    void testRejectInvalidInput() {
        assertThatThrownBy(() -> encode(DataTypes.INT().copy(false), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Null key");
        for (DataType unsupported :
                Arrays.asList(
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                        RowType.of(DataTypes.INT()))) {
            RowType schema = RowType.of(unsupported);
            assertThatThrownBy(
                            () ->
                                    OrderedKeyEncoder.createKeyEncoder(
                                            schema, schema.getFieldNames()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported");
        }
        RowType schema = RowType.of(DataTypes.INT());
        assertThatThrownBy(
                        () -> OrderedKeyEncoder.createKeyEncoder(schema, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> OrderedKeyEncoder.createKeyEncoder(schema, Arrays.asList("f0", "f0")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                OrderedKeyEncoder.createKeyEncoder(
                                        schema, Collections.singletonList("missing")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> encode(DataTypes.INT(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Null key");
        assertThatThrownBy(
                        () ->
                                encode(
                                        DataTypes.DECIMAL(2, 1),
                                        Decimal.fromBigDecimal(new BigDecimal("1.23"), 3, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scale");
        assertThatThrownBy(
                        () ->
                                encode(
                                        DataTypes.DECIMAL(2, 0),
                                        Decimal.fromBigDecimal(new BigDecimal("100"), 3, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
        assertThatThrownBy(() -> encode(DataTypes.TIMESTAMP(3), TimestampNtz.fromMillis(0, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("precision");
        assertThatThrownBy(() -> encode(DataTypes.TIMESTAMP(0), TimestampNtz.fromMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("precision");
    }

    @Test
    void testRejectMalformedKeys() {
        assertThatThrownBy(() -> decode(DataTypes.BOOLEAN(), new byte[] {2}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> decode(DataTypes.INT(), new byte[] {0}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Truncated");
        assertThatThrownBy(() -> decode(DataTypes.BOOLEAN(), new byte[] {0, 0}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Trailing");
        byte[] invalidMarker = new byte[9];
        invalidMarker[8] = (byte) 0xf6;
        assertThatThrownBy(() -> decode(DataTypes.STRING(), invalidMarker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("marker");
        byte[] invalidPadding = new byte[9];
        invalidPadding[0] = 1;
        invalidPadding[8] = (byte) 0xf7;
        assertThatThrownBy(() -> decode(DataTypes.STRING(), invalidPadding))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("padding");
        byte[] missingTerminator = new byte[9];
        missingTerminator[8] = (byte) 0xff;
        assertThatThrownBy(() -> decode(DataTypes.STRING(), missingTerminator))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Truncated");
        assertThatThrownBy(() -> decode(DataTypes.DECIMAL(2, 0), new byte[] {(byte) 0xff}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
    }

    @Test
    void testUnboundedPrefixAndCarry() {
        assertThat(OrderedKeyCodec.prefixUpperBound(new byte[0])).isNull();
        assertThat(OrderedKeyCodec.prefixUpperBound(new byte[] {(byte) 0xff})).isNull();
        byte[] prefix = {1, (byte) 0xff, (byte) 0xff};
        assertThat(OrderedKeyCodec.prefixUpperBound(prefix)).containsExactly(2);
        assertThat(prefix).containsExactly(1, 0xff, 0xff);
    }

    private static byte[] encode(DataType type, Object value) {
        RowType schema = RowType.of(type);
        return OrderedKeyEncoder.createKeyEncoder(schema, schema.getFieldNames())
                .encodeKey(GenericRow.of(value));
    }

    private static InternalRow decode(DataType type, byte[] bytes) {
        RowType schema = RowType.of(type);
        return OrderedKeyDecoder.createKeyDecoder(schema, schema.getFieldNames()).decodeKey(bytes);
    }

    private static List<byte[]> select(List<byte[]> keys, byte[] start, byte[] end) {
        List<byte[]> selected = new ArrayList<>();
        for (byte[] key : keys) {
            if (compare(key, start) >= 0 && (end == null || compare(key, end) < 0)) {
                selected.add(key);
            }
        }
        return selected;
    }

    private static int compare(byte[] left, byte[] right) {
        for (int i = 0; i < Math.min(left.length, right.length); i++) {
            int comparison = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
