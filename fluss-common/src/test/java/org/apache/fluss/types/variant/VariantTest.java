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

package org.apache.fluss.types.variant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Variant}. */
class VariantTest {

    @ParameterizedTest
    @ValueSource(strings = {"\uD800", "\uDC00", "x\uD800y", "\uD800\uD800", "\uDC00\uD800"})
    void testRejectUnpairedSurrogates(String value) {
        assertThatThrownBy(() -> Variant.ofString(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("surrogate");
        assertThatThrownBy(() -> Variant.fromJson("\"" + value + "\""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("surrogate");
        assertThatThrownBy(() -> Variant.fromJson("{\"" + value + "\":1}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("surrogate");
    }

    @Test
    void testRejectEscapedSurrogateBeforeDictionaryDeduplication() {
        assertThatThrownBy(() -> Variant.fromJson("{\"\\uD800\":1,\"?\":2}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("surrogate");
        assertThatThrownBy(() -> Variant.fromJson("\"\\uDC00\""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("surrogate");
        Variant valid = Variant.fromJson("{\"\\uD83D\\uDE00\":\"\\uD834\\uDD1E\",\"?\":2}");
        assertThat(valid.getFieldByName("\uD83D\uDE00").getString()).isEqualTo("\uD834\uDD1E");
        assertThat(valid.getFieldByName("?").getInt()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"NaN", "Infinity", "-Infinity"})
    void testNonFiniteNumbersUseJsonStrings(String number) {
        Variant floating = Variant.ofFloat(Float.parseFloat(number));
        Variant doubling = Variant.ofDouble(Double.parseDouble(number));
        assertThat(floating.toJson()).isEqualTo('"' + number + '"');
        assertThat(doubling.toJson()).isEqualTo('"' + number + '"');
        assertThat(Variant.fromJson(floating.toJson()).getString()).isEqualTo(number);
        Variant nested =
                new Variant(
                        Variant.emptyMetadata(),
                        VariantUtil.encodeArray(Arrays.asList(floating.value(), doubling.value())));
        assertThat(Variant.fromJson(nested.toJson()).getElementAt(1).getString()).isEqualTo(number);
        assertThat(Variant.fromBytes(doubling.toBytes())).isEqualTo(doubling);
    }

    @Test
    void testEqualityComparesEncodingRatherThanNumericValue() {
        Variant wide = Variant.ofInt(1);
        Variant narrow = Variant.fromJson("1");
        assertThat(wide.getLong()).isEqualTo(narrow.getLong());
        assertThat(wide).isNotEqualTo(narrow);
        assertThat(wide.copy()).isEqualTo(wide).hasSameHashCodeAs(wide);
    }

    @Test
    void testRejectTruncatedSerializedValue() {
        byte[] binary = {3, 0, 0, 0, 1, 0, 0, 60, 2, 0, 0, 0, 42};
        byte[] integer = {3, 0, 0, 0, 1, 0, 0, 20};
        assertThatThrownBy(() -> Variant.fromBytes(binary))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Variant.fromBytes(integer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCompactObjectEncoding() {
        Variant variant = Variant.fromJson("{\"a\":1}");
        assertThat(variant.metadata()).containsExactly(0x11, 1, 0, 1, 'a');
        assertThat(variant.value()).containsExactly(2, 1, 0, 0, 2, 12, 1);
        assertThat(variant.sizeInBytes()).isEqualTo(12);
    }

    @Test
    void testNestedViewAndIndependentCopy() {
        Variant parent =
                Variant.fromJson(
                        "{\"items\":[0,{\"name\":\"second\",\"value\":42}],\"tail\":false}");
        Variant view = parent.getFieldByName("items").getElementAt(1);
        assertThat(view.toJson()).isEqualTo("{\"name\":\"second\",\"value\":42}");
        assertThat(view.getFieldByName("name").getString()).isEqualTo("second");
        assertThat(view.getFieldByName("value").getLong()).isEqualTo(42);
        assertThat(view.metadata()).isSameAs(parent.metadata());
        assertThat(new Variant(view.metadata(), view.value())).isEqualTo(view);
        assertThat(Variant.fromBytes(view.toBytes())).isEqualTo(view);
        assertThat(view.sizeInBytes()).isEqualTo(view.metadata().length + view.value().length);

        Variant copy = view.copy();
        assertThat(copy).isEqualTo(view).hasSameHashCodeAs(view);
        assertThat(copy.metadata()).isNotSameAs(view.metadata());
        assertThat(copy.value()).containsExactly(view.value());
        assertThat(copy.getFieldByName("value").getInt()).isEqualTo(42);
        assertThat(parent.copy().value())
                .isNotSameAs(parent.value())
                .containsExactly(parent.value());
        assertThat(view).isNotEqualTo(parent.getFieldByName("items").getElementAt(0));
    }

    @Test
    void testTypedAccessThroughViews() {
        Variant array =
                new Variant(
                        Variant.emptyMetadata(),
                        VariantUtil.encodeArray(
                                Arrays.asList(
                                        VariantUtil.encodeShort((short) 256),
                                        VariantUtil.encodeLong(Long.MAX_VALUE),
                                        VariantUtil.encodeFloat(1.5f),
                                        VariantUtil.encodeDouble(2.5),
                                        VariantUtil.encodeDecimal(
                                                new java.math.BigDecimal("0.0000000001")),
                                        VariantUtil.encodeDate(-1),
                                        VariantUtil.encodeTimestamp(-1),
                                        VariantUtil.encodeTimestampNtz(1),
                                        new byte[] {60, 2, 0, 0, 0, 1, 2})));
        assertThat(array.getElementAt(0).getShort()).isEqualTo((short) 256);
        assertThat(array.getElementAt(1).getLong()).isEqualTo(Long.MAX_VALUE);
        assertThat(array.getElementAt(2).getFloat()).isEqualTo(1.5f);
        assertThat(array.getElementAt(3).getDouble()).isEqualTo(2.5);
        assertThat(array.getElementAt(4).getDecimal()).isEqualByComparingTo("0.0000000001");
        assertThat(array.getElementAt(5).getDate()).isEqualTo(-1);
        assertThat(array.getElementAt(6).getTimestamp()).isEqualTo(-1);
        assertThat(array.getElementAt(7).getTimestampNtz()).isEqualTo(1);
        assertThat(array.getElementAt(8).getBinary()).containsExactly(1, 2);
    }

    @Test
    void testUnicodeFieldOrderDiffersFromFlink23() {
        // Flink 2.3 sorts these keys by UTF-16. Parquet requires unsigned UTF-8 order,
        // where U+E000 precedes U+1F600. Include 34 fields to exercise binary searches.
        List<String> names = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        names.add("\uD83D\uDE00");
        names.add("\uE000");
        for (int i = 0; i < 32; i++) {
            names.add("f" + i);
        }
        for (int i = 0; i < names.size(); i++) {
            ids.add(i);
            values.add(VariantUtil.encodeInt(i));
        }
        byte[] metadata = VariantUtil.buildMetadata(names);
        Variant value = new Variant(metadata, VariantUtil.encodeObject(metadata, ids, values));
        assertThat(VariantUtil.isMetadataSorted(metadata)).isFalse();
        for (int i = 0; i < names.size(); i++) {
            assertThat(value.getFieldByName(names.get(i)).getInt()).isEqualTo(i);
        }
        assertThat(VariantUtil.objectFieldId(value.value(), 0, 32)).isEqualTo(1);
        assertThat(VariantUtil.objectFieldId(value.value(), 0, 33)).isZero();

        Variant parsed = Variant.fromJson(value.toJson());
        assertThat(VariantUtil.isMetadataSorted(parsed.metadata())).isTrue();
        for (int i = 0; i < names.size(); i++) {
            assertThat(parsed.getFieldByName(names.get(i)).getInt()).isEqualTo(i);
        }
    }

    @Test
    void testComponentSizeBoundary() {
        byte[] value = VariantUtil.encodeString(new byte[Variant.MAX_COMPONENT_SIZE - 5]);
        Variant variant = new Variant(Variant.emptyMetadata(), value);
        assertThat(variant.value()).hasSize(Variant.MAX_COMPONENT_SIZE);
        assertThat(Variant.fromBytes(variant.toBytes())).isEqualTo(variant);
        assertThatThrownBy(() -> VariantUtil.encodeString(new byte[Variant.MAX_COMPONENT_SIZE - 4]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");

        char[] name = new char[Variant.MAX_COMPONENT_SIZE - 10];
        Arrays.fill(name, 'a');
        byte[] metadata = VariantUtil.buildMetadata(Collections.singletonList(new String(name)));
        assertThat(metadata).hasSize(Variant.MAX_COMPONENT_SIZE);
        assertThat(new Variant(metadata, VariantUtil.encodeNull()).isNull()).isTrue();
        assertThatThrownBy(
                        () ->
                                VariantUtil.buildMetadata(
                                        Collections.singletonList(new String(name) + "a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
    }

    @Test
    void testJsonRejectsOversizedArray() {
        String json =
                "["
                        + String.join(
                                ",",
                                Collections.nCopies(
                                        257,
                                        "\""
                                                + String.join("", Collections.nCopies(65536, "a"))
                                                + "\""))
                        + "]";
        assertThatThrownBy(() -> Variant.fromJson(json))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
    }

    @ParameterizedTest
    @CsvSource({"0,1970-01-01", "-1,1969-12-31", "11016,2000-02-29"})
    void testDateJson(int days, String expected) {
        Variant variant = new Variant(Variant.emptyMetadata(), VariantUtil.encodeDate(days));
        assertThat(variant.toJson()).isEqualTo('"' + expected + '"');
        assertThat(variant.getDate()).isEqualTo(days);
    }

    @ParameterizedTest
    @CsvSource({
        "0,1970-01-01T00:00:00",
        "1,1970-01-01T00:00:00.000001",
        "-1,1969-12-31T23:59:59.999999",
        "-1000001,1969-12-31T23:59:58.999999",
        "123000,1970-01-01T00:00:00.123"
    })
    void testTimestampJson(long micros, String expected) {
        Variant timestamp =
                new Variant(Variant.emptyMetadata(), VariantUtil.encodeTimestamp(micros));
        Variant timestampNtz =
                new Variant(Variant.emptyMetadata(), VariantUtil.encodeTimestampNtz(micros));
        assertThat(timestamp.toJson()).isEqualTo('"' + expected + "+00:00\"");
        assertThat(timestampNtz.toJson()).isEqualTo('"' + expected + '"');
        assertThat(timestamp.getTimestamp()).isEqualTo(micros);
        assertThat(timestampNtz.getTimestampNtz()).isEqualTo(micros);
    }

    @ParameterizedTest
    @CsvSource({
        "0.000000001,8",
        "0.0000000001,9",
        "0.000000000000000001,9",
        "0.0000000000000000001,10",
        "-0.0000000001,9"
    })
    void testDecimalScaleCompatibleWithFlink(String json, int type) {
        Variant variant = Variant.fromJson(json);
        assertThat(VariantUtil.primitiveTypeId(variant.value(), 0)).isEqualTo(type);
        assertThat(variant.getDecimal()).isEqualByComparingTo(json);
    }

    @Test
    void testRejectOversizedComponents() {
        byte[] oversized = new byte[(16 << 20) + 1];
        assertThatThrownBy(() -> new Variant(Variant.emptyMetadata(), oversized))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
        assertThatThrownBy(() -> new Variant(oversized, VariantUtil.encodeNull()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
    }

    @Test
    void testReadAdjacentSerializedValues() {
        // The enclosing format supplies lengths; neither blob has its own total-length prefix.
        byte[] bytes = {99, 3, 0, 0, 0, 1, 0, 0, 12, 42, 3, 0, 0, 0, 1, 0, 0, 5, 'x', 99};
        assertThat(Variant.fromBytes(bytes, 1, 9).getInt()).isEqualTo(42);
        assertThat(Variant.fromBytes(bytes, 10, 9).getString()).isEqualTo("x");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 3, 4, Integer.MAX_VALUE})
    void testRejectInvalidSerializedMetadataLength(int metadataLength) {
        byte[] bytes = {0, 0, 0, 0, 1, 0, 0};
        VariantUtil.writeIntLE(bytes, 0, metadataLength);
        assertThatThrownBy(() -> Variant.fromBytes(bytes))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRejectInvalidSerializedRange() {
        byte[] bytes = {3, 0, 0, 0, 1, 0, 0, 0};
        assertThatThrownBy(() -> Variant.fromBytes(bytes, -1, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Variant.fromBytes(bytes, 1, Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Variant.fromBytes(bytes, 0, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // Apache parquet-testing, variant/{array_nested,primitive_decimal*}.{metadata,value}
    // at bcefdfdcc173d90106860c9e5356a10f25c2d3d0. These bytes are independent of Fluss.
    @Test
    void testReadParquetNestedArray() {
        byte[] metadata = Base64.getDecoder().decode("AQQAAgcMEGlkdGhpbmduYW1lc3R5cGU=");
        byte[] value =
                Base64.getDecoder()
                        .decode(
                                "AwMAJSZFAgIAAQACHgwBAgECABcDAgALEilDb250cmFyaWFuGVNwaWRlcgAC"
                                        + "AwACAwAFAhYMAglpZgMDAAYKCxVBcHBsZQ1SYXkA");
        Variant array = new Variant(metadata, value);
        Variant names = array.getElementAt(0).getFieldByName("thing").getFieldByName("names");
        assertThat(names.getElementAt(1).getString()).isEqualTo("Spider");
        assertThat(array.getElementAt(1).isNull()).isTrue();
        assertThat(array.getElementAt(2).getFieldByName("type").getString()).isEqualTo("if");
    }

    @ParameterizedTest
    @CsvSource({
        "IALSBAAA, 12.34",
        "JALSApZJAAAAAA==, 12345678.90",
        "KALStiPA9BAiEQAAAAAAAAAA, 12345678912345678.90"
    })
    void testReadParquetDecimal(String encodedValue, String expected) {
        Variant value = new Variant(new byte[] {1, 0, 0}, Base64.getDecoder().decode(encodedValue));
        assertThat(value.getDecimal()).isEqualByComparingTo(expected);
    }

    @Test
    void testLargeArrayWithUnicodeFieldNames() {
        String json = "[" + String.join(",", Collections.nCopies(256, "{\"é\":1,\"雪\":2}")) + "]";
        Variant array = Variant.fromJson(json);
        assertThat(array.arraySize()).isEqualTo(256);
        Variant last = array.getElementAt(255);
        assertThat(last.getFieldByName("雪").getInt()).isEqualTo(2);
        assertThat(last.getFieldByName("é").getInt()).isEqualTo(1);
        assertThat(array.getElementAt(256)).isNull();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                " ",
                "true garbage",
                "{\"a\":1} trailing",
                "01",
                "1.",
                "[1,]",
                "{\"a\":1,\"a\":2}",
                "\"line\nend\""
            })
    void testRejectInvalidJson(String json) {
        assertThatThrownBy(() -> Variant.fromJson(json))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --------------------------------------------------------------------------------------------
    // Factory methods
    // --------------------------------------------------------------------------------------------

    @Test
    void testOfNull() {
        Variant v = Variant.ofNull();
        assertThat(v.isNull()).isTrue();
        assertThat(v.isPrimitive()).isTrue();
        assertThat(v.isObject()).isFalse();
        assertThat(v.isArray()).isFalse();
        assertThat(v.toJson()).isEqualTo("null");
    }

    @Test
    void testOfBoolean() {
        Variant vTrue = Variant.ofBoolean(true);
        assertThat(vTrue.getBoolean()).isTrue();
        assertThat(vTrue.isPrimitive()).isTrue();
        assertThat(vTrue.toJson()).isEqualTo("true");

        Variant vFalse = Variant.ofBoolean(false);
        assertThat(vFalse.getBoolean()).isFalse();
        assertThat(vFalse.toJson()).isEqualTo("false");
    }

    @Test
    void testOfInt() {
        Variant v = Variant.ofInt(42);
        assertThat(v.getInt()).isEqualTo(42);
        assertThat(v.isPrimitive()).isTrue();
        assertThat(v.toJson()).isEqualTo("42");
    }

    @Test
    void testOfLong() {
        Variant v = Variant.ofLong(9876543210L);
        assertThat(v.getLong()).isEqualTo(9876543210L);
        assertThat(v.toJson()).isEqualTo("9876543210");
    }

    @Test
    void testOfFloat() {
        Variant v = Variant.ofFloat(3.14f);
        assertThat(v.getFloat()).isEqualTo(3.14f);
    }

    @Test
    void testOfDouble() {
        Variant v = Variant.ofDouble(2.718281828);
        assertThat(v.getDouble()).isEqualTo(2.718281828);
    }

    @Test
    void testOfString() {
        Variant v = Variant.ofString("hello world");
        assertThat(v.getString()).isEqualTo("hello world");
        assertThat(v.isShortString()).isTrue();
        assertThat(v.toJson()).isEqualTo("\"hello world\"");
    }

    // --------------------------------------------------------------------------------------------
    // Binary serialization roundtrip
    // --------------------------------------------------------------------------------------------

    @Test
    void testToBytesFromBytesRoundtrip() {
        Variant original = Variant.ofInt(42);
        byte[] bytes = original.toBytes();
        Variant deserialized = Variant.fromBytes(bytes);
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getInt()).isEqualTo(42);
    }

    @Test
    void testToBytesFromBytesWithOffset() {
        Variant original = Variant.ofString("test");
        byte[] bytes = original.toBytes();

        // Put the bytes at an offset in a larger array
        byte[] padded = new byte[bytes.length + 10];
        System.arraycopy(bytes, 0, padded, 5, bytes.length);

        Variant deserialized = Variant.fromBytes(padded, 5, bytes.length);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void testToBytesFromBytesNull() {
        Variant original = Variant.ofNull();
        byte[] bytes = original.toBytes();
        Variant deserialized = Variant.fromBytes(bytes);
        assertThat(deserialized.isNull()).isTrue();
    }

    // --------------------------------------------------------------------------------------------
    // JSON conversion
    // --------------------------------------------------------------------------------------------

    @Test
    void testFromJsonNull() {
        Variant v = Variant.fromJson("null");
        assertThat(v.isNull()).isTrue();
    }

    @Test
    void testFromJsonBoolean() {
        Variant v1 = Variant.fromJson("true");
        assertThat(v1.getBoolean()).isTrue();

        Variant v2 = Variant.fromJson("false");
        assertThat(v2.getBoolean()).isFalse();
    }

    @Test
    void testFromJsonInteger() {
        Variant v = Variant.fromJson("42");
        // Small integers are encoded as int8
        assertThat(v.getByte()).isEqualTo((byte) 42);
    }

    @Test
    void testFromJsonLargeInteger() {
        Variant v = Variant.fromJson("1000000");
        assertThat(v.getInt()).isEqualTo(1000000);
    }

    @Test
    void testFromJsonNegativeInteger() {
        Variant v = Variant.fromJson("-128");
        assertThat(v.getByte()).isEqualTo((byte) -128);
    }

    @Test
    void testFromJsonDecimal() {
        // Decimal point without exponent is encoded as Decimal (matching Paimon behavior)
        Variant v = Variant.fromJson("3.14");
        assertThat(v.getDecimal()).isEqualByComparingTo(new java.math.BigDecimal("3.14"));
    }

    @Test
    void testFromJsonDouble() {
        // Exponent notation is encoded as Double
        Variant v = Variant.fromJson("3.14e0");
        assertThat(v.getDouble()).isEqualTo(3.14);
    }

    @Test
    void testFromJsonString() {
        Variant v = Variant.fromJson("\"hello world\"");
        assertThat(v.getString()).isEqualTo("hello world");
    }

    @Test
    void testFromJsonStringWithEscapes() {
        Variant v = Variant.fromJson("\"line1\\nline2\"");
        assertThat(v.getString()).isEqualTo("line1\nline2");
    }

    @Test
    void testFromJsonSimpleObject() {
        Variant v = Variant.fromJson("{\"name\":\"Alice\",\"age\":30}");
        assertThat(v.isObject()).isTrue();
        assertThat(v.objectSize()).isEqualTo(2);

        Variant name = v.getFieldByName("name");
        assertThat(name).isNotNull();
        assertThat(name.getString()).isEqualTo("Alice");

        Variant age = v.getFieldByName("age");
        assertThat(age).isNotNull();
        assertThat(age.getByte()).isEqualTo((byte) 30);
    }

    @Test
    void testFromJsonNestedObject() {
        Variant v = Variant.fromJson("{\"user\":{\"name\":\"Bob\",\"active\":true},\"count\":100}");
        assertThat(v.isObject()).isTrue();

        Variant user = v.getFieldByName("user");
        assertThat(user).isNotNull();
        assertThat(user.isObject()).isTrue();

        Variant name = user.getFieldByName("name");
        assertThat(name).isNotNull();
        assertThat(name.getString()).isEqualTo("Bob");

        Variant active = user.getFieldByName("active");
        assertThat(active).isNotNull();
        assertThat(active.getBoolean()).isTrue();
    }

    @Test
    void testFromJsonArray() {
        Variant v = Variant.fromJson("[1, 2, 3]");
        assertThat(v.isArray()).isTrue();
        assertThat(v.arraySize()).isEqualTo(3);
        assertThat(v.getElementAt(0).getByte()).isEqualTo((byte) 1);
        assertThat(v.getElementAt(1).getByte()).isEqualTo((byte) 2);
        assertThat(v.getElementAt(2).getByte()).isEqualTo((byte) 3);
    }

    @Test
    void testFromJsonMixedArray() {
        Variant v = Variant.fromJson("[1, \"hello\", true, null]");
        assertThat(v.isArray()).isTrue();
        assertThat(v.arraySize()).isEqualTo(4);
        assertThat(v.getElementAt(0).getByte()).isEqualTo((byte) 1);
        assertThat(v.getElementAt(1).getString()).isEqualTo("hello");
        assertThat(v.getElementAt(2).getBoolean()).isTrue();
        assertThat(v.getElementAt(3).isNull()).isTrue();
    }

    @Test
    void testFromJsonEmptyObject() {
        Variant v = Variant.fromJson("{}");
        assertThat(v.isObject()).isTrue();
        assertThat(v.objectSize()).isZero();
    }

    @Test
    void testFromJsonEmptyArray() {
        Variant v = Variant.fromJson("[]");
        assertThat(v.isArray()).isTrue();
        assertThat(v.arraySize()).isZero();
    }

    // --------------------------------------------------------------------------------------------
    // JSON roundtrip
    // --------------------------------------------------------------------------------------------

    @Test
    void testJsonRoundtripPrimitives() {
        assertJsonRoundtrip("null");
        assertJsonRoundtrip("true");
        assertJsonRoundtrip("false");
        assertJsonRoundtrip("3.14");
        assertJsonRoundtrip("\"hello\"");
    }

    @Test
    void testJsonRoundtripObject() {
        Variant v = Variant.fromJson("{\"a\":1,\"b\":\"test\"}");
        String json = v.toJson();
        // Re-parse and verify
        Variant v2 = Variant.fromJson(json);
        assertThat(v2.getFieldByName("a")).isNotNull();
        assertThat(v2.getFieldByName("b")).isNotNull();
        assertThat(v2.getFieldByName("b").getString()).isEqualTo("test");
    }

    @Test
    void testJsonRoundtripArray() {
        Variant v = Variant.fromJson("[1,2,3]");
        String json = v.toJson();
        Variant v2 = Variant.fromJson(json);
        assertThat(v2.isArray()).isTrue();
        assertThat(v2.arraySize()).isEqualTo(3);
    }

    // --------------------------------------------------------------------------------------------
    // Object field access
    // --------------------------------------------------------------------------------------------

    @Test
    void testGetFieldByNameNonExistent() {
        Variant v = Variant.fromJson("{\"name\":\"Alice\"}");
        assertThat(v.getFieldByName("unknown")).isNull();
    }

    // --------------------------------------------------------------------------------------------
    // Equals & HashCode
    // --------------------------------------------------------------------------------------------

    @Test
    void testEquals() {
        Variant v1 = Variant.ofInt(42);
        Variant v2 = Variant.ofInt(42);
        assertThat(v1).isEqualTo(v2);
        assertThat(v1.hashCode()).isEqualTo(v2.hashCode());
    }

    @Test
    void testNotEquals() {
        Variant v1 = Variant.ofInt(42);
        Variant v2 = Variant.ofInt(43);
        assertThat(v1).isNotEqualTo(v2);
    }

    // --------------------------------------------------------------------------------------------
    // toString
    // --------------------------------------------------------------------------------------------

    @Test
    void testToString() {
        assertThat(Variant.ofNull().toString()).isEqualTo("null");
        assertThat(Variant.ofInt(42).toString()).isEqualTo("42");
        assertThat(Variant.ofString("hi").toString()).isEqualTo("\"hi\"");
    }

    // --------------------------------------------------------------------------------------------
    // Complex JSON roundtrip
    // --------------------------------------------------------------------------------------------

    @Test
    void testComplexJsonRoundtrip() {
        String json =
                "{\"users\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}],"
                        + "\"count\":2,\"active\":true}";
        Variant v = Variant.fromJson(json);
        assertThat(v.isObject()).isTrue();

        Variant users = v.getFieldByName("users");
        assertThat(users).isNotNull();
        assertThat(users.isArray()).isTrue();
        assertThat(users.arraySize()).isEqualTo(2);

        Variant firstUser = users.getElementAt(0);
        assertThat(firstUser.getFieldByName("name").getString()).isEqualTo("Alice");
    }

    @Test
    void testJsonWithWhitespace() {
        String json = "  { \"key\" : \"value\" , \"num\" : 42 }  ";
        Variant v = Variant.fromJson(json);
        assertThat(v.isObject()).isTrue();
        assertThat(v.getFieldByName("key").getString()).isEqualTo("value");
    }

    // --------------------------------------------------------------------------------------------
    // Helper
    // --------------------------------------------------------------------------------------------

    private void assertJsonRoundtrip(String json) {
        Variant v = Variant.fromJson(json);
        assertThat(v.toJson()).isEqualTo(json);
    }
}
