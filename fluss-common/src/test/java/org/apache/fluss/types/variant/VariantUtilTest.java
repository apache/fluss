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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VariantUtil}. */
class VariantUtilTest {

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 2, 33554432})
    void testRejectInvalidBinaryLengthBeforeCopy(int length) {
        byte[] value = {60, 0, 0, 0, 0, 42};
        VariantUtil.writeIntLE(value, 1, length);
        assertThatThrownBy(() -> VariantUtil.getBinary(value, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.valueSize(value, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBinaryAtNonzeroOffset() {
        byte[] bytes = {99, 60, 2, 0, 0, 0, 42, 43, 99};
        assertThat(VariantUtil.getBinary(bytes, 1)).containsExactly(42, 43);
        assertThat(VariantUtil.valueSize(bytes, 1)).isEqualTo(7);
        assertThat(VariantUtil.getBinary(new byte[] {60, 0, 0, 0, 0}, 0)).isEmpty();
    }

    @Test
    void testRejectOverflowingContainerCounts() {
        byte[] array = {0x13, -1, -1, -1, 127, 0};
        byte[] object = {0x7E, -1, -1, -1, 127, 0};
        assertThatThrownBy(() -> VariantUtil.valueSize(array, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.arrayElementOffset(array, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.valueSize(object, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.objectFieldValueOffset(object, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRejectFieldOutsideContainer() {
        // The element's int32 would consume bytes beyond its array's declared payload.
        byte[] array = {3, 1, 0, 1, 20, 42, 0, 0, 0};
        assertThatThrownBy(() -> VariantUtil.arrayElementOffset(array, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] object = {2, 1, 0, 0, 1, 20, 42, 0, 0, 0};
        assertThatThrownBy(() -> VariantUtil.objectFieldValueOffset(object, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRejectInvalidMetadataRanges() {
        byte[] overflowingCount = {(byte) 0xC1, -1, -1, -1, 127, 0};
        byte[] reversedOffsets = {1, 2, 0, 2, 1, 'a', 'b'};
        assertThatThrownBy(() -> VariantUtil.metadataFieldName(overflowingCount, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.findFieldId(reversedOffsets, "a"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MAX_VALUE})
    void testRejectInvalidReadOffset(int offset) {
        byte[] array = VariantUtil.encodeArray(Collections.singletonList(VariantUtil.encodeNull()));
        assertThatThrownBy(() -> VariantUtil.arrayElementOffset(array, offset, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.getBinary(array, offset))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.valueSize(array, offset))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadFourByteWidths() {
        // The original fixed-width writer's encoding of {"a":1} remains readable.
        byte[] metadata = {(byte) 0xD1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 'a'};
        byte[] object = {0x7E, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 12, 1};
        Variant variant = new Variant(metadata, object);
        assertThat(variant.toJson()).isEqualTo("{\"a\":1}");
        assertThat(variant.getFieldByName("a").getInt()).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({"255,1", "256,2", "65535,2", "65536,3"})
    void testMetadataOffsetWidths(int stringBytes, int width) {
        char[] chars = new char[stringBytes];
        Arrays.fill(chars, 'a');
        String name = new String(chars);
        byte[] metadata = VariantUtil.buildMetadata(Collections.singletonList(name));
        assertThat(VariantUtil.metadataOffsetSize(metadata)).isEqualTo(width);
        assertThat(VariantUtil.metadataFieldName(metadata, 0)).isEqualTo(name);
        assertThat(metadata).hasSize(1 + 3 * width + stringBytes);
    }

    @ParameterizedTest
    @CsvSource({"255,1", "256,2", "65535,2", "65536,3"})
    void testMetadataCountWidths(int count, int width) {
        byte[] metadata = VariantUtil.buildMetadata(Collections.nCopies(count, ""));
        assertThat(VariantUtil.metadataOffsetSize(metadata)).isEqualTo(width);
        assertThat(VariantUtil.metadataDictSize(metadata)).isEqualTo(count);
        assertThat(VariantUtil.metadataFieldName(metadata, count - 1)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"255,1", "256,2", "65535,2", "65536,3"})
    void testContainerOffsetWidths(int dataSize, int width) {
        byte[] element = VariantUtil.encodeString(new byte[dataSize - 5]);
        byte[] array = VariantUtil.encodeArray(Collections.singletonList(element));
        byte[] metadata = VariantUtil.buildMetadata(Collections.singletonList("a"));
        byte[] object =
                VariantUtil.encodeObject(
                        metadata, Collections.singletonList(0), Collections.singletonList(element));
        assertThat(array).hasSize(2 + 2 * width + dataSize);
        assertThat(object).hasSize(3 + 2 * width + dataSize);
        assertThat(new Variant(metadata, array).getElementAt(0).value()).containsExactly(element);
        assertThat(new Variant(metadata, object).getFieldByName("a").value())
                .containsExactly(element);
    }

    @ParameterizedTest
    @CsvSource({"255,1", "256,2", "65535,2", "65536,3"})
    void testObjectIdWidths(int id, int width) {
        List<String> names = new ArrayList<>(Collections.nCopies(id, ""));
        names.add("last");
        byte[] metadata = VariantUtil.buildMetadata(names);
        byte[] object =
                VariantUtil.encodeObject(
                        metadata,
                        Collections.singletonList(id),
                        Collections.singletonList(VariantUtil.encodeNull()));
        assertThat(object).hasSize(5 + width);
        assertThat(VariantUtil.objectFieldId(object, 0, 0)).isEqualTo(id);
        assertThat(new Variant(metadata, object).getFieldByName("last").isNull()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256})
    void testContainerCountWidths(int count) {
        List<String> names = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            names.add("f" + i);
            ids.add(i);
        }
        byte[] metadata = VariantUtil.buildMetadata(names);
        List<byte[]> values = Collections.nCopies(count, VariantUtil.encodeNull());
        byte[] array = VariantUtil.encodeArray(values);
        byte[] object = VariantUtil.encodeObject(metadata, ids, values);
        int countWidth = count <= 255 ? 1 : 4;
        int offsetWidth = count <= 255 ? 1 : 2;
        assertThat(array).hasSize(1 + countWidth + (count + 1) * offsetWidth + count);
        assertThat(object).hasSize(array.length + count);
        assertThat(new Variant(metadata, array).arraySize()).isEqualTo(count);
        assertThat(new Variant(metadata, object).objectSize()).isEqualTo(count);
        assertThat(VariantUtil.valueSize(array, 0)).isEqualTo(array.length);
        assertThat(VariantUtil.valueSize(object, 0)).isEqualTo(object.length);
    }

    @Test
    void testContainerSizeIncludesHeaders() {
        byte[] element = VariantUtil.encodeString(new byte[Variant.MAX_COMPONENT_SIZE - 5]);
        assertThatThrownBy(() -> VariantUtil.encodeArray(Collections.singletonList(element)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
        byte[] metadata = VariantUtil.buildMetadata(Collections.singletonList("a"));
        assertThatThrownBy(
                        () ->
                                VariantUtil.encodeObject(
                                        metadata,
                                        Collections.singletonList(0),
                                        Collections.singletonList(element)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
    }

    @Test
    void testRejectSizeArithmeticOverflowBeforeAllocation() {
        assertThatThrownBy(
                        () ->
                                VariantUtil.encodeArray(
                                        Collections.nCopies(
                                                Integer.MAX_VALUE, VariantUtil.encodeNull())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
        assertThatThrownBy(
                        () -> VariantUtil.buildMetadata(Collections.nCopies(Integer.MAX_VALUE, "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
        assertThatThrownBy(
                        () ->
                                VariantUtil.encodeObject(
                                        Variant.emptyMetadata(),
                                        Collections.nCopies(Integer.MAX_VALUE, 0),
                                        Collections.nCopies(
                                                Integer.MAX_VALUE, VariantUtil.encodeNull())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 MiB");
    }

    @Test
    void testReadParquetDecimalScaleIndependentlyOfWidth() {
        // Parquet allows a scale through 38 even in DECIMAL4, unlike Flink 2.3.
        assertThat(VariantUtil.getDecimal(new byte[] {32, 10, 1, 0, 0, 0}, 0))
                .isEqualByComparingTo("0.0000000001");
        assertThat(VariantUtil.getDecimal(new byte[] {32, 38, 1, 0, 0, 0}, 0))
                .isEqualByComparingTo("1E-38");
        assertThatThrownBy(() -> VariantUtil.getDecimal(new byte[] {32, 39, 1, 0, 0, 0}, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCanonicalLargeArrayFixture() {
        byte[] bytes = {0x13, 2, 0, 0, 0, 0, 2, 4, 12, 1, 12, 2};
        Variant array = new Variant(new byte[] {1, 0, 0}, bytes);
        assertThat(array.arraySize()).isEqualTo(2);
        assertThat(array.getElementAt(1).getLong()).isEqualTo(2);
        assertThat(VariantUtil.valueSize(bytes, 0)).isEqualTo(bytes.length);
    }

    @Test
    void testEmptyDictionaryIncludesFinalOffset() {
        byte[] metadata = Variant.emptyMetadata();
        int width = VariantUtil.metadataOffsetSize(metadata);
        assertThat(metadata).hasSize(1 + 2 * width);
        assertThat(VariantUtil.readUnsignedLE(metadata, 1 + width, width)).isZero();
    }

    @Test
    void testScalarGettersRejectShortStrings() {
        assertThatThrownBy(() -> Variant.ofString("a").getBoolean())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> Variant.ofString("abcdefgh").getDecimal())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> Variant.ofNull().getDecimal())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testDecimalPrecisionAndScaleLimits() {
        byte[] tenDigits = VariantUtil.encodeDecimal(new BigDecimal("1000000000"));
        assertThat(VariantUtil.primitiveTypeId(tenDigits, 0))
                .isEqualTo(VariantUtil.PRIMITIVE_TYPE_DECIMAL8);
        byte[] nineteenDigits = VariantUtil.encodeDecimal(new BigDecimal("1000000000000000000"));
        assertThat(VariantUtil.primitiveTypeId(nineteenDigits, 0))
                .isEqualTo(VariantUtil.PRIMITIVE_TYPE_DECIMAL16);
        assertThat(VariantUtil.getDecimal(VariantUtil.encodeDecimal(new BigDecimal("1E+2")), 0))
                .isEqualByComparingTo("100");
        assertThatThrownBy(() -> VariantUtil.encodeDecimal(new BigDecimal("1E-39")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantUtil.encodeDecimal(new BigDecimal("1E+38")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --------------------------------------------------------------------------------------------
    // Null encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeDecodeNull() {
        byte[] encoded = VariantUtil.encodeNull();
        assertThat(encoded).hasSize(1);
        assertThat(VariantUtil.isNull(encoded, 0)).isTrue();
        assertThat(VariantUtil.basicType(encoded, 0)).isEqualTo(VariantUtil.BASIC_TYPE_PRIMITIVE);
        assertThat(VariantUtil.primitiveTypeId(encoded, 0))
                .isEqualTo(VariantUtil.PRIMITIVE_TYPE_NULL);
    }

    // --------------------------------------------------------------------------------------------
    // Boolean encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeBooleanTrue() {
        byte[] encoded = VariantUtil.encodeBoolean(true);
        assertThat(encoded).hasSize(1);
        assertThat(VariantUtil.getBoolean(encoded, 0)).isTrue();
        assertThat(VariantUtil.isNull(encoded, 0)).isFalse();
    }

    @Test
    void testEncodeBooleanFalse() {
        byte[] encoded = VariantUtil.encodeBoolean(false);
        assertThat(encoded).hasSize(1);
        assertThat(VariantUtil.getBoolean(encoded, 0)).isFalse();
    }

    // --------------------------------------------------------------------------------------------
    // Integer type encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeByte() {
        byte[] encoded = VariantUtil.encodeByte((byte) 42);
        assertThat(encoded).hasSize(2);
        assertThat(VariantUtil.getByte(encoded, 0)).isEqualTo((byte) 42);
    }

    @Test
    void testEncodeByteNegative() {
        byte[] encoded = VariantUtil.encodeByte((byte) -128);
        assertThat(VariantUtil.getByte(encoded, 0)).isEqualTo((byte) -128);
    }

    @Test
    void testEncodeShort() {
        byte[] encoded = VariantUtil.encodeShort((short) 12345);
        assertThat(encoded).hasSize(3);
        assertThat(VariantUtil.getShort(encoded, 0)).isEqualTo((short) 12345);
    }

    @Test
    void testEncodeShortNegative() {
        byte[] encoded = VariantUtil.encodeShort((short) -32000);
        assertThat(VariantUtil.getShort(encoded, 0)).isEqualTo((short) -32000);
    }

    @Test
    void testEncodeInt() {
        byte[] encoded = VariantUtil.encodeInt(1234567);
        assertThat(encoded).hasSize(5);
        assertThat(VariantUtil.getInt(encoded, 0)).isEqualTo(1234567);
    }

    @Test
    void testEncodeIntNegative() {
        byte[] encoded = VariantUtil.encodeInt(Integer.MIN_VALUE);
        assertThat(VariantUtil.getInt(encoded, 0)).isEqualTo(Integer.MIN_VALUE);
    }

    @Test
    void testEncodeLong() {
        byte[] encoded = VariantUtil.encodeLong(9876543210L);
        assertThat(encoded).hasSize(9);
        assertThat(VariantUtil.getLong(encoded, 0)).isEqualTo(9876543210L);
    }

    @Test
    void testEncodeLongNegative() {
        byte[] encoded = VariantUtil.encodeLong(Long.MIN_VALUE);
        assertThat(VariantUtil.getLong(encoded, 0)).isEqualTo(Long.MIN_VALUE);
    }

    // --------------------------------------------------------------------------------------------
    // Float/Double encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeFloat() {
        byte[] encoded = VariantUtil.encodeFloat(3.14f);
        assertThat(encoded).hasSize(5);
        assertThat(VariantUtil.getFloat(encoded, 0)).isEqualTo(3.14f);
    }

    @Test
    void testEncodeDouble() {
        byte[] encoded = VariantUtil.encodeDouble(3.14159265358979);
        assertThat(encoded).hasSize(9);
        assertThat(VariantUtil.getDouble(encoded, 0)).isEqualTo(3.14159265358979);
    }

    // --------------------------------------------------------------------------------------------
    // String encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeShortString() {
        // String <= 31 bytes uses short string encoding
        byte[] strBytes = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] encoded = VariantUtil.encodeString(strBytes);
        assertThat(VariantUtil.basicType(encoded, 0))
                .isEqualTo(VariantUtil.BASIC_TYPE_SHORT_STRING);
        assertThat(VariantUtil.getString(encoded, 0)).isEqualTo("hello");
    }

    @Test
    void testEncodeLongString() {
        // String > 31 bytes uses primitive STRING encoding
        String longStr = "This is a long string that exceeds 31 bytes for testing purposes!!!";
        byte[] strBytes = longStr.getBytes(StandardCharsets.UTF_8);
        assertThat(strBytes.length).isGreaterThan(31);

        byte[] encoded = VariantUtil.encodeString(strBytes);
        assertThat(VariantUtil.basicType(encoded, 0)).isEqualTo(VariantUtil.BASIC_TYPE_PRIMITIVE);
        assertThat(VariantUtil.primitiveTypeId(encoded, 0))
                .isEqualTo(VariantUtil.PRIMITIVE_TYPE_STRING);
        assertThat(VariantUtil.getString(encoded, 0)).isEqualTo(longStr);
    }

    @Test
    void testEncodeEmptyString() {
        byte[] strBytes = "".getBytes(StandardCharsets.UTF_8);
        byte[] encoded = VariantUtil.encodeString(strBytes);
        assertThat(VariantUtil.getString(encoded, 0)).isEmpty();
    }

    @Test
    void testEncodeUnicodeString() {
        String unicodeStr = "你好世界🌍";
        byte[] strBytes = unicodeStr.getBytes(StandardCharsets.UTF_8);
        byte[] encoded = VariantUtil.encodeString(strBytes);
        assertThat(VariantUtil.getString(encoded, 0)).isEqualTo(unicodeStr);
    }

    // --------------------------------------------------------------------------------------------
    // Date encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeDate() {
        byte[] encoded = VariantUtil.encodeDate(18000);
        assertThat(encoded).hasSize(5);
        assertThat(VariantUtil.getDate(encoded, 0)).isEqualTo(18000);
    }

    // --------------------------------------------------------------------------------------------
    // Object encoding/decoding
    // --------------------------------------------------------------------------------------------

    @Test
    void testEncodeEmptyObject() {
        byte[] encoded =
                VariantUtil.encodeObject(
                        Variant.emptyMetadata(), new ArrayList<>(), new ArrayList<>());
        assertThat(VariantUtil.basicType(encoded, 0)).isEqualTo(VariantUtil.BASIC_TYPE_OBJECT);
        assertThat(VariantUtil.objectSize(encoded, 0)).isZero();
    }

    @Test
    void testEncodeObjectWithFields() {
        List<Integer> fieldIds = Arrays.asList(0, 1, 2);
        List<byte[]> fieldValues =
                Arrays.asList(
                        VariantUtil.encodeInt(42),
                        VariantUtil.encodeBoolean(true),
                        VariantUtil.encodeString("test".getBytes(StandardCharsets.UTF_8)));

        byte[] encoded =
                VariantUtil.encodeObject(
                        VariantUtil.buildMetadata(Arrays.asList("a", "b", "c")),
                        fieldIds,
                        fieldValues);
        assertThat(VariantUtil.basicType(encoded, 0)).isEqualTo(VariantUtil.BASIC_TYPE_OBJECT);
        assertThat(VariantUtil.objectSize(encoded, 0)).isEqualTo(3);
    }

    @Test
    void testObjectFieldAccess() {
        List<Integer> fieldIds = Arrays.asList(0, 1);
        List<byte[]> fieldValues =
                Arrays.asList(VariantUtil.encodeInt(100), VariantUtil.encodeDouble(3.14));

        byte[] encoded =
                VariantUtil.encodeObject(
                        VariantUtil.buildMetadata(Arrays.asList("a", "b", "c")),
                        fieldIds,
                        fieldValues);

        // Field 0 should be int 100
        int field0Id = VariantUtil.objectFieldId(encoded, 0, 0);
        assertThat(field0Id).isEqualTo(0);
        int field0Offset = VariantUtil.objectFieldValueOffset(encoded, 0, 0);
        assertThat(VariantUtil.getInt(encoded, field0Offset)).isEqualTo(100);

        // Field 1 should be double 3.14
        int field1Id = VariantUtil.objectFieldId(encoded, 0, 1);
        assertThat(field1Id).isEqualTo(1);
        int field1Offset = VariantUtil.objectFieldValueOffset(encoded, 0, 1);
        assertThat(VariantUtil.getDouble(encoded, field1Offset)).isEqualTo(3.14);
    }

    // --------------------------------------------------------------------------------------------
    // Value size calculation
    // --------------------------------------------------------------------------------------------

    @Test
    void testValueSizePrimitive() {
        assertThat(VariantUtil.valueSize(VariantUtil.encodeNull(), 0)).isEqualTo(1);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeBoolean(true), 0)).isEqualTo(1);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeByte((byte) 1), 0)).isEqualTo(2);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeShort((short) 1), 0)).isEqualTo(3);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeInt(1), 0)).isEqualTo(5);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeLong(1L), 0)).isEqualTo(9);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeFloat(1.0f), 0)).isEqualTo(5);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeDouble(1.0), 0)).isEqualTo(9);
        assertThat(VariantUtil.valueSize(VariantUtil.encodeDate(1), 0)).isEqualTo(5);
    }

    @Test
    void testValueSizeShortString() {
        byte[] encoded = VariantUtil.encodeString("hi".getBytes(StandardCharsets.UTF_8));
        // short string: 1 header + 2 bytes
        assertThat(VariantUtil.valueSize(encoded, 0)).isEqualTo(3);
    }

    @Test
    void testValueSizeObject() {
        List<Integer> fieldIds = Arrays.asList(0, 1);
        List<byte[]> fieldValues =
                Arrays.asList(VariantUtil.encodeInt(42), VariantUtil.encodeBoolean(true));
        byte[] encoded =
                VariantUtil.encodeObject(
                        VariantUtil.buildMetadata(Arrays.asList("a", "b", "c")),
                        fieldIds,
                        fieldValues);
        assertThat(VariantUtil.valueSize(encoded, 0)).isEqualTo(encoded.length);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata operations
    // --------------------------------------------------------------------------------------------

    @Test
    void testEmptyMetadata() {
        byte[] metadata = VariantUtil.EMPTY_METADATA;
        assertThat(VariantUtil.metadataDictSize(metadata)).isZero();
    }

    @Test
    void testMetadataWithFields() {
        Variant v = Variant.fromJson("{\"age\":1,\"city\":\"x\",\"name\":\"y\"}");
        byte[] metadata = v.metadata();

        assertThat(VariantUtil.metadataDictSize(metadata)).isEqualTo(3);

        // The JSON object keys "age", "city", "name" are inserted in that order
        assertThat(VariantUtil.metadataFieldName(metadata, 0)).isEqualTo("age");
        assertThat(VariantUtil.metadataFieldName(metadata, 1)).isEqualTo("city");
        assertThat(VariantUtil.metadataFieldName(metadata, 2)).isEqualTo("name");
    }

    @Test
    void testFindFieldId() {
        Variant v = Variant.fromJson("{\"age\":1,\"city\":\"x\",\"name\":\"y\"}");
        byte[] metadata = v.metadata();

        assertThat(VariantUtil.findFieldId(metadata, "age")).isEqualTo(0);
        assertThat(VariantUtil.findFieldId(metadata, "city")).isEqualTo(1);
        assertThat(VariantUtil.findFieldId(metadata, "name")).isEqualTo(2);
        assertThat(VariantUtil.findFieldId(metadata, "unknown")).isEqualTo(-1);
    }

    // --------------------------------------------------------------------------------------------
    // JSON round-trip via Variant
    // --------------------------------------------------------------------------------------------

    @Test
    void testToJsonNull() {
        Variant v = Variant.ofNull();
        assertThat(v.toJson()).isEqualTo("null");
    }

    @Test
    void testToJsonBoolean() {
        assertThat(Variant.ofBoolean(true).toJson()).isEqualTo("true");
        assertThat(Variant.ofBoolean(false).toJson()).isEqualTo("false");
    }

    @Test
    void testToJsonInt() {
        assertThat(Variant.ofInt(42).toJson()).isEqualTo("42");
    }

    @Test
    void testToJsonDouble() {
        assertThat(Variant.ofDouble(3.14).toJson()).isEqualTo("3.14");
    }

    @Test
    void testToJsonString() {
        assertThat(Variant.ofString("hello").toJson()).isEqualTo("\"hello\"");
    }

    @Test
    void testToJsonStringWithEscapes() {
        assertThat(Variant.ofString("line1\nline2\t\"quoted\"").toJson())
                .isEqualTo("\"line1\\nline2\\t\\\"quoted\\\"\"");
    }

    // --------------------------------------------------------------------------------------------
    // Little-endian read/write
    // --------------------------------------------------------------------------------------------

    @Test
    void testReadWriteIntLE() {
        byte[] buf = new byte[4];
        VariantUtil.writeIntLE(buf, 0, 0x12345678);
        assertThat(VariantUtil.readIntLE(buf, 0)).isEqualTo(0x12345678);
    }

    // --------------------------------------------------------------------------------------------
    // Error cases
    // --------------------------------------------------------------------------------------------

    @Test
    void testObjectSizeOnNonObject() {
        byte[] value = VariantUtil.encodeInt(42);
        assertThatThrownBy(() -> VariantUtil.objectSize(value, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Not an object");
    }

    @Test
    void testGetBooleanOnNonBoolean() {
        byte[] value = VariantUtil.encodeInt(42);
        assertThatThrownBy(() -> VariantUtil.getBoolean(value, 0))
                .isInstanceOf(IllegalStateException.class);
    }
}
