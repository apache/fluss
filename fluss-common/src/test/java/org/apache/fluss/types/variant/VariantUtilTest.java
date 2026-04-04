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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VariantUtil}. */
class VariantUtilTest {

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
        byte[] encoded = VariantUtil.encodeObject(new ArrayList<>(), new ArrayList<>());
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

        byte[] encoded = VariantUtil.encodeObject(fieldIds, fieldValues);
        assertThat(VariantUtil.basicType(encoded, 0)).isEqualTo(VariantUtil.BASIC_TYPE_OBJECT);
        assertThat(VariantUtil.objectSize(encoded, 0)).isEqualTo(3);
    }

    @Test
    void testObjectFieldAccess() {
        List<Integer> fieldIds = Arrays.asList(0, 1);
        List<byte[]> fieldValues =
                Arrays.asList(VariantUtil.encodeInt(100), VariantUtil.encodeDouble(3.14));

        byte[] encoded = VariantUtil.encodeObject(fieldIds, fieldValues);

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
        byte[] encoded = VariantUtil.encodeObject(fieldIds, fieldValues);
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
        VariantBuilder builder = new VariantBuilder();
        builder.addFieldName("age");
        builder.addFieldName("name");
        builder.addFieldName("city");
        byte[] metadata = builder.buildMetadata();

        assertThat(VariantUtil.metadataDictSize(metadata)).isEqualTo(3);
        assertThat(VariantUtil.isMetadataSorted(metadata)).isTrue();

        // Fields should be sorted: age, city, name
        assertThat(VariantUtil.metadataFieldName(metadata, 0)).isEqualTo("age");
        assertThat(VariantUtil.metadataFieldName(metadata, 1)).isEqualTo("city");
        assertThat(VariantUtil.metadataFieldName(metadata, 2)).isEqualTo("name");
    }

    @Test
    void testFindFieldId() {
        VariantBuilder builder = new VariantBuilder();
        builder.addFieldName("age");
        builder.addFieldName("name");
        builder.addFieldName("city");
        byte[] metadata = builder.buildMetadata();

        assertThat(VariantUtil.findFieldId(metadata, "age")).isEqualTo(0);
        assertThat(VariantUtil.findFieldId(metadata, "city")).isEqualTo(1);
        assertThat(VariantUtil.findFieldId(metadata, "name")).isEqualTo(2);
        assertThat(VariantUtil.findFieldId(metadata, "unknown")).isEqualTo(-1);
    }

    // --------------------------------------------------------------------------------------------
    // JSON serialization
    // --------------------------------------------------------------------------------------------

    @Test
    void testToJsonNull() {
        byte[] value = VariantUtil.encodeNull();
        StringBuilder sb = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, value, 0, sb);
        assertThat(sb.toString()).isEqualTo("null");
    }

    @Test
    void testToJsonBoolean() {
        StringBuilder sb1 = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, VariantUtil.encodeBoolean(true), 0, sb1);
        assertThat(sb1.toString()).isEqualTo("true");

        StringBuilder sb2 = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, VariantUtil.encodeBoolean(false), 0, sb2);
        assertThat(sb2.toString()).isEqualTo("false");
    }

    @Test
    void testToJsonInt() {
        StringBuilder sb = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, VariantUtil.encodeInt(42), 0, sb);
        assertThat(sb.toString()).isEqualTo("42");
    }

    @Test
    void testToJsonDouble() {
        StringBuilder sb = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, VariantUtil.encodeDouble(3.14), 0, sb);
        assertThat(sb.toString()).isEqualTo("3.14");
    }

    @Test
    void testToJsonString() {
        byte[] value = VariantUtil.encodeString("hello".getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, value, 0, sb);
        assertThat(sb.toString()).isEqualTo("\"hello\"");
    }

    @Test
    void testToJsonStringWithEscapes() {
        byte[] value =
                VariantUtil.encodeString(
                        "line1\nline2\t\"quoted\"".getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        VariantUtil.toJson(VariantUtil.EMPTY_METADATA, value, 0, sb);
        assertThat(sb.toString()).isEqualTo("\"line1\\nline2\\t\\\"quoted\\\"\"");
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
