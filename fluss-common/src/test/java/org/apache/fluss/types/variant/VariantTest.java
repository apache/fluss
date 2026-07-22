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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Variant}. */
class VariantTest {

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
