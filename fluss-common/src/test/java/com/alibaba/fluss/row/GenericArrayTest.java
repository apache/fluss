/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

import static com.alibaba.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Test for {@link com.alibaba.fluss.row.GenericArray} class. */
public class GenericArrayTest {

    @Test
    public void testObjectArrayConstructor() {
        BinaryString[] data = {fromString("a"), fromString("b"), fromString("c")};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isFalse();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getString(0).toString()).isEqualTo("a");
        assertThat(array.getString(1).toString()).isEqualTo("b");
        assertThat(array.getString(2).toString()).isEqualTo("c");
    }

    @Test
    public void testIntArrayConstructor() {
        int[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.getInt(1)).isEqualTo(2);
        assertThat(array.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testLongArrayConstructor() {
        long[] data = {1L, 2L, 3L};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getLong(0)).isEqualTo(1L);
        assertThat(array.getLong(1)).isEqualTo(2L);
        assertThat(array.getLong(2)).isEqualTo(3L);
    }

    @Test
    public void testFloatArrayConstructor() {
        float[] data = {1.0f, 2.0f, 3.0f};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getFloat(0)).isEqualTo(1.0f);
        assertThat(array.getFloat(1)).isEqualTo(2.0f);
        assertThat(array.getFloat(2)).isEqualTo(3.0f);
    }

    @Test
    public void testDoubleArrayConstructor() {
        double[] data = {1.0, 2.0, 3.0};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getDouble(0)).isEqualTo(1.0);
        assertThat(array.getDouble(1)).isEqualTo(2.0);
        assertThat(array.getDouble(2)).isEqualTo(3.0);
    }

    @Test
    public void testShortArrayConstructor() {
        short[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getShort(0)).isEqualTo((short) 1);
        assertThat(array.getShort(1)).isEqualTo((short) 2);
        assertThat(array.getShort(2)).isEqualTo((short) 3);
    }

    @Test
    public void testByteArrayConstructor() {
        byte[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getByte(0)).isEqualTo((byte) 1);
        assertThat(array.getByte(1)).isEqualTo((byte) 2);
        assertThat(array.getByte(2)).isEqualTo((byte) 3);
    }

    @Test
    public void testBooleanArrayConstructor() {
        boolean[] data = {true, false, true};
        GenericArray array = new GenericArray(data);

        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getBoolean(1)).isFalse();
        assertThat(array.getBoolean(2)).isTrue();
    }

    @Test
    public void testToObjectArray() {
        Integer[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat((Object[]) array.toObjectArray()).containsExactly(1, 2, 3);
    }

    @Test
    public void testToIntArray() {
        int[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.toIntArray()).containsExactly(1, 2, 3);
    }

    @Test
    public void testToIntegerArrayFromObject() {
        Integer[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.toIntArray()).containsExactly(1, 2, 3);
    }

    @Test
    public void testToBooleanArray() {
        boolean[] data = {true, false, true};
        GenericArray array = new GenericArray(data);

        assertThat(array.toBooleanArray()).containsExactly(true, false, true);
    }

    @Test
    public void testToByteArray() {
        byte[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.toByteArray()).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    public void testToShortArray() {
        short[] data = {1, 2, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.toShortArray()).containsExactly((short) 1, (short) 2, (short) 3);
    }

    @Test
    public void testToLongArray() {
        long[] data = {1L, 2L, 3L};
        GenericArray array = new GenericArray(data);

        assertThat(array.toLongArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    public void testToFloatArray() {
        float[] data = {1.0f, 2.0f, 3.0f};
        GenericArray array = new GenericArray(data);

        assertThat(array.toFloatArray()).containsExactly(1.0f, 2.0f, 3.0f);
    }

    @Test
    public void testToDoubleArray() {
        double[] data = {1.0, 2.0, 3.0};
        GenericArray array = new GenericArray(data);

        assertThat(array.toDoubleArray()).containsExactly(1.0, 2.0, 3.0);
    }

    @Test
    public void testNullValueHandling() {
        Object[] data = {1, null, 3};
        GenericArray array = new GenericArray(data);

        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.isNullAt(0)).isFalse();
        assertThat(array.isNullAt(2)).isFalse();
    }

    @Test
    public void testEqualsAndHashCode() {
        Integer[] data1 = {1, 2, 3};
        Integer[] data2 = {1, 2, 3};
        GenericArray array1 = new GenericArray(data1);
        GenericArray array2 = new GenericArray(data2);

        assertThat(array1).isEqualTo(array2);
        assertThat(array1.hashCode()).isEqualTo(array2.hashCode());
    }

    @Test
    public void testOfMethod() {
        GenericArray array = GenericArray.of(fromString("x"), fromString("y"), fromString("z"));

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getString(0).toString()).isEqualTo("x");
        assertThat(array.getString(1).toString()).isEqualTo("y");
        assertThat(array.getString(2).toString()).isEqualTo("z");
    }

    @Test
    public void testGetStringFromArray() {
        BinaryString[] data = {fromString("hello"), fromString("world")};
        GenericArray array = new GenericArray(data);

        assertThat(array.getString(0).toString()).isEqualTo("hello");
        assertThat(array.getString(1).toString()).isEqualTo("world");
    }

    @Test
    public void testGetDecimalFromArray() {
        Decimal decimal1 = Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2);
        Decimal decimal2 = Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2);
        GenericArray array = GenericArray.of(decimal1, decimal2);

        assertThat(array.getDecimal(0, 10, 2)).isEqualTo(decimal1);
        assertThat(array.getDecimal(1, 10, 2)).isEqualTo(decimal2);
    }

    @Test
    public void testGetCharFromArray() {
        BinaryString[] data = {fromString("A"), fromString("B")};
        GenericArray array = new GenericArray(data);

        assertThat(array.getChar(0, 1).toString()).isEqualTo("A");
        assertThat(array.getChar(1, 1).toString()).isEqualTo("B");
    }

    @Test
    public void testGetTimestampNtzFromArray() {
        TimestampNtz ts1 = TimestampNtz.fromLocalDateTime(LocalDateTime.of(2025, 1, 1, 12, 0));
        TimestampNtz ts2 = TimestampNtz.fromLocalDateTime(LocalDateTime.of(2025, 1, 2, 12, 0));
        GenericArray array = GenericArray.of(ts1, ts2);

        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(ts1);
        assertThat(array.getTimestampNtz(1, 3)).isEqualTo(ts2);
    }

    @Test
    public void testGetTimestampLtzFromArray() {
        TimestampLtz ts1 = TimestampLtz.fromInstant(Instant.ofEpochSecond(1700000000));
        TimestampLtz ts2 = TimestampLtz.fromInstant(Instant.ofEpochSecond(1800000000));
        GenericArray array = GenericArray.of(ts1, ts2);

        assertThat(array.getTimestampLtz(0, 3)).isEqualTo(ts1);
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(ts2);
    }

    @Test
    public void testGetBytesFromArray() {
        byte[] bytes1 = "abc".getBytes();
        byte[] bytes2 = "def".getBytes();
        GenericArray array = GenericArray.of(bytes1, bytes2);

        assertThat(array.getBytes(0)).isEqualTo(bytes1);
        assertThat(array.getBytes(1)).isEqualTo(bytes2);
    }

    @Test
    public void testGetNestedArray() {
        InternalArray nested = GenericArray.of(1, 2, 3);
        GenericArray array = GenericArray.of(nested);

        assertThat(array.getArray(0)).isEqualTo(nested);
    }

    @Test
    public void testGetFieldCount() {
        GenericArray array = GenericArray.of("a", "b", "c");
        assertThat(array.getFieldCount()).isEqualTo(1);
    }

    @Test
    public void testIsNullAtForPrimitiveArray() {
        GenericArray array = new GenericArray(new int[] {1, 2, 3});
        assertThat(array.isNullAt(0)).isFalse();
        assertThat(array.isNullAt(1)).isFalse();
        assertThat(array.isNullAt(2)).isFalse();
    }

    @Test
    public void testNestedArrayOfArrays() {
        InternalArray nested1 = GenericArray.of(1, 2);
        InternalArray nested2 = GenericArray.of(3, 4);
        GenericArray array = GenericArray.of(nested1, nested2);

        assertThat(array.getArray(0)).isEqualTo(nested1);
        assertThat(array.getArray(1)).isEqualTo(nested2);
    }

    @Test
    public void testEqualsWithDifferentLength() {
        GenericArray array1 = GenericArray.of(1, 2);
        GenericArray array2 = GenericArray.of(1, 2, 3);
        assertThat(array1).isNotEqualTo(array2);
    }

    @Test
    public void testHashCodeWithNullElement() {
        GenericArray array1 = GenericArray.of("a", null, "c");
        GenericArray array2 = GenericArray.of("a", null, "c");
        assertThat(array1.hashCode()).isEqualTo(array2.hashCode());
    }

    // Null handling tests

    @Test
    public void testLongArrayConstructor_withNull_shouldThrowException() {
        long[] data = null;
        assertThatThrownBy(() -> new GenericArray(data)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testFloatArrayConstructor_withNull_shouldThrowException() {
        float[] data = null;
        assertThatThrownBy(() -> new GenericArray(data)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testDoubleArrayConstructor_withNull_shouldThrowException() {
        double[] data = null;
        assertThatThrownBy(() -> new GenericArray(data)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testShortArrayConstructor_withNull_shouldThrowException() {
        short[] data = null;
        assertThatThrownBy(() -> new GenericArray(data)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testByteArrayConstructor_withNull_shouldThrowException() {
        byte[] data = null;
        assertThatThrownBy(() -> new GenericArray(data)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testIsNullAtWithInvalidPosition() {
        Integer[] data = {1, null, 3};
        GenericArray array = new GenericArray(data);

        assertThatThrownBy(() -> array.isNullAt(-1))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThatThrownBy(() -> array.isNullAt(5))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    public void testToIntArrayWithAllNulls() {
        Integer[] data = {null, null, null};
        GenericArray array = new GenericArray(data);

        assertThatThrownBy(array::toIntArray).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testEqualsWithDifferentContent() {
        GenericArray array1 = GenericArray.of(1, 2, 3);
        GenericArray array2 = GenericArray.of(4, 5, 6);

        assertThat(array1).isNotEqualTo(array2);
    }

    @Test
    public void testConstructorWithNullObjectArray() {
        assertThatThrownBy(() -> new GenericArray((Object[]) null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testConstructorWithEmptyArray() {
        Integer[] data = {};
        GenericArray array = new GenericArray(data);

        assertThat(array.size()).isEqualTo(0);
        assertThat(array.isPrimitiveArray()).isFalse();
    }

    @Test
    public void testOfWithNullValues() {
        GenericArray array = GenericArray.of("a", null, "b");

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isNullAt(1)).isTrue();
    }

    @Test
    public void testPrivateConstructorWithZeroSize() {
        Object[] data = {};
        GenericArray array = new GenericArray(data, 0, false);
        assertThat(array.size()).isEqualTo(0);
        assertThat(array.isPrimitiveArray()).isFalse();
    }

    @Test
    public void testGetBooleanWithInvalidPosition() {
        GenericArray array = new GenericArray(new boolean[] {true});
        assertThatThrownBy(() -> array.getBoolean(-1))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThatThrownBy(() -> array.getBoolean(1))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    public void testGetObjectOnPrimitiveArray() {
        GenericArray array = new GenericArray(new int[] {1, 2, 3});
        assertThatThrownBy(() -> array.getObject(0)).isInstanceOf(ClassCastException.class);
    }
}
