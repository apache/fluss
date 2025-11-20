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

package org.apache.fluss.row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link GenericArray}. */
public class GenericArrayTest {

    @Test
    public void testObjectArray() {
        Object[] objArray = {
            BinaryString.fromString("hello"),
            BinaryString.fromString("world"),
            null,
            BinaryString.fromString("test")
        };
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(4);
        assertThat(array.isPrimitiveArray()).isFalse();

        assertThat(array.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getString(1)).isEqualTo(BinaryString.fromString("world"));
        assertThat(array.isNullAt(2)).isTrue();
        assertThat(array.getString(3)).isEqualTo(BinaryString.fromString("test"));

        Object[] converted = array.toObjectArray();
        assertThat(converted).isEqualTo(objArray);
    }

    @Test
    public void testIntArray() {
        int[] intArray = {1, 2, 3, 4, 5};
        GenericArray array = new GenericArray(intArray);

        assertThat(array.size()).isEqualTo(5);
        assertThat(array.isPrimitiveArray()).isTrue();
        assertThat(array.isNullAt(0)).isFalse();

        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.getInt(4)).isEqualTo(5);

        int[] converted = array.toIntArray();
        assertThat(converted).isEqualTo(intArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testLongArray() {
        long[] longArray = {100L, 200L, 300L};
        GenericArray array = new GenericArray(longArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isPrimitiveArray()).isTrue();

        assertThat(array.getLong(0)).isEqualTo(100L);
        assertThat(array.getLong(2)).isEqualTo(300L);

        long[] converted = array.toLongArray();
        assertThat(converted).isEqualTo(longArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly(100L, 200L, 300L);
    }

    @Test
    public void testFloatArray() {
        float[] floatArray = {1.5f, 2.5f, 3.5f};
        GenericArray array = new GenericArray(floatArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isPrimitiveArray()).isTrue();

        assertThat(array.getFloat(0)).isEqualTo(1.5f);
        assertThat(array.getFloat(2)).isEqualTo(3.5f);

        float[] converted = array.toFloatArray();
        assertThat(converted).isEqualTo(floatArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly(1.5f, 2.5f, 3.5f);
    }

    @Test
    public void testDoubleArray() {
        double[] doubleArray = {1.1, 2.2, 3.3};
        GenericArray array = new GenericArray(doubleArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isPrimitiveArray()).isTrue();

        assertThat(array.getDouble(0)).isEqualTo(1.1);
        assertThat(array.getDouble(2)).isEqualTo(3.3);

        double[] converted = array.toDoubleArray();
        assertThat(converted).isEqualTo(doubleArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly(1.1, 2.2, 3.3);
    }

    @Test
    public void testShortArray() {
        short[] shortArray = {10, 20, 30};
        GenericArray array = new GenericArray(shortArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isPrimitiveArray()).isTrue();

        assertThat(array.getShort(0)).isEqualTo((short) 10);
        assertThat(array.getShort(2)).isEqualTo((short) 30);

        short[] converted = array.toShortArray();
        assertThat(converted).isEqualTo(shortArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly((short) 10, (short) 20, (short) 30);
    }

    @Test
    public void testByteArray() {
        byte[] byteArray = {1, 2, 3};
        GenericArray array = new GenericArray(byteArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isPrimitiveArray()).isTrue();

        assertThat(array.getByte(0)).isEqualTo((byte) 1);
        assertThat(array.getByte(2)).isEqualTo((byte) 3);

        byte[] converted = array.toByteArray();
        assertThat(converted).isEqualTo(byteArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    public void testBooleanArray() {
        boolean[] boolArray = {true, false, true};
        GenericArray array = new GenericArray(boolArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isPrimitiveArray()).isTrue();

        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getBoolean(1)).isFalse();
        assertThat(array.getBoolean(2)).isTrue();

        boolean[] converted = array.toBooleanArray();
        assertThat(converted).isEqualTo(boolArray);

        Object[] objArray = array.toObjectArray();
        assertThat(objArray).containsExactly(true, false, true);
    }

    @Test
    public void testNullInObjectArray() {
        Object[] objArray = {1, null, 3};
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isNullAt(0)).isFalse();
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.isNullAt(2)).isFalse();

        assertThatThrownBy(() -> array.toIntArray())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Primitive array must not contain a null value");
    }

    @Test
    public void testDecimalArray() {
        Object[] objArray = {
            Decimal.fromUnscaledLong(123, 5, 2), Decimal.fromUnscaledLong(456, 5, 2), null
        };
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getDecimal(0, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));
        assertThat(array.getDecimal(1, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(456, 5, 2));
        assertThat(array.isNullAt(2)).isTrue();
    }

    @Test
    public void testTimestampArray() {
        Object[] objArray = {TimestampNtz.fromMillis(1000L), TimestampNtz.fromMillis(2000L), null};
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(TimestampNtz.fromMillis(1000L));
        assertThat(array.getTimestampNtz(1, 3)).isEqualTo(TimestampNtz.fromMillis(2000L));
        assertThat(array.isNullAt(2)).isTrue();
    }

    @Test
    public void testTimestampLtzArray() {
        Object[] objArray = {
            TimestampLtz.fromEpochMillis(1000L), TimestampLtz.fromEpochMillis(2000L), null
        };
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getTimestampLtz(0, 3)).isEqualTo(TimestampLtz.fromEpochMillis(1000L));
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(TimestampLtz.fromEpochMillis(2000L));
        assertThat(array.isNullAt(2)).isTrue();
    }

    @Test
    public void testBinaryArray() {
        Object[] objArray = {new byte[] {1, 2}, new byte[] {3, 4}, null};
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getBytes(0)).isEqualTo(new byte[] {1, 2});
        assertThat(array.getBinary(1, 2)).isEqualTo(new byte[] {3, 4});
        assertThat(array.isNullAt(2)).isTrue();
    }

    @Test
    public void testCharArray() {
        Object[] objArray = {BinaryString.fromString("a"), BinaryString.fromString("b"), null};
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getChar(0, 1)).isEqualTo(BinaryString.fromString("a"));
        assertThat(array.getChar(1, 1)).isEqualTo(BinaryString.fromString("b"));
        assertThat(array.isNullAt(2)).isTrue();
    }

    @Test
    public void testNestedArray() {
        Object[] innerArray = {1, 2, 3};
        GenericArray inner = new GenericArray(innerArray);

        Object[] outerArray = {inner, null};
        GenericArray outer = new GenericArray(outerArray);

        assertThat(outer.size()).isEqualTo(2);
        assertThat(outer.getArray(0)).isEqualTo(inner);
        assertThat(outer.isNullAt(1)).isTrue();
    }

    @Test
    public void testEqualsAndHashCode() {
        int[] intArray1 = {1, 2, 3};
        int[] intArray2 = {1, 2, 3};
        int[] intArray3 = {1, 2, 4};

        GenericArray array1 = new GenericArray(intArray1);
        GenericArray array2 = new GenericArray(intArray2);
        GenericArray array3 = new GenericArray(intArray3);

        assertThat(array1).isEqualTo(array2);
        assertThat(array1.hashCode()).isEqualTo(array2.hashCode());

        assertThat(array1).isNotEqualTo(array3);
        assertThat(array1).isNotEqualTo(null);
        assertThat(array1).isEqualTo(array1);
    }

    @Test
    public void testOfMethod() {
        GenericArray array = GenericArray.of(1, 2, 3, 4, 5);

        assertThat(array.size()).isEqualTo(5);
        assertThat(array.toObjectArray()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testObjectArrayWithObjects() {
        Object[] objArray = {Integer.valueOf(100), Long.valueOf(200L), Float.valueOf(1.5f)};
        GenericArray array = new GenericArray(objArray);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(100);
        assertThat(array.getLong(1)).isEqualTo(200L);
        assertThat(array.getFloat(2)).isEqualTo(1.5f);
    }
}
