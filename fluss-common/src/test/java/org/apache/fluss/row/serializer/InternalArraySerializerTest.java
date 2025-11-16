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

package org.apache.fluss.row.serializer;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InternalArraySerializer}. */
public class InternalArraySerializerTest {

    @Test
    public void testCopyBinaryArray() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied).isNotSameAs(array);
        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getInt(0)).isEqualTo(1);
        assertThat(copied.getInt(1)).isEqualTo(2);
        assertThat(copied.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testCopyGenericArrayBoolean() {
        boolean[] primitiveArray = {true, false, true};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.BOOLEAN());

        InternalArray copied = serializer.copy(array);

        assertThat(copied).isNotSameAs(array);
        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getBoolean(0)).isTrue();
        assertThat(copied.getBoolean(1)).isFalse();
        assertThat(copied.getBoolean(2)).isTrue();
    }

    @Test
    public void testCopyGenericArrayByte() {
        byte[] primitiveArray = {1, 2, 3};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.TINYINT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getByte(0)).isEqualTo((byte) 1);
    }

    @Test
    public void testCopyGenericArrayShort() {
        short[] primitiveArray = {10, 20, 30};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.SMALLINT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getShort(0)).isEqualTo((short) 10);
    }

    @Test
    public void testCopyGenericArrayInt() {
        int[] primitiveArray = {100, 200, 300};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getInt(0)).isEqualTo(100);
    }

    @Test
    public void testCopyGenericArrayLong() {
        long[] primitiveArray = {1000L, 2000L, 3000L};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.BIGINT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getLong(0)).isEqualTo(1000L);
    }

    @Test
    public void testCopyGenericArrayFloat() {
        float[] primitiveArray = {1.5f, 2.5f, 3.5f};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.FLOAT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getFloat(0)).isEqualTo(1.5f);
    }

    @Test
    public void testCopyGenericArrayDouble() {
        double[] primitiveArray = {1.1, 2.2, 3.3};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.DOUBLE());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getDouble(0)).isEqualTo(1.1);
    }

    @Test
    public void testToBinaryArrayFromGenericArray() {
        int[] primitiveArray = {1, 2, 3, 4, 5};
        GenericArray genericArray = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        BinaryArray binaryArray = serializer.toBinaryArray(genericArray);

        assertThat(binaryArray.size()).isEqualTo(5);
        assertThat(binaryArray.getInt(0)).isEqualTo(1);
        assertThat(binaryArray.getInt(4)).isEqualTo(5);
    }

    @Test
    public void testToBinaryArrayFromBinaryArray() {
        BinaryArray originalArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        BinaryArray result = serializer.toBinaryArray(originalArray);

        assertThat(result).isSameAs(originalArray);
    }

    @Test
    public void testToBinaryArrayWithNulls() {
        Integer[] objectArray = {1, null, 3};
        GenericArray genericArray = new GenericArray(objectArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        BinaryArray binaryArray = serializer.toBinaryArray(genericArray);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getInt(0)).isEqualTo(1);
        assertThat(binaryArray.isNullAt(1)).isTrue();
        assertThat(binaryArray.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testConvertToJavaArray() {
        int[] primitiveArray = {1, 2, 3};
        BinaryArray array = BinaryArray.fromPrimitiveArray(primitiveArray);

        Object[] javaArray = InternalArraySerializer.convertToJavaArray(array, DataTypes.INT());

        assertThat(javaArray).hasSize(3);
        assertThat(javaArray[0]).isEqualTo(1);
        assertThat(javaArray[1]).isEqualTo(2);
        assertThat(javaArray[2]).isEqualTo(3);
    }

    @Test
    public void testConvertToJavaArrayWithNulls() {
        Integer[] objectArray = {10, null, 30};
        GenericArray array = new GenericArray(objectArray);

        Object[] javaArray = InternalArraySerializer.convertToJavaArray(array, DataTypes.INT());

        assertThat(javaArray).hasSize(3);
        assertThat(javaArray[0]).isEqualTo(10);
        assertThat(javaArray[1]).isNull();
        assertThat(javaArray[2]).isEqualTo(30);
    }

    @Test
    public void testEquals() {
        InternalArraySerializer serializer1 = new InternalArraySerializer(DataTypes.INT());
        InternalArraySerializer serializer2 = new InternalArraySerializer(DataTypes.INT());
        InternalArraySerializer serializer3 = new InternalArraySerializer(DataTypes.STRING());

        assertThat(serializer1.equals(serializer2)).isTrue();
        assertThat(serializer1.equals(serializer3)).isFalse();
        assertThat(serializer1.equals(null)).isFalse();
        assertThat(serializer1.equals(serializer1)).isTrue();
    }

    @Test
    public void testHashCode() {
        InternalArraySerializer serializer1 = new InternalArraySerializer(DataTypes.INT());
        InternalArraySerializer serializer2 = new InternalArraySerializer(DataTypes.INT());

        assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
    }

    @Test
    public void testToBinaryArrayReuseWriter() {
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        int[] array1 = {1, 2, 3};
        GenericArray genericArray1 = new GenericArray(array1);
        BinaryArray result1 = serializer.toBinaryArray(genericArray1);

        int[] array2 = {4, 5, 6};
        GenericArray genericArray2 = new GenericArray(array2);
        BinaryArray result2 = serializer.toBinaryArray(genericArray2);

        assertThat(result2.getInt(0)).isEqualTo(4);
        assertThat(result2.getInt(1)).isEqualTo(5);
        assertThat(result2.getInt(2)).isEqualTo(6);
    }

    @Test
    public void testToBinaryArrayWithDifferentSize() {
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        int[] array1 = {1, 2};
        GenericArray genericArray1 = new GenericArray(array1);
        BinaryArray result1 = serializer.toBinaryArray(genericArray1);

        int[] array2 = {3, 4, 5, 6};
        GenericArray genericArray2 = new GenericArray(array2);
        BinaryArray result2 = serializer.toBinaryArray(genericArray2);

        assertThat(result2.size()).isEqualTo(4);
        assertThat(result2.getInt(0)).isEqualTo(3);
        assertThat(result2.getInt(3)).isEqualTo(6);
    }

    @Test
    public void testCopyNonPrimitiveGenericArray() {
        int[] primitiveArray = {100, 200, 300};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied).isInstanceOf(GenericArray.class);
        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getInt(0)).isEqualTo(100);
        assertThat(copied.getInt(2)).isEqualTo(300);
    }

    @Test
    public void testCopyFromOtherInternalArray() {
        GenericArray array = new GenericArray(new int[] {1, 2, 3});
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getInt(0)).isEqualTo(1);
    }

    @Test
    public void testEqualsWithDifferentObject() {
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        assertThat(serializer.equals("string")).isFalse();
    }

    @Test
    public void testToBinaryArrayFromGenericArrayWithBytes() {
        byte[] primitiveArray = {1, 2, 3};
        GenericArray array = new GenericArray(primitiveArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.TINYINT());

        BinaryArray binaryArray = serializer.toBinaryArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getByte(0)).isEqualTo((byte) 1);
    }

    @Test
    public void testEqualsWithNull() {
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        assertThat(serializer.equals(null)).isFalse();
    }

    @Test
    public void testCopyGenericArrayAllNulls() {
        Integer[] objectArray = {null, null, null};
        GenericArray array = new GenericArray(objectArray);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());

        InternalArray copied = serializer.copy(array);

        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.isNullAt(0)).isTrue();
        assertThat(copied.isNullAt(1)).isTrue();
        assertThat(copied.isNullAt(2)).isTrue();
    }
}
