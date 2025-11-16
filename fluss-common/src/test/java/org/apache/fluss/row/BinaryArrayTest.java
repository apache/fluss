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

import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BinaryArray}. */
public class BinaryArrayTest {

    @Test
    public void testFromPrimitiveBooleanArray() {
        boolean[] array = {true, false, true, false};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(4);
        assertThat(binaryArray.getBoolean(0)).isTrue();
        assertThat(binaryArray.getBoolean(1)).isFalse();
        assertThat(binaryArray.getBoolean(2)).isTrue();
        assertThat(binaryArray.getBoolean(3)).isFalse();
    }

    @Test
    public void testFromPrimitiveByteArray() {
        byte[] array = {1, 2, 3, 4, 5};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(5);
        assertThat(binaryArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(binaryArray.getByte(4)).isEqualTo((byte) 5);
    }

    @Test
    public void testFromPrimitiveShortArray() {
        short[] array = {10, 20, 30};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getShort(0)).isEqualTo((short) 10);
        assertThat(binaryArray.getShort(2)).isEqualTo((short) 30);
    }

    @Test
    public void testFromPrimitiveIntArray() {
        int[] array = {100, 200, 300};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getInt(0)).isEqualTo(100);
        assertThat(binaryArray.getInt(2)).isEqualTo(300);
    }

    @Test
    public void testFromPrimitiveLongArray() {
        long[] array = {1000L, 2000L, 3000L};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getLong(0)).isEqualTo(1000L);
        assertThat(binaryArray.getLong(2)).isEqualTo(3000L);
    }

    @Test
    public void testFromPrimitiveFloatArray() {
        float[] array = {1.5f, 2.5f, 3.5f};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getFloat(0)).isEqualTo(1.5f);
        assertThat(binaryArray.getFloat(2)).isEqualTo(3.5f);
    }

    @Test
    public void testFromPrimitiveDoubleArray() {
        double[] array = {1.1, 2.2, 3.3};
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getDouble(0)).isEqualTo(1.1);
        assertThat(binaryArray.getDouble(2)).isEqualTo(3.3);
    }

    @Test
    public void testWriteAndReadInt() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.writeInt(1, 20);
        writer.writeInt(2, 30);
        writer.complete();

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(10);
        assertThat(array.getInt(1)).isEqualTo(20);
        assertThat(array.getInt(2)).isEqualTo(30);
    }

    @Test
    public void testWriteAndReadString() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        writer.writeString(0, BinaryString.fromString("hello"));
        writer.writeString(1, BinaryString.fromString("world"));
        writer.complete();

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getString(1)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testSetNull() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 8);

        writer.writeLong(0, 100L);
        writer.setNullLong(1);
        writer.writeLong(2, 300L);
        writer.complete();

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.isNullAt(0)).isFalse();
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.isNullAt(2)).isFalse();
        assertThat(array.getLong(0)).isEqualTo(100L);
        assertThat(array.getLong(2)).isEqualTo(300L);
    }

    @Test
    public void testSetAndGetDecimal() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        Decimal decimal1 = Decimal.fromUnscaledLong(123, 5, 2);
        Decimal decimal2 = Decimal.fromUnscaledLong(456, 5, 2);

        writer.writeDecimal(0, decimal1, 5);
        writer.writeDecimal(1, decimal2, 5);
        writer.complete();

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getDecimal(0, 5, 2)).isEqualTo(decimal1);
        assertThat(array.getDecimal(1, 5, 2)).isEqualTo(decimal2);
    }

    @Test
    public void testSetAndGetTimestampNtz() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz ts1 = TimestampNtz.fromMillis(1000L);
        TimestampNtz ts2 = TimestampNtz.fromMillis(2000L);

        writer.writeTimestampNtz(0, ts1, 3);
        writer.writeTimestampNtz(1, ts2, 3);
        writer.complete();

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(ts1);
        assertThat(array.getTimestampNtz(1, 3)).isEqualTo(ts2);
    }

    @Test
    public void testSetAndGetTimestampLtz() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1000L);
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(2000L);

        writer.writeTimestampLtz(0, ts1, 3);
        writer.writeTimestampLtz(1, ts2, 3);
        writer.complete();

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getTimestampLtz(0, 3)).isEqualTo(ts1);
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(ts2);
    }

    @Test
    public void testSetAndGetBinary() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        byte[] binary1 = {1, 2, 3};
        byte[] binary2 = {4, 5, 6};

        writer.writeBytes(0, binary1);
        writer.writeBytes(1, binary2);
        writer.complete();

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getBytes(0)).isEqualTo(binary1);
        assertThat(array.getBinary(1, 3)).isEqualTo(binary2);
    }

    @Test
    public void testCopy() {
        int[] intArray = {1, 2, 3, 4, 5};
        BinaryArray original = BinaryArray.fromPrimitiveArray(intArray);

        BinaryArray copied = original.copy();

        assertThat(copied.size()).isEqualTo(original.size());
        assertThat(copied.getInt(0)).isEqualTo(1);
        assertThat(copied.getInt(4)).isEqualTo(5);
    }

    @Test
    public void testCopyWithReuse() {
        int[] intArray = {1, 2, 3};
        BinaryArray original = BinaryArray.fromPrimitiveArray(intArray);

        BinaryArray reuse = new BinaryArray();
        BinaryArray copied = original.copy(reuse);

        assertThat(copied).isSameAs(reuse);
        assertThat(copied.size()).isEqualTo(3);
        assertThat(copied.getInt(0)).isEqualTo(1);
        assertThat(copied.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testHashCode() {
        int[] intArray = {1, 2, 3};
        BinaryArray array1 = BinaryArray.fromPrimitiveArray(intArray);
        BinaryArray array2 = BinaryArray.fromPrimitiveArray(intArray);

        assertThat(array1.hashCode()).isEqualTo(array2.hashCode());
    }

    @Test
    public void testFromLongArray() {
        Long[] longArray = {100L, 200L, null, 300L};
        BinaryArray binaryArray = BinaryArray.fromLongArray(longArray);

        assertThat(binaryArray.size()).isEqualTo(4);
        assertThat(binaryArray.getLong(0)).isEqualTo(100L);
        assertThat(binaryArray.getLong(1)).isEqualTo(200L);
        assertThat(binaryArray.isNullAt(2)).isTrue();
        assertThat(binaryArray.getLong(3)).isEqualTo(300L);
    }

    @Test
    public void testFromInternalArray() {
        GenericArray genericArray = new GenericArray(new long[] {10L, 20L, 30L});
        BinaryArray binaryArray = BinaryArray.fromLongArray(genericArray);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getLong(0)).isEqualTo(10L);
        assertThat(binaryArray.getLong(1)).isEqualTo(20L);
        assertThat(binaryArray.getLong(2)).isEqualTo(30L);
    }

    @Test
    public void testFromInternalArrayAlreadyBinary() {
        BinaryArray original = BinaryArray.fromPrimitiveArray(new long[] {1L, 2L, 3L});
        BinaryArray result = BinaryArray.fromLongArray(original);

        assertThat(result).isSameAs(original);
    }

    @Test
    public void testAnyNull() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.setNullInt(1);
        writer.writeInt(2, 30);
        writer.complete();

        assertThat(array.anyNull()).isTrue();
    }

    @Test
    public void testAnyNullWhenNoNulls() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        assertThat(array.anyNull()).isFalse();
    }

    @Test
    public void testToArrayWithNullThrowsException() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.setNullInt(1);
        writer.writeInt(2, 30);
        writer.complete();

        assertThatThrownBy(() -> array.toIntArray())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Primitive array must not contain a null value");
    }

    @Test
    public void testToObjectArray() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        Integer[] objects = array.toObjectArray(DataTypes.INT());

        assertThat(objects).containsExactly(1, 2, 3);
    }

    @Test
    public void testToObjectArrayWithNull() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.setNullInt(1);
        writer.writeInt(2, 30);
        writer.complete();

        Integer[] objects = array.toObjectArray(DataTypes.INT());
        assertThat(objects).containsExactly(10, null, 30);
    }

    @Test
    public void testGetChar() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        writer.writeString(0, BinaryString.fromString("hello"));
        writer.writeString(1, BinaryString.fromString("world"));
        writer.complete();

        assertThat(array.getChar(0, 5)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getChar(1, 5)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testSetBoolean() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 1);

        writer.writeBoolean(0, true);
        writer.writeBoolean(1, false);
        writer.writeBoolean(2, true);
        writer.complete();

        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getBoolean(1)).isFalse();
        assertThat(array.getBoolean(2)).isTrue();
    }

    @Test
    public void testSetByte() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 1);

        writer.writeByte(0, (byte) 1);
        writer.writeByte(1, (byte) 2);
        writer.writeByte(2, (byte) 3);
        writer.complete();

        assertThat(array.getByte(0)).isEqualTo((byte) 1);
        assertThat(array.getByte(1)).isEqualTo((byte) 2);
        assertThat(array.getByte(2)).isEqualTo((byte) 3);
    }

    @Test
    public void testSetShort() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 2);

        writer.writeShort(0, (short) 10);
        writer.writeShort(1, (short) 20);
        writer.writeShort(2, (short) 30);
        writer.complete();

        assertThat(array.getShort(0)).isEqualTo((short) 10);
        assertThat(array.getShort(1)).isEqualTo((short) 20);
        assertThat(array.getShort(2)).isEqualTo((short) 30);
    }

    @Test
    public void testSetFloat() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeFloat(0, 1.5f);
        writer.writeFloat(1, 2.5f);
        writer.writeFloat(2, Float.NaN);
        writer.complete();

        assertThat(array.getFloat(0)).isEqualTo(1.5f);
        assertThat(array.getFloat(1)).isEqualTo(2.5f);
        assertThat(array.getFloat(2)).isNaN();
    }

    @Test
    public void testSetDouble() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 8);

        writer.writeDouble(0, 1.1);
        writer.writeDouble(1, 2.2);
        writer.writeDouble(2, Double.NaN);
        writer.complete();

        assertThat(array.getDouble(0)).isEqualTo(1.1);
        assertThat(array.getDouble(1)).isEqualTo(2.2);
        assertThat(array.getDouble(2)).isNaN();
    }

    @Test
    public void testCalculateHeaderInBytes() {
        assertThat(BinaryArray.calculateHeaderInBytes(0)).isEqualTo(4);
        assertThat(BinaryArray.calculateHeaderInBytes(10)).isEqualTo(8);
        assertThat(BinaryArray.calculateHeaderInBytes(32)).isEqualTo(8);
        assertThat(BinaryArray.calculateHeaderInBytes(33)).isEqualTo(12);
    }

    @Test
    public void testCalculateFixLengthPartSize() {
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.BOOLEAN())).isEqualTo(1);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.TINYINT())).isEqualTo(1);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.SMALLINT())).isEqualTo(2);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.INT())).isEqualTo(4);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.BIGINT())).isEqualTo(8);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.FLOAT())).isEqualTo(4);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.DOUBLE())).isEqualTo(8);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.STRING())).isEqualTo(8);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.ARRAY(DataTypes.INT())))
                .isEqualTo(8);
    }

    @Test
    public void testPointTo() {
        int[] intArray = {1, 2, 3};
        BinaryArray array1 = BinaryArray.fromPrimitiveArray(intArray);

        BinaryArray array2 = new BinaryArray();
        array2.pointTo(array1.getSegments(), array1.getOffset(), array1.getSizeInBytes());

        assertThat(array2.size()).isEqualTo(3);
        assertThat(array2.getInt(0)).isEqualTo(1);
        assertThat(array2.getInt(1)).isEqualTo(2);
        assertThat(array2.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testHighPrecisionTimestamp() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz ts1 = TimestampNtz.fromMillis(1000L, 123456);
        TimestampNtz ts2 = TimestampNtz.fromMillis(2000L, 654321);

        writer.writeTimestampNtz(0, ts1, 9);
        writer.writeTimestampNtz(1, ts2, 9);
        writer.complete();

        assertThat(array.getTimestampNtz(0, 9)).isEqualTo(ts1);
        assertThat(array.getTimestampNtz(1, 9)).isEqualTo(ts2);
    }

    @Test
    public void testHighPrecisionTimestampLtz() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1000L, 123456);
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(2000L, 654321);

        writer.writeTimestampLtz(0, ts1, 9);
        writer.writeTimestampLtz(1, ts2, 9);
        writer.complete();

        assertThat(array.getTimestampLtz(0, 9)).isEqualTo(ts1);
        assertThat(array.getTimestampLtz(1, 9)).isEqualTo(ts2);
    }

    @Test
    public void testLargeDecimal() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        Decimal decimal1 = Decimal.fromBigDecimal(new java.math.BigDecimal("123.456"), 20, 3);
        Decimal decimal2 = Decimal.fromBigDecimal(new java.math.BigDecimal("789.012"), 20, 3);

        writer.writeDecimal(0, decimal1, 20);
        writer.writeDecimal(1, decimal2, 20);
        writer.complete();

        assertThat(array.getDecimal(0, 20, 3)).isEqualTo(decimal1);
        assertThat(array.getDecimal(1, 20, 3)).isEqualTo(decimal2);
    }

    @Test
    public void testSetNotNullAt() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 8);

        writer.setNullLong(0);
        writer.writeLong(1, 100L);
        writer.writeLong(2, 200L);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();

        array.setNotNullAt(0);
        assertThat(array.isNullAt(0)).isFalse();
    }

    @Test
    public void testSetNullAt() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 8);

        writer.writeLong(0, 100L);
        writer.writeLong(1, 200L);
        writer.writeLong(2, 300L);
        writer.complete();

        assertThat(array.isNullAt(1)).isFalse();

        array.setNullAt(1);
        assertThat(array.isNullAt(1)).isTrue();
    }

    @Test
    public void testSetLong() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new long[] {1L, 2L, 3L});

        array.setLong(1, 999L);

        assertThat(array.getLong(0)).isEqualTo(1L);
        assertThat(array.getLong(1)).isEqualTo(999L);
        assertThat(array.getLong(2)).isEqualTo(3L);
    }

    @Test
    public void testSetInt() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});

        array.setInt(1, 999);

        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.getInt(1)).isEqualTo(999);
        assertThat(array.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testSetDecimalCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        Decimal decimal1 = Decimal.fromUnscaledLong(123, 5, 2);
        Decimal decimal2 = Decimal.fromUnscaledLong(456, 5, 2);

        writer.writeDecimal(0, decimal1, 5);
        writer.writeDecimal(1, decimal2, 5);
        writer.complete();

        Decimal newDecimal = Decimal.fromUnscaledLong(999, 5, 2);
        array.setDecimal(0, newDecimal, 5);

        assertThat(array.getDecimal(0, 5, 2)).isEqualTo(newDecimal);
        assertThat(array.getDecimal(1, 5, 2)).isEqualTo(decimal2);
    }

    @Test
    public void testSetTimestampNtzCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz ts1 = TimestampNtz.fromMillis(1000L);
        TimestampNtz ts2 = TimestampNtz.fromMillis(2000L);

        writer.writeTimestampNtz(0, ts1, 3);
        writer.writeTimestampNtz(1, ts2, 3);
        writer.complete();

        TimestampNtz newTs = TimestampNtz.fromMillis(9999L);
        array.setTimestampNtz(0, newTs, 3);

        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(newTs);
        assertThat(array.getTimestampNtz(1, 3)).isEqualTo(ts2);
    }

    @Test
    public void testSetTimestampLtzCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1000L);
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(2000L);

        writer.writeTimestampLtz(0, ts1, 3);
        writer.writeTimestampLtz(1, ts2, 3);
        writer.complete();

        TimestampLtz newTs = TimestampLtz.fromEpochMillis(9999L);
        array.setTimestampLtz(0, newTs, 3);

        assertThat(array.getTimestampLtz(0, 3)).isEqualTo(newTs);
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(ts2);
    }

    @Test
    public void testToObjectArrayWithNulls() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 100);
        writer.setNullInt(1);
        writer.writeInt(2, 300);
        writer.complete();

        Integer[] objArray = array.toObjectArray(DataTypes.INT());
        assertThat(objArray).hasSize(3);
        assertThat(objArray[0]).isEqualTo(100);
        assertThat(objArray[1]).isNull();
        assertThat(objArray[2]).isEqualTo(300);
    }

    @Test
    public void testFromLongArrayWithBoxed() {
        Long[] array = {100L, 200L, 300L};
        BinaryArray binaryArray = BinaryArray.fromLongArray(array);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getLong(0)).isEqualTo(100L);
        assertThat(binaryArray.getLong(1)).isEqualTo(200L);
        assertThat(binaryArray.getLong(2)).isEqualTo(300L);
    }

    @Test
    public void testFromLongArrayWithInternalArray() {
        BinaryArray sourceArray = BinaryArray.fromPrimitiveArray(new long[] {111L, 222L, 333L});
        BinaryArray binaryArray = BinaryArray.fromLongArray(sourceArray);

        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getLong(0)).isEqualTo(111L);
        assertThat(binaryArray.getLong(1)).isEqualTo(222L);
        assertThat(binaryArray.getLong(2)).isEqualTo(333L);
    }

    @Test
    public void testGetBinaryWithoutLength() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        byte[] binary1 = {1, 2, 3, 4, 5};
        byte[] binary2 = {10, 20, 30};

        writer.writeBinary(0, binary1, 10);
        writer.writeBinary(1, binary2, 10);
        writer.complete();

        byte[] retrieved1 = array.getBinary(0);
        byte[] retrieved2 = array.getBinary(1);

        assertThat(retrieved1).isNotNull();
        assertThat(retrieved2).isNotNull();
    }

    @Test
    public void testGetSegments() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        assertThat(array.getSegments()).isNotNull();
    }

    @Test
    public void testGetOffset() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        assertThat(array.getOffset()).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testEquals() {
        BinaryArray array1 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray array2 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        assertThat(array1.equals(array2)).isTrue();
        assertThat(array1.equals(null)).isFalse();
    }

    @Test
    public void testSetNullLong() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
        writer.setNullLong(0);
        writer.writeLong(1, 100L);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getLong(1)).isEqualTo(100L);
    }

    @Test
    public void testSetNullInt() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
        writer.setNullInt(0);
        writer.writeInt(1, 100);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getInt(1)).isEqualTo(100);
    }

    @Test
    public void testSetNullFloat() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
        writer.setNullFloat(0);
        writer.writeFloat(1, 3.14f);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getFloat(1)).isEqualTo(3.14f);
    }

    @Test
    public void testSetNullDouble() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
        writer.setNullDouble(0);
        writer.writeDouble(1, 3.14159);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getDouble(1)).isEqualTo(3.14159);
    }

    @Test
    public void testSetNullBoolean() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);
        writer.setNullBoolean(0);
        writer.writeBoolean(1, true);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getBoolean(1)).isTrue();
    }

    @Test
    public void testSetNullByte() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);
        writer.setNullByte(0);
        writer.writeByte(1, (byte) 42);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getByte(1)).isEqualTo((byte) 42);
    }

    @Test
    public void testSetNullShort() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 2);
        writer.setNullShort(0);
        writer.writeShort(1, (short) 123);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getShort(1)).isEqualTo((short) 123);
    }

    @Test
    public void testGetString() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);
        writer.writeString(0, BinaryString.fromString("test"));
        writer.complete();

        assertThat(array.getString(0).toString()).isEqualTo("test");
    }

    @Test
    public void testGetBytes() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);
        byte[] bytes = {1, 2, 3, 4, 5};
        writer.writeBinary(0, bytes, 10);
        writer.complete();

        byte[] result = array.getBytes(0);
        assertThat(result).isNotNull();
    }

    @Test
    public void testFromLongArrayWithNull() {
        Long[] input = {1L, null, 3L};
        BinaryArray array = BinaryArray.fromLongArray(input);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getLong(0)).isEqualTo(1L);
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.getLong(2)).isEqualTo(3L);
    }

    @Test
    public void testGetSizeInBytes() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});

        int size = array.getSizeInBytes();

        assertThat(size).isGreaterThan(0);
    }

    @Test
    public void testCopyWithMultipleTypes() {
        BinaryArray array = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3, 4, 5});

        BinaryArray copied = array.copy();

        assertThat(copied).isNotSameAs(array);
        assertThat(copied.size()).isEqualTo(5);
        assertThat(copied.getInt(0)).isEqualTo(1);
        assertThat(copied.getInt(4)).isEqualTo(5);
    }

    @Test
    public void testHashCodeConsistency() {
        BinaryArray array1 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray array2 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});

        assertThat(array1.hashCode()).isEqualTo(array2.hashCode());
    }

    @Test
    public void testSetDecimalNonCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        Decimal largeDecimal =
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.123456789"), 30, 9);
        writer.writeDecimal(0, largeDecimal, 30);
        writer.writeDecimal(1, Decimal.fromUnscaledLong(123, 10, 2), 10);
        writer.complete();

        array.setDecimal(0, null, 30);
        assertThat(array.isNullAt(0)).isTrue();

        Decimal newDecimal =
                Decimal.fromBigDecimal(new BigDecimal("99999999999999999999.999999999"), 30, 9);
        array.setDecimal(0, newDecimal, 30);
        assertThat(array.getDecimal(0, 30, 9)).isEqualTo(newDecimal);
    }

    @Test
    public void testSetTimestampNtzNonCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz ts1 = TimestampNtz.fromMillis(1000L, 123456);
        TimestampNtz ts2 = TimestampNtz.fromMillis(2000L, 654321);
        writer.writeTimestampNtz(0, ts1, 9);
        writer.writeTimestampNtz(1, ts2, 9);
        writer.complete();

        array.setTimestampNtz(0, null, 9);
        assertThat(array.isNullAt(0)).isTrue();

        TimestampNtz newTs = TimestampNtz.fromMillis(9999L, 999999);
        array.setTimestampNtz(0, newTs, 9);
        assertThat(array.getTimestampNtz(0, 9)).isEqualTo(newTs);
    }

    @Test
    public void testSetTimestampLtzNonCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1000L, 123456);
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(2000L, 654321);
        writer.writeTimestampLtz(0, ts1, 9);
        writer.writeTimestampLtz(1, ts2, 9);
        writer.complete();

        array.setTimestampLtz(0, null, 9);
        assertThat(array.isNullAt(0)).isTrue();

        TimestampLtz newTs = TimestampLtz.fromEpochMillis(5000L, 123456);
        array.setTimestampLtz(0, newTs, 9);
        TimestampLtz retrieved = array.getTimestampLtz(0, 9);
        assertThat(retrieved).isNotNull();
        assertThat(array.isNullAt(0)).isFalse();
    }

    @Test
    public void testSetBooleanByteShortMethods() {
        BinaryArray boolArray = BinaryArray.fromPrimitiveArray(new boolean[] {false, false, false});
        boolArray.setBoolean(0, true);
        boolArray.setBoolean(1, false);
        boolArray.setBoolean(2, true);
        assertThat(boolArray.getBoolean(0)).isTrue();
        assertThat(boolArray.getBoolean(1)).isFalse();
        assertThat(boolArray.getBoolean(2)).isTrue();

        BinaryArray byteArray = BinaryArray.fromPrimitiveArray(new byte[] {1, 2, 3});
        byteArray.setByte(1, (byte) 99);
        assertThat(byteArray.getByte(1)).isEqualTo((byte) 99);

        BinaryArray shortArray = BinaryArray.fromPrimitiveArray(new short[] {1, 2, 3});
        shortArray.setShort(1, (short) 999);
        assertThat(shortArray.getShort(1)).isEqualTo((short) 999);
    }

    @Test
    public void testSetFloatDoubleMethods() {
        BinaryArray floatArray = BinaryArray.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f});
        floatArray.setFloat(1, 9.99f);
        assertThat(floatArray.getFloat(1)).isEqualTo(9.99f);

        BinaryArray doubleArray = BinaryArray.fromPrimitiveArray(new double[] {1.0, 2.0, 3.0});
        doubleArray.setDouble(1, 9.999);
        assertThat(doubleArray.getDouble(1)).isEqualTo(9.999);
    }

    @Test
    public void testGetBinaryWithLength() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        writer.writeBytes(0, data);
        writer.complete();

        byte[] retrieved = array.getBinary(0, 10);
        assertThat(retrieved).isEqualTo(data);
    }

    @Test
    public void testGetDecimalNonCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        Decimal largeDecimal =
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.12345"), 25, 5);
        writer.writeDecimal(0, largeDecimal, 25);
        writer.complete();

        Decimal retrieved = array.getDecimal(0, 25, 5);
        assertThat(retrieved).isEqualTo(largeDecimal);
    }

    @Test
    public void testGetTimestampNonCompact() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz tsNtz = TimestampNtz.fromMillis(1000L, 123456);
        TimestampLtz tsLtz = TimestampLtz.fromEpochMillis(2000L, 654321);

        writer.writeTimestampNtz(0, tsNtz, 9);
        writer.writeTimestampLtz(1, tsLtz, 9);
        writer.complete();

        assertThat(array.getTimestampNtz(0, 9)).isEqualTo(tsNtz);
        assertThat(array.getTimestampLtz(1, 9)).isEqualTo(tsLtz);
    }

    @Test
    public void testCalculateFixLengthPartSizeForArray() {
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.ARRAY(DataTypes.INT())))
                .isEqualTo(8);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.CHAR(10))).isEqualTo(8);
        assertThat(BinaryArray.calculateFixLengthPartSize(DataTypes.BINARY(20))).isEqualTo(8);
    }
}
