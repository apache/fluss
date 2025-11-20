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

package org.apache.fluss.row.columnar;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ColumnarArray}. */
public class ColumnarArrayTest {

    @Test
    public void testBooleanColumnVector() {
        boolean[] values = {true, false, true, false, true};
        TestBooleanColumnVector columnVector = new TestBooleanColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getBoolean(0)).isTrue();
        assertThat(columnarArray.getBoolean(1)).isFalse();
        assertThat(columnarArray.getBoolean(2)).isTrue();
        assertThat(columnarArray.getBoolean(3)).isFalse();
        assertThat(columnarArray.getBoolean(4)).isTrue();
    }

    @Test
    public void testByteColumnVector() {
        byte[] values = {1, 2, 3, 4, 5};
        TestByteColumnVector columnVector = new TestByteColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(columnarArray.getByte(4)).isEqualTo((byte) 5);
    }

    @Test
    public void testShortColumnVector() {
        short[] values = {10, 20, 30, 40, 50};
        TestShortColumnVector columnVector = new TestShortColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getShort(0)).isEqualTo((short) 10);
        assertThat(columnarArray.getShort(4)).isEqualTo((short) 50);
    }

    @Test
    public void testIntColumnVector() {
        int[] values = {100, 200, 300, 400, 500};
        TestIntColumnVector columnVector = new TestIntColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getInt(0)).isEqualTo(100);
        assertThat(columnarArray.getInt(4)).isEqualTo(500);
    }

    @Test
    public void testLongColumnVector() {
        long[] values = {1000L, 2000L, 3000L, 4000L, 5000L};
        TestLongColumnVector columnVector = new TestLongColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getLong(0)).isEqualTo(1000L);
        assertThat(columnarArray.getLong(4)).isEqualTo(5000L);
    }

    @Test
    public void testFloatColumnVector() {
        float[] values = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
        TestFloatColumnVector columnVector = new TestFloatColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getFloat(0)).isEqualTo(1.1f);
        assertThat(columnarArray.getFloat(4)).isEqualTo(5.5f);
    }

    @Test
    public void testDoubleColumnVector() {
        double[] values = {1.11, 2.22, 3.33, 4.44, 5.55};
        TestDoubleColumnVector columnVector = new TestDoubleColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getDouble(0)).isEqualTo(1.11);
        assertThat(columnarArray.getDouble(4)).isEqualTo(5.55);
    }

    @Test
    public void testStringColumnVector() {
        String[] values = {"hello", "world", "test", "fluss", "array"};
        TestBytesColumnVector columnVector = new TestBytesColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.size()).isEqualTo(5);
        assertThat(columnarArray.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(columnarArray.getString(4)).isEqualTo(BinaryString.fromString("array"));
    }

    @Test
    public void testGetChar() {
        String[] values = {"hello", "world"};
        TestBytesColumnVector columnVector = new TestBytesColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 2);

        assertThat(columnarArray.getChar(0, 5)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(columnarArray.getChar(1, 5)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testGetBytes() {
        byte[][] values = {{1, 2, 3}, {4, 5, 6}};
        TestBytesColumnVector columnVector = new TestBytesColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 2);

        assertThat(columnarArray.getBytes(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(columnarArray.getBytes(1)).isEqualTo(new byte[] {4, 5, 6});
    }

    @Test
    public void testGetBinary() {
        byte[][] values = {{1, 2, 3, 4, 5}, {6, 7, 8}};
        TestBytesColumnVector columnVector = new TestBytesColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 2);

        assertThat(columnarArray.getBinary(0, 5)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(columnarArray.getBinary(1, 3)).isEqualTo(new byte[] {6, 7, 8});
    }

    @Test
    public void testDecimalColumnVector() {
        Decimal[] values = {
            Decimal.fromUnscaledLong(123, 5, 2),
            Decimal.fromUnscaledLong(456, 5, 2),
            Decimal.fromUnscaledLong(789, 5, 2)
        };
        TestDecimalColumnVector columnVector = new TestDecimalColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        assertThat(columnarArray.size()).isEqualTo(3);
        assertThat(columnarArray.getDecimal(0, 5, 2)).isEqualTo(values[0]);
        assertThat(columnarArray.getDecimal(1, 5, 2)).isEqualTo(values[1]);
        assertThat(columnarArray.getDecimal(2, 5, 2)).isEqualTo(values[2]);
    }

    @Test
    public void testTimestampNtzColumnVector() {
        TimestampNtz[] values = {
            TimestampNtz.fromMillis(1000L),
            TimestampNtz.fromMillis(2000L),
            TimestampNtz.fromMillis(3000L)
        };
        TestTimestampNtzColumnVector columnVector = new TestTimestampNtzColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        assertThat(columnarArray.size()).isEqualTo(3);
        assertThat(columnarArray.getTimestampNtz(0, 3)).isEqualTo(values[0]);
        assertThat(columnarArray.getTimestampNtz(1, 3)).isEqualTo(values[1]);
        assertThat(columnarArray.getTimestampNtz(2, 3)).isEqualTo(values[2]);
    }

    @Test
    public void testTimestampLtzColumnVector() {
        TimestampLtz[] values = {
            TimestampLtz.fromEpochMillis(1000L),
            TimestampLtz.fromEpochMillis(2000L),
            TimestampLtz.fromEpochMillis(3000L)
        };
        TestTimestampLtzColumnVector columnVector = new TestTimestampLtzColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        assertThat(columnarArray.size()).isEqualTo(3);
        assertThat(columnarArray.getTimestampLtz(0, 3)).isEqualTo(values[0]);
        assertThat(columnarArray.getTimestampLtz(1, 3)).isEqualTo(values[1]);
        assertThat(columnarArray.getTimestampLtz(2, 3)).isEqualTo(values[2]);
    }

    @Test
    public void testArrayColumnVector() {
        TestArrayColumnVector columnVector = new TestArrayColumnVector(3);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        assertThat(columnarArray.size()).isEqualTo(3);
        InternalArray nestedArray = columnarArray.getArray(0);
        assertThat(nestedArray).isNotNull();
    }

    @Test
    public void testWithOffset() {
        int[] values = {100, 200, 300, 400, 500};
        TestIntColumnVector columnVector = new TestIntColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 2, 3);

        assertThat(columnarArray.size()).isEqualTo(3);
        assertThat(columnarArray.getInt(0)).isEqualTo(300); // offset + 0
        assertThat(columnarArray.getInt(1)).isEqualTo(400); // offset + 1
        assertThat(columnarArray.getInt(2)).isEqualTo(500); // offset + 2
    }

    @Test
    public void testIsNullAt() {
        boolean[] nulls = {false, true, false, true, false};
        int[] values = {100, 200, 300, 400, 500};
        TestIntColumnVector columnVector = new TestIntColumnVector(values, nulls);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        assertThat(columnarArray.isNullAt(0)).isFalse();
        assertThat(columnarArray.isNullAt(1)).isTrue();
        assertThat(columnarArray.isNullAt(2)).isFalse();
        assertThat(columnarArray.isNullAt(3)).isTrue();
        assertThat(columnarArray.isNullAt(4)).isFalse();
    }

    @Test
    public void testToBooleanArray() {
        boolean[] values = {true, false, true};
        TestBooleanColumnVector columnVector = new TestBooleanColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        boolean[] result = columnarArray.toBooleanArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testToByteArray() {
        byte[] values = {1, 2, 3, 4, 5};
        TestByteColumnVector columnVector = new TestByteColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 5);

        byte[] result = columnarArray.toByteArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testToShortArray() {
        short[] values = {10, 20, 30};
        TestShortColumnVector columnVector = new TestShortColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        short[] result = columnarArray.toShortArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testToIntArray() {
        int[] values = {100, 200, 300};
        TestIntColumnVector columnVector = new TestIntColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        int[] result = columnarArray.toIntArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testToLongArray() {
        long[] values = {1000L, 2000L, 3000L};
        TestLongColumnVector columnVector = new TestLongColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        long[] result = columnarArray.toLongArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testToFloatArray() {
        float[] values = {1.1f, 2.2f, 3.3f};
        TestFloatColumnVector columnVector = new TestFloatColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        float[] result = columnarArray.toFloatArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testToDoubleArray() {
        double[] values = {1.11, 2.22, 3.33};
        TestDoubleColumnVector columnVector = new TestDoubleColumnVector(values);
        ColumnarArray columnarArray = new ColumnarArray(columnVector, 0, 3);

        double[] result = columnarArray.toDoubleArray();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void testEquals() {
        int[] values = {1, 2, 3};
        TestIntColumnVector columnVector = new TestIntColumnVector(values);

        ColumnarArray array1 = new ColumnarArray(columnVector, 0, 3);
        ColumnarArray array2 = new ColumnarArray(columnVector, 0, 3);
        ColumnarArray array3 = new ColumnarArray(columnVector, 1, 2);

        assertThat(array1.equals(array1)).isTrue();
        assertThat(array1.equals(array2)).isTrue();
        assertThat(array1.equals(array3)).isFalse();
        assertThat(array1.equals(null)).isFalse();
        assertThat(array1.equals("string")).isFalse();
    }

    @Test
    public void testHashCode() {
        int[] values = {1, 2, 3};
        TestIntColumnVector columnVector = new TestIntColumnVector(values);

        ColumnarArray array1 = new ColumnarArray(columnVector, 0, 3);
        ColumnarArray array2 = new ColumnarArray(columnVector, 0, 3);

        // Same columnVector and same offset/size should produce same hashCode
        assertThat(array1.hashCode()).isEqualTo(array2.hashCode());
    }

    // Helper test implementations of ColumnVector interfaces

    private static class TestBooleanColumnVector implements BooleanColumnVector {
        private final boolean[] values;
        private final boolean[] nulls;

        TestBooleanColumnVector(boolean[] values) {
            this(values, new boolean[values.length]);
        }

        TestBooleanColumnVector(boolean[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public boolean getBoolean(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestByteColumnVector implements ByteColumnVector {
        private final byte[] values;
        private final boolean[] nulls;

        TestByteColumnVector(byte[] values) {
            this(values, new boolean[values.length]);
        }

        TestByteColumnVector(byte[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public byte getByte(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestShortColumnVector implements ShortColumnVector {
        private final short[] values;
        private final boolean[] nulls;

        TestShortColumnVector(short[] values) {
            this(values, new boolean[values.length]);
        }

        TestShortColumnVector(short[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public short getShort(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestIntColumnVector implements IntColumnVector {
        private final int[] values;
        private final boolean[] nulls;

        TestIntColumnVector(int[] values) {
            this(values, new boolean[values.length]);
        }

        TestIntColumnVector(int[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public int getInt(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestLongColumnVector implements LongColumnVector {
        private final long[] values;
        private final boolean[] nulls;

        TestLongColumnVector(long[] values) {
            this(values, new boolean[values.length]);
        }

        TestLongColumnVector(long[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public long getLong(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestFloatColumnVector implements FloatColumnVector {
        private final float[] values;
        private final boolean[] nulls;

        TestFloatColumnVector(float[] values) {
            this(values, new boolean[values.length]);
        }

        TestFloatColumnVector(float[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public float getFloat(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestDoubleColumnVector implements DoubleColumnVector {
        private final double[] values;
        private final boolean[] nulls;

        TestDoubleColumnVector(double[] values) {
            this(values, new boolean[values.length]);
        }

        TestDoubleColumnVector(double[] values, boolean[] nulls) {
            this.values = values;
            this.nulls = nulls;
        }

        @Override
        public double getDouble(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestBytesColumnVector implements BytesColumnVector {
        private final Bytes[] values;
        private final boolean[] nulls;

        TestBytesColumnVector(String[] stringValues) {
            this.values = new Bytes[stringValues.length];
            for (int i = 0; i < stringValues.length; i++) {
                byte[] bytes = stringValues[i].getBytes();
                this.values[i] = new Bytes(bytes, 0, bytes.length);
            }
            this.nulls = new boolean[stringValues.length];
        }

        TestBytesColumnVector(byte[][] byteValues) {
            this.values = new Bytes[byteValues.length];
            for (int i = 0; i < byteValues.length; i++) {
                this.values[i] = new Bytes(byteValues[i], 0, byteValues[i].length);
            }
            this.nulls = new boolean[byteValues.length];
        }

        @Override
        public Bytes getBytes(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestDecimalColumnVector implements DecimalColumnVector {
        private final Decimal[] values;
        private final boolean[] nulls;

        TestDecimalColumnVector(Decimal[] values) {
            this.values = values;
            this.nulls = new boolean[values.length];
        }

        @Override
        public Decimal getDecimal(int i, int precision, int scale) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestTimestampNtzColumnVector implements TimestampNtzColumnVector {
        private final TimestampNtz[] values;
        private final boolean[] nulls;

        TestTimestampNtzColumnVector(TimestampNtz[] values) {
            this.values = values;
            this.nulls = new boolean[values.length];
        }

        @Override
        public TimestampNtz getTimestampNtz(int i, int precision) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestTimestampLtzColumnVector implements TimestampLtzColumnVector {
        private final TimestampLtz[] values;
        private final boolean[] nulls;

        TestTimestampLtzColumnVector(TimestampLtz[] values) {
            this.values = values;
            this.nulls = new boolean[values.length];
        }

        @Override
        public TimestampLtz getTimestampLtz(int i, int precision) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            return nulls[i];
        }
    }

    private static class TestArrayColumnVector implements ArrayColumnVector {
        private final int size;

        TestArrayColumnVector(int size) {
            this.size = size;
        }

        @Override
        public InternalArray getArray(int i) {
            // Return a simple test array
            return new ColumnarArray(new TestIntColumnVector(new int[] {1, 2, 3}), 0, 3);
        }

        @Override
        public boolean isNullAt(int i) {
            return false;
        }
    }
}
