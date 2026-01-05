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
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ColumnarMap}. */
public class ColumnarMapTest {

    @Test
    public void testConstructorWithInternalArrays() {
        Object[] keys = {
            BinaryString.fromString("key1"),
            BinaryString.fromString("key2"),
            BinaryString.fromString("key3")
        };
        Object[] values = {1, 2, 3};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        ColumnarMap map = new ColumnarMap(keyArray, valueArray);

        assertThat(map.size()).isEqualTo(3);
        assertThat(map.keyArray()).isEqualTo(keyArray);
        assertThat(map.valueArray()).isEqualTo(valueArray);
    }

    @Test
    public void testConstructorWithColumnVectors() {
        int[] keys = {1, 2, 3, 4, 5};
        int[] values = {10, 20, 30, 40, 50};

        TestIntColumnVector keyColumnVector = new TestIntColumnVector(keys);
        TestIntColumnVector valueColumnVector = new TestIntColumnVector(values);

        ColumnarMap map = new ColumnarMap(keyColumnVector, valueColumnVector, 0, 5);

        assertThat(map.size()).isEqualTo(5);
        assertThat(map.keyArray()).isInstanceOf(ColumnarArray.class);
        assertThat(map.valueArray()).isInstanceOf(ColumnarArray.class);
        assertThat(map.keyArray().getInt(0)).isEqualTo(1);
        assertThat(map.valueArray().getInt(0)).isEqualTo(10);
    }

    @Test
    public void testConstructorWithOffsetAndNumElements() {
        int[] keys = {1, 2, 3, 4, 5};
        int[] values = {10, 20, 30, 40, 50};

        TestIntColumnVector keyColumnVector = new TestIntColumnVector(keys);
        TestIntColumnVector valueColumnVector = new TestIntColumnVector(values);

        ColumnarMap map = new ColumnarMap(keyColumnVector, valueColumnVector, 2, 2);

        assertThat(map.size()).isEqualTo(2);
        assertThat(map.keyArray().getInt(0)).isEqualTo(3);
        assertThat(map.keyArray().getInt(1)).isEqualTo(4);
        assertThat(map.valueArray().getInt(0)).isEqualTo(30);
        assertThat(map.valueArray().getInt(1)).isEqualTo(40);
    }

    @Test
    public void testConstructorWithNullKeyArray() {
        Object[] values = {1, 2, 3};
        InternalArray valueArray = new GenericArray(values);

        assertThatThrownBy(() -> new ColumnarMap(null, valueArray))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keyArray must not be null");
    }

    @Test
    public void testConstructorWithNullValueArray() {
        Object[] keys = {BinaryString.fromString("key1"), BinaryString.fromString("key2")};
        InternalArray keyArray = new GenericArray(keys);

        assertThatThrownBy(() -> new ColumnarMap(keyArray, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("valueArray must not be null");
    }

    @Test
    public void testConstructorWithMismatchedArraySizes() {
        Object[] keys = {BinaryString.fromString("key1"), BinaryString.fromString("key2")};
        Object[] values = {1, 2, 3};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        assertThatThrownBy(() -> new ColumnarMap(keyArray, valueArray))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Key array size and value array size must be equal");
    }

    @Test
    public void testEmptyMap() {
        Object[] keys = {};
        Object[] values = {};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        ColumnarMap map = new ColumnarMap(keyArray, valueArray);

        assertThat(map.size()).isEqualTo(0);
        assertThat(map.keyArray().size()).isEqualTo(0);
        assertThat(map.valueArray().size()).isEqualTo(0);
    }

    @Test
    public void testKeyArrayAndValueArray() {
        Object[] keys = {
            BinaryString.fromString("name"),
            BinaryString.fromString("age"),
            BinaryString.fromString("city")
        };
        Object[] values = {BinaryString.fromString("Alice"), 30, BinaryString.fromString("NYC")};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        ColumnarMap map = new ColumnarMap(keyArray, valueArray);

        assertThat(map.keyArray()).isSameAs(keyArray);
        assertThat(map.valueArray()).isSameAs(valueArray);

        assertThat(map.keyArray().getString(0)).isEqualTo(BinaryString.fromString("name"));
        assertThat(map.valueArray().getString(0)).isEqualTo(BinaryString.fromString("Alice"));
        assertThat(map.keyArray().getString(1)).isEqualTo(BinaryString.fromString("age"));
        assertThat(map.valueArray().getInt(1)).isEqualTo(30);
    }

    @Test
    public void testMapWithNullValues() {
        Object[] keys = {
            BinaryString.fromString("k1"),
            BinaryString.fromString("k2"),
            BinaryString.fromString("k3")
        };
        Object[] values = {1, null, 3};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        ColumnarMap map = new ColumnarMap(keyArray, valueArray);

        assertThat(map.size()).isEqualTo(3);
        assertThat(map.valueArray().isNullAt(0)).isFalse();
        assertThat(map.valueArray().isNullAt(1)).isTrue();
        assertThat(map.valueArray().isNullAt(2)).isFalse();
    }

    @Test
    public void testMapWithDifferentDataTypes() {
        long[] keys = {1L, 2L, 3L};
        double[] values = {1.1, 2.2, 3.3};

        TestLongColumnVector keyColumnVector = new TestLongColumnVector(keys);
        TestDoubleColumnVector valueColumnVector = new TestDoubleColumnVector(values);

        ColumnarMap map = new ColumnarMap(keyColumnVector, valueColumnVector, 0, 3);

        assertThat(map.size()).isEqualTo(3);
        assertThat(map.keyArray().getLong(0)).isEqualTo(1L);
        assertThat(map.valueArray().getDouble(0)).isEqualTo(1.1);
        assertThat(map.keyArray().getLong(2)).isEqualTo(3L);
        assertThat(map.valueArray().getDouble(2)).isEqualTo(3.3);
    }

    @Test
    public void testEqualsWithEqualMap() {
        Object[] keys = {BinaryString.fromString("key1"), BinaryString.fromString("key2")};
        Object[] values = {1, 2};

        InternalArray keyArray1 = new GenericArray(keys);
        InternalArray valueArray1 = new GenericArray(values);
        ColumnarMap map1 = new ColumnarMap(keyArray1, valueArray1);

        InternalArray keyArray2 = new GenericArray(keys);
        InternalArray valueArray2 = new GenericArray(values);
        ColumnarMap map2 = new ColumnarMap(keyArray2, valueArray2);

        assertThat(map1.equals(map2)).isTrue();
    }

    @Test
    public void testEqualsWithDifferentKeys() {
        Object[] keys1 = {BinaryString.fromString("key1")};
        Object[] keys2 = {BinaryString.fromString("key2")};
        Object[] values = {1};

        InternalArray keyArray1 = new GenericArray(keys1);
        InternalArray valueArray1 = new GenericArray(values);
        ColumnarMap map1 = new ColumnarMap(keyArray1, valueArray1);

        InternalArray keyArray2 = new GenericArray(keys2);
        InternalArray valueArray2 = new GenericArray(values);
        ColumnarMap map2 = new ColumnarMap(keyArray2, valueArray2);

        assertThat(map1.equals(map2)).isFalse();
    }

    @Test
    public void testEqualsWithDifferentValues() {
        Object[] keys = {BinaryString.fromString("key1")};
        Object[] values1 = {1};
        Object[] values2 = {2};

        InternalArray keyArray1 = new GenericArray(keys);
        InternalArray valueArray1 = new GenericArray(values1);
        ColumnarMap map1 = new ColumnarMap(keyArray1, valueArray1);

        InternalArray keyArray2 = new GenericArray(keys);
        InternalArray valueArray2 = new GenericArray(values2);
        ColumnarMap map2 = new ColumnarMap(keyArray2, valueArray2);

        assertThat(map1.equals(map2)).isFalse();
    }

    @Test
    public void testEqualsWithNonInternalMapObject() {
        Object[] keys = {BinaryString.fromString("key1")};
        Object[] values = {1};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);
        ColumnarMap map = new ColumnarMap(keyArray, valueArray);

        assertThat(map.equals("not a map")).isFalse();
        assertThat(map.equals(null)).isFalse();
    }

    @Test
    public void testHashCodeConsistency() {
        Object[] keys = {BinaryString.fromString("key1"), BinaryString.fromString("key2")};
        Object[] values = {1, 2};

        InternalArray keyArray1 = new GenericArray(keys);
        InternalArray valueArray1 = new GenericArray(values);
        ColumnarMap map1 = new ColumnarMap(keyArray1, valueArray1);

        InternalArray keyArray2 = new GenericArray(keys);
        InternalArray valueArray2 = new GenericArray(values);
        ColumnarMap map2 = new ColumnarMap(keyArray2, valueArray2);

        assertThat(map1.hashCode()).isEqualTo(map2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentMaps() {
        Object[] keys1 = {BinaryString.fromString("key1")};
        Object[] keys2 = {BinaryString.fromString("key2")};
        Object[] values = {1};

        InternalArray keyArray1 = new GenericArray(keys1);
        InternalArray valueArray1 = new GenericArray(values);
        ColumnarMap map1 = new ColumnarMap(keyArray1, valueArray1);

        InternalArray keyArray2 = new GenericArray(keys2);
        InternalArray valueArray2 = new GenericArray(values);
        ColumnarMap map2 = new ColumnarMap(keyArray2, valueArray2);

        assertThat(map1.hashCode()).isNotEqualTo(map2.hashCode());
    }

    private static class TestIntColumnVector implements IntColumnVector {
        private final int[] values;

        TestIntColumnVector(int[] values) {
            this.values = values;
        }

        @Override
        public int getInt(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            // Primitive int values cannot be null
            return false;
        }
    }

    private static class TestLongColumnVector implements LongColumnVector {
        private final long[] values;

        TestLongColumnVector(long[] values) {
            this.values = values;
        }

        @Override
        public long getLong(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            // Primitive long values cannot be null
            return false;
        }
    }

    private static class TestDoubleColumnVector implements DoubleColumnVector {
        private final double[] values;

        TestDoubleColumnVector(double[] values) {
            this.values = values;
        }

        @Override
        public double getDouble(int i) {
            return values[i];
        }

        @Override
        public boolean isNullAt(int i) {
            // Primitive double values cannot be null
            return false;
        }
    }
}
