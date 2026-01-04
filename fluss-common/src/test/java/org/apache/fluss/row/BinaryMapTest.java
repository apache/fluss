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

import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.row.map.AlignedMap;
import org.apache.fluss.row.map.CompactedMap;
import org.apache.fluss.row.map.IndexedMap;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryMap} implementations. */
public class BinaryMapTest {

    @Test
    public void testAlignedMapCreation() {
        AlignedMap map = new AlignedMap();
        assertThat(map).isNotNull();
    }

    @Test
    public void testAlignedMapKeyArrayInstance() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});

        BinaryMap result = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.keyArray().getInt(0)).isEqualTo(1);
        assertThat(result.valueArray().getInt(0)).isEqualTo(10);
    }

    @Test
    public void testAlignedMapValueArrayInstance() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new long[] {100L, 200L});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new long[] {1000L, 2000L});

        BinaryMap result = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(result.valueArray()).isNotNull();
        assertThat(result.valueArray().getLong(0)).isEqualTo(1000L);
        assertThat(result.valueArray().getLong(1)).isEqualTo(2000L);
    }

    @Test
    public void testAlignedMapCopy() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20});

        BinaryMap original = BinaryMap.valueOf(keyArray, valueArray, map);
        BinaryMap copied = original.copy();

        assertThat(copied).isNotNull();
        assertThat(copied.size()).isEqualTo(original.size());
        assertThat(copied.keyArray().getInt(0)).isEqualTo(1);
        assertThat(copied.valueArray().getInt(0)).isEqualTo(10);
    }

    @Test
    public void testAlignedMapCopyWithReuse() {
        AlignedMap map1 = new AlignedMap();
        AlignedMap map2 = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});

        BinaryMap original = BinaryMap.valueOf(keyArray, valueArray, map1);
        BinaryMap copied = original.copy(map2);

        assertThat(copied).isSameAs(map2);
        assertThat(copied.size()).isEqualTo(3);
    }

    @Test
    public void testAlignedMapEquals() {
        AlignedMap map1 = new AlignedMap();
        AlignedMap map2 = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20});

        BinaryMap binaryMap1 = BinaryMap.valueOf(keyArray, valueArray, map1);
        BinaryMap binaryMap2 = BinaryMap.valueOf(keyArray, valueArray, map2);

        assertThat(binaryMap1.equals(binaryMap2)).isTrue();
    }

    @Test
    public void testAlignedMapEqualsSameObject() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10});

        BinaryMap binaryMap = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(binaryMap.equals(binaryMap)).isTrue();
    }

    @Test
    public void testAlignedMapNotEquals() {
        AlignedMap map1 = new AlignedMap();
        AlignedMap map2 = new AlignedMap();

        BinaryArray keyArray1 = BinaryArray.fromPrimitiveArray(new int[] {1, 2});
        BinaryArray valueArray1 = BinaryArray.fromPrimitiveArray(new int[] {10, 20});

        BinaryArray keyArray2 = BinaryArray.fromPrimitiveArray(new int[] {1, 3});
        BinaryArray valueArray2 = BinaryArray.fromPrimitiveArray(new int[] {10, 30});

        BinaryMap binaryMap1 = BinaryMap.valueOf(keyArray1, valueArray1, map1);
        BinaryMap binaryMap2 = BinaryMap.valueOf(keyArray2, valueArray2, map2);

        assertThat(binaryMap1.equals(binaryMap2)).isFalse();
    }

    @Test
    public void testAlignedMapHashCode() {
        AlignedMap map1 = new AlignedMap();
        AlignedMap map2 = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});

        BinaryMap binaryMap1 = BinaryMap.valueOf(keyArray, valueArray, map1);
        BinaryMap binaryMap2 = BinaryMap.valueOf(keyArray, valueArray, map2);

        assertThat(binaryMap1.hashCode()).isEqualTo(binaryMap2.hashCode());
    }

    @Test
    public void testAlignedMapWithStringKeys() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = new PrimitiveBinaryArray();
        BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyArray, 2, 8);
        keyWriter.writeString(0, BinaryString.fromString("key1"));
        keyWriter.writeString(1, BinaryString.fromString("key2"));
        keyWriter.complete();

        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {100, 200});

        BinaryMap binaryMap = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(binaryMap.size()).isEqualTo(2);
        assertThat(binaryMap.keyArray().getString(0)).isEqualTo(BinaryString.fromString("key1"));
        assertThat(binaryMap.keyArray().getString(1)).isEqualTo(BinaryString.fromString("key2"));
    }

    @Test
    public void testCompactedMapKeyValueArrays() {
        CompactedMap map = new CompactedMap(DataTypes.INT(), DataTypes.INT());

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});

        BinaryMap result = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.keyArray().getInt(0)).isEqualTo(1);
        assertThat(result.valueArray().getInt(0)).isEqualTo(10);
    }

    @Test
    public void testCompactedMapCopy() {
        CompactedMap map = new CompactedMap(DataTypes.BIGINT(), DataTypes.DOUBLE());

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new long[] {100L, 200L});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new double[] {1.5, 2.5});

        BinaryMap original = BinaryMap.valueOf(keyArray, valueArray, map);
        BinaryMap copied = original.copy();

        assertThat(copied.size()).isEqualTo(2);
        assertThat(copied.keyArray().getLong(0)).isEqualTo(100L);
        assertThat(copied.valueArray().getDouble(0)).isEqualTo(1.5);
    }

    @Test
    public void testIndexedMapKeyValueArrays() {
        IndexedMap map = new IndexedMap(DataTypes.INT(), DataTypes.INT());

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {5, 10, 15});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {50, 100, 150});

        BinaryMap result = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.keyArray().getInt(0)).isEqualTo(5);
        assertThat(result.valueArray().getInt(0)).isEqualTo(50);
    }

    @Test
    public void testIndexedMapCopy() {
        IndexedMap map = new IndexedMap(DataTypes.INT(), DataTypes.INT());

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20});

        BinaryMap original = BinaryMap.valueOf(keyArray, valueArray, map);
        BinaryMap copied = original.copy();

        assertThat(copied.size()).isEqualTo(2);
        assertThat(copied.keyArray().getInt(0)).isEqualTo(1);
        assertThat(copied.valueArray().getInt(1)).isEqualTo(20);
    }

    @Test
    public void testBinaryMapEmptyMap() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {});

        BinaryMap binaryMap = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(binaryMap.size()).isEqualTo(0);
    }

    @Test
    public void testBinaryMapPointTo() {
        AlignedMap map1 = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20});

        BinaryMap binaryMap1 = BinaryMap.valueOf(keyArray, valueArray, map1);

        AlignedMap map2 = new AlignedMap();
        map2.pointTo(binaryMap1.getSegments(), binaryMap1.getOffset(), binaryMap1.getSizeInBytes());

        assertThat(map2.size()).isEqualTo(2);
        assertThat(map2.keyArray().getInt(0)).isEqualTo(1);
        assertThat(map2.valueArray().getInt(1)).isEqualTo(20);
    }

    @Test
    public void testBinaryMapWithNullValues() {
        AlignedMap map = new AlignedMap();

        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});

        BinaryArray valueArray = new PrimitiveBinaryArray();
        BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueArray, 3, 4);
        valueWriter.writeInt(0, 10);
        valueWriter.setNullInt(1);
        valueWriter.writeInt(2, 30);
        valueWriter.complete();

        BinaryMap binaryMap = BinaryMap.valueOf(keyArray, valueArray, map);

        assertThat(binaryMap.size()).isEqualTo(3);
        assertThat(binaryMap.valueArray().isNullAt(0)).isFalse();
        assertThat(binaryMap.valueArray().isNullAt(1)).isTrue();
        assertThat(binaryMap.valueArray().isNullAt(2)).isFalse();
    }
}
