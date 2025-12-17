/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.map.AlignedMap;
import org.apache.fluss.row.map.CompactedMap;
import org.apache.fluss.row.map.IndexedMap;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.ALIGNED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.COMPACTED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MapSerializer}. */
public class MapSerializerTest {

    @Test
    public void testConstructor() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        assertThat(serializer).isNotNull();
    }

    @Test
    public void testToBinaryMapWithBinaryMapInput() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        IndexedMap indexedMap = new IndexedMap(keyType, valueType);
        BinaryMap result = serializer.toBinaryMap(indexedMap);

        assertThat(result).isSameAs(indexedMap);
    }

    @Test
    public void testToBinaryMapWithGenericMap() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(1, BinaryString.fromString("one"));
        javaMap.put(2, BinaryString.fromString("two"));
        javaMap.put(3, BinaryString.fromString("three"));

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);
    }

    @Test
    public void testToBinaryMapWithCompactedFormat() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        MapSerializer serializer = new MapSerializer(keyType, valueType, COMPACTED);

        Map<Object, Object> javaMap = new LinkedHashMap<>();
        javaMap.put(BinaryString.fromString("a"), 1);
        javaMap.put(BinaryString.fromString("b"), 2);

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(2);
        assertThat(result).isInstanceOf(CompactedMap.class);
    }

    @Test
    public void testToBinaryMapWithIndexedFormat() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(BinaryString.fromString("x"), 10);
        javaMap.put(BinaryString.fromString("y"), 20);

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(2);
        assertThat(result).isInstanceOf(IndexedMap.class);
    }

    @Test
    public void testToBinaryMapWithAlignedFormat() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        MapSerializer serializer = new MapSerializer(keyType, valueType, ALIGNED);

        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(BinaryString.fromString("key"), 100);

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(1);
        assertThat(result).isInstanceOf(AlignedMap.class);
    }

    @Test
    public void testToBinaryMapMultipleCalls() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, COMPACTED);

        Map<Object, Object> javaMap1 = new HashMap<>();
        javaMap1.put(1, BinaryString.fromString("first"));
        GenericMap genericMap1 = new GenericMap(javaMap1);

        BinaryMap result1 = serializer.toBinaryMap(genericMap1);
        assertThat(result1.size()).isEqualTo(1);

        Map<Object, Object> javaMap2 = new HashMap<>();
        javaMap2.put(1, BinaryString.fromString("a"));
        javaMap2.put(2, BinaryString.fromString("b"));
        GenericMap genericMap2 = new GenericMap(javaMap2);

        BinaryMap result2 = serializer.toBinaryMap(genericMap2);
        assertThat(result2.size()).isEqualTo(2);
    }

    @Test
    public void testConvertToJavaMap() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        Object[] keys = {1, 2, 3};
        Object[] values = {
            BinaryString.fromString("one"),
            BinaryString.fromString("two"),
            BinaryString.fromString("three")
        };

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        InternalMap internalMap =
                new org.apache.fluss.row.columnar.ColumnarMap(keyArray, valueArray);

        Map<Object, Object> result =
                MapSerializer.convertToJavaMap(internalMap, keyType, valueType);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(1)).isEqualTo(BinaryString.fromString("one"));
        assertThat(result.get(2)).isEqualTo(BinaryString.fromString("two"));
        assertThat(result.get(3)).isEqualTo(BinaryString.fromString("three"));
    }

    @Test
    public void testConvertToJavaMapWithNullValues() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        Object[] keys = {
            BinaryString.fromString("a"), BinaryString.fromString("b"), BinaryString.fromString("c")
        };
        Object[] values = {1, null, 3};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        InternalMap internalMap =
                new org.apache.fluss.row.columnar.ColumnarMap(keyArray, valueArray);

        Map<Object, Object> result =
                MapSerializer.convertToJavaMap(internalMap, keyType, valueType);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(BinaryString.fromString("a"))).isEqualTo(1);
        assertThat(result.get(BinaryString.fromString("b"))).isNull();
        assertThat(result.get(BinaryString.fromString("c"))).isEqualTo(3);
    }

    @Test
    public void testConvertToJavaMapEmpty() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        Object[] keys = {};
        Object[] values = {};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        InternalMap internalMap =
                new org.apache.fluss.row.columnar.ColumnarMap(keyArray, valueArray);

        Map<Object, Object> result =
                MapSerializer.convertToJavaMap(internalMap, keyType, valueType);

        assertThat(result).isNotNull();
        assertThat(result).isEmpty();
    }

    @Test
    public void testConvertToJavaMapWithDifferentTypes() {
        DataType keyType = DataTypes.BIGINT();
        DataType valueType = DataTypes.DOUBLE();

        Object[] keys = {100L, 200L, 300L};
        Object[] values = {1.5, 2.5, 3.5};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        InternalMap internalMap =
                new org.apache.fluss.row.columnar.ColumnarMap(keyArray, valueArray);

        Map<Object, Object> result =
                MapSerializer.convertToJavaMap(internalMap, keyType, valueType);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(100L)).isEqualTo(1.5);
        assertThat(result.get(200L)).isEqualTo(2.5);
        assertThat(result.get(300L)).isEqualTo(3.5);
    }

    @Test
    public void testToBinaryMapWithEmptyMap() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        Map<Object, Object> javaMap = new HashMap<>();
        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testToBinaryMapWithComplexValues() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.ARRAY(DataTypes.INT());

        MapSerializer serializer = new MapSerializer(keyType, valueType, COMPACTED);

        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(BinaryString.fromString("array1"), new GenericArray(new int[] {1, 2, 3}));
        javaMap.put(BinaryString.fromString("array2"), new GenericArray(new int[] {4, 5, 6}));

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void testSerializationWithBooleanTypes() {
        DataType keyType = DataTypes.BOOLEAN();
        DataType valueType = DataTypes.INT();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(true, 1);
        javaMap.put(false, 0);

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void testSerializationWithAllFormats() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(1, BinaryString.fromString("test"));

        GenericMap genericMap = new GenericMap(javaMap);

        MapSerializer compactedSerializer = new MapSerializer(keyType, valueType, COMPACTED);
        BinaryMap compactedResult = compactedSerializer.toBinaryMap(genericMap);
        assertThat(compactedResult).isInstanceOf(CompactedMap.class);

        MapSerializer indexedSerializer = new MapSerializer(keyType, valueType, INDEXED);
        BinaryMap indexedResult = indexedSerializer.toBinaryMap(genericMap);
        assertThat(indexedResult).isInstanceOf(IndexedMap.class);

        MapSerializer alignedSerializer = new MapSerializer(keyType, valueType, ALIGNED);
        BinaryMap alignedResult = alignedSerializer.toBinaryMap(genericMap);
        assertThat(alignedResult).isInstanceOf(AlignedMap.class);
    }

    @Test
    public void testConvertToJavaMapWithBooleanKeys() {
        DataType keyType = DataTypes.BOOLEAN();
        DataType valueType = DataTypes.STRING();

        Object[] keys = {true, false};
        Object[] values = {BinaryString.fromString("yes"), BinaryString.fromString("no")};

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        InternalMap internalMap =
                new org.apache.fluss.row.columnar.ColumnarMap(keyArray, valueArray);

        Map<Object, Object> result =
                MapSerializer.convertToJavaMap(internalMap, keyType, valueType);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(true)).isEqualTo(BinaryString.fromString("yes"));
        assertThat(result.get(false)).isEqualTo(BinaryString.fromString("no"));
    }

    @Test
    public void testReuseOfInternalStructures() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, INDEXED);

        Map<Object, Object> javaMap1 = new HashMap<>();
        javaMap1.put(1, BinaryString.fromString("first"));
        GenericMap genericMap1 = new GenericMap(javaMap1);

        BinaryMap result1 = serializer.toBinaryMap(genericMap1);

        Map<Object, Object> javaMap2 = new HashMap<>();
        javaMap2.put(2, BinaryString.fromString("second"));
        GenericMap genericMap2 = new GenericMap(javaMap2);

        BinaryMap result2 = serializer.toBinaryMap(genericMap2);

        assertThat(result1).isNotNull();
        assertThat(result2).isNotNull();
        assertThat(result1.size()).isEqualTo(1);
        assertThat(result2.size()).isEqualTo(1);
    }

    @Test
    public void testWithLargeMap() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        MapSerializer serializer = new MapSerializer(keyType, valueType, COMPACTED);

        Map<Object, Object> javaMap = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            javaMap.put(i, BinaryString.fromString("value" + i));
        }

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryMap result = serializer.toBinaryMap(genericMap);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(100);
    }

    @Test
    public void testConvertToJavaMapWithLargeMap() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.INT();

        Object[] keys = new Object[50];
        Object[] values = new Object[50];

        for (int i = 0; i < 50; i++) {
            keys[i] = i;
            values[i] = i * 10;
        }

        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);

        InternalMap internalMap =
                new org.apache.fluss.row.columnar.ColumnarMap(keyArray, valueArray);

        Map<Object, Object> result =
                MapSerializer.convertToJavaMap(internalMap, keyType, valueType);

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(50);
        assertThat(result.get(0)).isEqualTo(0);
        assertThat(result.get(49)).isEqualTo(490);
    }
}
