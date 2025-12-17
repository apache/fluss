/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.map;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexedMap}. */
public class IndexedMapTest {

    @Test
    public void testConstructorWithSimpleTypes() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        IndexedMap map = new IndexedMap(keyType, valueType);

        assertThat(map).isNotNull();
        assertThat(map).isInstanceOf(BinaryMap.class);
    }

    @Test
    public void testConstructorWithMapValueType() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        IndexedMap map = new IndexedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testMapInstanceCreation() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        IndexedMap map = new IndexedMap(keyType, valueType);

        assertThat(map).isNotNull();
        assertThat(map).isInstanceOf(BinaryMap.class);
    }

    @Test
    public void testWithIntKeyAndStringValue() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        IndexedMap map = new IndexedMap(keyType, valueType);

        GenericArray keyArray = new GenericArray(new Object[] {1, 2, 3});
        GenericArray valueArray =
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.BinaryString.fromString("a"),
                            org.apache.fluss.row.BinaryString.fromString("b"),
                            org.apache.fluss.row.BinaryString.fromString("c")
                        });

        ArraySerializer keySerializer = new ArraySerializer(keyType, INDEXED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, INDEXED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap result = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(result.size()).isEqualTo(3);
    }

    @Test
    public void testWithStringKeyAndLongValue() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.BIGINT();

        IndexedMap map = new IndexedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testWithBooleanKeyAndDoubleValue() {
        DataType keyType = DataTypes.BOOLEAN();
        DataType valueType = DataTypes.DOUBLE();

        IndexedMap map = new IndexedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testNestedMapWithMultipleLevels() {
        DataType innerMostKeyType = DataTypes.INT();
        DataType innerMostValueType = DataTypes.STRING();
        MapType innerMapType = DataTypes.MAP(innerMostKeyType, innerMostValueType);

        DataType outerKeyType = DataTypes.STRING();
        MapType outerMapType = DataTypes.MAP(outerKeyType, innerMapType);

        IndexedMap outerMap = new IndexedMap(outerKeyType, outerMapType);

        assertThat(outerMap).isNotNull();
        assertThat(outerMap).isInstanceOf(IndexedMap.class);
    }

    @Test
    public void testWithAllPrimitiveTypes() {
        DataType[] primitiveTypes =
                new DataType[] {
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE()
                };

        for (DataType keyType : primitiveTypes) {
            for (DataType valueType : primitiveTypes) {
                IndexedMap map = new IndexedMap(keyType, valueType);
                assertThat(map).isNotNull();
            }
        }
    }

    @Test
    public void testWithComplexKeyType() {
        DataType keyType = DataTypes.ARRAY(DataTypes.INT());
        DataType valueType = DataTypes.STRING();

        IndexedMap map = new IndexedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testSerializationRoundTrip() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        IndexedMap map = new IndexedMap(keyType, valueType);

        java.util.Map<Object, Object> javaMap = new java.util.HashMap<>();
        javaMap.put(org.apache.fluss.row.BinaryString.fromString("key1"), 100);
        javaMap.put(org.apache.fluss.row.BinaryString.fromString("key2"), 200);

        GenericMap genericMap = new GenericMap(javaMap);

        ArraySerializer keySerializer = new ArraySerializer(keyType, INDEXED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, INDEXED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(genericMap.keyArray());
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(genericMap.valueArray());

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(binaryMap.size()).isEqualTo(2);
        assertThat(binaryMap.keyArray().size()).isEqualTo(2);
        assertThat(binaryMap.valueArray().size()).isEqualTo(2);
    }

    @Test
    public void testCopyMethodWithReuse() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        IndexedMap map = new IndexedMap(keyType, valueType);

        GenericArray keyArray = new GenericArray(new Object[] {1, 2});
        GenericArray valueArray =
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.BinaryString.fromString("a"),
                            org.apache.fluss.row.BinaryString.fromString("b")
                        });

        ArraySerializer keySerializer = new ArraySerializer(keyType, INDEXED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, INDEXED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        IndexedMap reuseMap = new IndexedMap(keyType, valueType);
        BinaryMap copiedMap = binaryMap.copy(reuseMap);

        assertThat(copiedMap).isNotNull();
        assertThat(copiedMap.size()).isEqualTo(2);
        assertThat(copiedMap).isSameAs(reuseMap);
    }

    @Test
    public void testNestedMapWithCopyNoReuse() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        IndexedMap map = new IndexedMap(keyType, valueType);

        GenericArray keyArray =
                new GenericArray(
                        new Object[] {org.apache.fluss.row.BinaryString.fromString("map1")});

        java.util.Map<Object, Object> innerJavaMap = new java.util.HashMap<>();
        innerJavaMap.put(1, org.apache.fluss.row.BinaryString.fromString("value1"));
        GenericMap innerGenericMap = new GenericMap(innerJavaMap);
        GenericArray valueArray = new GenericArray(new Object[] {innerGenericMap});

        ArraySerializer keySerializer = new ArraySerializer(keyType, INDEXED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, INDEXED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        BinaryMap copiedMap = binaryMap.copy();

        assertThat(copiedMap).isNotNull();
        assertThat(copiedMap.size()).isEqualTo(1);
        assertThat(copiedMap).isInstanceOf(IndexedMap.class);
    }

    @Test
    public void testNestedMapWithCopyAndReuse() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        IndexedMap map = new IndexedMap(keyType, valueType);

        GenericArray keyArray =
                new GenericArray(
                        new Object[] {org.apache.fluss.row.BinaryString.fromString("map1")});

        java.util.Map<Object, Object> innerJavaMap = new java.util.HashMap<>();
        innerJavaMap.put(1, org.apache.fluss.row.BinaryString.fromString("value1"));
        GenericMap innerGenericMap = new GenericMap(innerJavaMap);
        GenericArray valueArray = new GenericArray(new Object[] {innerGenericMap});

        ArraySerializer keySerializer = new ArraySerializer(keyType, INDEXED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, INDEXED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        IndexedMap reuseMap = new IndexedMap(keyType, valueType);
        BinaryMap copiedMap = binaryMap.copy(reuseMap);

        assertThat(copiedMap).isNotNull();
        assertThat(copiedMap.size()).isEqualTo(1);
        assertThat(copiedMap).isSameAs(reuseMap);
    }

    @Test
    public void testNestedMapValueExtraction() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.DOUBLE();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        IndexedMap map = new IndexedMap(keyType, valueType);

        java.util.Map<Object, Object> outerJavaMap = new java.util.HashMap<>();

        java.util.Map<Object, Object> innerJavaMap1 = new java.util.HashMap<>();
        innerJavaMap1.put(1, 1.1);
        innerJavaMap1.put(2, 2.2);
        GenericMap innerGenericMap1 = new GenericMap(innerJavaMap1);

        outerJavaMap.put(org.apache.fluss.row.BinaryString.fromString("nested1"), innerGenericMap1);

        GenericMap outerGenericMap = new GenericMap(outerJavaMap);

        ArraySerializer keySerializer = new ArraySerializer(keyType, INDEXED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, INDEXED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(outerGenericMap.keyArray());
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(outerGenericMap.valueArray());

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(binaryMap).isNotNull();
        assertThat(binaryMap.size()).isEqualTo(1);
    }
}
