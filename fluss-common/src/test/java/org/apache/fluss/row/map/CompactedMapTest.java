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

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.COMPACTED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactedMap}. */
public class CompactedMapTest {

    @Test
    public void testConstructorWithSimpleTypes() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
        assertThat(map).isInstanceOf(BinaryMap.class);
    }

    @Test
    public void testConstructorWithMapValueType() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testMapInstanceCreation() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
        assertThat(map).isInstanceOf(BinaryMap.class);
    }

    @Test
    public void testWithIntKeyAndStringValue() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        CompactedMap map = new CompactedMap(keyType, valueType);

        GenericArray keyArray = new GenericArray(new Object[] {1, 2, 3});
        GenericArray valueArray =
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.BinaryString.fromString("a"),
                            org.apache.fluss.row.BinaryString.fromString("b"),
                            org.apache.fluss.row.BinaryString.fromString("c")
                        });

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap result = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(result.size()).isEqualTo(3);
    }

    @Test
    public void testWithStringKeyAndLongValue() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.BIGINT();

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testWithBooleanKeyAndDoubleValue() {
        DataType keyType = DataTypes.BOOLEAN();
        DataType valueType = DataTypes.DOUBLE();

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testNestedMapWithMultipleLevels() {
        DataType innerMostKeyType = DataTypes.INT();
        DataType innerMostValueType = DataTypes.STRING();
        MapType innerMapType = DataTypes.MAP(innerMostKeyType, innerMostValueType);

        DataType outerKeyType = DataTypes.STRING();
        MapType outerMapType = DataTypes.MAP(outerKeyType, innerMapType);

        CompactedMap outerMap = new CompactedMap(outerKeyType, outerMapType);

        assertThat(outerMap).isNotNull();
        assertThat(outerMap).isInstanceOf(CompactedMap.class);
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
                CompactedMap map = new CompactedMap(keyType, valueType);
                assertThat(map).isNotNull();
            }
        }
    }

    @Test
    public void testWithComplexKeyType() {
        DataType keyType = DataTypes.ARRAY(DataTypes.INT());
        DataType valueType = DataTypes.STRING();

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testSerializationRoundTrip() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        CompactedMap map = new CompactedMap(keyType, valueType);

        java.util.Map<Object, Object> javaMap = new java.util.HashMap<>();
        javaMap.put(org.apache.fluss.row.BinaryString.fromString("key1"), 100);
        javaMap.put(org.apache.fluss.row.BinaryString.fromString("key2"), 200);

        GenericMap genericMap = new GenericMap(javaMap);

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(genericMap.keyArray());
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(genericMap.valueArray());

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(binaryMap.size()).isEqualTo(2);
        assertThat(binaryMap.keyArray().size()).isEqualTo(2);
        assertThat(binaryMap.valueArray().size()).isEqualTo(2);
    }

    @Test
    public void testWithTimestampTypes() {
        DataType keyType = DataTypes.TIMESTAMP(3);
        DataType valueType = DataTypes.TIMESTAMP_LTZ(3);

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testWithDecimalType() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.DECIMAL(10, 2);

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testWithBinaryAndCharTypes() {
        DataType keyType = DataTypes.CHAR(10);
        DataType valueType = DataTypes.BINARY(20);

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testEmptyMap() {
        DataType keyType = DataTypes.STRING();
        DataType valueType = DataTypes.INT();

        CompactedMap map = new CompactedMap(keyType, valueType);

        GenericArray keyArray = new GenericArray(new Object[] {});
        GenericArray valueArray = new GenericArray(new Object[] {});

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap result = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testWithDateAndTimeTypes() {
        DataType keyType = DataTypes.DATE();
        DataType valueType = DataTypes.TIME();

        CompactedMap map = new CompactedMap(keyType, valueType);

        assertThat(map).isNotNull();
    }

    @Test
    public void testCopyMethodWithReuse() {
        DataType keyType = DataTypes.INT();
        DataType valueType = DataTypes.STRING();

        CompactedMap map = new CompactedMap(keyType, valueType);

        GenericArray keyArray = new GenericArray(new Object[] {1, 2});
        GenericArray valueArray =
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.BinaryString.fromString("a"),
                            org.apache.fluss.row.BinaryString.fromString("b")
                        });

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        CompactedMap reuseMap = new CompactedMap(keyType, valueType);
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

        CompactedMap map = new CompactedMap(keyType, valueType);

        GenericArray keyArray =
                new GenericArray(
                        new Object[] {org.apache.fluss.row.BinaryString.fromString("map1")});

        java.util.Map<Object, Object> innerJavaMap = new java.util.HashMap<>();
        innerJavaMap.put(1, org.apache.fluss.row.BinaryString.fromString("value1"));
        GenericMap innerGenericMap = new GenericMap(innerJavaMap);
        GenericArray valueArray = new GenericArray(new Object[] {innerGenericMap});

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        BinaryMap copiedMap = binaryMap.copy();

        assertThat(copiedMap).isNotNull();
        assertThat(copiedMap.size()).isEqualTo(1);
        assertThat(copiedMap).isInstanceOf(CompactedMap.class);
    }

    @Test
    public void testNestedMapWithCopyAndReuse() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.INT();
        DataType innerValueType = DataTypes.STRING();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        CompactedMap map = new CompactedMap(keyType, valueType);

        GenericArray keyArray =
                new GenericArray(
                        new Object[] {org.apache.fluss.row.BinaryString.fromString("map1")});

        java.util.Map<Object, Object> innerJavaMap = new java.util.HashMap<>();
        innerJavaMap.put(1, org.apache.fluss.row.BinaryString.fromString("value1"));
        GenericMap innerGenericMap = new GenericMap(innerJavaMap);
        GenericArray valueArray = new GenericArray(new Object[] {innerGenericMap});

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        CompactedMap reuseMap = new CompactedMap(keyType, valueType);
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

        CompactedMap map = new CompactedMap(keyType, valueType);

        java.util.Map<Object, Object> outerJavaMap = new java.util.HashMap<>();

        java.util.Map<Object, Object> innerJavaMap1 = new java.util.HashMap<>();
        innerJavaMap1.put(1, 1.1);
        innerJavaMap1.put(2, 2.2);
        GenericMap innerGenericMap1 = new GenericMap(innerJavaMap1);

        outerJavaMap.put(org.apache.fluss.row.BinaryString.fromString("nested1"), innerGenericMap1);

        GenericMap outerGenericMap = new GenericMap(outerJavaMap);

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(outerGenericMap.keyArray());
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(outerGenericMap.valueArray());

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        assertThat(binaryMap).isNotNull();
        assertThat(binaryMap.size()).isEqualTo(1);
    }

    @Test
    public void testCopyWithNestedReuse() {
        DataType keyType = DataTypes.STRING();
        DataType innerKeyType = DataTypes.BIGINT();
        DataType innerValueType = DataTypes.BOOLEAN();
        MapType valueType = DataTypes.MAP(innerKeyType, innerValueType);

        CompactedMap map = new CompactedMap(keyType, valueType);

        GenericArray keyArray =
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.BinaryString.fromString("key1"),
                            org.apache.fluss.row.BinaryString.fromString("key2")
                        });

        java.util.Map<Object, Object> innerJavaMap1 = new java.util.HashMap<>();
        innerJavaMap1.put(100L, true);
        innerJavaMap1.put(200L, false);
        GenericMap innerGenericMap1 = new GenericMap(innerJavaMap1);

        java.util.Map<Object, Object> innerJavaMap2 = new java.util.HashMap<>();
        innerJavaMap2.put(300L, true);
        GenericMap innerGenericMap2 = new GenericMap(innerJavaMap2);

        GenericArray valueArray =
                new GenericArray(new Object[] {innerGenericMap1, innerGenericMap2});

        ArraySerializer keySerializer = new ArraySerializer(keyType, COMPACTED);
        ArraySerializer valueSerializer = new ArraySerializer(valueType, COMPACTED);

        BinaryArray binaryKeyArray = keySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueSerializer.toBinaryArray(valueArray);

        BinaryMap binaryMap = BinaryMap.valueOf(binaryKeyArray, binaryValueArray, map);

        CompactedMap reuseMap = new CompactedMap(keyType, valueType);
        BinaryMap copiedMap = binaryMap.copy(reuseMap);

        assertThat(copiedMap).isNotNull();
        assertThat(copiedMap.size()).isEqualTo(2);
        assertThat(copiedMap).isSameAs(reuseMap);
    }
}
