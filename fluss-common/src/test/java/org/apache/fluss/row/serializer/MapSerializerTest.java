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
import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.map.AlignedMap;
import org.apache.fluss.row.map.CompactedMap;
import org.apache.fluss.row.map.IndexedMap;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MapSerializer}. */
public class MapSerializerTest {

    @Test
    public void testToBinaryMapWithCompactedFormat() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.STRING(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap =
                GenericMap.of(
                        1, BinaryString.fromString("one"),
                        2, BinaryString.fromString("two"));

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap).isNotNull();
        assertThat(binaryMap).isInstanceOf(CompactedMap.class);
        assertThat(binaryMap.size()).isEqualTo(2);
    }

    @Test
    public void testToBinaryMapWithIndexedFormat() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.STRING(), BinaryRow.BinaryRowFormat.INDEXED);

        GenericMap genericMap =
                GenericMap.of(
                        1, BinaryString.fromString("one"),
                        2, BinaryString.fromString("two"));

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap).isNotNull();
        assertThat(binaryMap).isInstanceOf(IndexedMap.class);
        assertThat(binaryMap.size()).isEqualTo(2);
    }

    @Test
    public void testToBinaryMapWithAlignedFormat() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.STRING(), BinaryRow.BinaryRowFormat.ALIGNED);

        GenericMap genericMap =
                GenericMap.of(
                        1, BinaryString.fromString("one"),
                        2, BinaryString.fromString("two"));

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap).isNotNull();
        assertThat(binaryMap).isInstanceOf(AlignedMap.class);
        assertThat(binaryMap.size()).isEqualTo(2);
    }

    @Test
    public void testToBinaryMapAlreadyBinary() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.INT(), BinaryRow.BinaryRowFormat.COMPACTED);

        CompactedMap compactedMap = new CompactedMap(DataTypes.INT(), DataTypes.INT());
        BinaryArray keyArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray valueArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});
        BinaryMap originalBinaryMap = BinaryMap.valueOf(keyArray, valueArray, compactedMap);

        BinaryMap result = serializer.toBinaryMap(originalBinaryMap);

        assertThat(result).isSameAs(originalBinaryMap);
    }

    @Test
    public void testToBinaryMapWithEmptyMap() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.STRING(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap = GenericMap.of();

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap).isNotNull();
        assertThat(binaryMap.size()).isEqualTo(0);
    }

    @Test
    public void testToBinaryMapWithNullValues() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.STRING(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap =
                GenericMap.of(
                        1, BinaryString.fromString("one"),
                        2, null,
                        3, BinaryString.fromString("three"));

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap.size()).isEqualTo(3);
        assertThat(binaryMap.valueArray().isNullAt(1)).isTrue();
    }

    @Test
    public void testToBinaryMapReuseInstance() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.INT(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap1 = GenericMap.of(1, 10, 2, 20);
        BinaryMap binaryMap1 = serializer.toBinaryMap(genericMap1);

        GenericMap genericMap2 = GenericMap.of(3, 30, 4, 40);
        BinaryMap binaryMap2 = serializer.toBinaryMap(genericMap2);

        assertThat(binaryMap1.size()).isEqualTo(2);
        assertThat(binaryMap2.size()).isEqualTo(2);
        assertThat(binaryMap2.keyArray().getInt(0)).isEqualTo(3);
    }

    @Test
    public void testToBinaryMapWithLongKeys() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.BIGINT(),
                        DataTypes.DOUBLE(),
                        BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap =
                GenericMap.of(
                        100L, 1.5,
                        200L, 2.5,
                        300L, 3.5);

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap.size()).isEqualTo(3);
        assertThat(binaryMap.keyArray().getLong(0)).isEqualTo(100L);
        assertThat(binaryMap.valueArray().getDouble(0)).isEqualTo(1.5);
    }

    @Test
    public void testToBinaryMapWithStringKeys() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.STRING(), DataTypes.INT(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap =
                GenericMap.of(
                        BinaryString.fromString("key1"), 1,
                        BinaryString.fromString("key2"), 2);

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap.size()).isEqualTo(2);
        assertThat(binaryMap.keyArray().getString(0)).isEqualTo(BinaryString.fromString("key1"));
        assertThat(binaryMap.valueArray().getInt(0)).isEqualTo(1);
    }

    @Test
    public void testMultipleCallsToToBinaryMap() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.STRING(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap map1 = GenericMap.of(1, BinaryString.fromString("first"));
        GenericMap map2 = GenericMap.of(2, BinaryString.fromString("second"));
        GenericMap map3 = GenericMap.of(3, BinaryString.fromString("third"));

        BinaryMap binaryMap1 = serializer.toBinaryMap(map1);
        BinaryMap binaryMap1Copy = binaryMap1.copy();

        BinaryMap binaryMap2 = serializer.toBinaryMap(map2);
        BinaryMap binaryMap2Copy = binaryMap2.copy();

        BinaryMap binaryMap3 = serializer.toBinaryMap(map3);

        assertThat(binaryMap1Copy.keyArray().getInt(0)).isEqualTo(1);
        assertThat(binaryMap2Copy.keyArray().getInt(0)).isEqualTo(2);
        assertThat(binaryMap3.keyArray().getInt(0)).isEqualTo(3);
    }

    @Test
    public void testToBinaryMapWithBooleanValues() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.BOOLEAN(), BinaryRow.BinaryRowFormat.INDEXED);

        GenericMap genericMap =
                GenericMap.of(
                        1, true,
                        2, false,
                        3, true);

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap.size()).isEqualTo(3);
        assertThat(binaryMap.valueArray().getBoolean(0)).isTrue();
        assertThat(binaryMap.valueArray().getBoolean(1)).isFalse();
        assertThat(binaryMap.valueArray().getBoolean(2)).isTrue();
    }

    @Test
    public void testSerializationPreservesOrder() {
        MapSerializer serializer =
                new MapSerializer(
                        DataTypes.INT(), DataTypes.INT(), BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap genericMap =
                GenericMap.of(
                        1, 10,
                        2, 20,
                        3, 30,
                        4, 40,
                        5, 50);

        BinaryMap binaryMap = serializer.toBinaryMap(genericMap);

        assertThat(binaryMap.size()).isEqualTo(5);
        InternalArray keys = binaryMap.keyArray();
        InternalArray values = binaryMap.valueArray();

        for (int i = 0; i < 5; i++) {
            assertThat(values.getInt(i)).isEqualTo((keys.getInt(i)) * 10);
        }
    }
}
