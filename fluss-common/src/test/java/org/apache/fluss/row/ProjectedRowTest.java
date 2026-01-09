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

import org.apache.fluss.exception.SchemaChangeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProjectedRow}. */
class ProjectedRowTest {
    @Test
    void testProjectedRows() {
        final ProjectedRow projectedRow = ProjectedRow.from(new int[] {2, 0, 1, 4});
        assertThat(projectedRow.getFieldCount()).isEqualTo(4);

        projectedRow.replaceRow(GenericRow.of(0L, 1L, 2L, 3L, 4L));
        assertThat(projectedRow.getLong(0)).isEqualTo(2);
        assertThat(projectedRow.getLong(1)).isEqualTo(0);
        assertThat(projectedRow.getLong(2)).isEqualTo(1);
        assertThat(projectedRow.getLong(3)).isEqualTo(4);

        projectedRow.replaceRow(GenericRow.of(5L, 6L, 7L, 8L, 9L, 10L));
        assertThat(projectedRow.getLong(0)).isEqualTo(7);
        assertThat(projectedRow.getLong(1)).isEqualTo(5);
        assertThat(projectedRow.getLong(2)).isEqualTo(6);
        assertThat(projectedRow.getLong(3)).isEqualTo(9);

        // test other types
        projectedRow.replaceRow(GenericRow.of(0, 1, 2, 3, 4));
        assertThat(projectedRow.getInt(0)).isEqualTo(2);
        assertThat(projectedRow.getInt(1)).isEqualTo(0);
        assertThat(projectedRow.getInt(2)).isEqualTo(1);
        assertThat(projectedRow.getInt(3)).isEqualTo(4);

        projectedRow.replaceRow(
                GenericRow.of((short) 5, (short) 6, (short) 7, (short) 8, (short) 9, (short) 10));
        assertThat(projectedRow.getShort(0)).isEqualTo((short) 7);
        assertThat(projectedRow.getShort(1)).isEqualTo((short) 5);
        assertThat(projectedRow.getShort(2)).isEqualTo((short) 6);
        assertThat(projectedRow.getShort(3)).isEqualTo((short) 9);

        projectedRow.replaceRow(
                GenericRow.of((byte) 5, (byte) 6, (byte) 7, (byte) 8, (byte) 9, (byte) 10));
        assertThat(projectedRow.getByte(0)).isEqualTo((byte) 7);
        assertThat(projectedRow.getByte(1)).isEqualTo((byte) 5);
        assertThat(projectedRow.getByte(2)).isEqualTo((byte) 6);
        assertThat(projectedRow.getByte(3)).isEqualTo((byte) 9);

        projectedRow.replaceRow(GenericRow.of(true, false, true, false, true, false));
        assertThat(projectedRow.getBoolean(0)).isEqualTo(true);
        assertThat(projectedRow.getBoolean(1)).isEqualTo(true);
        assertThat(projectedRow.getBoolean(2)).isEqualTo(false);
        assertThat(projectedRow.getBoolean(3)).isEqualTo(true);

        projectedRow.replaceRow(GenericRow.of(0.0f, 0.1f, 0.2f, 0.3f, 0.4f));
        assertThat(projectedRow.getFloat(0)).isEqualTo(0.2f);
        assertThat(projectedRow.getFloat(1)).isEqualTo(0.0f);
        assertThat(projectedRow.getFloat(2)).isEqualTo(0.1f);
        assertThat(projectedRow.getFloat(3)).isEqualTo(0.4f);

        projectedRow.replaceRow(GenericRow.of(0.5d, 0.6d, 0.7d, 0.8d, 0.9d, 1.0d));
        assertThat(projectedRow.getDouble(0)).isEqualTo(0.7d);
        assertThat(projectedRow.getDouble(1)).isEqualTo(0.5d);
        assertThat(projectedRow.getDouble(2)).isEqualTo(0.6d);
        assertThat(projectedRow.getDouble(3)).isEqualTo(0.9d);

        projectedRow.replaceRow(
                GenericRow.of(
                        BinaryString.fromString("0"),
                        BinaryString.fromString("1"),
                        BinaryString.fromString("2"),
                        BinaryString.fromString("3"),
                        BinaryString.fromString("4")));
        assertThat(projectedRow.getChar(0, 1).toString()).isEqualTo("2");
        assertThat(projectedRow.getChar(1, 1).toString()).isEqualTo("0");
        assertThat(projectedRow.getChar(2, 1).toString()).isEqualTo("1");
        assertThat(projectedRow.getChar(3, 1).toString()).isEqualTo("4");
        assertThat(projectedRow.getString(0).toString()).isEqualTo("2");
        assertThat(projectedRow.getString(1).toString()).isEqualTo("0");
        assertThat(projectedRow.getString(2).toString()).isEqualTo("1");
        assertThat(projectedRow.getString(3).toString()).isEqualTo("4");

        projectedRow.replaceRow(
                GenericRow.of(
                        Decimal.fromBigDecimal(new BigDecimal("0"), 18, 0),
                        Decimal.fromBigDecimal(new BigDecimal("1"), 18, 0),
                        Decimal.fromBigDecimal(new BigDecimal("2"), 18, 0),
                        Decimal.fromBigDecimal(new BigDecimal("3"), 18, 0),
                        Decimal.fromBigDecimal(new BigDecimal("4"), 18, 0)));
        assertThat(projectedRow.getDecimal(0, 18, 0).toString()).isEqualTo("2");
        assertThat(projectedRow.getDecimal(1, 18, 0).toString()).isEqualTo("0");
        assertThat(projectedRow.getDecimal(2, 18, 0).toString()).isEqualTo("1");
        assertThat(projectedRow.getDecimal(3, 18, 0).toString()).isEqualTo("4");

        projectedRow.replaceRow(
                GenericRow.of(
                        TimestampNtz.fromMillis(5L),
                        TimestampNtz.fromMillis(6L),
                        TimestampNtz.fromMillis(7L),
                        TimestampNtz.fromMillis(8L),
                        TimestampNtz.fromMillis(9L),
                        TimestampNtz.fromMillis(10L)));
        assertThat(projectedRow.getTimestampNtz(0, 3)).isEqualTo(TimestampNtz.fromMillis(7));
        assertThat(projectedRow.getTimestampNtz(1, 3)).isEqualTo(TimestampNtz.fromMillis(5));
        assertThat(projectedRow.getTimestampNtz(2, 3)).isEqualTo(TimestampNtz.fromMillis(6));
        assertThat(projectedRow.getTimestampNtz(3, 3)).isEqualTo(TimestampNtz.fromMillis(9));

        projectedRow.replaceRow(
                GenericRow.of(
                        TimestampLtz.fromEpochMicros(5L),
                        TimestampLtz.fromEpochMicros(6L),
                        TimestampLtz.fromEpochMicros(7L),
                        TimestampLtz.fromEpochMicros(8L),
                        TimestampLtz.fromEpochMicros(9L),
                        TimestampLtz.fromEpochMicros(10L)));
        assertThat(projectedRow.getTimestampLtz(0, 3)).isEqualTo(TimestampLtz.fromEpochMicros(7));
        assertThat(projectedRow.getTimestampLtz(1, 3)).isEqualTo(TimestampLtz.fromEpochMicros(5));
        assertThat(projectedRow.getTimestampLtz(2, 3)).isEqualTo(TimestampLtz.fromEpochMicros(6));
        assertThat(projectedRow.getTimestampLtz(3, 3)).isEqualTo(TimestampLtz.fromEpochMicros(9));

        projectedRow.replaceRow(
                GenericRow.of(
                        new byte[] {5},
                        new byte[] {6},
                        new byte[] {7},
                        new byte[] {8},
                        new byte[] {9},
                        new byte[] {10}));
        assertThat(projectedRow.getBytes(0)).isEqualTo(new byte[] {7});
        assertThat(projectedRow.getBytes(1)).isEqualTo(new byte[] {5});
        assertThat(projectedRow.getBytes(2)).isEqualTo(new byte[] {6});
        assertThat(projectedRow.getBytes(3)).isEqualTo(new byte[] {9});
        assertThat(projectedRow.getBinary(0, 1)).isEqualTo(new byte[] {7});
        assertThat(projectedRow.getBinary(1, 1)).isEqualTo(new byte[] {5});
        assertThat(projectedRow.getBinary(2, 1)).isEqualTo(new byte[] {6});
        assertThat(projectedRow.getBinary(3, 1)).isEqualTo(new byte[] {9});

        // test null
        projectedRow.replaceRow(GenericRow.of(5L, 6L, null, 8L, null, 10L));
        assertThat(projectedRow.isNullAt(0)).isTrue();
        assertThat(projectedRow.isNullAt(1)).isFalse();
        assertThat(projectedRow.isNullAt(2)).isFalse();
        assertThat(projectedRow.isNullAt(3)).isTrue();
    }

    @Test
    void testProjectedRowsWithDifferentSchema() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.DECIMAL(10, 1))
                        .build();
        assertThatThrownBy(
                        () ->
                                ProjectedRow.from(
                                        schema,
                                        Schema.newBuilder().column("a", DataTypes.INT()).build()))
                .isExactlyInstanceOf(SchemaChangeException.class)
                .hasMessage(
                        "Expected datatype of column(id=0,name=a) is [INT], while the actual datatype is [BIGINT]");

        assertThatThrownBy(
                        () ->
                                ProjectedRow.from(
                                        schema,
                                        Schema.newBuilder()
                                                .fromColumns(
                                                        Collections.singletonList(
                                                                new Schema.Column(
                                                                        "f",
                                                                        DataTypes.DECIMAL(20, 1),
                                                                        null,
                                                                        2)))
                                                .build()))
                .isExactlyInstanceOf(SchemaChangeException.class)
                .hasMessage(
                        "Expected datatype of column(id=2,name=f) is [DECIMAL(20, 1)], while the actual datatype is [DECIMAL(10, 1)]");

        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        new Schema.Column("a", DataTypes.BIGINT(), null, 0),
                                        // colum e share same id as the column c in the original
                                        // schema.
                                        new Schema.Column("e", DataTypes.DECIMAL(10, 1), null, 2),
                                        // column b has different id the column b the original
                                        // schema.
                                        new Schema.Column("b", DataTypes.DECIMAL(10, 1), null, 3),
                                        new Schema.Column("f", DataTypes.STRING(), null, 4)))
                        .build();

        ProjectedRow projectedRow = ProjectedRow.from(schema, newSchema);
        projectedRow.replaceRow(
                GenericRow.of(
                        1L,
                        BinaryString.fromString("value"),
                        Decimal.fromBigDecimal(new BigDecimal("13145678.1"), 10, 1)));
        assertThat(projectedRow.getFieldCount()).isEqualTo(4);
        assertThat(projectedRow.getLong(0)).isEqualTo(1L);
        assertThat(projectedRow.getDecimal(1, 10, 1).decimalVal)
                .isEqualTo(new BigDecimal("13145678.1"));
        assertThat(projectedRow.isNullAt(2)).isTrue();
        assertThat(projectedRow.isNullAt(3)).isTrue();
    }

    @Test
    void testProjectedRowsWithComplexTypes() {
        // Test projection with Array, Map, and Row types
        // Original row: (a bigint, b string, c map<string,int>, d row<d1 int, d2 string>,
        //                e array<string>, f date)
        // Projection: [0, 2, 3, 5] -> select a, c, d, f
        final ProjectedRow projectedRow = ProjectedRow.from(new int[] {0, 2, 3, 5});
        assertThat(projectedRow.getFieldCount()).isEqualTo(4);

        // Create test data with complex types
        Map<BinaryString, Integer> map1 = new HashMap<>();
        map1.put(BinaryString.fromString("key1"), 100);
        map1.put(BinaryString.fromString("key2"), 200);
        GenericMap genericMap1 = new GenericMap(map1);

        GenericRow nestedRow1 = GenericRow.of(10, BinaryString.fromString("nested1"));

        GenericArray array1 =
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("a"),
                            BinaryString.fromString("b"),
                            BinaryString.fromString("c")
                        });

        // Original row: (100L, "value1", map1, nestedRow1, array1, 20231)
        GenericRow originalRow1 =
                GenericRow.of(
                        100L,
                        BinaryString.fromString("value1"),
                        genericMap1,
                        nestedRow1,
                        array1,
                        20231);

        projectedRow.replaceRow(originalRow1);

        // Verify projection: should be (100L, map1, nestedRow1, 20231)
        assertThat(projectedRow.getLong(0)).isEqualTo(100L);

        // Verify map projection
        InternalMap projectedMap = projectedRow.getMap(1);
        assertThat(projectedMap).isNotNull();
        assertThat(projectedMap.size()).isEqualTo(2);

        // Verify row projection
        InternalRow projectedNestedRow = projectedRow.getRow(2, 2);
        assertThat(projectedNestedRow).isNotNull();
        assertThat(projectedNestedRow.getInt(0)).isEqualTo(10);
        assertThat(projectedNestedRow.getString(1).toString()).isEqualTo("nested1");

        // Verify simple type at the end
        assertThat(projectedRow.getInt(3)).isEqualTo(20231);

        // Test with different data
        Map<BinaryString, Integer> map2 = new HashMap<>();
        map2.put(BinaryString.fromString("key3"), 300);
        GenericMap genericMap2 = new GenericMap(map2);

        GenericRow nestedRow2 = GenericRow.of(20, BinaryString.fromString("nested2"));

        GenericArray array2 =
                new GenericArray(
                        new Object[] {BinaryString.fromString("d"), BinaryString.fromString("e")});

        GenericRow originalRow2 =
                GenericRow.of(
                        200L,
                        BinaryString.fromString("value2"),
                        genericMap2,
                        nestedRow2,
                        array2,
                        20232);

        projectedRow.replaceRow(originalRow2);

        // Verify second projection
        assertThat(projectedRow.getLong(0)).isEqualTo(200L);

        InternalMap projectedMap2 = projectedRow.getMap(1);
        assertThat(projectedMap2).isNotNull();
        assertThat(projectedMap2.size()).isEqualTo(1);

        InternalRow projectedNestedRow2 = projectedRow.getRow(2, 2);
        assertThat(projectedNestedRow2).isNotNull();
        assertThat(projectedNestedRow2.getInt(0)).isEqualTo(20);
        assertThat(projectedNestedRow2.getString(1).toString()).isEqualTo("nested2");

        assertThat(projectedRow.getInt(3)).isEqualTo(20232);
    }

    @Test
    void testProjectedRowsWithArrayOnly() {
        // Test projection focusing on array types
        // Original row: (a int, b array<int>, c array<string>, d bigint)
        // Projection: [1, 2] -> select b, c
        final ProjectedRow projectedRow = ProjectedRow.from(new int[] {1, 2});
        assertThat(projectedRow.getFieldCount()).isEqualTo(2);

        GenericArray intArray = new GenericArray(new Object[] {1, 2, 3, 4, 5});
        GenericArray stringArray =
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("x"),
                            BinaryString.fromString("y"),
                            BinaryString.fromString("z")
                        });

        GenericRow originalRow = GenericRow.of(100, intArray, stringArray, 1000L);
        projectedRow.replaceRow(originalRow);

        // Verify int array projection
        InternalArray projectedIntArray = projectedRow.getArray(0);
        assertThat(projectedIntArray).isNotNull();
        assertThat(projectedIntArray.size()).isEqualTo(5);
        assertThat(projectedIntArray.getInt(0)).isEqualTo(1);
        assertThat(projectedIntArray.getInt(4)).isEqualTo(5);

        // Verify string array projection
        InternalArray projectedStringArray = projectedRow.getArray(1);
        assertThat(projectedStringArray).isNotNull();
        assertThat(projectedStringArray.size()).isEqualTo(3);
        assertThat(projectedStringArray.getString(0).toString()).isEqualTo("x");
        assertThat(projectedStringArray.getString(2).toString()).isEqualTo("z");
    }

    @Test
    void testProjectedRowsWithMapOnly() {
        // Test projection focusing on map types
        // Original row: (a int, b map<string,int>, c map<int,string>, d bigint)
        // Projection: [1, 2] -> select b, c
        final ProjectedRow projectedRow = ProjectedRow.from(new int[] {1, 2});
        assertThat(projectedRow.getFieldCount()).isEqualTo(2);

        Map<BinaryString, Integer> stringIntMap = new HashMap<>();
        stringIntMap.put(BinaryString.fromString("k1"), 10);
        stringIntMap.put(BinaryString.fromString("k2"), 20);
        GenericMap genericMap1 = new GenericMap(stringIntMap);

        Map<Integer, BinaryString> intStringMap = new HashMap<>();
        intStringMap.put(1, BinaryString.fromString("v1"));
        intStringMap.put(2, BinaryString.fromString("v2"));
        GenericMap genericMap2 = new GenericMap(intStringMap);

        GenericRow originalRow = GenericRow.of(100, genericMap1, genericMap2, 1000L);
        projectedRow.replaceRow(originalRow);

        // Verify string-int map projection
        InternalMap projectedMap1 = projectedRow.getMap(0);
        assertThat(projectedMap1).isNotNull();
        assertThat(projectedMap1.size()).isEqualTo(2);

        // Verify int-string map projection
        InternalMap projectedMap2 = projectedRow.getMap(1);
        assertThat(projectedMap2).isNotNull();
        assertThat(projectedMap2.size()).isEqualTo(2);
    }

    @Test
    void testProjectedRowsWithNullComplexTypes() {
        // Test projection with null complex type values
        // Projection: [0, 2, 3, 4] -> select a, c (map), d (row), e (array)
        final ProjectedRow projectedRow = ProjectedRow.from(new int[] {0, 2, 3, 4});
        assertThat(projectedRow.getFieldCount()).isEqualTo(4);

        // Row with null complex types
        GenericRow originalRow =
                GenericRow.of(100L, BinaryString.fromString("value"), null, null, null);
        projectedRow.replaceRow(originalRow);

        assertThat(projectedRow.getLong(0)).isEqualTo(100L);
        assertThat(projectedRow.isNullAt(1)).isTrue();
        assertThat(projectedRow.isNullAt(2)).isTrue();
        assertThat(projectedRow.isNullAt(3)).isTrue();

        // Row with non-null complex types
        Map<BinaryString, Integer> map = new HashMap<>();
        map.put(BinaryString.fromString("key"), 100);
        GenericMap genericMap = new GenericMap(map);
        GenericRow nestedRow = GenericRow.of(10, BinaryString.fromString("nested"));
        GenericArray array = new GenericArray(new Object[] {BinaryString.fromString("a")});

        GenericRow originalRow2 =
                GenericRow.of(
                        200L, BinaryString.fromString("value2"), genericMap, nestedRow, array);
        projectedRow.replaceRow(originalRow2);

        assertThat(projectedRow.getLong(0)).isEqualTo(200L);
        assertThat(projectedRow.isNullAt(1)).isFalse();
        assertThat(projectedRow.getMap(1)).isNotNull();
        assertThat(projectedRow.isNullAt(2)).isFalse();
        assertThat(projectedRow.getRow(2, 2)).isNotNull();
        assertThat(projectedRow.isNullAt(3)).isFalse();
        assertThat(projectedRow.getArray(3)).isNotNull();
    }

    @Test
    void testProjectedRowsWithReordering() {
        // Test projection with reordering of complex types
        // Original row: (a array<int>, b map<string,int>, c row<x int, y string>, d bigint)
        // Projection: [3, 1, 0] -> select d, b, a (reordered)
        final ProjectedRow projectedRow = ProjectedRow.from(new int[] {3, 1, 0});
        assertThat(projectedRow.getFieldCount()).isEqualTo(3);

        GenericArray array = new GenericArray(new Object[] {1, 2, 3});
        Map<BinaryString, Integer> map = new HashMap<>();
        map.put(BinaryString.fromString("key"), 100);
        GenericMap genericMap = new GenericMap(map);
        GenericRow nestedRow = GenericRow.of(10, BinaryString.fromString("test"));

        GenericRow originalRow = GenericRow.of(array, genericMap, nestedRow, 1000L);
        projectedRow.replaceRow(originalRow);

        // Verify reordered projection: (1000L, map, array)
        assertThat(projectedRow.getLong(0)).isEqualTo(1000L);

        InternalMap projectedMap = projectedRow.getMap(1);
        assertThat(projectedMap).isNotNull();
        assertThat(projectedMap.size()).isEqualTo(1);

        InternalArray projectedArray = projectedRow.getArray(2);
        assertThat(projectedArray).isNotNull();
        assertThat(projectedArray.size()).isEqualTo(3);
        assertThat(projectedArray.getInt(0)).isEqualTo(1);
        assertThat(projectedArray.getInt(2)).isEqualTo(3);
    }
}
