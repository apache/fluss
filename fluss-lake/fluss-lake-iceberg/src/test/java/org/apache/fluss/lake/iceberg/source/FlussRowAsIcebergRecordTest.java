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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FlussRowAsIcebergRecord} with array types. */
class FlussRowAsIcebergRecordTest {

    @Test
    void testArrayWithAllTypes() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "bool_array",
                                Types.ListType.ofRequired(1, Types.BooleanType.get())),
                        Types.NestedField.required(
                                2,
                                "byte_array",
                                Types.ListType.ofRequired(3, Types.IntegerType.get())),
                        Types.NestedField.required(
                                4,
                                "short_array",
                                Types.ListType.ofRequired(5, Types.IntegerType.get())),
                        Types.NestedField.required(
                                6,
                                "int_array",
                                Types.ListType.ofRequired(7, Types.IntegerType.get())),
                        Types.NestedField.required(
                                8,
                                "long_array",
                                Types.ListType.ofRequired(9, Types.LongType.get())),
                        Types.NestedField.required(
                                10,
                                "float_array",
                                Types.ListType.ofRequired(11, Types.FloatType.get())),
                        Types.NestedField.required(
                                12,
                                "double_array",
                                Types.ListType.ofRequired(13, Types.DoubleType.get())),
                        Types.NestedField.required(
                                14,
                                "string_array",
                                Types.ListType.ofRequired(15, Types.StringType.get())),
                        Types.NestedField.required(
                                16,
                                "decimal_array",
                                Types.ListType.ofRequired(17, Types.DecimalType.of(10, 2))),
                        Types.NestedField.required(
                                18,
                                "timestamp_ntz_array",
                                Types.ListType.ofRequired(19, Types.TimestampType.withoutZone())),
                        Types.NestedField.required(
                                20,
                                "timestamp_ltz_array",
                                Types.ListType.ofRequired(21, Types.TimestampType.withZone())),
                        Types.NestedField.required(
                                22,
                                "binary_array",
                                Types.ListType.ofRequired(23, Types.BinaryType.get())),
                        Types.NestedField.required(
                                24,
                                "nested_array",
                                Types.ListType.ofRequired(
                                        25,
                                        Types.ListType.ofRequired(26, Types.IntegerType.get()))),
                        Types.NestedField.required(
                                27,
                                "nullable_int_array",
                                Types.ListType.ofOptional(28, Types.IntegerType.get())),
                        Types.NestedField.optional(
                                29,
                                "null_array",
                                Types.ListType.ofRequired(30, Types.IntegerType.get())));

        RowType flussRowType =
                RowType.of(
                        DataTypes.ARRAY(DataTypes.BOOLEAN()),
                        DataTypes.ARRAY(DataTypes.TINYINT()),
                        DataTypes.ARRAY(DataTypes.SMALLINT()),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.BIGINT()),
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        DataTypes.ARRAY(DataTypes.DOUBLE()),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)),
                        DataTypes.ARRAY(DataTypes.TIMESTAMP(6)),
                        DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ(6)),
                        DataTypes.ARRAY(DataTypes.BYTES()),
                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.INT()));

        GenericRow genericRow = new GenericRow(15);
        genericRow.setField(0, new GenericArray(new boolean[] {true, false, true}));
        genericRow.setField(1, new GenericArray(new byte[] {1, 2, 3}));
        genericRow.setField(2, new GenericArray(new short[] {100, 200, 300}));
        genericRow.setField(3, new GenericArray(new int[] {1000, 2000, 3000}));
        genericRow.setField(4, new GenericArray(new long[] {10000L, 20000L, 30000L}));
        genericRow.setField(5, new GenericArray(new float[] {1.1f, 2.2f, 3.3f}));
        genericRow.setField(6, new GenericArray(new double[] {1.11, 2.22, 3.33}));
        genericRow.setField(
                7,
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("hello"),
                            BinaryString.fromString("world"),
                            BinaryString.fromString("test")
                        }));
        genericRow.setField(
                8,
                new GenericArray(
                        new Object[] {
                            Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2)
                        }));
        genericRow.setField(
                9,
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.TimestampNtz.fromLocalDateTime(
                                    LocalDateTime.now()),
                            org.apache.fluss.row.TimestampNtz.fromLocalDateTime(
                                    LocalDateTime.now().plusSeconds(1))
                        }));
        genericRow.setField(
                10,
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.TimestampLtz.fromEpochMillis(
                                    System.currentTimeMillis()),
                            org.apache.fluss.row.TimestampLtz.fromEpochMillis(
                                    System.currentTimeMillis() + 1000)
                        }));
        genericRow.setField(
                11,
                new GenericArray(
                        new Object[] {"hello".getBytes(), "world".getBytes(), "test".getBytes()}));
        genericRow.setField(
                12,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));
        genericRow.setField(13, new GenericArray(new Object[] {1, null, 3}));
        genericRow.setField(14, null);

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        // Test boolean array
        List<?> boolArray = (List<?>) record.get(0);
        assertThat(boolArray.size()).isEqualTo(3);
        assertThat(boolArray.get(0)).isEqualTo(true);
        assertThat(boolArray.get(1)).isEqualTo(false);
        assertThat(boolArray.get(2)).isEqualTo(true);

        // Test byte array
        List<?> byteArray = (List<?>) record.get(1);
        assertThat(byteArray.size()).isEqualTo(3);
        assertThat(byteArray.get(0)).isEqualTo(1);
        assertThat(byteArray.get(1)).isEqualTo(2);
        assertThat(byteArray.get(2)).isEqualTo(3);

        // Test short array
        List<?> shortArray = (List<?>) record.get(2);
        assertThat(shortArray.size()).isEqualTo(3);
        assertThat(shortArray.get(0)).isEqualTo(100);
        assertThat(shortArray.get(1)).isEqualTo(200);
        assertThat(shortArray.get(2)).isEqualTo(300);

        // Test int array
        List<?> intArray = (List<?>) record.get(3);
        assertThat(intArray.size()).isEqualTo(3);
        assertThat(intArray.get(0)).isEqualTo(1000);
        assertThat(intArray.get(1)).isEqualTo(2000);
        assertThat(intArray.get(2)).isEqualTo(3000);

        // Test long array
        List<?> longArray = (List<?>) record.get(4);
        assertThat(longArray.size()).isEqualTo(3);
        assertThat(longArray.get(0)).isEqualTo(10000L);
        assertThat(longArray.get(1)).isEqualTo(20000L);
        assertThat(longArray.get(2)).isEqualTo(30000L);

        // Test float array
        List<?> floatArray = (List<?>) record.get(5);
        assertThat(floatArray.size()).isEqualTo(3);
        assertThat(floatArray.get(0)).isEqualTo(1.1f);
        assertThat(floatArray.get(1)).isEqualTo(2.2f);
        assertThat(floatArray.get(2)).isEqualTo(3.3f);

        // Test double array
        List<?> doubleArray = (List<?>) record.get(6);
        assertThat(doubleArray.size()).isEqualTo(3);
        assertThat(doubleArray.get(0)).isEqualTo(1.11);
        assertThat(doubleArray.get(1)).isEqualTo(2.22);
        assertThat(doubleArray.get(2)).isEqualTo(3.33);

        // Test string array
        List<?> stringArray = (List<?>) record.get(7);
        assertThat(stringArray.size()).isEqualTo(3);
        assertThat(stringArray.get(0)).isEqualTo("hello");
        assertThat(stringArray.get(1)).isEqualTo("world");
        assertThat(stringArray.get(2)).isEqualTo("test");

        // Test decimal array
        List<?> decimalArray = (List<?>) record.get(8);
        assertThat(decimalArray.size()).isEqualTo(2);
        assertThat(decimalArray.get(0)).isEqualTo(new BigDecimal("123.45"));
        assertThat(decimalArray.get(1)).isEqualTo(new BigDecimal("678.90"));

        // Test timestamp array
        List<?> timestampNtzArray = (List<?>) record.get(9);
        assertThat(timestampNtzArray).isNotNull();
        assertThat(timestampNtzArray.size()).isEqualTo(2);
        assertThat(timestampNtzArray.get(0)).isInstanceOf(LocalDateTime.class);
        assertThat(timestampNtzArray.get(1)).isInstanceOf(LocalDateTime.class);

        // Test timestamp_ltz array
        List<?> timestampLtzArray = (List<?>) record.get(10);
        assertThat(timestampLtzArray).isNotNull();
        assertThat(timestampLtzArray.size()).isEqualTo(2);
        assertThat(timestampLtzArray.get(0)).isInstanceOf(OffsetDateTime.class);
        assertThat(timestampLtzArray.get(1)).isInstanceOf(OffsetDateTime.class);

        // Test binary array
        List<?> binaryArray = (List<?>) record.get(11);
        assertThat(binaryArray).isNotNull();
        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.get(0)).isInstanceOf(ByteBuffer.class);
        assertThat(((ByteBuffer) binaryArray.get(0)).array()).isEqualTo("hello".getBytes());
        assertThat(((ByteBuffer) binaryArray.get(1)).array()).isEqualTo("world".getBytes());
        assertThat(((ByteBuffer) binaryArray.get(2)).array()).isEqualTo("test".getBytes());

        // Test nested array (array<array<int>>)
        List<?> outerArray = (List<?>) record.get(12);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        List<?> innerArray1 = (List<?>) outerArray.get(0);
        assertThat(innerArray1.size()).isEqualTo(2);
        assertThat(innerArray1.get(0)).isEqualTo(1);
        assertThat(innerArray1.get(1)).isEqualTo(2);

        List<?> innerArray2 = (List<?>) outerArray.get(1);
        assertThat(innerArray2.size()).isEqualTo(3);
        assertThat(innerArray2.get(0)).isEqualTo(3);
        assertThat(innerArray2.get(1)).isEqualTo(4);
        assertThat(innerArray2.get(2)).isEqualTo(5);

        // Test array with null elements
        List<?> nullableArray = (List<?>) record.get(13);
        assertThat(nullableArray).isNotNull();
        assertThat(nullableArray.size()).isEqualTo(3);
        assertThat(nullableArray.get(0)).isEqualTo(1);
        assertThat(nullableArray.get(1)).isNull();
        assertThat(nullableArray.get(2)).isEqualTo(3);

        // Test null array
        assertThat(record.get(14)).isNull();
    }

    @Test
    void testNestedRow() {
        Types.StructType structType =
                Types.StructType.of(
                        // simple row
                        Types.NestedField.required(
                                0,
                                "simple_row",
                                Types.StructType.of(
                                        Types.NestedField.required(
                                                1, "id", Types.IntegerType.get()),
                                        Types.NestedField.required(
                                                2, "name", Types.StringType.get()))),
                        // nested row
                        Types.NestedField.required(
                                3,
                                "nested_row",
                                Types.StructType.of(
                                        Types.NestedField.required(
                                                4, "id", Types.IntegerType.get()),
                                        Types.NestedField.required(
                                                5,
                                                "inner",
                                                Types.StructType.of(
                                                        Types.NestedField.required(
                                                                6, "val", Types.DoubleType.get()),
                                                        Types.NestedField.required(
                                                                7,
                                                                "flag",
                                                                Types.BooleanType.get()))))),
                        // array row
                        Types.NestedField.required(
                                8,
                                "array_row",
                                Types.StructType.of(
                                        Types.NestedField.required(
                                                9,
                                                "ids",
                                                Types.ListType.ofRequired(
                                                        10, Types.IntegerType.get())))),
                        // nullable row
                        Types.NestedField.optional(
                                11,
                                "nullable_row",
                                Types.StructType.of(
                                        Types.NestedField.required(
                                                12, "id", Types.IntegerType.get()))));

        RowType flussRowType =
                RowType.of(
                        // simple row
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT()),
                                DataTypes.FIELD("name", DataTypes.STRING())),
                        // nested row
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT()),
                                DataTypes.FIELD(
                                        "inner",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("val", DataTypes.DOUBLE()),
                                                DataTypes.FIELD("flag", DataTypes.BOOLEAN())))),
                        // row_with array
                        DataTypes.ROW(DataTypes.FIELD("ids", DataTypes.ARRAY(DataTypes.INT()))),
                        // nullable row
                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())));

        GenericRow genericRow = new GenericRow(4);

        // Simple Row
        GenericRow simpleRow = new GenericRow(2);
        simpleRow.setField(0, 100);
        simpleRow.setField(1, BinaryString.fromString("fluss"));
        genericRow.setField(0, simpleRow);

        // Nested Row
        GenericRow innerRow = new GenericRow(2);
        innerRow.setField(0, 3.14);
        innerRow.setField(1, true);
        GenericRow nestedRow = new GenericRow(2);
        nestedRow.setField(0, 200);
        nestedRow.setField(1, innerRow);
        genericRow.setField(1, nestedRow);

        // Array Row
        GenericRow rowWithArray = new GenericRow(1);
        rowWithArray.setField(0, new GenericArray(new int[] {1, 2, 3}));
        genericRow.setField(2, rowWithArray);

        // Nullable Row
        genericRow.setField(3, null);

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        // Verify Simple Row
        Record icebergSimpleRow = (Record) record.get(0);
        assertThat(icebergSimpleRow.get(0)).isEqualTo(100);
        assertThat(icebergSimpleRow.get(1)).isEqualTo("fluss");

        // Verify Nested Row
        Record icebergNestedRow = (Record) record.get(1);
        assertThat(icebergNestedRow.get(0)).isEqualTo(200);
        Record icebergInnerRow = (Record) icebergNestedRow.get(1);
        assertThat(icebergInnerRow.get(0)).isEqualTo(3.14);
        assertThat(icebergInnerRow.get(1)).isEqualTo(true);

        // Verify Row with Array
        Record icebergRowWithArray = (Record) record.get(2);
        List<?> ids = (List<?>) icebergRowWithArray.get(0);
        assertThat(ids.size()).isEqualTo(3);
        assertThat(ids.get(0)).isEqualTo(1);
        assertThat(ids.get(1)).isEqualTo(2);
        assertThat(ids.get(2)).isEqualTo(3);

        // Verify Nullable Row
        assertThat(record.get(3)).isNull();
    }

    @Test
    void testMapWithAllTypes() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "string_to_string_map",
                                Types.MapType.ofRequired(
                                        1, 2, Types.StringType.get(), Types.StringType.get())),
                        Types.NestedField.required(
                                3,
                                "string_to_int_map",
                                Types.MapType.ofRequired(
                                        4, 5, Types.StringType.get(), Types.IntegerType.get())),
                        Types.NestedField.required(
                                6,
                                "int_to_string_map",
                                Types.MapType.ofRequired(
                                        7, 8, Types.IntegerType.get(), Types.StringType.get())),
                        Types.NestedField.required(
                                9,
                                "string_to_long_map",
                                Types.MapType.ofRequired(
                                        10, 11, Types.StringType.get(), Types.LongType.get())),
                        Types.NestedField.required(
                                12,
                                "string_to_double_map",
                                Types.MapType.ofRequired(
                                        13, 14, Types.StringType.get(), Types.DoubleType.get())),
                        Types.NestedField.required(
                                15,
                                "string_to_decimal_map",
                                Types.MapType.ofRequired(
                                        16,
                                        17,
                                        Types.StringType.get(),
                                        Types.DecimalType.of(10, 2))),
                        Types.NestedField.required(
                                18,
                                "string_to_boolean_map",
                                Types.MapType.ofRequired(
                                        19, 20, Types.StringType.get(), Types.BooleanType.get())),
                        Types.NestedField.optional(
                                21,
                                "null_map",
                                Types.MapType.ofRequired(
                                        22, 23, Types.StringType.get(), Types.StringType.get())));

        RowType flussRowType =
                RowType.of(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(10, 2)),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BOOLEAN()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));

        GenericRow genericRow = new GenericRow(8);

        // String to String map
        org.apache.fluss.row.GenericMap stringToStringMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(
                                        BinaryString.fromString("key1"),
                                        BinaryString.fromString("value1"));
                                put(
                                        BinaryString.fromString("key2"),
                                        BinaryString.fromString("value2"));
                                put(
                                        BinaryString.fromString("key3"),
                                        BinaryString.fromString("value3"));
                            }
                        });
        genericRow.setField(0, stringToStringMap);

        // String to Int map
        org.apache.fluss.row.GenericMap stringToIntMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("num1"), 100);
                                put(BinaryString.fromString("num2"), 200);
                                put(BinaryString.fromString("num3"), 300);
                            }
                        });
        genericRow.setField(1, stringToIntMap);

        // Int to String map
        org.apache.fluss.row.GenericMap intToStringMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(1, BinaryString.fromString("one"));
                                put(2, BinaryString.fromString("two"));
                                put(3, BinaryString.fromString("three"));
                            }
                        });
        genericRow.setField(2, intToStringMap);

        // String to Long map
        org.apache.fluss.row.GenericMap stringToLongMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("big1"), 10000L);
                                put(BinaryString.fromString("big2"), 20000L);
                            }
                        });
        genericRow.setField(3, stringToLongMap);

        // String to Double map
        org.apache.fluss.row.GenericMap stringToDoubleMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("pi"), 3.14);
                                put(BinaryString.fromString("e"), 2.71);
                            }
                        });
        genericRow.setField(4, stringToDoubleMap);

        // String to Decimal map
        org.apache.fluss.row.GenericMap stringToDecimalMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(
                                        BinaryString.fromString("dec1"),
                                        Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
                                put(
                                        BinaryString.fromString("dec2"),
                                        Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2));
                            }
                        });
        genericRow.setField(5, stringToDecimalMap);

        // String to Boolean map
        org.apache.fluss.row.GenericMap stringToBooleanMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("flag1"), true);
                                put(BinaryString.fromString("flag2"), false);
                                put(BinaryString.fromString("flag3"), true);
                            }
                        });
        genericRow.setField(6, stringToBooleanMap);

        // Null map
        genericRow.setField(7, null);

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        // Test String to String map
        java.util.Map<Object, Object> stringToStringResult =
                (java.util.Map<Object, Object>) record.get(0);
        assertThat(stringToStringResult).isNotNull();
        assertThat(stringToStringResult)
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2")
                .containsEntry("key3", "value3");

        // Test String to Int map
        java.util.Map<Object, Object> stringToIntResult =
                (java.util.Map<Object, Object>) record.get(1);
        assertThat(stringToIntResult).isNotNull();
        assertThat(stringToIntResult)
                .containsEntry("num1", 100)
                .containsEntry("num2", 200)
                .containsEntry("num3", 300);

        // Test Int to String map
        java.util.Map<Object, Object> intToStringResult =
                (java.util.Map<Object, Object>) record.get(2);
        assertThat(intToStringResult).isNotNull();
        assertThat(intToStringResult)
                .containsEntry(1, "one")
                .containsEntry(2, "two")
                .containsEntry(3, "three");

        // Test String to Long map
        java.util.Map<Object, Object> stringToLongResult =
                (java.util.Map<Object, Object>) record.get(3);
        assertThat(stringToLongResult).isNotNull();
        assertThat(stringToLongResult).containsEntry("big1", 10000L).containsEntry("big2", 20000L);

        // Test String to Double map
        java.util.Map<Object, Object> stringToDoubleResult =
                (java.util.Map<Object, Object>) record.get(4);
        assertThat(stringToDoubleResult).isNotNull();
        assertThat(stringToDoubleResult.get("pi")).isEqualTo(3.14);
        assertThat(stringToDoubleResult.get("e")).isEqualTo(2.71);

        // Test String to Decimal map
        java.util.Map<Object, Object> stringToDecimalResult =
                (java.util.Map<Object, Object>) record.get(5);
        assertThat(stringToDecimalResult).isNotNull();
        assertThat(stringToDecimalResult.get("dec1")).isEqualTo(new BigDecimal("123.45"));
        assertThat(stringToDecimalResult.get("dec2")).isEqualTo(new BigDecimal("678.90"));

        // Test String to Boolean map
        java.util.Map<Object, Object> stringToBooleanResult =
                (java.util.Map<Object, Object>) record.get(6);
        assertThat(stringToBooleanResult).isNotNull();
        assertThat(stringToBooleanResult)
                .containsEntry("flag1", true)
                .containsEntry("flag2", false)
                .containsEntry("flag3", true);

        // Test null map
        assertThat(record.get(7)).isNull();
    }

    @Test
    void testMapWithNullValues() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "map_with_nulls",
                                Types.MapType.ofOptional(
                                        1, 2, Types.StringType.get(), Types.StringType.get())));

        RowType flussRowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));

        GenericRow genericRow = new GenericRow(1);

        org.apache.fluss.row.GenericMap mapWithNulls =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(
                                        BinaryString.fromString("key1"),
                                        BinaryString.fromString("value1"));
                                put(BinaryString.fromString("key2"), null);
                                put(
                                        BinaryString.fromString("key3"),
                                        BinaryString.fromString("value3"));
                                put(BinaryString.fromString("key4"), null);
                            }
                        });
        genericRow.setField(0, mapWithNulls);

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        java.util.Map<Object, Object> resultMap = (java.util.Map<Object, Object>) record.get(0);
        assertThat(resultMap).isNotNull();
        assertThat(resultMap).hasSize(4);
        assertThat(resultMap)
                .containsEntry("key1", "value1")
                .containsEntry("key2", null)
                .containsEntry("key3", "value3")
                .containsEntry("key4", null);
    }

    @Test
    void testNestedMapInArray() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "map_array",
                                Types.ListType.ofRequired(
                                        1,
                                        Types.MapType.ofRequired(
                                                2,
                                                3,
                                                Types.StringType.get(),
                                                Types.IntegerType.get()))));

        RowType flussRowType =
                RowType.of(DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));

        GenericRow genericRow = new GenericRow(1);

        org.apache.fluss.row.GenericMap map1 =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("a"), 1);
                                put(BinaryString.fromString("b"), 2);
                            }
                        });

        org.apache.fluss.row.GenericMap map2 =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("c"), 3);
                                put(BinaryString.fromString("d"), 4);
                            }
                        });

        genericRow.setField(0, new GenericArray(new Object[] {map1, map2}));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> arrayResult = (List<?>) record.get(0);
        assertThat(arrayResult).hasSize(2);

        java.util.Map<Object, Object> firstMap = (java.util.Map<Object, Object>) arrayResult.get(0);
        assertThat(firstMap).containsEntry("a", 1).containsEntry("b", 2);

        java.util.Map<Object, Object> secondMap =
                (java.util.Map<Object, Object>) arrayResult.get(1);
        assertThat(secondMap).containsEntry("c", 3).containsEntry("d", 4);
    }

    @Test
    void testNestedMap() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "nested_map",
                                Types.MapType.ofRequired(
                                        1,
                                        2,
                                        Types.StringType.get(),
                                        Types.MapType.ofRequired(
                                                3,
                                                4,
                                                Types.StringType.get(),
                                                Types.IntegerType.get()))));

        RowType flussRowType =
                RowType.of(
                        DataTypes.MAP(
                                DataTypes.STRING(),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));

        GenericRow genericRow = new GenericRow(1);

        // Create inner map
        org.apache.fluss.row.GenericMap innerMap1 =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("inner_a"), 100);
                                put(BinaryString.fromString("inner_b"), 200);
                            }
                        });

        org.apache.fluss.row.GenericMap innerMap2 =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("inner_c"), 300);
                            }
                        });

        // Create outer map containing inner maps
        org.apache.fluss.row.GenericMap outerMap =
                new org.apache.fluss.row.GenericMap(
                        new java.util.HashMap<Object, Object>() {
                            {
                                put(BinaryString.fromString("outer_1"), innerMap1);
                                put(BinaryString.fromString("outer_2"), innerMap2);
                            }
                        });

        genericRow.setField(0, outerMap);

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        java.util.Map<Object, Object> resultMap = (java.util.Map<Object, Object>) record.get(0);
        assertThat(resultMap).isNotNull();
        assertThat(resultMap).hasSize(2);

        java.util.Map<Object, Object> result1 =
                (java.util.Map<Object, Object>) resultMap.get("outer_1");
        assertThat(result1).containsEntry("inner_a", 100).containsEntry("inner_b", 200);

        java.util.Map<Object, Object> result2 =
                (java.util.Map<Object, Object>) resultMap.get("outer_2");
        assertThat(result2).containsEntry("inner_c", 300);
    }

    @Test
    void testMapWithRowValue() {
        // Create Iceberg StructType with Map<String, Struct<id:Int, name:String>>
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "map_with_row",
                                Types.MapType.ofRequired(
                                        1,
                                        2,
                                        Types.StringType.get(),
                                        Types.StructType.of(
                                                Types.NestedField.required(
                                                        3, "id", Types.IntegerType.get()),
                                                Types.NestedField.required(
                                                        4, "name", Types.StringType.get())))));

        // Create Fluss RowType with MAP(STRING(), ROW(FIELD("id", INT()), FIELD("name", STRING())))
        RowType flussRowType =
                RowType.of(
                        DataTypes.MAP(
                                DataTypes.STRING(),
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()))));

        GenericRow genericRow = new GenericRow(1);

        // Create GenericRow instances for the map values
        GenericRow person1 = new GenericRow(2);
        person1.setField(0, 100);
        person1.setField(1, BinaryString.fromString("Alice"));

        GenericRow person2 = new GenericRow(2);
        person2.setField(0, 200);
        person2.setField(1, BinaryString.fromString("Bob"));

        // Create HashMap with person data
        java.util.HashMap<Object, Object> mapData = new java.util.HashMap<>();
        mapData.put(BinaryString.fromString("person1"), person1);
        mapData.put(BinaryString.fromString("person2"), person2);

        // Wrap in GenericMap and set to genericRow
        org.apache.fluss.row.GenericMap flussMap = new org.apache.fluss.row.GenericMap(mapData);
        genericRow.setField(0, flussMap);

        // Create FlussRowAsIcebergRecord
        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        // Verify the map conversion
        java.util.Map<Object, Object> resultMap = (java.util.Map<Object, Object>) record.get(0);
        assertThat(resultMap).isNotNull();
        assertThat(resultMap).hasSize(2);

        // Verify person1 row
        Record person1Row = (Record) resultMap.get("person1");
        assertThat(person1Row).isNotNull();
        assertThat(person1Row.get(0)).isEqualTo(100);
        assertThat(person1Row.get(1)).isEqualTo("Alice");

        // Verify person2 row
        Record person2Row = (Record) resultMap.get("person2");
        assertThat(person2Row).isNotNull();
        assertThat(person2Row.get(0)).isEqualTo(200);
        assertThat(person2Row.get(1)).isEqualTo("Bob");
    }
}
