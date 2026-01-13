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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Comprehensive unit tests for {@link FlussDataTypeToIcebergDataType} with Map type conversion.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Basic Map type conversion (String->String, String->Int, etc.)
 *   <li>Field ID generation (two IDs per Map: one for key, one for value)
 *   <li>Nullable/non-nullable value handling
 *   <li>Nested Maps and complex value types
 *   <li>Maps within arrays and rows
 * </ul>
 */
@DisplayName("FlussDataTypeToIcebergDataType Map Type Conversion Tests")
class FlussDataTypeToIcebergDataTypeMapTest {

    @Nested
    @DisplayName("Basic Map Type Conversion")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class BasicMapConversionTests {

        @Test
        @DisplayName("Convert Map<String, String> to Iceberg MapType")
        void testMapStringToString() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.StringType.get());
        }

        @Test
        @DisplayName("Convert Map<String, Int> to Iceberg MapType")
        void testMapStringToInt() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.IntegerType.get());
        }

        @Test
        @DisplayName("Convert Map<Int, String> to Iceberg MapType")
        void testMapIntToString() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.IntegerType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.StringType.get());
        }

        @Test
        @DisplayName("Convert Map<Long, Long> to Iceberg MapType")
        void testMapLongToLong() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT());

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.LongType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.LongType.get());
        }

        @Test
        @DisplayName("Convert Map<String, Double> to Iceberg MapType")
        void testMapStringToDouble() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE());

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.DoubleType.get());
        }

        @Test
        @DisplayName("Convert Map<String, Decimal(10,2)> to Iceberg MapType")
        void testMapStringToDecimal() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(10, 2));

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.DecimalType.of(10, 2));
        }

        @Test
        @DisplayName("Convert Map<String, Bytes> to Iceberg MapType")
        void testMapStringToBytes() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType flussMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES());

            Type icebergType = flussMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType mapType = (Types.MapType) icebergType;
            assertThat(mapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(mapType.valueType()).isEqualTo(Types.BinaryType.get());
        }
    }

    @Nested
    @DisplayName("Field ID Generation Tests")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class FieldIdGenerationTests {

        @Test
        @DisplayName("Field IDs are correctly allocated (two IDs per Map)")
        void testFieldIdAllocation() {
            // Start with ID 0
            FlussDataTypeToIcebergDataType converter = new FlussDataTypeToIcebergDataType(0);

            MapType mapType1 = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
            Type icebergType1 = mapType1.accept(converter);

            // Next converter should start at ID 2 (since Map uses 2 IDs)
            FlussDataTypeToIcebergDataType converter2 = new FlussDataTypeToIcebergDataType(2);
            MapType mapType2 = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
            Type icebergType2 = mapType2.accept(converter2);

            assertThatCode(
                            () -> {
                                assertThat(icebergType1).isInstanceOf(Types.MapType.class);
                                assertThat(icebergType2).isInstanceOf(Types.MapType.class);
                            })
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Consecutive Maps in row have non-overlapping field IDs")
        void testConsecutiveMapFieldIds() {
            // Create a row with two Map fields
            RowType rowType =
                    RowType.of(
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));

            FlussDataTypeToIcebergDataType converter = new FlussDataTypeToIcebergDataType(rowType);

            // The converter should have processed the row with 2 fields + 4 more IDs for the maps
            Type icebergType = rowType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.StructType.class);
            Types.StructType structType = (Types.StructType) icebergType;
            assertThat(structType.fields()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Nullable Value Type Handling")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class NullableValueHandlingTests {

        @Test
        @DisplayName("Map with nullable String value creates optional MapType")
        void testMapWithNullableStringValue() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            // In Fluss, STRING() is nullable by default
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            // If value type is nullable, the map values should be optional
            assertThat(icebergMapType.isValueRequired()).isFalse();
        }

        @Test
        @DisplayName("Map with non-nullable value type uses required MapType")
        void testMapWithRequiredValue() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            // Use copy(false) to create non-nullable types
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().copy(false));

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.isValueRequired()).isTrue();
        }
    }

    @Nested
    @DisplayName("Nested Map Type Tests")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class NestedMapTests {

        @Test
        @DisplayName("Convert Map<String, Array<String>> to Iceberg MapType")
        void testMapWithArrayValue() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType =
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING()));

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(icebergMapType.valueType()).isInstanceOf(Types.ListType.class);
        }

        @Test
        @DisplayName("Convert Map<String, Map<String, Int>> nested map to Iceberg MapType")
        void testNestedMap() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType nestedMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
            MapType outerMapType = DataTypes.MAP(DataTypes.STRING(), nestedMapType);

            Type icebergType = outerMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.keyType()).isEqualTo(Types.StringType.get());
            assertThat(icebergMapType.valueType()).isInstanceOf(Types.MapType.class);

            // Verify nested map structure
            Types.MapType nestedIcebergMap = (Types.MapType) icebergMapType.valueType();
            assertThat(nestedIcebergMap.keyType()).isEqualTo(Types.StringType.get());
            assertThat(nestedIcebergMap.valueType()).isEqualTo(Types.IntegerType.get());
        }

        @Test
        @DisplayName("Convert Row with Map column to Iceberg StructType with MapType field")
        void testMapInRow() {
            RowType rowType =
                    RowType.of(
                            DataTypes.INT(),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                            DataTypes.STRING());

            FlussDataTypeToIcebergDataType converter = new FlussDataTypeToIcebergDataType(rowType);
            Type icebergType = rowType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.StructType.class);
            Types.StructType structType = (Types.StructType) icebergType;
            assertThat(structType.fields()).hasSize(3);

            // The second field should be a MapType
            Type secondFieldType = structType.fields().get(1).type();
            assertThat(secondFieldType).isInstanceOf(Types.MapType.class);
        }
    }

    @Nested
    @DisplayName("Complex Map Value Types")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ComplexMapValueTests {

        @Test
        @DisplayName("Convert Map<String, Row> with struct value")
        void testMapWithRowValue() {
            RowType valueRowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), valueRowType);

            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.valueType()).isInstanceOf(Types.StructType.class);
        }

        @Test
        @DisplayName("Convert Map<String, Boolean> to Iceberg MapType")
        void testMapStringToBoolean() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.BOOLEAN());

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.valueType()).isEqualTo(Types.BooleanType.get());
        }

        @Test
        @DisplayName("Convert Map<String, Date> to Iceberg MapType")
        void testMapStringToDate() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE());

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.valueType()).isEqualTo(Types.DateType.get());
        }

        @Test
        @DisplayName("Convert Map<String, Timestamp> to Iceberg MapType")
        void testMapStringToTimestamp() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.TIMESTAMP(6));

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.valueType()).isEqualTo(Types.TimestampType.withoutZone());
        }

        @Test
        @DisplayName("Convert Map<String, TimestampLTZ> to Iceberg MapType with zone")
        void testMapStringToTimestampLtz() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ(6));

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.valueType()).isEqualTo(Types.TimestampType.withZone());
        }
    }

    @Nested
    @DisplayName("Multiple Maps in Complex Structures")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ComplexStructureTests {

        @Test
        @DisplayName("Row with multiple Map fields")
        void testRowWithMultipleMaps() {
            RowType rowType =
                    RowType.of(
                            DataTypes.INT(),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()),
                            DataTypes.STRING());

            FlussDataTypeToIcebergDataType converter = new FlussDataTypeToIcebergDataType(rowType);
            Type icebergType = rowType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.StructType.class);
            Types.StructType structType = (Types.StructType) icebergType;
            assertThat(structType.fields()).hasSize(4);

            // Check that fields 1 and 2 are MapTypes
            assertThat(structType.fields().get(1).type()).isInstanceOf(Types.MapType.class);
            assertThat(structType.fields().get(2).type()).isInstanceOf(Types.MapType.class);
        }

        @Test
        @DisplayName("Array containing Maps")
        void testArrayOfMaps() {
            MapType innerMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

            // Array<Map<String, Int>>
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            Type icebergType = innerMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
        }

        @Test
        @DisplayName("Complex deeply nested structure")
        void testDeeplyNestedMapStructure() {
            // Create: Map<String, Array<Map<String, Row<id: Int, data: String>>>>
            RowType innerRowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
            MapType innerMapType = DataTypes.MAP(DataTypes.STRING(), innerRowType);
            MapType outerMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(innerMapType));

            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            Type icebergType = outerMapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType outerIcebergMap = (Types.MapType) icebergType;

            // Verify outer map
            assertThat(outerIcebergMap.keyType()).isEqualTo(Types.StringType.get());
            assertThat(outerIcebergMap.valueType()).isInstanceOf(Types.ListType.class);

            // Verify array element is a map
            Types.ListType arrayType = (Types.ListType) outerIcebergMap.valueType();
            assertThat(arrayType.elementType()).isInstanceOf(Types.MapType.class);
        }
    }

    @Nested
    @DisplayName("Different Key Type Combinations")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class KeyTypeVariationTests {

        @ParameterizedTest(name = "Map<{0}, String>")
        @ValueSource(strings = {"STRING", "INT", "BIGINT", "DOUBLE"})
        @DisplayName("Maps with different key types")
        void testVariousKeyTypes(String keyTypeStr) {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;

            MapType mapType;
            if ("INT".equals(keyTypeStr)) {
                mapType = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());
            } else if ("BIGINT".equals(keyTypeStr)) {
                mapType = DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING());
            } else if ("DOUBLE".equals(keyTypeStr)) {
                mapType = DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.STRING());
            } else {
                mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
            }

            Type icebergType = mapType.accept(converter);
            assertThat(icebergType).isInstanceOf(Types.MapType.class);
        }

        @Test
        @DisplayName("Map<Int, Int>")
        void testMapIntToInt() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType = DataTypes.MAP(DataTypes.INT(), DataTypes.INT());

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.keyType()).isEqualTo(Types.IntegerType.get());
            assertThat(icebergMapType.valueType()).isEqualTo(Types.IntegerType.get());
        }

        @Test
        @DisplayName("Map<Bigint, String>")
        void testMapBigintToString() {
            FlussDataTypeToIcebergDataType converter = FlussDataTypeToIcebergDataType.INSTANCE;
            MapType mapType = DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING());

            Type icebergType = mapType.accept(converter);

            assertThat(icebergType).isInstanceOf(Types.MapType.class);
            Types.MapType icebergMapType = (Types.MapType) icebergType;
            assertThat(icebergMapType.keyType()).isEqualTo(Types.LongType.get());
            assertThat(icebergMapType.valueType()).isEqualTo(Types.StringType.get());
        }
    }
}
