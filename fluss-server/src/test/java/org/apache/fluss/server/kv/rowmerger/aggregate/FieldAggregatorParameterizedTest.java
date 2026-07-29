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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.kv.rowmerger.AggregateRowMerger;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldBoolAndAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldBoolOrAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldFirstNonNullValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldFirstValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastNonNullValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldListaggAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldMaxAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldMinAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldProductAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldRoaringBitmap32Agg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldRoaringBitmap64Agg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldSumAgg;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.StringType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parameterized tests for all aggregation functions with different data types. */
class FieldAggregatorParameterizedTest {

    private static final short SCHEMA_ID = (short) 1;

    private BinaryValue toBinaryValue(BinaryRow row) {
        return new BinaryValue(SCHEMA_ID, row);
    }

    // ===================================================================================
    // Sum Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "sum aggregation with {0}")
    @MethodSource("sumAggregationTestData")
    void testSumAggregation(
            String typeName, DataType dataType, Object val1, Object val2, Object expected) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, val1});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, val2});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        // Verify result based on data type
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id stays the same
        assertAggregatedValue(merged.row, 1, dataType, expected);
    }

    static Stream<Arguments> sumAggregationTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 5, (byte) 3, (byte) 8),
                Arguments.of(
                        "SMALLINT", DataTypes.SMALLINT(), (short) 100, (short) 200, (short) 300),
                Arguments.of("INT", DataTypes.INT(), 1000, 2000, 3000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 10000L, 20000L, 30000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 1.5f, 2.5f, 4.0f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 10.5, 20.5, 31.0),
                Arguments.of(
                        "DECIMAL(10,2)",
                        DataTypes.DECIMAL(10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("100.50"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("200.75"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("301.25"), 10, 2)));
    }

    @ParameterizedTest(name = "sum aggregation with null values - {0}")
    @MethodSource("sumAggregationNullTestData")
    void testSumAggregationWithNull(
            String typeName, DataType dataType, Object val, Object expected) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // Test: null + value = value
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, null});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, val});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertAggregatedValue(merged.row, 1, dataType, expected);

        // Test: value + null = value
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, val});
        BinaryRow row4 = compactedRow(schema.getRowType(), new Object[] {1, null});

        BinaryValue merged2 = merger.merge(toBinaryValue(row3), toBinaryValue(row4));
        assertAggregatedValue(merged2.row, 1, dataType, expected);
    }

    static Stream<Arguments> sumAggregationNullTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 10, (byte) 10),
                Arguments.of("SMALLINT", DataTypes.SMALLINT(), (short) 100, (short) 100),
                Arguments.of("INT", DataTypes.INT(), 1000, 1000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 10000L, 10000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 5.5f, 5.5f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 10.5, 10.5));
    }

    // ===================================================================================
    // Product Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "product aggregation with {0}")
    @MethodSource("productAggregationTestData")
    void testProductAggregation(
            String typeName, DataType dataType, Object val1, Object val2, Object expected) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.PRODUCT())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, val1});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, val2});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1);
        assertAggregatedValue(merged.row, 1, dataType, expected);
    }

    static Stream<Arguments> productAggregationTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 5, (byte) 3, (byte) 15),
                Arguments.of("SMALLINT", DataTypes.SMALLINT(), (short) 10, (short) 20, (short) 200),
                Arguments.of("INT", DataTypes.INT(), 100, 200, 20000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 100L, 200L, 20000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 2.5f, 4.0f, 10.0f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 2.5, 4.0, 10.0),
                Arguments.of(
                        "DECIMAL(10,2)",
                        DataTypes.DECIMAL(10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("2.50"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("4.00"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("10.00"), 10, 2)));
    }

    // ===================================================================================
    // Max Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "max aggregation with {0}")
    @MethodSource("maxAggregationTestData")
    void testMaxAggregation(
            String typeName,
            DataType dataType,
            Object val1,
            Object val2,
            Object expectedMax,
            Object expectedMin) {
        // Test max
        Schema schemaMax =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfigMax = new TableConfig(new Configuration());

        AggregateRowMerger mergerMax = createMerger(schemaMax, tableConfigMax);

        BinaryRow row1 = compactedRow(schemaMax.getRowType(), new Object[] {1, val1});
        BinaryRow row2 = compactedRow(schemaMax.getRowType(), new Object[] {1, val2});

        BinaryValue mergedMax = mergerMax.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(mergedMax.row.getInt(0)).isEqualTo(1);
        assertAggregatedValue(mergedMax.row, 1, dataType, expectedMax);

        // Test min
        Schema schemaMin =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.MIN())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfigMin = new TableConfig(new Configuration());

        AggregateRowMerger mergerMin = createMerger(schemaMin, tableConfigMin);

        BinaryRow row3 = compactedRow(schemaMin.getRowType(), new Object[] {1, val1});
        BinaryRow row4 = compactedRow(schemaMin.getRowType(), new Object[] {1, val2});

        BinaryValue mergedMin = mergerMin.merge(toBinaryValue(row3), toBinaryValue(row4));

        assertThat(mergedMin.row.getInt(0)).isEqualTo(1);
        assertAggregatedValue(mergedMin.row, 1, dataType, expectedMin);
    }

    static Stream<Arguments> maxAggregationTestData() {
        return Stream.of(
                // Numeric types
                Arguments.of(
                        "TINYINT", DataTypes.TINYINT(), (byte) 5, (byte) 10, (byte) 10, (byte) 5),
                Arguments.of(
                        "TINYINT_NEGATIVE",
                        DataTypes.TINYINT(),
                        (byte) -5,
                        (byte) -10,
                        (byte) -5,
                        (byte) -10),
                Arguments.of(
                        "SMALLINT",
                        DataTypes.SMALLINT(),
                        (short) 100,
                        (short) 200,
                        (short) 200,
                        (short) 100),
                Arguments.of("INT", DataTypes.INT(), 1000, 2000, 2000, 1000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 10000L, 20000L, 20000L, 10000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 1.5f, 2.5f, 2.5f, 1.5f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 10.5, 20.5, 20.5, 10.5),
                Arguments.of(
                        "DECIMAL(10,2)",
                        DataTypes.DECIMAL(10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("100.50"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("200.75"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("200.75"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("100.50"), 10, 2)),
                // String type
                Arguments.of("STRING", DataTypes.STRING(), "apple", "banana", "banana", "apple"),
                Arguments.of("STRING_EMPTY", DataTypes.STRING(), "", "test", "test", ""),
                // Date and time types
                Arguments.of(
                        "DATE",
                        DataTypes.DATE(),
                        (int) LocalDate.of(2025, 1, 1).toEpochDay(),
                        (int) LocalDate.of(2025, 12, 31).toEpochDay(),
                        (int) LocalDate.of(2025, 12, 31).toEpochDay(),
                        (int) LocalDate.of(2025, 1, 1).toEpochDay()),
                Arguments.of(
                        "TIME",
                        DataTypes.TIME(),
                        (int) (LocalTime.of(10, 0, 0).toNanoOfDay() / 1_000_000),
                        (int) (LocalTime.of(14, 30, 0).toNanoOfDay() / 1_000_000),
                        (int) (LocalTime.of(14, 30, 0).toNanoOfDay() / 1_000_000),
                        (int) (LocalTime.of(10, 0, 0).toNanoOfDay() / 1_000_000)),
                Arguments.of(
                        "TIMESTAMP",
                        DataTypes.TIMESTAMP(),
                        timestampNtz("2025-01-10T12:00:00"),
                        timestampNtz("2025-12-31T23:59:59"),
                        timestampNtz("2025-12-31T23:59:59"),
                        timestampNtz("2025-01-10T12:00:00")),
                Arguments.of(
                        "TIMESTAMP_LTZ",
                        DataTypes.TIMESTAMP_LTZ(),
                        timestampLtz("2025-01-10T12:00:00"),
                        timestampLtz("2025-12-31T23:59:59"),
                        timestampLtz("2025-12-31T23:59:59"),
                        timestampLtz("2025-01-10T12:00:00")));
    }

    // ===================================================================================
    // Last Value Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "last_value with {0}")
    @MethodSource("lastValueTestData")
    void testLastValueAggregation(
            String typeName, DataType dataType, Object val1, Object val2, Object val3) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.LAST_VALUE())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, val1});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, val2});
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, val3});

        // First merge: last value should be val2
        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertAggregatedValue(merged1.row, 1, dataType, val2);

        // Second merge: last value should be val3
        BinaryValue merged2 = merger.merge(merged1, toBinaryValue(row3));
        assertAggregatedValue(merged2.row, 1, dataType, val3);
    }

    static Stream<Arguments> lastValueTestData() {
        return Stream.of(
                // Numeric types
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 1, (byte) 2, (byte) 3),
                Arguments.of("SMALLINT", DataTypes.SMALLINT(), (short) 10, (short) 20, (short) 30),
                Arguments.of("INT", DataTypes.INT(), 100, 200, 300),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 1000L, 2000L, 3000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 1.5f, 2.5f, 3.5f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 10.5, 20.5, 30.5),
                Arguments.of(
                        "DECIMAL(10,2)",
                        DataTypes.DECIMAL(10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("10.50"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("20.50"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("30.50"), 10, 2)),
                // String types
                Arguments.of("STRING", DataTypes.STRING(), "first", "second", "third"),
                Arguments.of("CHAR(10)", DataTypes.CHAR(10), "aaa", "bbb", "ccc"),
                // Boolean type
                Arguments.of("BOOLEAN", DataTypes.BOOLEAN(), true, false, true),
                // Date and time types
                Arguments.of(
                        "DATE",
                        DataTypes.DATE(),
                        (int) LocalDate.of(2025, 1, 1).toEpochDay(),
                        (int) LocalDate.of(2025, 6, 15).toEpochDay(),
                        (int) LocalDate.of(2025, 12, 31).toEpochDay()),
                Arguments.of(
                        "TIME",
                        DataTypes.TIME(),
                        (int) (LocalTime.of(10, 0, 0).toNanoOfDay() / 1_000_000),
                        (int) (LocalTime.of(12, 30, 0).toNanoOfDay() / 1_000_000),
                        (int) (LocalTime.of(15, 0, 0).toNanoOfDay() / 1_000_000)),
                Arguments.of(
                        "TIMESTAMP",
                        DataTypes.TIMESTAMP(),
                        timestampNtz("2025-01-10T12:00:00"),
                        timestampNtz("2025-06-15T13:30:00"),
                        timestampNtz("2025-12-31T23:59:59")),
                Arguments.of(
                        "TIMESTAMP_LTZ",
                        DataTypes.TIMESTAMP_LTZ(),
                        timestampLtz("2025-01-10T12:00:00"),
                        timestampLtz("2025-06-15T13:30:00"),
                        timestampLtz("2025-12-31T23:59:59")),
                // Binary types
                Arguments.of(
                        "BYTES",
                        DataTypes.BYTES(),
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5, 6},
                        new byte[] {7, 8, 9}),
                Arguments.of(
                        "BINARY(4)",
                        DataTypes.BINARY(4),
                        new byte[] {1, 2, 3, 4},
                        new byte[] {5, 6, 7, 8},
                        new byte[] {9, 10, 11, 12}));
    }

    // ===================================================================================
    // Last Non-Null Value Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "last_value_ignore_nulls with {0}")
    @MethodSource("lastNonNullValueTestData")
    void testLastNonNullValueAggregation(String typeName, DataType dataType, Object val) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.LAST_VALUE_IGNORE_NULLS())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // Test: value + null should keep value
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, val});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, null});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertAggregatedValue(merged.row, 1, dataType, val);
    }

    static Stream<Arguments> lastNonNullValueTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 10),
                Arguments.of("SMALLINT", DataTypes.SMALLINT(), (short) 100),
                Arguments.of("INT", DataTypes.INT(), 1000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 10000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 5.5f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 10.5),
                Arguments.of("STRING", DataTypes.STRING(), "test"),
                Arguments.of("BOOLEAN", DataTypes.BOOLEAN(), true),
                Arguments.of(
                        "TIMESTAMP", DataTypes.TIMESTAMP(), timestampNtz("2025-01-10T12:00:00")));
    }

    // ===================================================================================
    // First Value Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "first_value with {0}")
    @MethodSource("firstValueTestData")
    void testFirstValueAggregation(String typeName, DataType dataType, Object val1, Object val2) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.FIRST_VALUE())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, val1});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, val2});

        // First value should always be val1
        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertAggregatedValue(merged.row, 1, dataType, val1);
    }

    static Stream<Arguments> firstValueTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 10, (byte) 20),
                Arguments.of("SMALLINT", DataTypes.SMALLINT(), (short) 100, (short) 200),
                Arguments.of("INT", DataTypes.INT(), 1000, 2000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 10000L, 20000L),
                Arguments.of("STRING", DataTypes.STRING(), "first", "second"),
                Arguments.of("BOOLEAN", DataTypes.BOOLEAN(), true, false));
    }

    // ===================================================================================
    // First Non-Null Value Aggregation Tests
    // ===================================================================================

    @ParameterizedTest(name = "first_value_ignore_nulls with {0}")
    @MethodSource("firstNonNullValueTestData")
    void testFirstNonNullValueAggregation(String typeName, DataType dataType, Object val) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", dataType, AggFunctions.FIRST_VALUE_IGNORE_NULLS())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // Test: null + value should keep value
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, null});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, val});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertAggregatedValue(merged.row, 1, dataType, val);

        // Test: value + non-null should keep first value
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, val});
        BinaryRow row4 = compactedRow(schema.getRowType(), new Object[] {1, val});

        BinaryValue merged2 = merger.merge(toBinaryValue(row3), toBinaryValue(row4));
        assertAggregatedValue(merged2.row, 1, dataType, val);
    }

    static Stream<Arguments> firstNonNullValueTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 10),
                Arguments.of("SMALLINT", DataTypes.SMALLINT(), (short) 100),
                Arguments.of("INT", DataTypes.INT(), 1000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 10000L),
                Arguments.of("STRING", DataTypes.STRING(), "test"));
    }

    // ===================================================================================
    // Boolean Aggregation Tests
    // ===================================================================================

    @Test
    void testBoolAndAggregation() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BOOLEAN(), AggFunctions.BOOL_AND())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // true AND true = true
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged1.row.getBoolean(1)).isTrue();

        // true AND false = false
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryRow row4 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryValue merged2 = merger.merge(toBinaryValue(row3), toBinaryValue(row4));
        assertThat(merged2.row.getBoolean(1)).isFalse();

        // false AND false = false
        BinaryRow row5 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryRow row6 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryValue merged3 = merger.merge(toBinaryValue(row5), toBinaryValue(row6));
        assertThat(merged3.row.getBoolean(1)).isFalse();
    }

    @Test
    void testBoolOrAggregation() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BOOLEAN(), AggFunctions.BOOL_OR())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // false OR false = false
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged1.row.getBoolean(1)).isFalse();

        // true OR false = true
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryRow row4 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryValue merged2 = merger.merge(toBinaryValue(row3), toBinaryValue(row4));
        assertThat(merged2.row.getBoolean(1)).isTrue();

        // true OR true = true
        BinaryRow row5 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryRow row6 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryValue merged3 = merger.merge(toBinaryValue(row5), toBinaryValue(row6));
        assertThat(merged3.row.getBoolean(1)).isTrue();
    }

    @Test
    void testBoolAndWithNull() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BOOLEAN(), AggFunctions.BOOL_AND())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // null AND true = true
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, null});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, true});
        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged.row.getBoolean(1)).isTrue();
    }

    @Test
    void testBoolOrWithNull() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BOOLEAN(), AggFunctions.BOOL_OR())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // null OR false = false
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, null});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, false});
        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged.row.getBoolean(1)).isFalse();
    }

    // ===================================================================================
    // Listagg Aggregation Tests
    // ===================================================================================

    @Test
    void testListaggAggregation() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING(), AggFunctions.LISTAGG())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // Test basic concatenation with default delimiter (comma)
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, "apple"});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, "banana"});
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, "cherry"});

        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged1.row.getString(1).toString()).isEqualTo("apple,banana");

        BinaryValue merged2 = merger.merge(merged1, toBinaryValue(row3));
        assertThat(merged2.row.getString(1).toString()).isEqualTo("apple,banana,cherry");
    }

    @Test
    void testListaggWithCustomDelimiter() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING(), AggFunctions.LISTAGG("|"))
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, "a"});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, "b"});
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, "c"});

        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged1.row.getString(1).toString()).isEqualTo("a|b");

        BinaryValue merged2 = merger.merge(merged1, toBinaryValue(row3));
        assertThat(merged2.row.getString(1).toString()).isEqualTo("a|b|c");
    }

    @Test
    void testListaggWithNull() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING(), AggFunctions.LISTAGG())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // Test null handling: null values should be skipped
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, "a"});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, null});
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, "b"});

        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged1.row.getString(1).toString()).isEqualTo("a");

        BinaryValue merged2 = merger.merge(merged1, toBinaryValue(row3));
        assertThat(merged2.row.getString(1).toString()).isEqualTo("a,b");
    }

    // ===================================================================================
    // Roaring Bitmap Aggregation Tests
    // ===================================================================================

    @Test
    void testRbm32Aggregation() throws IOException {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BYTES(), AggFunctions.RBM32())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(schema, tableConfig);

        RoaringBitmap bitmap1 = new RoaringBitmap();
        bitmap1.add(1);
        bitmap1.add(2);
        RoaringBitmap bitmap2 = new RoaringBitmap();
        bitmap2.add(2);
        bitmap2.add(3);

        BinaryRow row1 =
                compactedRow(
                        schema.getRowType(),
                        new Object[] {1, RoaringBitmapUtils.serializeRoaringBitmap32(bitmap1)});
        BinaryRow row2 =
                compactedRow(
                        schema.getRowType(),
                        new Object[] {1, RoaringBitmapUtils.serializeRoaringBitmap32(bitmap2)});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        RoaringBitmap expected = bitmap1.clone();
        expected.or(bitmap2);
        byte[] expectedBytes = RoaringBitmapUtils.serializeRoaringBitmap32(expected);

        assertThat(merged.row.getBinary(1, expectedBytes.length)).isEqualTo(expectedBytes);
    }

    @Test
    void testRbm64Aggregation() throws IOException {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BYTES(), AggFunctions.RBM64())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(schema, tableConfig);

        Roaring64Bitmap bitmap1 = new Roaring64Bitmap();
        bitmap1.add(10L);
        bitmap1.add(20L);
        Roaring64Bitmap bitmap2 = new Roaring64Bitmap();
        bitmap2.add(20L);
        bitmap2.add(30L);

        BinaryRow row1 =
                compactedRow(
                        schema.getRowType(),
                        new Object[] {1, RoaringBitmapUtils.serializeRoaringBitmap64(bitmap1)});
        BinaryRow row2 =
                compactedRow(
                        schema.getRowType(),
                        new Object[] {1, RoaringBitmapUtils.serializeRoaringBitmap64(bitmap2)});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        Roaring64Bitmap expected = new Roaring64Bitmap();
        expected.or(bitmap1);
        expected.or(bitmap2);
        byte[] expectedBytes = RoaringBitmapUtils.serializeRoaringBitmap64(expected);

        assertThat(merged.row.getBinary(1, expectedBytes.length)).isEqualTo(expectedBytes);
    }

    // ===================================================================================
    // Retract (Sum Subtraction) Tests
    // ===================================================================================

    @ParameterizedTest(name = "sum retract with {0}")
    @MethodSource("sumRetractTestData")
    void testSumRetract(
            String typeName,
            DataType dataType,
            Object accumulator,
            Object retractVal,
            Object expected) {
        FieldSumAgg sumAgg = new FieldSumAgg(dataType);
        Object result = sumAgg.retract(accumulator, retractVal);
        assertRetractResult(result, expected);
    }

    static Stream<Arguments> sumRetractTestData() {
        return Stream.of(
                Arguments.of("TINYINT", DataTypes.TINYINT(), (byte) 10, (byte) 3, (byte) 7),
                Arguments.of(
                        "SMALLINT", DataTypes.SMALLINT(), (short) 300, (short) 100, (short) 200),
                Arguments.of("INT", DataTypes.INT(), 3000, 1000, 2000),
                Arguments.of("BIGINT", DataTypes.BIGINT(), 30000L, 10000L, 20000L),
                Arguments.of("FLOAT", DataTypes.FLOAT(), 4.0f, 1.5f, 2.5f),
                Arguments.of("DOUBLE", DataTypes.DOUBLE(), 31.0, 10.5, 20.5),
                Arguments.of(
                        "DECIMAL(10,2)",
                        DataTypes.DECIMAL(10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("301.25"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("100.50"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("200.75"), 10, 2)),
                // Negative result cases: retract value exceeds accumulator
                Arguments.of("INT_negative", DataTypes.INT(), 5, 10, -5),
                Arguments.of("BIGINT_negative", DataTypes.BIGINT(), 5L, 10L, -5L));
    }

    @ParameterizedTest(name = "sum retract overflow with {0}")
    @MethodSource("sumRetractOverflowTestData")
    void testSumRetractOverflow(
            String typeName, DataType dataType, Object accumulator, Object retractVal) {
        FieldSumAgg sumAgg = new FieldSumAgg(dataType);
        assertThatThrownBy(() -> sumAgg.retract(accumulator, retractVal))
                .isInstanceOf(ArithmeticException.class);
    }

    static Stream<Arguments> sumRetractOverflowTestData() {
        return Stream.of(
                // Underflow cases
                Arguments.of("TINYINT_underflow", DataTypes.TINYINT(), Byte.MIN_VALUE, (byte) 1),
                Arguments.of(
                        "SMALLINT_underflow", DataTypes.SMALLINT(), Short.MIN_VALUE, (short) 1),
                Arguments.of("INT_underflow", DataTypes.INT(), Integer.MIN_VALUE, 1),
                Arguments.of("BIGINT_underflow", DataTypes.BIGINT(), Long.MIN_VALUE, 1L),
                // Overflow cases (subtracting a negative value)
                Arguments.of("TINYINT_overflow", DataTypes.TINYINT(), Byte.MAX_VALUE, (byte) -1),
                Arguments.of(
                        "SMALLINT_overflow", DataTypes.SMALLINT(), Short.MAX_VALUE, (short) -1),
                Arguments.of("INT_overflow", DataTypes.INT(), Integer.MAX_VALUE, -1),
                Arguments.of("BIGINT_overflow", DataTypes.BIGINT(), Long.MAX_VALUE, -1L));
    }

    @ParameterizedTest(name = "sum retract with null - {0}")
    @MethodSource("sumRetractNullTestData")
    void testSumRetractWithNull(
            String typeName,
            DataType dataType,
            Object accumulator,
            Object retractVal,
            Object expected) {
        FieldSumAgg sumAgg = new FieldSumAgg(dataType);
        Object result = sumAgg.retract(accumulator, retractVal);
        assertThat(result).isEqualTo(expected);
    }

    static Stream<Arguments> sumRetractNullTestData() {
        return Stream.of(
                // null accumulator, non-null retract -> null (cannot subtract from nothing)
                Arguments.of("BIGINT_null_acc", DataTypes.BIGINT(), null, 10L, null),
                // non-null accumulator, null retract -> accumulator unchanged
                Arguments.of("BIGINT_null_retract", DataTypes.BIGINT(), 10L, null, 10L),
                // both null -> null
                Arguments.of("BIGINT_both_null", DataTypes.BIGINT(), null, null, null));
    }

    // ===================================================================================
    // Retract (Last Value) Tests
    // ===================================================================================

    @ParameterizedTest(name = "last_value retract - {0}")
    @MethodSource("lastValueRetractTestData")
    void testLastValueRetract(
            String caseName, Object accumulator, Object retractVal, Object expected) {
        FieldLastValueAgg agg = new FieldLastValueAgg(DataTypes.INT());
        Object result = agg.retract(accumulator, retractVal);
        assertThat(result).isEqualTo(expected);
    }

    static Stream<Arguments> lastValueRetractTestData() {
        return Stream.of(
                // last_value retract always returns null regardless of inputs
                Arguments.of("non_null_acc_non_null_retract", 10, 5, null),
                Arguments.of("non_null_acc_null_retract", 10, null, null),
                Arguments.of("null_acc_non_null_retract", null, 5, null),
                Arguments.of("both_null", null, null, null));
    }

    @ParameterizedTest(name = "last_value_ignore_nulls retract - {0}")
    @MethodSource("lastNonNullValueRetractTestData")
    void testLastNonNullValueRetract(
            String caseName, Object accumulator, Object retractVal, Object expected) {
        FieldLastNonNullValueAgg agg = new FieldLastNonNullValueAgg(DataTypes.INT());
        Object result = agg.retract(accumulator, retractVal);
        assertThat(result).isEqualTo(expected);
    }

    static Stream<Arguments> lastNonNullValueRetractTestData() {
        return Stream.of(
                // non-null retract -> null (retract the value)
                Arguments.of("non_null_acc_non_null_retract", 10, 5, null),
                // null retract -> accumulator unchanged
                Arguments.of("non_null_acc_null_retract", 10, null, 10),
                // null accumulator, non-null retract -> null
                Arguments.of("null_acc_non_null_retract", null, 5, null),
                // both null -> null (accumulator is null, retract is null -> keep accumulator)
                Arguments.of("both_null", null, null, null));
    }

    @ParameterizedTest(name = "non-retractable function throws: {0}")
    @MethodSource("nonRetractableAggregators")
    void testNonRetractableFunctionThrows(
            String name, FieldAggregator agg, Object acc, Object val) {
        assertThatThrownBy(() -> agg.retract(acc, val))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support retract");
    }

    static Stream<Arguments> nonRetractableAggregators() {
        return Stream.of(
                Arguments.of("FieldMaxAgg", new FieldMaxAgg(DataTypes.INT()), 10, 5),
                Arguments.of("FieldMinAgg", new FieldMinAgg(DataTypes.INT()), 10, 5),
                Arguments.of("FieldFirstValueAgg", new FieldFirstValueAgg(DataTypes.INT()), 10, 5),
                Arguments.of(
                        "FieldFirstNonNullValueAgg",
                        new FieldFirstNonNullValueAgg(DataTypes.INT()),
                        10,
                        5),
                Arguments.of("FieldProductAgg", new FieldProductAgg(DataTypes.INT()), 10, 5),
                Arguments.of(
                        "FieldListaggAgg", new FieldListaggAgg(new StringType(), ","), "a,b", "a"),
                Arguments.of(
                        "FieldBoolAndAgg", new FieldBoolAndAgg(new BooleanType()), true, false),
                Arguments.of("FieldBoolOrAgg", new FieldBoolOrAgg(new BooleanType()), true, false),
                Arguments.of(
                        "FieldRoaringBitmap32Agg",
                        new FieldRoaringBitmap32Agg(DataTypes.BYTES()),
                        new byte[] {1, 2},
                        new byte[] {3, 4}),
                Arguments.of(
                        "FieldRoaringBitmap64Agg",
                        new FieldRoaringBitmap64Agg(DataTypes.BYTES()),
                        new byte[] {1, 2},
                        new byte[] {3, 4}));
    }

    // ===================================================================================
    // Helper Methods
    // ===================================================================================

    private AggregateRowMerger createMerger(Schema schema, TableConfig tableConfig) {
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, (short) 1));
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, (short) 1, schema);
        return merger;
    }

    private static void assertRetractResult(Object result, Object expected) {
        if (expected instanceof Decimal) {
            assertThat(((Decimal) result).toBigDecimal())
                    .isEqualByComparingTo(((Decimal) expected).toBigDecimal());
        } else {
            assertThat(result).isEqualTo(expected);
        }
    }

    private void assertAggregatedValue(BinaryRow row, int pos, DataType dataType, Object expected) {
        if (expected == null) {
            assertThat(row.isNullAt(pos)).isTrue();
            return;
        }

        String typeName = dataType.getTypeRoot().name();
        switch (typeName) {
            case "TINYINT":
                assertThat(row.getByte(pos)).isEqualTo((Byte) expected);
                break;
            case "SMALLINT":
                assertThat(row.getShort(pos)).isEqualTo((Short) expected);
                break;
            case "INTEGER":
                assertThat(row.getInt(pos)).isEqualTo((Integer) expected);
                break;
            case "BIGINT":
                assertThat(row.getLong(pos)).isEqualTo((Long) expected);
                break;
            case "FLOAT":
                assertThat(row.getFloat(pos)).isEqualTo((Float) expected);
                break;
            case "DOUBLE":
                assertThat(row.getDouble(pos)).isEqualTo((Double) expected);
                break;
            case "DECIMAL":
                int precision = DataTypeChecks.getPrecision(dataType);
                int scale = DataTypeChecks.getScale(dataType);
                Decimal actualDecimal = row.getDecimal(pos, precision, scale);
                Decimal expectedDecimal = (Decimal) expected;
                assertThat(actualDecimal.toBigDecimal())
                        .isEqualByComparingTo(expectedDecimal.toBigDecimal());
                break;
            case "BOOLEAN":
                assertThat(row.getBoolean(pos)).isEqualTo((Boolean) expected);
                break;
            case "STRING":
            case "VARCHAR":
                assertThat(row.getString(pos).toString()).isEqualTo((String) expected);
                break;
            case "CHAR":
                // CHAR type is padded with spaces, need to trim
                assertThat(row.getString(pos).toString().trim()).isEqualTo((String) expected);
                break;
            case "DATE":
                assertThat(row.getInt(pos)).isEqualTo((Integer) expected);
                break;
            case "TIME_WITHOUT_TIME_ZONE":
                assertThat(row.getInt(pos)).isEqualTo((Integer) expected);
                break;
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                int tsPrecision = DataTypeChecks.getPrecision(dataType);
                TimestampNtz actualTs = row.getTimestampNtz(pos, tsPrecision);
                TimestampNtz expectedTs = (TimestampNtz) expected;
                assertThat(actualTs.getMillisecond()).isEqualTo(expectedTs.getMillisecond());
                assertThat(actualTs.getNanoOfMillisecond())
                        .isEqualTo(expectedTs.getNanoOfMillisecond());
                break;
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                int tsLtzPrecision = DataTypeChecks.getPrecision(dataType);
                TimestampLtz actualTsLtz = row.getTimestampLtz(pos, tsLtzPrecision);
                TimestampLtz expectedTsLtz = (TimestampLtz) expected;
                assertThat(actualTsLtz.getEpochMillisecond())
                        .isEqualTo(expectedTsLtz.getEpochMillisecond());
                assertThat(actualTsLtz.getNanoOfMillisecond())
                        .isEqualTo(expectedTsLtz.getNanoOfMillisecond());
                break;
            case "BYTES":
            case "VARBINARY":
            case "BINARY":
                byte[] expectedBytes = (byte[]) expected;
                assertThat(row.getBinary(pos, expectedBytes.length)).isEqualTo(expectedBytes);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type for assertion: " + typeName);
        }
    }

    private static TimestampNtz timestampNtz(String timestamp) {
        return TimestampNtz.fromLocalDateTime(LocalDateTime.parse(timestamp));
    }

    private static TimestampLtz timestampLtz(String timestamp) {
        Instant instant = LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC);
        return TimestampLtz.fromInstant(instant);
    }

    // ===================================================================================
    // Retract Overflow / Boundary Tests (M10)
    // ===================================================================================

    @Test
    void testSumRetractFloatInfinity() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("val", DataTypes.FLOAT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        AggregateRowMerger merger = createMerger(schema, new TableConfig(new Configuration()));

        // Retract Float.MAX_VALUE from -Float.MAX_VALUE → should produce -Infinity
        BinaryRow oldRow = compactedRow(schema.getRowType(), new Object[] {1, -Float.MAX_VALUE});
        BinaryRow retractRow = compactedRow(schema.getRowType(), new Object[] {1, Float.MAX_VALUE});

        BinaryValue result = merger.retract(toBinaryValue(oldRow), toBinaryValue(retractRow));
        assertThat(result).isNotNull();
        assertThat(Float.isInfinite(result.row.getFloat(1))).isTrue();
    }

    @Test
    void testSumRetractDoubleInfinity() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("val", DataTypes.DOUBLE(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        AggregateRowMerger merger = createMerger(schema, new TableConfig(new Configuration()));

        // Retract Double.MAX_VALUE from -Double.MAX_VALUE → should produce -Infinity
        BinaryRow oldRow = compactedRow(schema.getRowType(), new Object[] {1, -Double.MAX_VALUE});
        BinaryRow retractRow =
                compactedRow(schema.getRowType(), new Object[] {1, Double.MAX_VALUE});

        BinaryValue result = merger.retract(toBinaryValue(oldRow), toBinaryValue(retractRow));
        assertThat(result).isNotNull();
        assertThat(Double.isInfinite(result.row.getDouble(1))).isTrue();
    }

    @Test
    void testSumRetractDecimalOverflow() {
        // DECIMAL(10,2): max is 99999999.99
        DataType decimalType = DataTypes.DECIMAL(10, 2);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("val", decimalType, AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        AggregateRowMerger merger = createMerger(schema, new TableConfig(new Configuration()));

        // old = 99999999.99, retract = -99999999.99 → result would be 199999999.98 which overflows
        // DECIMAL(10,2). The behavior depends on the Decimal implementation — verify it doesn't
        // throw an unhandled exception.
        Decimal maxDecimal = Decimal.fromBigDecimal(new BigDecimal("99999999.99"), 10, 2);
        Decimal negMaxDecimal = Decimal.fromBigDecimal(new BigDecimal("-99999999.99"), 10, 2);

        BinaryRow oldRow = compactedRow(schema.getRowType(), new Object[] {1, maxDecimal});
        BinaryRow retractRow = compactedRow(schema.getRowType(), new Object[] {1, negMaxDecimal});

        // This should either produce a result or handle overflow gracefully
        BinaryValue result = merger.retract(toBinaryValue(oldRow), toBinaryValue(retractRow));
        assertThat(result).isNotNull();
        // The result decimal may be null (overflow) or a valid value
        // Just verify no unhandled exception is thrown
    }
}
