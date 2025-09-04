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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogRecordBatchStatisticsCollector}. */
public class LogRecordBatchStatisticsCollectorTest {

    private LogRecordBatchStatisticsCollector collector;
    private RowType testRowType;

    // Test data for different data types
    private static final List<Object[]> MIXED_TYPE_DATA =
            Arrays.asList(
                    new Object[] {1, "a", 10.5, true, 100L, 1.23f},
                    new Object[] {2, "b", 20.3, false, 200L, 2.34f},
                    new Object[] {3, "c", 15.7, true, 150L, 3.45f},
                    new Object[] {4, "d", 8.9, false, 300L, 4.56f},
                    new Object[] {5, "e", 30.1, true, 250L, 5.67f});

    private static final RowType MIXED_TYPE_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()),
                    new DataField("flag", DataTypes.BOOLEAN()),
                    new DataField("bigint_val", DataTypes.BIGINT()),
                    new DataField("float_val", DataTypes.FLOAT()));

    // Helper method to create stats index mapping for all columns
    private static int[] createAllColumnsStatsMapping(RowType rowType) {
        int[] statsIndexMapping = new int[rowType.getFieldCount()];
        for (int i = 0; i < statsIndexMapping.length; i++) {
            statsIndexMapping[i] = i;
        }
        return statsIndexMapping;
    }

    @BeforeEach
    void setUp() {
        testRowType = MIXED_TYPE_ROW_TYPE;
        collector =
                new LogRecordBatchStatisticsCollector(
                        testRowType, createAllColumnsStatsMapping(testRowType));
    }

    @Test
    void testConstructor() {
        assertThat(collector.getRowType()).isEqualTo(testRowType);
        assertThat(collector.getRowType().getFieldCount()).isEqualTo(6);
    }

    @Test
    void testProcessRowBasic() throws IOException {
        // Process single row
        Object[] singleData = MIXED_TYPE_DATA.get(0);
        InternalRow row = DataTestUtils.row(singleData);
        collector.processRow(row);

        // Should be able to write statistics after processing a row
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));
        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testProcessMultipleRows() throws IOException {
        // Process multiple rows
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Should be able to write statistics after processing multiple rows
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));
        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testWriteStatisticsWithNoRows() throws IOException {
        // Don't process any rows
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));

        // Even with no rows, statistics structure is still written
        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testReset() throws IOException {
        // Process some rows
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write statistics before reset
        MemorySegment segment1 = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten1 = collector.writeStatistics(new MemorySegmentOutputView(segment1));
        assertThat(bytesWritten1).isGreaterThan(0);

        // Reset collector
        collector.reset();

        // Write statistics after reset
        MemorySegment segment2 = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten2 = collector.writeStatistics(new MemorySegmentOutputView(segment2));

        // Even after reset, statistics structure is still written
        assertThat(bytesWritten2).isGreaterThan(0);
    }

    @Test
    void testResetAndReprocess() throws IOException {
        // Process initial data
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write first statistics
        MemorySegment segment1 = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten1 = collector.writeStatistics(new MemorySegmentOutputView(segment1));
        assertThat(bytesWritten1).isGreaterThan(0);

        // Reset and process new data with same row type
        collector.reset();
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write second statistics
        MemorySegment segment2 = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten2 = collector.writeStatistics(new MemorySegmentOutputView(segment2));
        assertThat(bytesWritten2).isGreaterThan(0);
    }

    @Test
    void testProcessDifferentDataTypes() throws IOException {
        // Test different types of data
        RowType simpleRowType =
                DataTypes.ROW(
                        new DataField("boolean_val", DataTypes.BOOLEAN()),
                        new DataField("int_val", DataTypes.INT()),
                        new DataField("string_val", DataTypes.STRING()));

        LogRecordBatchStatisticsCollector typeCollector =
                new LogRecordBatchStatisticsCollector(
                        simpleRowType, createAllColumnsStatsMapping(simpleRowType));

        // Test data for different types
        Object[] testData = {true, 42, "hello"};

        InternalRow row = DataTestUtils.row(testData);
        typeCollector.processRow(row);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = typeCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testProcessNullValues() throws IOException {
        // Test data with null values
        RowType nullableRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()));

        LogRecordBatchStatisticsCollector nullCollector =
                new LogRecordBatchStatisticsCollector(
                        nullableRowType, createAllColumnsStatsMapping(nullableRowType));

        // Process rows with nulls
        List<Object[]> dataWithNulls =
                Arrays.asList(
                        new Object[] {1, "a"}, new Object[] {null, "b"}, new Object[] {3, null});

        for (Object[] data : dataWithNulls) {
            InternalRow row = DataTestUtils.row(data);
            nullCollector.processRow(row);
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = nullCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testPartialStatsIndexMapping() throws IOException {
        // Test with partial stats collection (only collect stats for some columns)
        RowType partialRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        // Only collect stats for columns 0 and 2 (skip column 1)
        int[] partialStatsMapping = new int[] {0, 2};

        LogRecordBatchStatisticsCollector partialCollector =
                new LogRecordBatchStatisticsCollector(partialRowType, partialStatsMapping);

        // Process some rows
        List<Object[]> partialData =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {2, "b", 20.3},
                        new Object[] {3, "c", 15.7});

        for (Object[] data : partialData) {
            InternalRow row = DataTestUtils.row(data);
            partialCollector.processRow(row);
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = partialCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testLargeDataset() throws IOException {
        // Create a new collector for this test to avoid interference
        LogRecordBatchStatisticsCollector largeDatasetCollector =
                new LogRecordBatchStatisticsCollector(
                        testRowType, createAllColumnsStatsMapping(testRowType));

        // Process large dataset
        for (int i = 0; i < 1000; i++) {
            Object[] data = {i, "row" + i, (double) i, i % 2 == 0, (long) i, (float) i};
            InternalRow row = DataTestUtils.row(data);
            largeDatasetCollector.processRow(row);
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten =
                largeDatasetCollector.writeStatistics(new MemorySegmentOutputView(segment));

        // Should write statistics for large dataset
        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testSingleColumnCollection() throws IOException {
        // Test collecting statistics for just one column
        RowType singleColRowType = DataTypes.ROW(new DataField("value", DataTypes.INT()));
        int[] singleColMapping = new int[] {0};

        LogRecordBatchStatisticsCollector singleColCollector =
                new LogRecordBatchStatisticsCollector(singleColRowType, singleColMapping);

        // Process just 3 rows with clear min/max
        singleColCollector.processRow(DataTestUtils.row(new Object[] {1}));
        singleColCollector.processRow(DataTestUtils.row(new Object[] {3}));
        singleColCollector.processRow(DataTestUtils.row(new Object[] {2}));

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = singleColCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testStatisticsContentForSingleRow() throws IOException {
        // Test with single row to verify min/max values are correctly collected
        Object[] singleData = {42, "test", 3.14, true, 1000L, 2.5f};
        InternalRow row = DataTestUtils.row(singleData);
        collector.processRow(row);

        // Write and parse statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int bytesWritten = collector.writeStatistics(outputView);
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse statistics from written data
        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, testRowType, 1);
        assertThat(statistics).isNotNull();

        // Verify statistics content
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        Long[] nullCounts = statistics.getNullCounts();

        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(nullCounts).isNotNull();

        // Verify values for each field - since it's a single row, min = max
        assertThat(minValues.getInt(0)).isEqualTo(42); // id
        assertThat(maxValues.getInt(0)).isEqualTo(42); // id
        assertThat(nullCounts[0]).isEqualTo(0L); // no nulls

        assertThat(minValues.getString(1)).isEqualTo(BinaryString.fromString("test")); // name
        assertThat(maxValues.getString(1)).isEqualTo(BinaryString.fromString("test")); // name
        assertThat(nullCounts[1]).isEqualTo(0L); // no nulls

        assertThat(minValues.getDouble(2)).isEqualTo(3.14); // value
        assertThat(maxValues.getDouble(2)).isEqualTo(3.14); // value
        assertThat(nullCounts[2]).isEqualTo(0L); // no nulls

        assertThat(minValues.getBoolean(3)).isEqualTo(true); // flag
        assertThat(maxValues.getBoolean(3)).isEqualTo(true); // flag
        assertThat(nullCounts[3]).isEqualTo(0L); // no nulls

        assertThat(minValues.getLong(4)).isEqualTo(1000L); // bigint_val
        assertThat(maxValues.getLong(4)).isEqualTo(1000L); // bigint_val
        assertThat(nullCounts[4]).isEqualTo(0L); // no nulls

        assertThat(minValues.getFloat(5)).isEqualTo(2.5f); // float_val
        assertThat(maxValues.getFloat(5)).isEqualTo(2.5f); // float_val
        assertThat(nullCounts[5]).isEqualTo(0L); // no nulls
    }

    @Test
    void testStatisticsContentForMultipleRows() throws IOException {
        // Process multiple rows with known min/max values
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write and parse statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int bytesWritten = collector.writeStatistics(outputView);
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse statistics from written data
        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, testRowType, 1);
        assertThat(statistics).isNotNull();

        // Verify statistics content
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        Long[] nullCounts = statistics.getNullCounts();

        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(nullCounts).isNotNull();

        // Verify expected min/max values based on MIXED_TYPE_DATA
        assertThat(minValues.getInt(0)).isEqualTo(1); // min id
        assertThat(maxValues.getInt(0)).isEqualTo(5); // max id
        assertThat(nullCounts[0]).isEqualTo(0L); // no nulls

        assertThat(minValues.getString(1)).isEqualTo(BinaryString.fromString("a")); // min name
        assertThat(maxValues.getString(1)).isEqualTo(BinaryString.fromString("e")); // max name
        assertThat(nullCounts[1]).isEqualTo(0L); // no nulls

        assertThat(minValues.getDouble(2)).isEqualTo(8.9); // min value
        assertThat(maxValues.getDouble(2)).isEqualTo(30.1); // max value
        assertThat(nullCounts[2]).isEqualTo(0L); // no nulls

        assertThat(minValues.getBoolean(3)).isEqualTo(false); // min flag
        assertThat(maxValues.getBoolean(3)).isEqualTo(true); // max flag
        assertThat(nullCounts[3]).isEqualTo(0L); // no nulls

        assertThat(minValues.getLong(4)).isEqualTo(100L); // min bigint_val
        assertThat(maxValues.getLong(4)).isEqualTo(300L); // max bigint_val
        assertThat(nullCounts[4]).isEqualTo(0L); // no nulls

        assertThat(minValues.getFloat(5)).isEqualTo(1.23f); // min float_val
        assertThat(maxValues.getFloat(5)).isEqualTo(5.67f); // max float_val
        assertThat(nullCounts[5]).isEqualTo(0L); // no nulls
    }

    @Test
    void testStatisticsContentWithNullValues() throws IOException {
        // Create test data with null values
        RowType nullableRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsCollector nullCollector =
                new LogRecordBatchStatisticsCollector(
                        nullableRowType, createAllColumnsStatsMapping(nullableRowType));

        // Process rows with nulls - mix of data and nulls
        List<Object[]> dataWithNulls =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {null, "b", 20.3},
                        new Object[] {3, null, 15.7},
                        new Object[] {2, "c", null});

        for (Object[] data : dataWithNulls) {
            InternalRow row = DataTestUtils.row(data);
            nullCollector.processRow(row);
        }

        // Write and parse statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int bytesWritten = nullCollector.writeStatistics(outputView);
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse statistics from written data
        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, nullableRowType, 1);
        assertThat(statistics).isNotNull();

        // Verify statistics content
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        Long[] nullCounts = statistics.getNullCounts();

        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(nullCounts).isNotNull();

        // Verify null counts
        assertThat(nullCounts[0]).isEqualTo(1L); // one null in id field
        assertThat(nullCounts[1]).isEqualTo(1L); // one null in name field
        assertThat(nullCounts[2]).isEqualTo(1L); // one null in value field

        // Verify min/max values (should exclude nulls)
        assertThat(minValues.getInt(0)).isEqualTo(1); // min id (excluding null)
        assertThat(maxValues.getInt(0)).isEqualTo(3); // max id (excluding null)

        assertThat(minValues.getString(1))
                .isEqualTo(BinaryString.fromString("a")); // min name (excluding null)
        assertThat(maxValues.getString(1))
                .isEqualTo(BinaryString.fromString("c")); // max name (excluding null)

        assertThat(minValues.getDouble(2)).isEqualTo(10.5); // min value (excluding null)
        assertThat(maxValues.getDouble(2)).isEqualTo(20.3); // max value (excluding null)
    }

    @Test
    void testStatisticsContentWithPartialStatsMapping() throws IOException {
        // Test with partial stats collection (only collect stats for some columns)
        RowType fullRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        // Only collect stats for columns 0 and 2 (skip column 1)
        int[] partialStatsMapping = new int[] {0, 2};

        LogRecordBatchStatisticsCollector partialCollector =
                new LogRecordBatchStatisticsCollector(fullRowType, partialStatsMapping);

        // Process some rows
        List<Object[]> originalRowData =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {5, "b", 5.3},
                        new Object[] {3, "c", 15.7});

        for (Object[] data : originalRowData) {
            InternalRow row = DataTestUtils.row(data);
            partialCollector.processRow(row);
        }

        // Write and parse statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int bytesWritten = partialCollector.writeStatistics(outputView);
        assertThat(bytesWritten).isGreaterThan(0);

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, fullRowType, 1);
        assertThat(statistics).isNotNull();

        // Verify statistics content
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        Long[] nullCounts = statistics.getNullCounts();

        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(nullCounts).isNotNull();

        // Only 2 columns should have statistics
        assertThat(nullCounts.length).isEqualTo(3);

        // Verify statistics for collected columns
        assertThat(minValues.getInt(0)).isEqualTo(1); // min id from column 0
        assertThat(maxValues.getInt(0)).isEqualTo(5); // max id from column 0
        assertThat(nullCounts[0]).isEqualTo(0L); // no nulls

        assertThat(minValues.getDouble(2)).isEqualTo(5.3); // min value from column 2
        assertThat(maxValues.getDouble(2)).isEqualTo(15.7); // max value from column 2
        assertThat(nullCounts[2]).isEqualTo(0L); // no nulls
    }

    @Test
    void testStatisticsContentAfterReset() throws IOException {
        // Process initial data
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Reset collector
        collector.reset();

        // Process new data with different values
        List<Object[]> newData =
                Arrays.asList(
                        new Object[] {10, "x", 100.0, false, 500L, 8.9f},
                        new Object[] {15, "y", 200.0, true, 600L, 9.1f});

        for (Object[] data : newData) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write and parse statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int bytesWritten = collector.writeStatistics(outputView);
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse statistics from written data
        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, testRowType, 1);
        assertThat(statistics).isNotNull();

        // Verify statistics content reflects the new data only
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        Long[] nullCounts = statistics.getNullCounts();

        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(nullCounts).isNotNull();

        // Verify values from new data (should not contain old data after reset)
        assertThat(minValues.getInt(0)).isEqualTo(10); // min id from new data
        assertThat(maxValues.getInt(0)).isEqualTo(15); // max id from new data
        assertThat(nullCounts[0]).isEqualTo(0L); // no nulls

        assertThat(minValues.getString(1))
                .isEqualTo(BinaryString.fromString("x")); // min name from new data
        assertThat(maxValues.getString(1))
                .isEqualTo(BinaryString.fromString("y")); // max name from new data
        assertThat(nullCounts[1]).isEqualTo(0L); // no nulls

        assertThat(minValues.getDouble(2)).isEqualTo(100.0); // min value from new data
        assertThat(maxValues.getDouble(2)).isEqualTo(200.0); // max value from new data
        assertThat(nullCounts[2]).isEqualTo(0L); // no nulls

        assertThat(minValues.getBoolean(3)).isEqualTo(false); // min flag from new data
        assertThat(maxValues.getBoolean(3)).isEqualTo(true); // max flag from new data
        assertThat(nullCounts[3]).isEqualTo(0L); // no nulls

        assertThat(minValues.getLong(4)).isEqualTo(500L); // min bigint_val from new data
        assertThat(maxValues.getLong(4)).isEqualTo(600L); // max bigint_val from new data
        assertThat(nullCounts[4]).isEqualTo(0L); // no nulls

        assertThat(minValues.getFloat(5)).isEqualTo(8.9f); // min float_val from new data
        assertThat(maxValues.getFloat(5)).isEqualTo(9.1f); // max float_val from new data
        assertThat(nullCounts[5]).isEqualTo(0L); // no nulls
    }

    @Test
    void testStatisticsContentForDifferentDataTypes() throws IOException {
        // Test comprehensive data type support
        RowType comprehensiveRowType =
                DataTypes.ROW(
                        new DataField("boolean_val", DataTypes.BOOLEAN()),
                        new DataField("byte_val", DataTypes.TINYINT()),
                        new DataField("short_val", DataTypes.SMALLINT()),
                        new DataField("int_val", DataTypes.INT()),
                        new DataField("long_val", DataTypes.BIGINT()),
                        new DataField("float_val", DataTypes.FLOAT()),
                        new DataField("double_val", DataTypes.DOUBLE()),
                        new DataField("string_val", DataTypes.STRING()));

        LogRecordBatchStatisticsCollector comprehensiveCollector =
                new LogRecordBatchStatisticsCollector(
                        comprehensiveRowType, createAllColumnsStatsMapping(comprehensiveRowType));

        // Test data with different ranges for each type
        List<Object[]> comprehensiveData =
                Arrays.asList(
                        new Object[] {true, (byte) 1, (short) 100, 1000, 10000L, 1.1f, 1.11, "a"},
                        new Object[] {false, (byte) 5, (short) 50, 500, 5000L, 5.5f, 5.55, "z"},
                        new Object[] {true, (byte) 3, (short) 200, 2000, 20000L, 2.2f, 2.22, "m"});

        for (Object[] data : comprehensiveData) {
            InternalRow row = DataTestUtils.row(data);
            comprehensiveCollector.processRow(row);
        }

        // Write and parse statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int bytesWritten = comprehensiveCollector.writeStatistics(outputView);
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse statistics from written data
        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, comprehensiveRowType, 1);
        assertThat(statistics).isNotNull();

        // Verify statistics content
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        Long[] nullCounts = statistics.getNullCounts();

        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(nullCounts).isNotNull();

        // Verify all data types have correct min/max values
        assertThat(minValues.getBoolean(0)).isEqualTo(false); // false < true
        assertThat(maxValues.getBoolean(0)).isEqualTo(true);

        assertThat(minValues.getByte(1)).isEqualTo((byte) 1);
        assertThat(maxValues.getByte(1)).isEqualTo((byte) 5);

        assertThat(minValues.getShort(2)).isEqualTo((short) 50);
        assertThat(maxValues.getShort(2)).isEqualTo((short) 200);

        assertThat(minValues.getInt(3)).isEqualTo(500);
        assertThat(maxValues.getInt(3)).isEqualTo(2000);

        assertThat(minValues.getLong(4)).isEqualTo(5000L);
        assertThat(maxValues.getLong(4)).isEqualTo(20000L);

        assertThat(minValues.getFloat(5)).isEqualTo(1.1f);
        assertThat(maxValues.getFloat(5)).isEqualTo(5.5f);

        assertThat(minValues.getDouble(6)).isEqualTo(1.11);
        assertThat(maxValues.getDouble(6)).isEqualTo(5.55);

        assertThat(minValues.getString(7)).isEqualTo(BinaryString.fromString("a"));
        assertThat(maxValues.getString(7)).isEqualTo(BinaryString.fromString("z"));

        // Verify no nulls
        for (int i = 0; i < nullCounts.length; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }
    }

    @Test
    void testEstimatedSizeInBytesAccuracy() throws IOException {
        // Test that estimatedSizeInBytes() provides accurate size estimation
        // without the overhead of actually writing to a temporary output view

        // Process multiple rows
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Get estimated size using the new efficient method
        int estimatedSize = collector.estimatedSizeInBytes();

        // Get actual size by writing to memory
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int actualSize = collector.writeStatistics(outputView);

        // The estimated size should be very close to the actual size
        // Allow small differences due to alignment and padding
        assertThat(estimatedSize).isGreaterThan(0);
        assertThat(actualSize).isGreaterThan(0);
        assertThat(estimatedSize).isEqualTo(actualSize);
    }

    @Test
    void testEstimatedSizeInBytesWithNoRows() {
        // Test estimated size with no processed rows
        int estimatedSize = collector.estimatedSizeInBytes();
        assertThat(estimatedSize).isGreaterThan(0); // Should still have header overhead
    }

    @Test
    void testEstimatedSizeInBytesWithPartialStats() throws IOException {
        // Test estimated size with partial stats mapping
        RowType partialRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        // Only collect stats for columns 0 and 2 (skip column 1)
        int[] partialStatsMapping = new int[] {0, 2};

        LogRecordBatchStatisticsCollector partialCollector =
                new LogRecordBatchStatisticsCollector(partialRowType, partialStatsMapping);

        // Process some rows
        List<Object[]> partialData =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {2, "b", 20.3},
                        new Object[] {3, "c", 15.7});

        for (Object[] data : partialData) {
            InternalRow row = DataTestUtils.row(data);
            partialCollector.processRow(row);
        }

        // Get estimated size and actual size
        int estimatedSize = partialCollector.estimatedSizeInBytes();

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int actualSize = partialCollector.writeStatistics(outputView);

        // Verify that estimated size matches actual size
        assertThat(estimatedSize).isGreaterThan(0);
        assertThat(actualSize).isGreaterThan(0);
        assertThat(estimatedSize).isEqualTo(actualSize);
    }

    @Test
    void testEstimatedSizeInBytesWithNullValues() throws IOException {
        // Test estimated size with null values
        RowType nullableRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()));

        LogRecordBatchStatisticsCollector nullCollector =
                new LogRecordBatchStatisticsCollector(
                        nullableRowType, createAllColumnsStatsMapping(nullableRowType));

        // Process rows with nulls
        List<Object[]> dataWithNulls =
                Arrays.asList(
                        new Object[] {1, "a"}, new Object[] {null, "b"}, new Object[] {3, null});

        for (Object[] data : dataWithNulls) {
            InternalRow row = DataTestUtils.row(data);
            nullCollector.processRow(row);
        }

        // Get estimated size and actual size
        int estimatedSize = nullCollector.estimatedSizeInBytes();

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);
        int actualSize = nullCollector.writeStatistics(outputView);

        // Verify that estimated size matches actual size
        assertThat(estimatedSize).isGreaterThan(0);
        assertThat(actualSize).isGreaterThan(0);
        assertThat(estimatedSize).isEqualTo(actualSize);
    }
}
