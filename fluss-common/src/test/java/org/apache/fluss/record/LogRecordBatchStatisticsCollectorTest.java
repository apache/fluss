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

    @BeforeEach
    void setUp() {
        testRowType = TestData.STATISTICS_MIXED_TYPE_ROW_TYPE;
        collector =
                new LogRecordBatchStatisticsCollector(
                        testRowType,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(
                                testRowType));
    }

    @Test
    void testProcessRowAndWriteStatistics() throws IOException {
        // Process single row
        InternalRow row = DataTestUtils.row(TestData.STATISTICS_MIXED_TYPE_DATA.get(0));
        collector.processRow(row);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));
        assertThat(bytesWritten).isGreaterThan(0);
    }

    @Test
    void testProcessMultipleRowsAndVerifyStatistics() throws IOException {
        for (Object[] data : TestData.STATISTICS_MIXED_TYPE_DATA) {
            collector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));
        assertThat(bytesWritten).isGreaterThan(0);

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, testRowType, 1);
        assertThat(statistics).isNotNull();

        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();

        // Verify min/max values based on MIXED_TYPE_DATA
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(maxValues.getInt(0)).isEqualTo(5);
        assertThat(minValues.getString(1)).isEqualTo(BinaryString.fromString("a"));
        assertThat(maxValues.getString(1)).isEqualTo(BinaryString.fromString("e"));
        assertThat(minValues.getDouble(2)).isEqualTo(8.9);
        assertThat(maxValues.getDouble(2)).isEqualTo(30.1);
        assertThat(minValues.getBoolean(3)).isEqualTo(false);
        assertThat(maxValues.getBoolean(3)).isEqualTo(true);
        assertThat(minValues.getLong(4)).isEqualTo(100L);
        assertThat(maxValues.getLong(4)).isEqualTo(300L);
        assertThat(minValues.getFloat(5)).isEqualTo(1.23f);
        assertThat(maxValues.getFloat(5)).isEqualTo(5.67f);

        // V2 format: null counts are null (will come from Arrow metadata later)
        assertThat(statistics.getNullCounts()).isNull();
    }

    @Test
    void testResetAndReprocess() throws IOException {
        // Process initial data
        for (Object[] data : TestData.STATISTICS_MIXED_TYPE_DATA) {
            collector.processRow(DataTestUtils.row(data));
        }

        // Reset and process new data
        collector.reset();
        List<Object[]> newData =
                Arrays.asList(
                        new Object[] {10, "x", 100.0, false, 500L, 8.9f},
                        new Object[] {15, "y", 200.0, true, 600L, 9.1f});

        for (Object[] data : newData) {
            collector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        collector.writeStatistics(new MemorySegmentOutputView(segment));

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, testRowType, 1);

        // Verify values from new data only
        assertThat(statistics.getMinValues().getInt(0)).isEqualTo(10);
        assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(15);
    }

    @Test
    void testProcessNullValues() throws IOException {
        RowType nullableRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsCollector nullCollector =
                new LogRecordBatchStatisticsCollector(
                        nullableRowType,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(
                                nullableRowType));

        List<Object[]> dataWithNulls =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {null, "b", 20.3},
                        new Object[] {3, null, 15.7},
                        new Object[] {2, "c", null});

        for (Object[] data : dataWithNulls) {
            nullCollector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        nullCollector.writeStatistics(new MemorySegmentOutputView(segment));

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, nullableRowType, 1);

        // V2 format: null counts are null (will come from Arrow metadata later)
        assertThat(statistics.getNullCounts()).isNull();

        // Verify min/max values exclude nulls
        assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(3);
    }

    @Test
    void testPartialStatsIndexMapping() throws IOException {
        RowType fullRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        int[] partialStatsMapping = new int[] {0, 2}; // Skip column 1

        LogRecordBatchStatisticsCollector partialCollector =
                new LogRecordBatchStatisticsCollector(fullRowType, partialStatsMapping);

        List<Object[]> data =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {5, "b", 5.3},
                        new Object[] {3, "c", 15.7});

        for (Object[] rowData : data) {
            partialCollector.processRow(DataTestUtils.row(rowData));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        partialCollector.writeStatistics(new MemorySegmentOutputView(segment));

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, fullRowType, 1);

        // Verify statistics for collected columns
        assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(5);
        assertThat(statistics.getMinValues().getDouble(2)).isEqualTo(5.3);
        assertThat(statistics.getMaxValues().getDouble(2)).isEqualTo(15.7);
    }

    @Test
    void testDifferentDataTypes() throws IOException {
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

        LogRecordBatchStatisticsCollector typeCollector =
                new LogRecordBatchStatisticsCollector(
                        comprehensiveRowType,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(
                                comprehensiveRowType));

        List<Object[]> comprehensiveData =
                Arrays.asList(
                        new Object[] {true, (byte) 1, (short) 100, 1000, 10000L, 1.1f, 1.11, "a"},
                        new Object[] {false, (byte) 5, (short) 50, 500, 5000L, 5.5f, 5.55, "z"},
                        new Object[] {true, (byte) 3, (short) 200, 2000, 20000L, 2.2f, 2.22, "m"});

        for (Object[] data : comprehensiveData) {
            typeCollector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        typeCollector.writeStatistics(new MemorySegmentOutputView(segment));

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, comprehensiveRowType, 1);

        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();

        assertThat(minValues.getBoolean(0)).isEqualTo(false);
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
    }

    @Test
    void testEstimatedSizeInBytesAccuracy() throws IOException {
        for (Object[] data : TestData.STATISTICS_MIXED_TYPE_DATA) {
            collector.processRow(DataTestUtils.row(data));
        }

        int estimatedSize = collector.estimatedSizeInBytes();

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int actualSize = collector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(estimatedSize).isGreaterThan(0);
        // estimatedSizeInBytes() is a heuristic estimate, not a guaranteed upper bound.
        // It may underestimate for rows with large variable-length values (STRING, BYTES, etc.)
        // since it uses a fixed constant for variable-length fields. We verify the estimate
        // is reasonably close to the actual size (within 2x).
        assertThat((double) estimatedSize)
                .as("Heuristic estimate should be reasonably close to actual size")
                .isGreaterThan(actualSize * 0.5)
                .isLessThan(actualSize * 2.0);
    }

    @Test
    void testZeroRows() throws IOException {
        // Create a collector but do NOT call processRow()
        RowType rowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsCollector zeroRowCollector =
                new LogRecordBatchStatisticsCollector(
                        rowType,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(rowType));

        // Write statistics without processing any rows
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = zeroRowCollector.writeStatistics(new MemorySegmentOutputView(segment));
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse back and verify
        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, rowType, 1);
        assertThat(statistics).isNotNull();

        // V2 format: null counts are null (will come from Arrow metadata later)
        assertThat(statistics.getNullCounts()).isNull();

        // Min/max values row exists but all fields should be null since no rows were processed
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            assertThat(minValues.isNullAt(i)).isTrue();
            assertThat(maxValues.isNullAt(i)).isTrue();
        }
    }

    @Test
    void testAllNullColumn() throws IOException {
        // Create data where column 2 (value) has ALL null values
        RowType rowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsCollector allNullCollector =
                new LogRecordBatchStatisticsCollector(
                        rowType,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(rowType));

        List<Object[]> dataWithAllNullColumn =
                Arrays.asList(
                        new Object[] {1, "a", null},
                        new Object[] {3, "c", null},
                        new Object[] {5, "e", null},
                        new Object[] {2, "b", null});

        for (Object[] data : dataWithAllNullColumn) {
            allNullCollector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        allNullCollector.writeStatistics(new MemorySegmentOutputView(segment));

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, rowType, 1);
        assertThat(statistics).isNotNull();

        // V2 format: null counts are null (will come from Arrow metadata later)
        assertThat(statistics.getNullCounts()).isNull();

        // Column 2 min/max should be null (all values were null)
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();
        assertThat(minValues.isNullAt(2)).isTrue();
        assertThat(maxValues.isNullAt(2)).isTrue();

        // Other columns should have correct min/max
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(maxValues.getInt(0)).isEqualTo(5);
        assertThat(minValues.getString(1)).isEqualTo(BinaryString.fromString("a"));
        assertThat(maxValues.getString(1)).isEqualTo(BinaryString.fromString("e"));
    }

    @Test
    void testLargeDataset() throws IOException {
        LogRecordBatchStatisticsCollector largeCollector =
                new LogRecordBatchStatisticsCollector(
                        testRowType,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(
                                testRowType));

        for (int i = 0; i < 1000; i++) {
            Object[] data = {i, "row" + i, (double) i, i % 2 == 0, (long) i, (float) i};
            largeCollector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten = largeCollector.writeStatistics(new MemorySegmentOutputView(segment));
        assertThat(bytesWritten).isGreaterThan(0);

        DefaultLogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, testRowType, 1);

        // Verify min/max for large dataset
        assertThat(statistics.getMinValues().getInt(0)).isEqualTo(0);
        assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(999);
    }
}
