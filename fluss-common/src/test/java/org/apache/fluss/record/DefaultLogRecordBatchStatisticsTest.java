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
import org.apache.fluss.row.BinaryRowData;
import org.apache.fluss.row.BinaryRowWriter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultLogRecordBatchStatistics}. */
public class DefaultLogRecordBatchStatisticsTest {

    private static final int SCHEMA_ID = 1;
    private static final RowType TEST_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()));

    @Test
    public void testConstructorAndBasicMethods() {
        // Create test data
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        int position = 0;
        int size = 100;
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int minValuesOffset = 50;
        int maxValuesOffset = 80;
        int minValuesSize = 20;
        int maxValuesSize = 15;
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        position,
                        size,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        minValuesOffset,
                        maxValuesOffset,
                        minValuesSize,
                        maxValuesSize,
                        statsIndexMapping);

        // Test basic accessors
        assertThat(stats.getSegment()).isEqualTo(segment);
        assertThat(stats.getPosition()).isEqualTo(position);
        assertThat(stats.getSize()).isEqualTo(size);
        assertThat(stats.getRowType()).isEqualTo(TEST_ROW_TYPE);
        assertThat(stats.getSchemaId()).isEqualTo(SCHEMA_ID);
        assertThat(stats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(stats.hasMinValues()).isTrue();
        assertThat(stats.hasMaxValues()).isTrue();

        // Test hasColumnStatistics
        assertThat(stats.hasColumnStatistics(0)).isTrue();
        assertThat(stats.hasColumnStatistics(1)).isTrue();
        assertThat(stats.hasColumnStatistics(2)).isTrue();

        // Test getNullCount
        assertThat(stats.getNullCount(0)).isEqualTo(0L);
        assertThat(stats.getNullCount(1)).isEqualTo(1L);
        assertThat(stats.getNullCount(2)).isEqualTo(2L);
    }

    @Test
    public void testConstructorWithPartialStatistics() {
        // Create test data with partial statistics (only for columns 0 and 2)
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        int position = 0;
        int size = 100;
        Long[] nullCounts = new Long[] {0L, 2L}; // Only for columns 0 and 2
        int minValuesOffset = 50;
        int maxValuesOffset = 80;
        int minValuesSize = 20;
        int maxValuesSize = 15;
        int[] statsIndexMapping = new int[] {0, 2}; // Skip column 1

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        position,
                        size,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        minValuesOffset,
                        maxValuesOffset,
                        minValuesSize,
                        maxValuesSize,
                        statsIndexMapping);

        // Test hasColumnStatistics - should be true for 0 and 2, false for 1
        assertThat(stats.hasColumnStatistics(0)).isTrue();
        assertThat(stats.hasColumnStatistics(1)).isFalse();
        assertThat(stats.hasColumnStatistics(2)).isTrue();
    }

    @Test
    public void testConstructorWithNoMinMaxValues() {
        // Create test data with no min/max values
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        int position = 0;
        int size = 50;
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int minValuesOffset = 0;
        int maxValuesOffset = 0;
        int minValuesSize = 0;
        int maxValuesSize = 0;
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        position,
                        size,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        minValuesOffset,
                        maxValuesOffset,
                        minValuesSize,
                        maxValuesSize,
                        statsIndexMapping);

        // Test no min/max values available
        assertThat(stats.hasMinValues()).isFalse();
        assertThat(stats.hasMaxValues()).isFalse();
        assertThat(stats.getMinValues()).isNull();
        assertThat(stats.getMaxValues()).isNull();
    }

    @Test
    public void testWithSerializedStatistics() throws IOException {
        // Create test rows
        InternalRow minRow = DataTestUtils.row(new Object[] {1, "a", 10.5});
        InternalRow maxRow = DataTestUtils.row(new Object[] {100, "z", 99.9});
        Long[] nullCounts = new Long[] {0L, 2L, 1L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        // Create writer and serialize statistics
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(TEST_ROW_TYPE, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        // Convert to BinaryRowData for serialization
        BinaryRowData minBinaryRow = createBinaryRow(minRow);
        BinaryRowData maxBinaryRow = createBinaryRow(maxRow);

        int bytesWritten =
                writer.writeStatistics(minBinaryRow, maxBinaryRow, nullCounts, outputView);

        // Parse the written statistics
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, TEST_ROW_TYPE, SCHEMA_ID);

        // Verify parsed statistics
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getSchemaId()).isEqualTo(SCHEMA_ID);
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(parsedStats.hasMinValues()).isTrue();
        assertThat(parsedStats.hasMaxValues()).isTrue();

        // Verify min/max values can be accessed
        InternalRow parsedMinRow = parsedStats.getMinValues();
        InternalRow parsedMaxRow = parsedStats.getMaxValues();

        assertThat(parsedMinRow).isNotNull();
        assertThat(parsedMaxRow).isNotNull();

        assertThat(parsedMinRow.getInt(0)).isEqualTo(1);
        assertThat(parsedMinRow.getString(1).toString()).isEqualTo("a");
        assertThat(parsedMinRow.getDouble(2)).isEqualTo(10.5);

        assertThat(parsedMaxRow.getInt(0)).isEqualTo(100);
        assertThat(parsedMaxRow.getString(1).toString()).isEqualTo("z");
        assertThat(parsedMaxRow.getDouble(2)).isEqualTo(99.9);
    }

    @Test
    public void testFullRowWrapperBounds() throws IOException {
        // Create statistics with all columns
        InternalRow minRow = DataTestUtils.row(new Object[] {1, "a", 10.5});
        InternalRow maxRow = DataTestUtils.row(new Object[] {100, "z", 99.9});
        Long[] nullCounts = new Long[] {0L, 2L, 1L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        // Serialize and parse
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(TEST_ROW_TYPE, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        BinaryRowData minBinaryRow = createBinaryRow(minRow);
        BinaryRowData maxBinaryRow = createBinaryRow(maxRow);

        int bytesWritten =
                writer.writeStatistics(minBinaryRow, maxBinaryRow, nullCounts, outputView);

        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, TEST_ROW_TYPE, SCHEMA_ID);

        // Test FullRowWrapper bounds checking
        InternalRow minValues = parsedStats.getMinValues();
        assertThat(minValues.getFieldCount()).isEqualTo(3);

        // Valid accesses should work
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(minValues.getString(1).toString()).isEqualTo("a");
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);

        // Test cached min/max rows
        InternalRow cachedMinValues = parsedStats.getMinValues();
        assertThat(cachedMinValues).isSameAs(minValues); // Should return cached instance
    }

    @Test
    public void testEqualsAndHashCode() {
        MemorySegment segment1 = MemorySegment.allocateHeapMemory(512);
        MemorySegment segment2 = MemorySegment.allocateHeapMemory(512);

        // Create identical statistics objects
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats1 =
                new DefaultLogRecordBatchStatistics(
                        segment1,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        DefaultLogRecordBatchStatistics stats2 =
                new DefaultLogRecordBatchStatistics(
                        segment1,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        // Test equals
        assertThat(stats1).isEqualTo(stats2);
        assertThat(stats1.hashCode()).isEqualTo(stats2.hashCode());

        // Test not equals with different segment
        DefaultLogRecordBatchStatistics stats3 =
                new DefaultLogRecordBatchStatistics(
                        segment2,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        assertThat(stats1).isNotEqualTo(stats3);
    }

    @Test
    public void testToString() {
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        String toString = stats.toString();
        assertThat(toString).contains("DefaultLogRecordBatchStatistics");
        assertThat(toString).contains("hasMinValues=true");
        assertThat(toString).contains("hasMaxValues=true");
        assertThat(toString).contains("size=100");
    }

    @Test
    public void testPartialStatisticsWrapperExceptionHandling() throws IOException {
        // Create test data for partial statistics - only collect stats for columns 0 and 2 (skip
        // column 1)
        InternalRow minRow = DataTestUtils.row(1, 10.5); // Only id and value, skip name
        InternalRow maxRow = DataTestUtils.row(100, 99.9); // Only id and value, skip name
        Long[] nullCounts = new Long[] {0L, 1L}; // Null counts only for columns 0 and 2
        int[] statsIndexMapping = new int[] {0, 2}; // Skip column 1 (name column)

        // Create a row type that matches the partial stats
        RowType fullRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        // Serialize and parse statistics with partial columns
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(fullRowType, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        writer.writeStatistics(minRow, maxRow, nullCounts, outputView);

        // Create statistics with original TEST_ROW_TYPE (3 columns) but only partial mapping
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, fullRowType, 0);

        // Verify that columns 0 and 2 have statistics, but column 1 doesn't
        assertThat(parsedStats.hasColumnStatistics(0)).isTrue();
        assertThat(parsedStats.hasColumnStatistics(1)).isFalse();
        assertThat(parsedStats.hasColumnStatistics(2)).isTrue();

        // Get min and max values wrapper
        InternalRow minValues = parsedStats.getMinValues();
        InternalRow maxValues = parsedStats.getMaxValues();

        // Valid accesses should work for columns with statistics (0 and 2)
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);
        assertThat(maxValues.getInt(0)).isEqualTo(100);
        assertThat(maxValues.getDouble(2)).isEqualTo(99.9);

        // Accessing column 1 (name) should throw IllegalArgumentException
        // since it's not available in the statistics
        assertThatThrownBy(() -> minValues.getString(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available in underlying row data");

        assertThatThrownBy(() -> maxValues.getString(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available in underlying row data");

        // Also test other access methods that should throw for column 1
        assertThatThrownBy(() -> minValues.isNullAt(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available in underlying row data");

        // Test accessing out-of-bounds column
        assertThatThrownBy(() -> minValues.getInt(3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index out of range");
    }

    private BinaryRowData createBinaryRow(InternalRow row) {
        BinaryRowData binaryRow = new BinaryRowData(row.getFieldCount());
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);

        for (int i = 0; i < row.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                writer.setNullAt(i);
            } else {
                switch (TEST_ROW_TYPE.getTypeAt(i).getTypeRoot()) {
                    case INTEGER:
                        writer.writeInt(i, row.getInt(i));
                        break;
                    case STRING:
                        writer.writeString(i, row.getString(i));
                        break;
                    case DOUBLE:
                        writer.writeDouble(i, row.getDouble(i));
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported type: " + TEST_ROW_TYPE.getTypeAt(i));
                }
            }
        }

        writer.complete();
        return binaryRow;
    }
}
