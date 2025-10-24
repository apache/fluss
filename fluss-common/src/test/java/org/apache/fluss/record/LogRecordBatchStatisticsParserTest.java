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
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for LogRecordBatchStatisticsParser. */
public class LogRecordBatchStatisticsParserTest {

    // Helper method to create stats index mapping for all columns
    private static int[] createAllColumnsStatsMapping(RowType rowType) {
        int[] statsIndexMapping = new int[rowType.getFieldCount()];
        for (int i = 0; i < statsIndexMapping.length; i++) {
            statsIndexMapping[i] = i;
        }
        return statsIndexMapping;
    }

    @Test
    public void testParseStatisticsFromByteArray() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {10L, 0L};
        IndexedRow minValues = createTestIndexedRow(rowType, -100, "aaa");
        IndexedRow maxValues = createTestIndexedRow(rowType, 999, "zzz");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using byte array
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        // Verify parsing results - just basic validation
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.getNullCounts()).hasSize(2);
    }

    @Test
    public void testParseStatisticsFromByteBuffer() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {3L, 7L};
        IndexedRow minValues = createTestIndexedRow(rowType, 0, "first");
        IndexedRow maxValues = createTestIndexedRow(rowType, 500, "last");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(buffer, rowType, DEFAULT_SCHEMA_ID);

        // Verify parsing results - just basic validation
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.getNullCounts()).hasSize(2);
    }

    @Test
    public void testParseStatisticsFromMemorySegment() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 42, "test");
        IndexedRow maxValues = createTestIndexedRow(rowType, 84, "testing");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using MemorySegment
        MemorySegment segment = MemorySegment.wrap(writtenData);
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, rowType, DEFAULT_SCHEMA_ID);

        // Verify parsing results - just basic validation
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.getNullCounts()).hasSize(2);
    }

    @Test
    public void testIsValidStatistics() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create valid test statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1, "min");
        IndexedRow maxValues = createTestIndexedRow(rowType, 100, "max");

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Test with valid data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(writtenData, rowType)).isTrue();

        // Test with direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(buffer, rowType)).isTrue();

        // Test with empty data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(new byte[0], rowType))
                .isFalse();

        // Test with null data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((byte[]) null, rowType))
                .isFalse();
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((ByteBuffer) null, rowType))
                .isFalse();
    }

    @Test
    public void testGetStatisticsSize() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1, "a");
        IndexedRow maxValues = createTestIndexedRow(rowType, 100, "z");

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Test with byte array
        int size = LogRecordBatchStatisticsParser.getStatisticsSize(writtenData);
        assertThat(size).isEqualTo(writtenData.length);

        // Test with direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        size = LogRecordBatchStatisticsParser.getStatisticsSize(buffer);
        assertThat(size).isEqualTo(writtenData.length);
    }

    @Test
    public void testGetStatisticsSizeWithInvalidData() {
        // Test with invalid data
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize(ByteBuffer.allocateDirect(0)))
                .isEqualTo(-1);

        // Test with null data
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize((byte[]) null)).isEqualTo(-1);
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize((ByteBuffer) null))
                .isEqualTo(-1);
    }

    @Test
    public void testBasicFunctionality() throws Exception {
        // Test basic write and parse cycle without detailed verification
        RowType rowType = DataTypes.ROW(new IntType(false));

        Long[] nullCounts = new Long[] {0L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1);
        IndexedRow maxValues = createTestIndexedRow(rowType, 100);

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Verify we can parse it back
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.hasMinValues()).isTrue();
        assertThat(parsedStats.hasMaxValues()).isTrue();
    }

    // Helper methods for creating test data
    private IndexedRow createTestIndexedRow(RowType rowType, Object... values) throws IOException {
        IndexedRowWriter writer =
                new IndexedRowWriter(rowType.getChildren().toArray(new DataType[0]));
        writer.reset();

        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                writer.setNullAt(i);
            } else {
                DataType fieldType = rowType.getTypeAt(i);
                if (fieldType instanceof StringType) {
                    // Convert String to BinaryString for StringType fields
                    writer.writeString(BinaryString.fromString((String) values[i]));
                } else if (fieldType instanceof IntType) {
                    // Write int values directly
                    writer.writeInt((Integer) values[i]);
                }
            }
        }

        IndexedRow row = new IndexedRow(rowType.getChildren().toArray(new DataType[0]));
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private byte[] writeStatisticsForTest(
            LogRecordBatchStatisticsWriter writer,
            InternalRow minValues,
            InternalRow maxValues,
            Long[] nullCounts)
            throws IOException {

        // Allocate enough memory for statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten =
                writer.writeStatistics(
                        minValues, maxValues, nullCounts, new MemorySegmentOutputView(segment));

        // Copy the written data to a byte array
        byte[] result = new byte[bytesWritten];
        segment.get(0, result, 0, bytesWritten);
        return result;
    }
}
