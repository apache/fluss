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

import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.types.RowType;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.STATISTICS_VERSION;

/**
 * A high-performance writer for LogRecordBatch statistics that efficiently serializes statistical
 * information directly to memory streams without creating intermediate heap objects.
 *
 * <p>This writer provides schema-aware statistics serialization capabilities, supporting selective
 * column statistics based on index mappings. It can write min/max values, null counts, and other
 * statistical metadata for log record batches in a compact binary format.
 *
 * <h3>Binary Format Structure:</h3>
 *
 * <pre>
 * [Version(1 byte)] [Column Count(2 bytes)] [Column Indexes(2*N bytes)]
 * [Null Counts(4*N bytes)] [Min Values Row] [Max Values Row]
 * </pre>
 *
 * <h3>Usage Example:</h3>
 *
 * <pre>
 * RowType rowType = ...;
 * int[] statsMapping = {0, 2, 5}; // Only collect stats for columns 0, 2, 5
 * LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType, statsMapping);
 *
 * InternalRow minValues = ...;
 * InternalRow maxValues = ...;
 * Long[] nullCounts = ...;
 *
 * int bytesWritten = writer.writeStatistics(minValues, maxValues, nullCounts, outputView);
 * </pre>
 *
 * <p><b>Thread Safety:</b> This class is NOT thread-safe. Each thread should use its own instance.
 *
 * <p><b>Memory Management:</b> The writer reuses internal buffers where possible but may allocate
 * temporary byte arrays for row data serialization. For high-throughput scenarios, consider object
 * pooling at the application level.
 *
 * @see LogRecordBatchStatistics
 * @see LogRecordBatchStatisticsCollector
 * @see AlignedRow
 */
public class LogRecordBatchStatisticsWriter {

    private final RowType rowType;
    private final int[] statsIndexMapping;
    private final RowType statsRowType;

    public LogRecordBatchStatisticsWriter(RowType rowType, int[] statsIndexMapping) {
        this.rowType = rowType;
        this.statsIndexMapping = statsIndexMapping;
        RowType.Builder statsRowTypeBuilder = RowType.builder();
        for (int fullRowIndex : statsIndexMapping) {
            statsRowTypeBuilder.field(
                    rowType.getFieldNames().get(fullRowIndex), rowType.getTypeAt(fullRowIndex));
        }
        this.statsRowType = statsRowTypeBuilder.build();
    }

    /**
     * Write statistics to an OutputView in schema-aware format.
     *
     * @param minValues The minimum values as InternalRow, can be null
     * @param maxValues The maximum values as InternalRow, can be null
     * @param nullCounts The null counts array
     * @param outputView The target output view
     * @return The number of bytes written
     * @throws IOException If writing fails
     */
    public int writeStatistics(
            InternalRow minValues, InternalRow maxValues, Long[] nullCounts, OutputView outputView)
            throws IOException {

        int totalBytesWritten = 0;

        // Write version (1 byte)
        outputView.writeByte(STATISTICS_VERSION);
        totalBytesWritten += 1;

        // Write statistics column count (2 bytes)
        outputView.writeShort(statsIndexMapping.length);
        totalBytesWritten += 2;

        // Write statistics column indexes (2 bytes per index)
        for (int fullRowIndex : statsIndexMapping) {
            outputView.writeShort(fullRowIndex);
            totalBytesWritten += 2;
        }

        // Write null counts for statistics columns only (4 bytes per count)
        for (Long count : nullCounts) {
            long nullCount = count != null ? count : 0;
            outputView.writeInt((int) nullCount);
            totalBytesWritten += 4;
        }

        // Write min values
        int minRowBytes = writeRowData(minValues, outputView);
        totalBytesWritten += minRowBytes;

        // Write max values
        int maxRowBytes = writeRowData(maxValues, outputView);
        totalBytesWritten += maxRowBytes;

        return totalBytesWritten;
    }

    private int writeRowData(InternalRow row, OutputView outputView) throws IOException {
        if (row != null) {
            // Use AlignedRow.from() to handle conversion uniformly
            AlignedRow binaryRowData = AlignedRow.from(statsRowType, row);
            int rowSize = binaryRowData.getSizeInBytes();
            outputView.writeInt(rowSize);
            // Copy data using byte array since copyTo requires byte[] parameter
            byte[] rowData = new byte[rowSize];
            binaryRowData.copyTo(rowData, 0);
            outputView.write(rowData);
            return 4 + rowSize;
        } else {
            outputView.writeInt(0);
            return 4;
        }
    }

    /**
     * Get the row type used by this writer.
     *
     * @return The row type
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Get the field count.
     *
     * @return The number of fields
     */
    public int getFieldCount() {
        return statsRowType.getFieldCount();
    }

    /**
     * Estimate the size in bytes that would be required to serialize the given statistics data.
     * This method provides a more efficient way to calculate statistics size without actually
     * writing the data to memory.
     *
     * @param minValues The minimum values as InternalRow, can be null
     * @param maxValues The maximum values as InternalRow, can be null
     * @param nullCounts The null counts array
     * @return The estimated number of bytes required for serialization
     */
    public int estimatedSizeInBytes(
            InternalRow minValues, InternalRow maxValues, Long[] nullCounts) {

        int totalEstimatedBytes = 0;

        // Fixed size header components:
        // Version (1 byte) + Column count (2 bytes)
        totalEstimatedBytes += 3;

        // Column indexes (2 bytes per index)
        totalEstimatedBytes += statsIndexMapping.length * 2;

        // Null counts (4 bytes per count)
        totalEstimatedBytes += statsIndexMapping.length * 4;

        // Estimate min values size
        totalEstimatedBytes += estimateRowDataSize(minValues);

        // Estimate max values size
        totalEstimatedBytes += estimateRowDataSize(maxValues);

        return totalEstimatedBytes;
    }

    private int estimateRowDataSize(InternalRow row) {
        if (row == null) {
            // 4 bytes for size field when null
            return 4;
        }

        // Use AlignedRow.from() to get accurate size estimation
        // This is still more efficient than writing to a temporary output view
        // as it only creates the binary representation without doing I/O
        AlignedRow alignedRow = AlignedRow.from(statsRowType, row);
        int rowSize = alignedRow.getSizeInBytes();
        // 4 bytes for size field + actual row data size
        return 4 + rowSize;
    }
}
