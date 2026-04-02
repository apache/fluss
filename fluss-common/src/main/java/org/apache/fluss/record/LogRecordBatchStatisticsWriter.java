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
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.CURRENT_STATISTICS_VERSION;

/**
 * A high-performance writer for LogRecordBatch statistics that efficiently serializes statistical
 * information directly to memory streams without creating intermediate heap objects.
 *
 * <p>This writer provides schema-aware statistics serialization capabilities, supporting selective
 * column statistics based on index mappings. It can write min/max values and other statistical
 * metadata for log record batches in a compact binary format.
 *
 * <h3>Binary Format Structure (V2):</h3>
 *
 * <pre>
 * [Version(1 byte)] [Column Count(2 bytes)] [Column Indexes(2*N bytes)]
 * [Min Values Row] [Max Values Row]
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
 *
 * int bytesWritten = writer.writeStatistics(minValues, maxValues, outputView);
 * </pre>
 *
 * <p><b>Thread Safety:</b> This class is NOT thread-safe. Each thread should use its own instance.
 *
 * <p><b>Memory Management:</b> The writer reuses internal buffers where possible. The internal
 * {@link AlignedRowWriter} is created once and reused across writes to avoid repeated allocations.
 *
 * @see LogRecordBatchStatistics
 * @see LogRecordBatchStatisticsCollector
 * @see AlignedRow
 */
public class LogRecordBatchStatisticsWriter {

    /** Assumed size for variable-length fields (STRING, BYTES, non-compact DECIMAL, etc.). */
    private static final int VARIABLE_LENGTH_FIELD_ESTIMATE = 16;

    private final RowType rowType;
    private final int[] statsIndexMapping;
    private final RowType statsRowType;

    /** Cached rough estimate of a single row's serialized size. -1 means not yet computed. */
    private int cachedRowSizeEstimate = -1;

    /** Reusable AlignedRow and AlignedRowWriter to avoid per-write allocations. */
    private final AlignedRow reusableRow;

    private final AlignedRowWriter reusableRowWriter;

    public LogRecordBatchStatisticsWriter(RowType rowType, int[] statsIndexMapping) {
        this.rowType = rowType;
        this.statsIndexMapping = statsIndexMapping;
        RowType.Builder statsRowTypeBuilder = RowType.builder();
        for (int fullRowIndex : statsIndexMapping) {
            statsRowTypeBuilder.field(
                    rowType.getFieldNames().get(fullRowIndex), rowType.getTypeAt(fullRowIndex));
        }
        this.statsRowType = statsRowTypeBuilder.build();

        // Create reusable AlignedRow and AlignedRowWriter once
        int fieldCount = statsRowType.getFieldCount();
        this.reusableRow = new AlignedRow(fieldCount);
        this.reusableRowWriter = new AlignedRowWriter(reusableRow, fieldCount * 64);
    }

    /**
     * Write statistics to an OutputView in schema-aware format (V2: no null counts).
     *
     * @param minValues The minimum values as InternalRow, can be null
     * @param maxValues The maximum values as InternalRow, can be null
     * @param outputView The target output view
     * @return The number of bytes written
     * @throws IOException If writing fails
     */
    public int writeStatistics(InternalRow minValues, InternalRow maxValues, OutputView outputView)
            throws IOException {

        int totalBytesWritten = 0;

        // Write version (1 byte)
        outputView.writeByte(CURRENT_STATISTICS_VERSION);
        totalBytesWritten += 1;

        // Write statistics column count (2 bytes)
        outputView.writeShort(statsIndexMapping.length);
        totalBytesWritten += 2;

        // Write statistics column indexes (2 bytes per index)
        for (int fullRowIndex : statsIndexMapping) {
            outputView.writeShort(fullRowIndex);
            totalBytesWritten += 2;
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
            AlignedRow binaryRowData = convertToAlignedRow(row);
            int rowSize = binaryRowData.getSizeInBytes();
            outputView.writeInt(rowSize);
            // The reusable writer always produces heap-backed segments, so getArray() is safe.
            MemorySegment segment = binaryRowData.getSegments()[0];
            outputView.write(segment.getArray(), binaryRowData.getOffset(), rowSize);
            return 4 + rowSize;
        } else {
            outputView.writeInt(0);
            return 4;
        }
    }

    /**
     * Converts an InternalRow to AlignedRow using the internal reusable AlignedRowWriter. Always
     * goes through the writer path to guarantee the result is backed by a heap MemorySegment, which
     * is required by {@link #writeRowData} (it calls {@link MemorySegment#getArray()}).
     */
    private AlignedRow convertToAlignedRow(InternalRow row) {
        // Always use the reusable writer to ensure heap-backed segments.
        // Do NOT shortcut with "instanceof AlignedRow" — an AlignedRow may be backed by an
        // off-heap MemorySegment, and getArray() would throw on off-heap segments.
        reusableRowWriter.reset();
        int fieldCount = statsRowType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                reusableRowWriter.setNullAt(i);
            } else {
                DataType fieldType = statsRowType.getTypeAt(i);
                switch (fieldType.getTypeRoot()) {
                    case BOOLEAN:
                        reusableRowWriter.writeBoolean(i, row.getBoolean(i));
                        break;
                    case TINYINT:
                        reusableRowWriter.writeByte(i, row.getByte(i));
                        break;
                    case SMALLINT:
                        reusableRowWriter.writeShort(i, row.getShort(i));
                        break;
                    case INTEGER:
                    case DATE:
                    case TIME_WITHOUT_TIME_ZONE:
                        reusableRowWriter.writeInt(i, row.getInt(i));
                        break;
                    case BIGINT:
                        reusableRowWriter.writeLong(i, row.getLong(i));
                        break;
                    case FLOAT:
                        reusableRowWriter.writeFloat(i, row.getFloat(i));
                        break;
                    case DOUBLE:
                        reusableRowWriter.writeDouble(i, row.getDouble(i));
                        break;
                    case STRING:
                        reusableRowWriter.writeString(i, row.getString(i));
                        break;
                    case DECIMAL:
                        DecimalType decimalType = (DecimalType) fieldType;
                        reusableRowWriter.writeDecimal(
                                i,
                                row.getDecimal(
                                        i, decimalType.getPrecision(), decimalType.getScale()),
                                decimalType.getPrecision());
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        TimestampType timestampType = (TimestampType) fieldType;
                        reusableRowWriter.writeTimestampNtz(
                                i,
                                row.getTimestampNtz(i, timestampType.getPrecision()),
                                timestampType.getPrecision());
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        LocalZonedTimestampType ltzType = (LocalZonedTimestampType) fieldType;
                        reusableRowWriter.writeTimestampLtz(
                                i,
                                row.getTimestampLtz(i, ltzType.getPrecision()),
                                ltzType.getPrecision());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Statistics collection is not supported for type: "
                                        + fieldType.getTypeRoot());
                }
            }
        }
        reusableRowWriter.complete();
        return reusableRow;
    }

    /**
     * Returns a heuristic estimate of the serialized size in bytes for the given statistics data.
     *
     * <p><b>Note:</b> This is a rough estimate, not a guaranteed upper bound. For fixed-length
     * types the size is exact, but for variable-length types (STRING, BYTES, non-compact DECIMAL,
     * etc.) a constant estimate ({@value VARIABLE_LENGTH_FIELD_ESTIMATE} bytes) is assumed. As a
     * result, the estimate may underestimate the actual size when rows contain large
     * variable-length values.
     *
     * @param minValues The minimum values as InternalRow, can be null
     * @param maxValues The maximum values as InternalRow, can be null
     * @return The estimated number of bytes required for serialization
     */
    public int estimatedSizeInBytes(InternalRow minValues, InternalRow maxValues) {

        int totalEstimatedBytes = 0;

        // Fixed size header components:
        // Version (1 byte) + Column count (2 bytes)
        totalEstimatedBytes += 3;

        // Column indexes (2 bytes per index)
        totalEstimatedBytes += statsIndexMapping.length * 2;

        // Estimate min values size: 4 bytes for size field + row data
        totalEstimatedBytes += 4 + (minValues != null ? getRowSizeEstimate() : 0);

        // Estimate max values size: 4 bytes for size field + row data
        totalEstimatedBytes += 4 + (maxValues != null ? getRowSizeEstimate() : 0);

        return totalEstimatedBytes;
    }

    /**
     * Returns a cached rough estimate of the serialized row size based on the stats row type. The
     * fixed-length part size is exact; variable-length fields use a constant estimate.
     */
    private int getRowSizeEstimate() {
        if (cachedRowSizeEstimate < 0) {
            int fieldCount = statsRowType.getFieldCount();
            // Fixed-length part: null bits + 8 bytes per field
            int estimate = AlignedRow.calculateFixPartSizeInBytes(fieldCount);
            // Add variable-length field estimates
            for (int i = 0; i < fieldCount; i++) {
                if (!AlignedRow.isInFixedLengthPart(statsRowType.getTypeAt(i))) {
                    estimate += VARIABLE_LENGTH_FIELD_ESTIMATE;
                }
            }
            cachedRowSizeEstimate = estimate;
        }
        return cachedRowSizeEstimate;
    }
}
