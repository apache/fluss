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
import org.apache.fluss.row.BinaryRowData;
import org.apache.fluss.row.BinaryRowWriter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.STATISTICS_VERSION;

/**
 * Writer for LogRecordBatchStatistics that writes statistics directly to memory without creating
 * intermediate heap objects. Supports schema-aware statistics format.
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
            int rowSize = 0;
            // If the input is already an IndexedRow, write it directly (preferred for efficiency)
            if (row instanceof BinaryRowData) {
                // If the input is already a BinaryRowData, get size and data directly
                BinaryRowData binaryRowData = (BinaryRowData) row;
                rowSize = binaryRowData.getSizeInBytes();
                outputView.writeInt(rowSize);
                // Copy data using byte array since copyTo requires byte[] parameter
                byte[] rowData = new byte[rowSize];
                binaryRowData.copyTo(rowData, 0);
                outputView.write(rowData);
            } else {
                // For other row types, convert them to BinaryRowData for backward compatibility
                BinaryRowData binaryRowData = convertToBinaryRowData(row);
                rowSize = binaryRowData.getSizeInBytes();
                outputView.writeInt(rowSize);
                // Copy data using byte array since copyTo requires byte[] parameter
                byte[] rowData = new byte[rowSize];
                binaryRowData.copyTo(rowData, 0);
                outputView.write(rowData);
            }
            return 4 + rowSize;
        } else {
            outputView.writeInt(0);
            return 4;
        }
    }

    /**
     * Convert an InternalRow to a BinaryRowData for consistency in handling row data. This method
     * ensures that we always work with BinaryRowData instances.
     *
     * @param row The InternalRow to convert
     * @return The converted BinaryRowData
     */
    private BinaryRowData convertToBinaryRowData(InternalRow row) {
        int fieldCount = statsRowType.getFieldCount();
        BinaryRowData binaryRowData = new BinaryRowData(fieldCount);
        // Allocate sufficient space for variable-length fields
        // 64 bytes per field should be sufficient for most cases
        int initialSize = fieldCount * 64;
        BinaryRowWriter writer = new BinaryRowWriter(binaryRowData, initialSize);
        writer.reset();

        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                writer.setNullAt(i);
            } else {
                DataType fieldType = statsRowType.getTypeAt(i);
                switch (fieldType.getTypeRoot()) {
                    case BOOLEAN:
                        writer.writeBoolean(i, row.getBoolean(i));
                        break;
                    case TINYINT:
                        writer.writeByte(i, row.getByte(i));
                        break;
                    case SMALLINT:
                        writer.writeShort(i, row.getShort(i));
                        break;
                    case INTEGER:
                    case DATE:
                    case TIME_WITHOUT_TIME_ZONE:
                        writer.writeInt(i, row.getInt(i));
                        break;
                    case BIGINT:
                        writer.writeLong(i, row.getLong(i));
                        break;
                    case FLOAT:
                        writer.writeFloat(i, row.getFloat(i));
                        break;
                    case DOUBLE:
                        writer.writeDouble(i, row.getDouble(i));
                        break;
                    case STRING:
                        writer.writeString(i, row.getString(i));
                        break;
                    case DECIMAL:
                        DecimalType decimalType = (DecimalType) fieldType;
                        writer.writeDecimal(
                                i,
                                row.getDecimal(
                                        i, decimalType.getPrecision(), decimalType.getScale()),
                                decimalType.getPrecision());
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        TimestampType timestampType = (TimestampType) fieldType;
                        writer.writeTimestampNtz(
                                i,
                                row.getTimestampNtz(i, timestampType.getPrecision()),
                                timestampType.getPrecision());
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        LocalZonedTimestampType localZonedTimestampType =
                                (LocalZonedTimestampType) fieldType;
                        writer.writeTimestampLtz(
                                i,
                                row.getTimestampLtz(i, localZonedTimestampType.getPrecision()),
                                localZonedTimestampType.getPrecision());
                        break;
                    default:
                        // For unsupported types, write as null
                        writer.setNullAt(i);
                        break;
                }
            }
        }

        writer.complete();
        return binaryRowData;
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
}
