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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Collector for {@link LogRecordBatchStatistics} that accumulates statistics during record batch
 * construction. Manages statistics data in memory arrays and supports schema-aware statistics
 * format.
 */
public class LogRecordBatchStatisticsCollector {

    private final RowType rowType;
    private final int[] statsIndexMapping;

    // Statistics arrays (only for columns that need statistics)
    private final Object[] minValues;
    private final Object[] maxValues;
    private final Long[] nullCounts;
    private final boolean[] minSet;
    private final boolean[] maxSet;

    private final LogRecordBatchStatisticsWriter statisticsWriter;

    public LogRecordBatchStatisticsCollector(RowType rowType, int[] statsIndexMapping) {
        this.rowType = rowType;
        this.statsIndexMapping = statsIndexMapping;

        // Initialize statistics arrays
        this.minValues = new Object[statsIndexMapping.length];
        this.maxValues = new Object[statsIndexMapping.length];
        this.nullCounts = new Long[statsIndexMapping.length];
        this.minSet = new boolean[statsIndexMapping.length];
        this.maxSet = new boolean[statsIndexMapping.length];

        this.statisticsWriter = new LogRecordBatchStatisticsWriter(rowType, statsIndexMapping);

        Arrays.fill(minSet, false);
        Arrays.fill(maxSet, false);
        Arrays.fill(nullCounts, 0L);
    }

    /**
     * Process a row and update statistics for configured columns only.
     *
     * @param row The row to process
     */
    public void processRow(InternalRow row) {
        for (int statIndex = 0; statIndex < statsIndexMapping.length; statIndex++) {
            int fullRowIndex = statsIndexMapping[statIndex];
            if (row.isNullAt(fullRowIndex)) {
                nullCounts[statIndex]++;
            } else {
                updateMinMax(statIndex, fullRowIndex, row);
            }
        }
    }

    /**
     * Write the collected statistics to an OutputView.
     *
     * @param outputView The target output view
     * @return The number of bytes written, or 0 if no statistics collected
     * @throws IOException If writing fails
     */
    public int writeStatistics(OutputView outputView) throws IOException {
        return statisticsWriter.writeStatistics(
                GenericRow.of(minValues), GenericRow.of(maxValues), nullCounts, outputView);
    }

    /**
     * Get the row type used by this collector.
     *
     * @return The row type
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Estimate the size in bytes that would be required to serialize the currently collected
     * statistics. This method provides an efficient way to calculate statistics size without
     * actually writing the data to memory.
     *
     * @return The estimated number of bytes required for serialization
     */
    public int estimatedSizeInBytes() {
        return statisticsWriter.estimatedSizeInBytes(
                GenericRow.of(minValues), GenericRow.of(maxValues), nullCounts);
    }

    /** Reset the collector to collect new statistics. */
    public void reset() {
        Arrays.fill(minSet, false);
        Arrays.fill(maxSet, false);
        Arrays.fill(nullCounts, 0L);
        Arrays.fill(minValues, null);
        Arrays.fill(maxValues, null);
    }

    /**
     * Update min/max values for a specific field.
     *
     * @param statsIndex the index in the statistics arrays
     * @param schemaIndex the index in the full schema
     * @param row the row being processed
     */
    private void updateMinMax(int statsIndex, int schemaIndex, InternalRow row) {
        DataType fieldType = rowType.getTypeAt(schemaIndex);

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                boolean boolValue = row.getBoolean(schemaIndex);
                updateMinMaxInternal(statsIndex, boolValue, Boolean::compare);
                break;
            case TINYINT:
                byte byteValue = row.getByte(schemaIndex);
                updateMinMaxInternal(statsIndex, byteValue, Byte::compare);
                break;
            case SMALLINT:
                short shortValue = row.getShort(schemaIndex);
                updateMinMaxInternal(statsIndex, shortValue, Short::compare);
                break;
            case INTEGER:
                int intValue = row.getInt(schemaIndex);
                updateMinMaxInternal(statsIndex, intValue, Integer::compare);
                break;
            case BIGINT:
                long longValue = row.getLong(schemaIndex);
                updateMinMaxInternal(statsIndex, longValue, Long::compare);
                break;
            case FLOAT:
                float floatValue = row.getFloat(schemaIndex);
                updateMinMaxInternal(statsIndex, floatValue, Float::compare);
                break;
            case DOUBLE:
                double doubleValue = row.getDouble(schemaIndex);
                updateMinMaxInternal(statsIndex, doubleValue, Double::compare);
                break;
            case STRING:
                BinaryString stringValue = row.getString(schemaIndex);
                updateMinMaxInternal(
                        statsIndex, stringValue, BinaryString::compareTo, BinaryString::copy);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                Decimal decimalValue =
                        row.getDecimal(
                                schemaIndex, decimalType.getPrecision(), decimalType.getScale());
                updateMinMaxInternal(statsIndex, decimalValue, Decimal::compareTo);
                break;
            case DATE:
                int dateValue = row.getInt(schemaIndex);
                updateMinMaxInternal(statsIndex, dateValue, Integer::compare);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                int timeValue = row.getInt(schemaIndex);
                updateMinMaxInternal(statsIndex, timeValue, Integer::compare);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                TimestampNtz timestampNtzValue =
                        row.getTimestampNtz(schemaIndex, timestampType.getPrecision());
                updateMinMaxInternal(statsIndex, timestampNtzValue, TimestampNtz::compareTo);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) fieldType;
                TimestampLtz timestampLtzValue =
                        row.getTimestampLtz(schemaIndex, localZonedTimestampType.getPrecision());
                updateMinMaxInternal(statsIndex, timestampLtzValue, TimestampLtz::compareTo);
                break;
            default:
                // For unsupported types, don't collect min/max
                break;
        }
    }

    // Unified generic method for all min/max updates with optional value transformation
    private <T> void updateMinMaxInternal(
            int statsIndex,
            T value,
            java.util.Comparator<T> comparator,
            Function<T, T> valueTransformer) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = valueTransformer.apply(value);
            minSet[statsIndex] = true;
        } else {
            T currentMin = (T) minValues[statsIndex];
            if (comparator.compare(value, currentMin) < 0) {
                minValues[statsIndex] = valueTransformer.apply(value);
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = valueTransformer.apply(value);
            maxSet[statsIndex] = true;
        } else {
            T currentMax = (T) maxValues[statsIndex];
            if (comparator.compare(value, currentMax) > 0) {
                maxValues[statsIndex] = valueTransformer.apply(value);
            }
        }
    }

    // Convenience method for values that don't need transformation
    private <T> void updateMinMaxInternal(
            int statsIndex, T value, java.util.Comparator<T> comparator) {
        updateMinMaxInternal(statsIndex, value, comparator, Function.identity());
    }
}
