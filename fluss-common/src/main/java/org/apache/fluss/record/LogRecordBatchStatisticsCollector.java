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

/**
 * Collector for {@link LogRecordBatchStatistics} that accumulates statistics during record batch
 * construction using IndexedRow for optimal space efficiency and better deserialization
 * performance. Only supports schema-aware statistics format.
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
                updateBooleanMinMax(statsIndex, boolValue);
                break;
            case TINYINT:
                byte byteValue = row.getByte(schemaIndex);
                updateByteMinMax(statsIndex, byteValue);
                break;
            case SMALLINT:
                short shortValue = row.getShort(schemaIndex);
                updateShortMinMax(statsIndex, shortValue);
                break;
            case INTEGER:
                int intValue = row.getInt(schemaIndex);
                updateIntMinMax(statsIndex, intValue);
                break;
            case BIGINT:
                long longValue = row.getLong(schemaIndex);
                updateLongMinMax(statsIndex, longValue);
                break;
            case FLOAT:
                float floatValue = row.getFloat(schemaIndex);
                updateFloatMinMax(statsIndex, floatValue);
                break;
            case DOUBLE:
                double doubleValue = row.getDouble(schemaIndex);
                updateDoubleMinMax(statsIndex, doubleValue);
                break;
            case STRING:
                BinaryString stringValue = row.getString(schemaIndex);
                updateStringMinMax(statsIndex, stringValue);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                Decimal decimalValue =
                        row.getDecimal(
                                schemaIndex, decimalType.getPrecision(), decimalType.getScale());
                updateDecimalMinMax(statsIndex, decimalValue);
                break;
            case DATE:
                int dateValue = row.getInt(schemaIndex);
                updateDateMinMax(statsIndex, dateValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                int timeValue = row.getInt(schemaIndex);
                updateTimeMinMax(statsIndex, timeValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                TimestampNtz timestampNtzValue =
                        row.getTimestampNtz(schemaIndex, timestampType.getPrecision());
                updateTimestampNtzMinMax(statsIndex, timestampNtzValue);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) fieldType;
                TimestampLtz timestampLtzValue =
                        row.getTimestampLtz(schemaIndex, localZonedTimestampType.getPrecision());
                updateTimestampLtzMinMax(statsIndex, timestampLtzValue);
                break;
            default:
                // For unsupported types, don't collect min/max
                break;
        }
    }

    private void updateBooleanMinMax(int statsIndex, boolean value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            boolean currentMin = (Boolean) minValues[statsIndex];
            if (!value && currentMin) {
                minValues[statsIndex] = false;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            boolean currentMax = (Boolean) maxValues[statsIndex];
            if (value && !currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateByteMinMax(int statsIndex, byte value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            byte currentMin = (Byte) minValues[statsIndex];
            if (value < currentMin) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            byte currentMax = (Byte) maxValues[statsIndex];
            if (value > currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateShortMinMax(int statsIndex, short value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            short currentMin = (Short) minValues[statsIndex];
            if (value < currentMin) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            short currentMax = (Short) maxValues[statsIndex];
            if (value > currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateIntMinMax(int statsIndex, int value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            int currentMin = (Integer) minValues[statsIndex];
            if (value < currentMin) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            int currentMax = (Integer) maxValues[statsIndex];
            if (value > currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateLongMinMax(int statsIndex, long value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            long currentMin = (Long) minValues[statsIndex];
            if (value < currentMin) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            long currentMax = (Long) maxValues[statsIndex];
            if (value > currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateFloatMinMax(int statsIndex, float value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            float currentMin = (Float) minValues[statsIndex];
            if (value < currentMin) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            float currentMax = (Float) maxValues[statsIndex];
            if (value > currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateDoubleMinMax(int statsIndex, double value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            double currentMin = (Double) minValues[statsIndex];
            if (value < currentMin) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            double currentMax = (Double) maxValues[statsIndex];
            if (value > currentMax) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateStringMinMax(int statsIndex, BinaryString value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value.copy();
            minSet[statsIndex] = true;
        } else {
            BinaryString currentMin = (BinaryString) minValues[statsIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[statsIndex] = value.copy();
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value.copy();
            maxSet[statsIndex] = true;
        } else {
            BinaryString currentMax = (BinaryString) maxValues[statsIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[statsIndex] = value.copy();
            }
        }
    }

    private void updateDecimalMinMax(int statsIndex, Decimal value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            Decimal currentMin = (Decimal) minValues[statsIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            Decimal currentMax = (Decimal) maxValues[statsIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateDateMinMax(int statsIndex, int value) {
        updateIntMinMax(statsIndex, value);
    }

    private void updateTimeMinMax(int statsIndex, int value) {
        updateIntMinMax(statsIndex, value);
    }

    private void updateTimestampNtzMinMax(int statsIndex, TimestampNtz value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            TimestampNtz currentMin = (TimestampNtz) minValues[statsIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            TimestampNtz currentMax = (TimestampNtz) maxValues[statsIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[statsIndex] = value;
            }
        }
    }

    private void updateTimestampLtzMinMax(int statsIndex, TimestampLtz value) {
        if (!minSet[statsIndex]) {
            minValues[statsIndex] = value;
            minSet[statsIndex] = true;
        } else {
            TimestampLtz currentMin = (TimestampLtz) minValues[statsIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[statsIndex] = value;
            }
        }

        if (!maxSet[statsIndex]) {
            maxValues[statsIndex] = value;
            maxSet[statsIndex] = true;
        } else {
            TimestampLtz currentMax = (TimestampLtz) maxValues[statsIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[statsIndex] = value;
            }
        }
    }
}
