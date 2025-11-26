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

package org.apache.fluss.flink.row;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import javax.annotation.Nullable;

import java.util.List;

/** Wraps a Flink {@link RowData} as a Fluss {@link InternalRow}. */
public class FlinkAsFlussRow implements InternalRow {

    private RowData flinkRow;

    /**
     * Creates a FlinkAsFlussRow with a mapping from Fluss field index to Flink field index. If the
     * indexMapping is null, flink and flink share same field name.
     */
    @Nullable private final int[] indexMapping;

    public FlinkAsFlussRow() {
        this(null);
    }

    public FlinkAsFlussRow(int[] indexMapping) {
        this.indexMapping = indexMapping;
    }

    public FlinkAsFlussRow replace(RowData flinkRow) {
        this.flinkRow = flinkRow;
        return this;
    }

    @Override
    public int getFieldCount() {
        if (indexMapping == null) {
            return flinkRow.getArity();
        } else {
            return indexMapping.length;
        }
    }

    @Override
    public boolean isNullAt(int pos) {
        // if no mapping, means that the field is not found in Flink, just ignore it.
        if ((indexMapping != null && indexMapping[pos] == -1)) {
            return true;
        }

        // If pos is larger than flinkRow.getArity(), it indicates that the Fluss table's data type
        // is wider than the data provided by Flink.
        // This often occurs when a schema change happens to the catalog table before job restart.
        // Only appending columns at the end is compatible, just need to  ignore the trailing
        // columns.
        // Other types of schema changes (e.g., dropping columns) would lead to incompatibility.
        // Therefore, Fluss currently only supports appending columns, and here we simply ignore any
        // extra columns.
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        if (flussPos >= flinkRow.getArity()) {
            return true;
        }

        return flinkRow.isNullAt(flussPos);
    }

    @Override
    public boolean getBoolean(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getBoolean(flussPos);
    }

    @Override
    public byte getByte(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getByte(flussPos);
    }

    @Override
    public short getShort(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getShort(flussPos);
    }

    @Override
    public int getInt(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getInt(flussPos);
    }

    @Override
    public long getLong(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getLong(flussPos);
    }

    @Override
    public float getFloat(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getFloat(flussPos);
    }

    @Override
    public double getDouble(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getDouble(flussPos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return BinaryString.fromBytes(flinkRow.getString(flussPos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return BinaryString.fromBytes(flinkRow.getString(flussPos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return fromFlinkDecimal(flinkRow.getDecimal(flussPos, precision, scale));
    }

    public static Decimal fromFlinkDecimal(DecimalData decimal) {
        if (decimal.isCompact()) {
            return Decimal.fromUnscaledLong(
                    decimal.toUnscaledLong(), decimal.precision(), decimal.scale());
        } else {
            return Decimal.fromBigDecimal(
                    decimal.toBigDecimal(), decimal.precision(), decimal.scale());
        }
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        TimestampData timestamp = flinkRow.getTimestamp(flussPos, precision);
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        TimestampData timestamp = flinkRow.getTimestamp(flussPos, precision);
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getBinary(flussPos);
    }

    @Override
    public byte[] getBytes(int pos) {
        int flussPos = indexMapping == null ? pos : indexMapping[pos];
        return flinkRow.getBinary(flussPos);
    }

    /** Converts a Flink {@link RowType} to a Fluss {@link RowType}, based on the field name. */
    public static FlinkAsFlussRow from(
            org.apache.flink.table.types.logical.RowType flinkRowType, RowType flussRowType) {
        int[] indexMapping = new int[flussRowType.getFieldCount()];
        List<String> fieldNames = flussRowType.getFieldNames();
        for (int i = 0; i < flussRowType.getFieldCount(); i++) {
            String fieldName = fieldNames.get(i);
            int fieldIndex = flinkRowType.getFieldIndex(fieldName);
            indexMapping[i] = fieldIndex;
        }
        return new FlinkAsFlussRow(indexMapping);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new FlinkAsFlussArray(flinkRow.getArray(pos));
    }

    // TODO: Support Map type conversion from Flink to Fluss
    // TODO: Support Row type conversion from Flink to Fluss
}
