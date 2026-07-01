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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.variant.Variant;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import static org.apache.fluss.lake.hudi.HudiLakeCatalog.SYSTEM_COLUMNS;

/** Wraps a Hudi/Flink {@link RowData} as a Fluss {@link InternalRow}. */
public class HudiRowAsFlussRow implements InternalRow {

    private RowData rowData;
    private final boolean stripSystemColumns;

    public HudiRowAsFlussRow() {
        this(true);
    }

    public HudiRowAsFlussRow(RowData rowData) {
        this(rowData, true);
    }

    HudiRowAsFlussRow(RowData rowData, boolean stripSystemColumns) {
        this.rowData = rowData;
        this.stripSystemColumns = stripSystemColumns;
    }

    private HudiRowAsFlussRow(boolean stripSystemColumns) {
        this.stripSystemColumns = stripSystemColumns;
    }

    public HudiRowAsFlussRow replaceRow(RowData rowData) {
        this.rowData = rowData;
        return this;
    }

    @Override
    public int getFieldCount() {
        return stripSystemColumns ? rowData.getArity() - SYSTEM_COLUMNS.size() : rowData.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return rowData.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return rowData.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return rowData.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return rowData.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return rowData.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return rowData.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return rowData.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return rowData.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(rowData.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(rowData.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        DecimalData decimalData = rowData.getDecimal(pos, precision, scale);
        if (decimalData.isCompact()) {
            return Decimal.fromUnscaledLong(decimalData.toUnscaledLong(), precision, scale);
        }
        return Decimal.fromBigDecimal(decimalData.toBigDecimal(), precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        TimestampData timestamp = rowData.getTimestamp(pos, precision);
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(timestamp.getMillisecond());
        }
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        TimestampData timestamp = rowData.getTimestamp(pos, precision);
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(timestamp.getMillisecond());
        }
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return rowData.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return rowData.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        ArrayData value = rowData.getArray(pos);
        return value == null ? null : new HudiArrayAsFlussArray(value);
    }

    @Override
    public InternalMap getMap(int pos) {
        MapData value = rowData.getMap(pos);
        return value == null ? null : new HudiMapAsFlussMap(value);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        RowData value = rowData.getRow(pos, numFields);
        return value == null ? null : new HudiRowAsFlussRow(value, false);
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("Variant type is not supported in Hudi.");
    }
}
