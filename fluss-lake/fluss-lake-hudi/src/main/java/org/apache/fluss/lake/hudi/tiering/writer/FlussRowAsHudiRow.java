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

package org.apache.fluss.lake.hudi.tiering.writer;

import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.indexed.IndexedRow;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.util.ObjectSizeCalculator;

/** Wraps a Fluss {@link InternalRow} as a Hudi/Flink {@link RowData}. */
public class FlussRowAsHudiRow implements RowData {

    protected InternalRow internalRow;
    protected final RowType rowType;

    public FlussRowAsHudiRow(RowType rowType) {
        this.rowType = rowType;
    }

    public FlussRowAsHudiRow(InternalRow internalRow, RowType rowType) {
        this.internalRow = internalRow;
        this.rowType = rowType;
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public boolean isNullAt(int pos) {
        return internalRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return internalRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return internalRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return internalRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return internalRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return internalRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return internalRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return internalRow.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromBytes(internalRow.getString(pos).toBytes());
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        Decimal flussDecimal = internalRow.getDecimal(pos, precision, scale);
        if (flussDecimal.isCompact()) {
            return DecimalData.fromUnscaledLong(flussDecimal.toUnscaledLong(), precision, scale);
        }
        return DecimalData.fromBigDecimal(flussDecimal.toBigDecimal(), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        LogicalTypeRoot typeRoot = rowType.getTypeAt(pos).getTypeRoot();
        if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            TimestampNtz timestampNtz = internalRow.getTimestampNtz(pos, precision);
            return TimestampData.fromLocalDateTime(timestampNtz.toLocalDateTime());
        } else if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            TimestampLtz timestampLtz = internalRow.getTimestampLtz(pos, precision);
            return TimestampData.fromEpochMillis(
                    timestampLtz.getEpochMillisecond(), timestampLtz.getNanoOfMillisecond());
        }
        throw new UnsupportedOperationException("Unsupported timestamp type: " + typeRoot);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException("getRawValue is not supported for Fluss records.");
    }

    @Override
    public byte[] getBinary(int pos) {
        return internalRow.getBytes(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        InternalArray array = internalRow.getArray(pos);
        return array == null
                ? null
                : new FlussArrayAsHudiArray(
                        array, ((ArrayType) rowType.getTypeAt(pos)).getElementType());
    }

    @Override
    public MapData getMap(int pos) {
        InternalMap map = internalRow.getMap(pos);
        MapType mapType = (MapType) rowType.getTypeAt(pos);
        return map == null
                ? null
                : new FlussMapAsHudiMap(map, mapType.getKeyType(), mapType.getValueType());
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        InternalRow row = internalRow.getRow(pos, numFields);
        return row == null ? null : new FlussRowAsHudiRow(row, (RowType) rowType.getTypeAt(pos));
    }

    @Override
    public int getArity() {
        return rowType.getFieldCount();
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // Fluss records expose their row kind through LogRecord change type.
    }

    public long sizeInBytes() {
        if (internalRow instanceof IndexedRow) {
            return ((IndexedRow) internalRow).getSizeInBytes();
        } else if (internalRow instanceof CompactedRow) {
            return ((CompactedRow) internalRow).getSizeInBytes();
        }
        return ObjectSizeCalculator.getObjectSize(internalRow);
    }
}
