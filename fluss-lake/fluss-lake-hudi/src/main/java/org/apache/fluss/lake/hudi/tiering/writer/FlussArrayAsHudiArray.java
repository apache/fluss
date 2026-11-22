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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

/** Wraps a Fluss {@link InternalArray} as a Hudi/Flink {@link ArrayData}. */
public class FlussArrayAsHudiArray implements ArrayData {

    private final InternalArray flussArray;
    private final LogicalType elementType;

    public FlussArrayAsHudiArray(InternalArray flussArray, LogicalType elementType) {
        this.flussArray = flussArray;
        this.elementType = elementType;
    }

    @Override
    public int size() {
        return flussArray.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return flussArray.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return flussArray.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return flussArray.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return flussArray.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return flussArray.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return flussArray.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return flussArray.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return flussArray.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromBytes(flussArray.getString(pos).toBytes());
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        Decimal flussDecimal = flussArray.getDecimal(pos, precision, scale);
        if (flussDecimal.isCompact()) {
            return DecimalData.fromUnscaledLong(flussDecimal.toUnscaledLong(), precision, scale);
        }
        return DecimalData.fromBigDecimal(flussDecimal.toBigDecimal(), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        LogicalTypeRoot typeRoot = elementType.getTypeRoot();
        if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            TimestampNtz timestampNtz = flussArray.getTimestampNtz(pos, precision);
            return TimestampData.fromLocalDateTime(timestampNtz.toLocalDateTime());
        } else if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            TimestampLtz timestampLtz = flussArray.getTimestampLtz(pos, precision);
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
        return flussArray.getBytes(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        InternalArray innerArray = flussArray.getArray(pos);
        return innerArray == null
                ? null
                : new FlussArrayAsHudiArray(innerArray, ((ArrayType) elementType).getElementType());
    }

    @Override
    public MapData getMap(int pos) {
        InternalMap flussMap = flussArray.getMap(pos);
        MapType mapType = (MapType) elementType;
        return flussMap == null
                ? null
                : new FlussMapAsHudiMap(flussMap, mapType.getKeyType(), mapType.getValueType());
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        InternalRow nestedFlussRow = flussArray.getRow(pos, numFields);
        return nestedFlussRow == null
                ? null
                : new FlussRowAsHudiRow(nestedFlussRow, (RowType) elementType);
    }

    @Override
    public boolean[] toBooleanArray() {
        return flussArray.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return flussArray.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return flussArray.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return flussArray.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return flussArray.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return flussArray.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return flussArray.toDoubleArray();
    }
}
