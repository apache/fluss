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

/** Wraps a Hudi/Flink {@link ArrayData} as a Fluss {@link InternalArray}. */
public class HudiArrayAsFlussArray implements InternalArray {

    private final ArrayData hudiArray;

    public HudiArrayAsFlussArray(ArrayData hudiArray) {
        this.hudiArray = hudiArray;
    }

    @Override
    public int size() {
        return hudiArray.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return hudiArray.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return hudiArray.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return hudiArray.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return hudiArray.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return hudiArray.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return hudiArray.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return hudiArray.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return hudiArray.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(hudiArray.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(hudiArray.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        DecimalData decimalData = hudiArray.getDecimal(pos, precision, scale);
        if (decimalData.isCompact()) {
            return Decimal.fromUnscaledLong(decimalData.toUnscaledLong(), precision, scale);
        }
        return Decimal.fromBigDecimal(decimalData.toBigDecimal(), precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        TimestampData timestamp = hudiArray.getTimestamp(pos, precision);
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(timestamp.getMillisecond());
        }
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        TimestampData timestamp = hudiArray.getTimestamp(pos, precision);
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(timestamp.getMillisecond());
        }
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return hudiArray.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return hudiArray.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        ArrayData nestedArray = hudiArray.getArray(pos);
        return nestedArray == null ? null : new HudiArrayAsFlussArray(nestedArray);
    }

    @Override
    public InternalMap getMap(int pos) {
        MapData nestedMap = hudiArray.getMap(pos);
        return nestedMap == null ? null : new HudiMapAsFlussMap(nestedMap);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        RowData nestedRow = hudiArray.getRow(pos, numFields);
        return nestedRow == null ? null : new HudiRowAsFlussRow(nestedRow, false);
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("Variant type is not supported in Hudi.");
    }

    @Override
    public boolean[] toBooleanArray() {
        int arraySize = hudiArray.size();
        boolean[] result = new boolean[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getBoolean(i);
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        int arraySize = hudiArray.size();
        byte[] result = new byte[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getByte(i);
        }
        return result;
    }

    @Override
    public short[] toShortArray() {
        int arraySize = hudiArray.size();
        short[] result = new short[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getShort(i);
        }
        return result;
    }

    @Override
    public int[] toIntArray() {
        int arraySize = hudiArray.size();
        int[] result = new int[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getInt(i);
        }
        return result;
    }

    @Override
    public long[] toLongArray() {
        int arraySize = hudiArray.size();
        long[] result = new long[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getLong(i);
        }
        return result;
    }

    @Override
    public float[] toFloatArray() {
        int arraySize = hudiArray.size();
        float[] result = new float[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getFloat(i);
        }
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        int arraySize = hudiArray.size();
        double[] result = new double[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = hudiArray.getDouble(i);
        }
        return result;
    }
}
