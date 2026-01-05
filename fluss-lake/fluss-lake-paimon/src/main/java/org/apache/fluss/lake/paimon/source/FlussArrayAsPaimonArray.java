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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;

/** Adapter class for converting Fluss InternalArray to Paimon InternalArray. */
public class FlussArrayAsPaimonArray implements InternalArray {

    private final org.apache.fluss.row.InternalArray flussArray;
    private final DataType elementType;

    public FlussArrayAsPaimonArray(
            org.apache.fluss.row.InternalArray flussArray, DataType elementType) {
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
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(flussArray.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        org.apache.fluss.row.Decimal flussDecimal = flussArray.getDecimal(pos, precision, scale);
        if (flussDecimal.isCompact()) {
            return Decimal.fromUnscaledLong(flussDecimal.toUnscaledLong(), precision, scale);
        } else {
            return Decimal.fromBigDecimal(flussDecimal.toBigDecimal(), precision, scale);
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        // Default to TIMESTAMP_WITHOUT_TIME_ZONE behavior for arrays
        switch (elementType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (TimestampNtz.isCompact(precision)) {
                    return Timestamp.fromEpochMillis(
                            flussArray.getTimestampNtz(pos, precision).getMillisecond());
                } else {
                    TimestampNtz timestampNtz = flussArray.getTimestampNtz(pos, precision);
                    return Timestamp.fromEpochMillis(
                            timestampNtz.getMillisecond(), timestampNtz.getNanoOfMillisecond());
                }

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (TimestampLtz.isCompact(precision)) {
                    return Timestamp.fromEpochMillis(
                            flussArray.getTimestampLtz(pos, precision).getEpochMillisecond());
                } else {
                    TimestampLtz timestampLtz = flussArray.getTimestampLtz(pos, precision);
                    return Timestamp.fromEpochMillis(
                            timestampLtz.getEpochMillisecond(),
                            timestampLtz.getNanoOfMillisecond());
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported array element type for getTimestamp. "
                                + "Only TIMESTAMP_WITHOUT_TIME_ZONE and "
                                + "TIMESTAMP_WITH_LOCAL_TIME_ZONE are supported, but got: "
                                + elementType.getTypeRoot()
                                + " ("
                                + elementType
                                + ").");
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        return flussArray.getBytes(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException(
                "getVariant is not supported for Fluss array currently.");
    }

    @Override
    public InternalArray getArray(int pos) {
        org.apache.fluss.row.InternalArray innerArray = flussArray.getArray(pos);
        return innerArray == null
                ? null
                : new FlussArrayAsPaimonArray(
                        innerArray, ((ArrayType) elementType).getElementType());
    }

    @Override
    public InternalMap getMap(int pos) {
        org.apache.fluss.row.InternalMap flussMap = flussArray.getMap(pos);
        if (flussMap == null) {
            return null;
        }
        org.apache.paimon.types.MapType mapType = (org.apache.paimon.types.MapType) elementType;
        return new FlussMapAsPaimonMap(flussMap, mapType.getKeyType(), mapType.getValueType());
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        org.apache.fluss.row.InternalRow nestedFlussRow = flussArray.getRow(pos, numFields);
        return nestedFlussRow == null
                ? null
                : new FlussRowAsPaimonRow(
                        nestedFlussRow, (org.apache.paimon.types.RowType) elementType);
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
