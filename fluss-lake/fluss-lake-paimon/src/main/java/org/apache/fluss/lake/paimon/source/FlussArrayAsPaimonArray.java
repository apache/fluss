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

import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;

/** Adapter class for converting Fluss InternalArray to Paimon InternalArray. */
public class FlussArrayAsPaimonArray implements InternalArray {

    private final org.apache.fluss.row.InternalArray flussArray;

    public FlussArrayAsPaimonArray(org.apache.fluss.row.InternalArray flussArray) {
        this.flussArray = flussArray;
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
        if (TimestampNtz.isCompact(precision)) {
            return Timestamp.fromEpochMillis(
                    flussArray.getTimestampNtz(pos, precision).getMillisecond());
        } else {
            TimestampNtz timestampNtz = flussArray.getTimestampNtz(pos, precision);
            return Timestamp.fromEpochMillis(
                    timestampNtz.getMillisecond(), timestampNtz.getNanoOfMillisecond());
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
        return innerArray == null ? null : new FlussArrayAsPaimonArray(innerArray);
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException(
                "getMap is not supported for Fluss array currently.");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException(
                "getRow is not supported for Fluss array currently.");
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
