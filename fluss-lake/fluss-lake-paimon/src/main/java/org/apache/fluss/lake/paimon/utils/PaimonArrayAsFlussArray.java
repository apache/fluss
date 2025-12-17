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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.Timestamp;

/** Wraps a Paimon {@link org.apache.paimon.data.InternalArray} as a Fluss {@link InternalArray}. */
public class PaimonArrayAsFlussArray implements InternalArray {

    private final org.apache.paimon.data.InternalArray paimonArray;

    public PaimonArrayAsFlussArray(org.apache.paimon.data.InternalArray paimonArray) {
        this.paimonArray = paimonArray;
    }

    @Override
    public int size() {
        return paimonArray.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return paimonArray.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return paimonArray.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return paimonArray.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return paimonArray.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return paimonArray.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return paimonArray.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return paimonArray.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return paimonArray.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(paimonArray.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(paimonArray.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        org.apache.paimon.data.Decimal paimonDecimal =
                paimonArray.getDecimal(pos, precision, scale);
        if (paimonDecimal.isCompact()) {
            return Decimal.fromUnscaledLong(paimonDecimal.toUnscaledLong(), precision, scale);
        } else {
            return Decimal.fromBigDecimal(paimonDecimal.toBigDecimal(), precision, scale);
        }
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        Timestamp timestamp = paimonArray.getTimestamp(pos, precision);
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(timestamp.getMillisecond());
        } else {
            return TimestampNtz.fromMillis(
                    timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
        }
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        Timestamp timestamp = paimonArray.getTimestamp(pos, precision);
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(timestamp.getMillisecond());
        } else {
            return TimestampLtz.fromEpochMillis(
                    timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
        }
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return paimonArray.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return paimonArray.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new PaimonArrayAsFlussArray(paimonArray.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new PaimonMapAsFlussMap(paimonArray.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new PaimonRowAsFlussRow(paimonArray.getRow(pos, numFields));
    }

    @Override
    public boolean[] toBooleanArray() {
        return paimonArray.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return paimonArray.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return paimonArray.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return paimonArray.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return paimonArray.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return paimonArray.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return paimonArray.toDoubleArray();
    }
}
