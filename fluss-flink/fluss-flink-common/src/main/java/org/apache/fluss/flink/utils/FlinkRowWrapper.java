/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;

/** Convert from Flink row data. */
public class FlinkRowWrapper implements InternalRow {

    private final org.apache.flink.table.data.RowData row;

    public FlinkRowWrapper(org.apache.flink.table.data.RowData row) {
        this.row = row;
    }

    @Override
    public int getFieldCount() {
        return row.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(row.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return fromFlinkString(row.getString(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(row.getDecimal(pos, precision, scale));
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return fromFlinkTimestampNtz(row.getTimestamp(pos, precision));
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return fromFlinkTimestampLtz(row.getTimestamp(pos, precision));
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return row.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new FlinkArrayWrapper(row.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new FlinkMapWrapper(row.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new FlinkRowWrapper(row.getRow(pos, numFields));
    }

    public static BinaryString fromFlinkString(StringData str) {
        return BinaryString.fromBytes(str.toBytes());
    }

    public static TimestampNtz fromFlinkTimestampNtz(
            org.apache.flink.table.data.TimestampData timestamp) {
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    public static TimestampLtz fromFlinkTimestampLtz(
            org.apache.flink.table.data.TimestampData timestamp) {
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
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
}
