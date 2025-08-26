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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

/** Wraps a Flink {@link RowData} as a Fluss {@link InternalRow}. */
public class FlinkAsFlussRow implements InternalRow {

    private RowData flinkRow;

    public FlinkAsFlussRow() {}

    public FlinkAsFlussRow replace(RowData flinkRow) {
        this.flinkRow = flinkRow;
        return this;
    }

    @Override
    public int getFieldCount() {
        return flinkRow.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return flinkRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return flinkRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return flinkRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return flinkRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return flinkRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return flinkRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return flinkRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return flinkRow.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(flinkRow.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(flinkRow.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(flinkRow.getDecimal(pos, precision, scale));
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
        TimestampData timestamp = flinkRow.getTimestamp(pos, precision);
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        TimestampData timestamp = flinkRow.getTimestamp(pos, precision);
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return flinkRow.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return flinkRow.getBinary(pos);
    }
}
