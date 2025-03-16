/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lakehouse.paimon.reader;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

/**
 * A wrapper for Paimon's InternalRow to bridge the Paimon's InternalRow to Fluss's InternalRow. .
 */
public class PaimonRowWrapper implements InternalRow {
    private final org.apache.paimon.data.InternalRow paimonRow;

    public PaimonRowWrapper(org.apache.paimon.data.InternalRow paimonRow) {
        this.paimonRow = paimonRow;
    }

    @Override
    public int getFieldCount() {
        return paimonRow.getFieldCount();
    }

    @Override
    public boolean isNullAt(int pos) {
        return paimonRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return paimonRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return paimonRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return paimonRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return paimonRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return paimonRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return paimonRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return paimonRow.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(paimonRow.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(paimonRow.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return Decimal.fromBigDecimal(
                paimonRow.getDecimal(pos, precision, scale).toBigDecimal(), precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return TimestampNtz.fromLocalDateTime(
                paimonRow.getTimestamp(pos, precision).toLocalDateTime());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return TimestampLtz.fromInstant(paimonRow.getTimestamp(pos, precision).toInstant());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return paimonRow.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return paimonRow.getBinary(pos);
    }
}
