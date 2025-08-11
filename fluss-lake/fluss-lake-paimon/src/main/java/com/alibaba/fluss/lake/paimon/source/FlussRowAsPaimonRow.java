/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

/** Adapter class for converting Fluss row to Paimon row. */
public class FlussRowAsPaimonRow implements InternalRow {

    protected com.alibaba.fluss.row.InternalRow internalRow;
    protected final RowType tableRowType;

    public FlussRowAsPaimonRow(RowType tableTowType) {
        this.tableRowType = tableTowType;
    }

    public FlussRowAsPaimonRow(
            com.alibaba.fluss.row.InternalRow internalRow, RowType tableTowType) {
        this.internalRow = internalRow;
        this.tableRowType = tableTowType;
    }

    @Override
    public int getFieldCount() {
        return internalRow.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // do nothing
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
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(internalRow.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        com.alibaba.fluss.row.Decimal flussDecimal = internalRow.getDecimal(pos, precision, scale);
        if (flussDecimal.isCompact()) {
            return Decimal.fromUnscaledLong(flussDecimal.toUnscaledLong(), precision, scale);
        } else {
            return Decimal.fromBigDecimal(flussDecimal.toBigDecimal(), precision, scale);
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        DataType paimonTimestampType = tableRowType.getTypeAt(pos);
        switch (paimonTimestampType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (TimestampNtz.isCompact(precision)) {
                    return Timestamp.fromEpochMillis(
                            internalRow.getTimestampNtz(pos, precision).getMillisecond());
                } else {
                    TimestampNtz timestampNtz = internalRow.getTimestampNtz(pos, precision);
                    return Timestamp.fromEpochMillis(
                            timestampNtz.getMillisecond(), timestampNtz.getNanoOfMillisecond());
                }

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (TimestampLtz.isCompact(precision)) {
                    return Timestamp.fromEpochMillis(
                            internalRow.getTimestampLtz(pos, precision).getEpochMillisecond());
                } else {
                    TimestampLtz timestampLtz = internalRow.getTimestampLtz(pos, precision);
                    return Timestamp.fromEpochMillis(
                            timestampLtz.getEpochMillisecond(),
                            timestampLtz.getNanoOfMillisecond());
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type to get timestamp: " + paimonTimestampType);
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        return internalRow.getBytes(pos);
    }

    @Override
    public Variant getVariant(int i) {
        throw new UnsupportedOperationException(
                "getVariant is not support for Fluss record currently.");
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException(
                "getArray is not support for Fluss record currently.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException(
                "getMap is not support for Fluss record currently.");
    }

    @Override
    public InternalRow getRow(int pos, int pos1) {
        throw new UnsupportedOperationException(
                "getRow is not support for Fluss record currently.");
    }
}
