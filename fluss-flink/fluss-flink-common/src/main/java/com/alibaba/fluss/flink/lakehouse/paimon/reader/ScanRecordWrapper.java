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

package com.alibaba.fluss.flink.lakehouse.paimon.reader;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.record.ChangeType;
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

/** A wrapper of {@link ScanRecord} which bridges {@link ScanRecord} to Paimon' internal row. */
public class ScanRecordWrapper implements InternalRow {

    private final ChangeType changeType;
    private final com.alibaba.fluss.row.InternalRow flussRow;
    private final RowType rowType;

    public ScanRecordWrapper(ScanRecord scanRecord, RowType rowType) {
        this.changeType = scanRecord.getChangeType();
        this.flussRow = scanRecord.getRow();
        this.rowType = rowType;
    }

    @Override
    public int getFieldCount() {
        return flussRow.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        switch (changeType) {
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // do nothing
    }

    @Override
    public boolean isNullAt(int pos) {
        return flussRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return flussRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return flussRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return flussRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return flussRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return flussRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return flussRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return flussRow.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromString(flussRow.getString(pos).toString());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return Decimal.fromBigDecimal(
                flussRow.getDecimal(pos, precision, scale).toBigDecimal(), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        DataType paimonTimestampType = rowType.getTypeAt(pos);
        switch (paimonTimestampType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (TimestampNtz.isCompact(precision)) {
                    return Timestamp.fromEpochMillis(
                            flussRow.getTimestampNtz(pos, precision).getMillisecond());
                } else {
                    TimestampNtz timestampNtz = flussRow.getTimestampNtz(pos, precision);
                    return Timestamp.fromEpochMillis(
                            timestampNtz.getMillisecond(), timestampNtz.getNanoOfMillisecond());
                }

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (TimestampLtz.isCompact(precision)) {
                    return Timestamp.fromEpochMillis(
                            flussRow.getTimestampLtz(pos, precision).getEpochMillisecond());
                } else {
                    TimestampLtz timestampLtz = flussRow.getTimestampLtz(pos, precision);
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
        return flussRow.getBytes(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow getRow(int pos, int pos1) {
        throw new UnsupportedOperationException();
    }
}
