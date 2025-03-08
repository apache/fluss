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

package com.alibaba.fluss.connector.flink.lakehouse.paimon.reader;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.record.ChangeType;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

/** A wrapper of {@link ScanRecord} which bridges {@link ScanRecord} to Paimon' internal row. */
public class ScanRecordWrapper implements InternalRow {

    private final ChangeType changeType;
    private final com.alibaba.fluss.row.InternalRow flussRow;

    public ScanRecordWrapper(ScanRecord scanRecord) {
        this.changeType = scanRecord.getChangeType();
        this.flussRow = scanRecord.getRow();
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
        return Timestamp.fromInstant(flussRow.getTimestampLtz(pos, precision).toInstant());
    }

    @Override
    public byte[] getBinary(int pos) {
        return flussRow.getBytes(pos);
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
