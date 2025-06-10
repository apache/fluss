/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.TimestampLtz;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toRowKind;

/** To wrap Fluss {@link LogRecord} as paimon {@link InternalRow}. */
public class FlussRecordAsPaimonRow implements InternalRow {

    private final int bucket;
    private LogRecord logRecord;
    private int originRowFieldCount;
    private com.alibaba.fluss.row.InternalRow internalRow;

    public FlussRecordAsPaimonRow(int bucket) {
        this.bucket = bucket;
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.originRowFieldCount = internalRow.getFieldCount();
    }

    @Override
    public int getFieldCount() {
        return
        //  business (including partitions) + system (three system fields: bucket, offset,
        // timestamp)
        originRowFieldCount + 3;
    }

    @Override
    public RowKind getRowKind() {
        return toRowKind(logRecord.getChangeType());
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // do nothing
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.isNullAt(pos);
        }
        // is the last three system fields: bucket, offset, timestamp which are never null
        return false;
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
        if (pos == originRowFieldCount) {
            // bucket system column
            return bucket;
        }
        return internalRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (pos == originRowFieldCount + 1) {
            //  offset system column
            return logRecord.logOffset();
        } else if (pos == originRowFieldCount + 2) {
            //  timestamp system column
            return logRecord.timestamp();
        }
        //  the origin RowData
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
        // it's timestamp system column
        if (pos == originRowFieldCount + 2) {
            return Timestamp.fromEpochMillis(logRecord.timestamp());
        }
        if (TimestampLtz.isCompact(precision)) {
            return Timestamp.fromEpochMillis(
                    internalRow.getTimestampLtz(pos, precision).getEpochMillisecond());
        } else {
            TimestampLtz timestampLtz = internalRow.getTimestampLtz(pos, precision);
            return Timestamp.fromEpochMillis(
                    timestampLtz.getEpochMillisecond(), timestampLtz.getNanoOfMillisecond());
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        return internalRow.getBytes(pos);
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
