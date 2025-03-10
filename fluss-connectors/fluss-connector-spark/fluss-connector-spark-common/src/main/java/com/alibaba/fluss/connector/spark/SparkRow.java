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

package com.alibaba.fluss.connector.spark;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.DateTimeUtils;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

/** A {@link InternalRow} wraps spark {@link Row}. */
public class SparkRow implements InternalRow, Serializable {

    private final RowType type;
    private final Row row;

    public SparkRow(RowType type, Row row) {
        this.type = type;
        this.row = row;
    }

    @Override
    public int getFieldCount() {
        return type.getFieldCount();
    }

    @Override
    public boolean isNullAt(int i) {
        return row.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return row.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return row.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return row.getShort(i);
    }

    @Override
    public int getInt(int i) {
        if (type.getTypeAt(i) instanceof DateType) {
            return toFlussDate(row.get(i));
        }
        return row.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return row.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return row.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return row.getDouble(i);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromString(row.getString(pos).substring(0, length));
    }

    @Override
    public BinaryString getString(int i) {
        return BinaryString.fromString(row.getString(i));
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        return Decimal.fromBigDecimal(row.getDecimal(i), precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        Object object = row.get(pos);
        if (object instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) object;
            return TimestampNtz.fromLocalDateTime(ts.toLocalDateTime());
        } else if (object instanceof LocalDateTime) {
            return TimestampNtz.fromLocalDateTime((LocalDateTime) object);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported type for TimestampNtz: " + object.getClass().getName());
        }
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        Object object = row.get(pos);
        if (object instanceof java.time.Instant) {
            Instant instant = (Instant) object;
            return TimestampLtz.fromInstant(instant);
        } else if (object instanceof Timestamp) {
            return TimestampLtz.fromEpochMillis(
                    ((Timestamp) object).getTime(), ((Timestamp) object).getNanos());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported type for TimestampLtz: " + object.getClass().getName());
        }
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return new byte[0];
    }

    @Override
    public byte[] getBytes(int pos) {
        return new byte[0];
    }

    private static int toFlussDate(Object object) {
        if (object instanceof Date) {
            return DateTimeUtils.toInternal((Date) object);
        } else {
            return DateTimeUtils.toInternal((LocalDate) object);
        }
    }
}
