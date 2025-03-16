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

package com.alibaba.fluss.spark;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeChecks;
import com.alibaba.fluss.types.RowType;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static com.alibaba.fluss.utils.InternalRowUtils.copyInternalRow;

/** Spark {@link org.apache.spark.sql.catalyst.InternalRow} to wrap {@link InternalRow}. */
public class SparkInternalRow extends org.apache.spark.sql.catalyst.InternalRow {

    private final RowType rowType;

    private InternalRow row;

    public SparkInternalRow(RowType rowType) {
        this.rowType = rowType;
    }

    public SparkInternalRow replace(InternalRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int numFields() {
        return row.getFieldCount();
    }

    @Override
    public void setNullAt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(int i, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.spark.sql.catalyst.InternalRow copy() {
        return new SparkInternalRow(rowType).replace(copyInternalRow(row, rowType));
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return row.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return row.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
        return row.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
        return row.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
        return row.getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
        if (rowType.getTypeAt(ordinal) instanceof BigIntType) {
            return row.getLong(ordinal);
        }

        return getTimestampMicros(ordinal);
    }

    private long getTimestampMicros(int ordinal) {
        DataType type = rowType.getTypeAt(ordinal);
        if (type instanceof com.alibaba.fluss.types.TimestampType) {
            return fromFluss(row.getTimestampNtz(ordinal, DataTypeChecks.getPrecision(type)));
        } else {
            return fromFluss(row.getTimestampLtz(ordinal, DataTypeChecks.getPrecision(type)));
        }
    }

    @Override
    public float getFloat(int ordinal) {
        return row.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
        return row.getDouble(ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        com.alibaba.fluss.row.Decimal decimal = row.getDecimal(ordinal, precision, scale);
        return fromFluss(decimal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        return fromFluss(row.getString(ordinal));
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return row.getBinary(ordinal, -1);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields) {
        // todo: support struct
        return null;
    }

    @Override
    public ArrayData getArray(int ordinal) {
        // TODO, fluss support array type
        // https://github.com/alibaba/fluss/issues/168
        return null;
    }

    @Override
    public MapData getMap(int ordinal) {
        // TODO, fluss support map type
        https: // github.com/alibaba/fluss/issues/169
        return null;
    }

    @Override
    public Object get(int ordinal, org.apache.spark.sql.types.DataType dataType) {
        return SpecializedGettersReader.read(this, ordinal, dataType);
    }

    public static Object fromFluss(Object o, DataType type) {
        if (o == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return fromFluss((TimestampNtz) o);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return fromFluss((TimestampLtz) o);
            case CHAR:
            case STRING:
                return fromFluss((BinaryString) o);
            case DECIMAL:
                return fromFluss((com.alibaba.fluss.row.Decimal) o);
            case ROW:
                return fromFluss((InternalRow) o, (RowType) type);
            default:
                return o;
        }
    }

    public static UTF8String fromFluss(BinaryString string) {
        return UTF8String.fromBytes(string.toBytes());
    }

    public static Decimal fromFluss(com.alibaba.fluss.row.Decimal decimal) {
        return Decimal.apply(decimal.toBigDecimal());
    }

    public static org.apache.spark.sql.catalyst.InternalRow fromFluss(
            InternalRow row, RowType rowType) {
        return new SparkInternalRow(rowType).replace(row);
    }

    public static long fromFluss(TimestampNtz timestamp) {
        return timestamp.toMicros();
    }

    public static long fromFluss(TimestampLtz timestamp) {
        return timestamp.toEpochMicros();
    }
}
