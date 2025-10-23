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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.fluss.utils.DateTimeUtils;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

/** Wrap Fluss {@link InternalRow} as Iceberg {@link Record}. */
public class FlussRowAsIcebergRecord implements Record {

    protected InternalRow internalRow;
    protected final Types.StructType structType;
    protected final RowType flussRowType;
    private final FlussRowToIcebergFieldConverter[] fieldConverters;

    public FlussRowAsIcebergRecord(Types.StructType structType, RowType flussRowType) {
        this.structType = structType;
        this.flussRowType = flussRowType;
        fieldConverters = new FlussRowToIcebergFieldConverter[flussRowType.getFieldCount()];
        for (int pos = 0; pos < flussRowType.getFieldCount(); pos++) {
            DataType flussType = flussRowType.getTypeAt(pos);
            fieldConverters[pos] = createTypeConverter(flussType, pos);
        }
    }

    public FlussRowAsIcebergRecord(
            Types.StructType structType, RowType flussRowType, InternalRow internalRow) {
        this(structType, flussRowType);
        this.internalRow = internalRow;
    }

    @Override
    public Types.StructType struct() {
        return structType;
    }

    @Override
    public Object getField(String name) {
        return get(structType.fields().indexOf(structType.field(name)));
    }

    @Override
    public void setField(String name, Object value) {
        throw new UnsupportedOperationException("method setField is not supported.");
    }

    @Override
    public Object get(int pos) {
        // handle normal columns
        if (internalRow.isNullAt(pos)) {
            return null;
        }
        return fieldConverters[pos].convert(internalRow);
    }

    @Override
    public Record copy() {
        throw new UnsupportedOperationException("method copy is not supported.");
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        throw new UnsupportedOperationException("method copy is not supported.");
    }

    @Override
    public int size() {
        return structType.fields().size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        Object value = get(pos);
        if (value == null || javaClass.isInstance(value)) {
            return javaClass.cast(value);
        } else {
            throw new IllegalStateException(
                    "Not an instance of " + javaClass.getName() + ": " + value);
        }
    }

    @Override
    public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException("method set is not supported.");
    }

    private interface FlussRowToIcebergFieldConverter {
        Object convert(InternalRow value);
    }

    private FlussRowToIcebergFieldConverter createTypeConverter(DataType flussType, int pos) {
        if (flussType instanceof BooleanType) {
            return row -> row.getBoolean(pos);
        } else if (flussType instanceof TinyIntType) {
            return row -> (int) row.getByte(pos);
        } else if (flussType instanceof SmallIntType) {
            return row -> (int) row.getShort(pos);
        } else if (flussType instanceof IntType) {
            return row -> row.getInt(pos);
        } else if (flussType instanceof BigIntType) {
            return row -> row.getLong(pos);
        } else if (flussType instanceof FloatType) {
            return row -> row.getFloat(pos);
        } else if (flussType instanceof DoubleType) {
            return row -> row.getDouble(pos);
        } else if (flussType instanceof StringType) {
            return row -> row.getString(pos).toString();
        } else if (flussType instanceof CharType) {
            CharType charType = (CharType) flussType;
            return row -> row.getChar(pos, charType.getLength()).toString();
        } else if (flussType instanceof BytesType || flussType instanceof BinaryType) {
            return row -> ByteBuffer.wrap(row.getBytes(pos));
        } else if (flussType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) flussType;
            return row ->
                    row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                            .toBigDecimal();
        } else if (flussType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) flussType;
            return row ->
                    toIcebergTimestampLtz(
                            row.getTimestampLtz(pos, ltzType.getPrecision()).toInstant());
        } else if (flussType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) flussType;
            return row -> row.getTimestampNtz(pos, tsType.getPrecision()).toLocalDateTime();
        } else if (flussType instanceof DateType) {
            return row -> DateTimeUtils.toLocalDate(row.getInt(pos));
        } else if (flussType instanceof TimeType) {
            return row -> DateTimeUtils.toLocalTime(row.getInt(pos));
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported data type conversion for Fluss type: "
                            + flussType.getClass().getSimpleName());
        }
    }

    private OffsetDateTime toIcebergTimestampLtz(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
