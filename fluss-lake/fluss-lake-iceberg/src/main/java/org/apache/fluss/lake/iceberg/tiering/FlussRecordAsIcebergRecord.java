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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.record.LogRecord;
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

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Wrap Fluss {@link LogRecord} as Iceberg {@link Record}.
 *
 * <p>todo: refactor to implement ParquetWriters, OrcWriters, AvroWriters just like Flink & Spark
 * write to iceberg for higher performance
 */
public class FlussRecordAsIcebergRecord implements Record {

    // Lake table for iceberg will append three system columns: __bucket, __offset,__timestamp
    private static final int LAKE_ICEBERG_SYSTEM_COLUMNS = 3;

    private LogRecord logRecord;
    private final int bucket;
    private final Schema icebergSchema;
    private final RowType flussRowType;

    // the origin row fields in fluss, excluding the system columns in iceberg
    private int originRowFieldCount;
    private InternalRow internalRow;

    public FlussRecordAsIcebergRecord(int bucket, Schema icebergSchema, RowType flussRowType) {
        this.bucket = bucket;
        this.icebergSchema = icebergSchema;
        this.flussRowType = flussRowType;
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.originRowFieldCount = internalRow.getFieldCount();
        checkState(
                originRowFieldCount
                        == icebergSchema.asStruct().fields().size() - LAKE_ICEBERG_SYSTEM_COLUMNS,
                "The Iceberg table fields count must equals to LogRecord's fields count.");
    }

    @Override
    public Types.StructType struct() {
        return icebergSchema.asStruct();
    }

    @Override
    public Object getField(String name) {
        return icebergSchema;
    }

    @Override
    public void setField(String name, Object value) {
        throw new UnsupportedOperationException("method setField is not supported.");
    }

    @Override
    public Object get(int pos) {
        // firstly, for system columns
        if (pos == originRowFieldCount) {
            // bucket column
            return bucket;
        } else if (pos == originRowFieldCount + 1) {
            // log offset column
            return logRecord.logOffset();
        } else if (pos == originRowFieldCount + 2) {
            // timestamp column
            return getTimestampLtz(logRecord.timestamp());
        }

        // handle normal columns
        if (internalRow.isNullAt(pos)) {
            return null;
        }

        DataType dataType = flussRowType.getTypeAt(pos);
        if (dataType instanceof BooleanType) {
            return internalRow.getBoolean(pos);
        } else if (dataType instanceof TinyIntType) {
            return (int) internalRow.getByte(pos);
        } else if (dataType instanceof SmallIntType) {
            return (int) internalRow.getShort(pos);
        } else if (dataType instanceof IntType) {
            return internalRow.getInt(pos);
        } else if (dataType instanceof BigIntType) {
            return internalRow.getLong(pos);
        } else if (dataType instanceof FloatType) {
            return internalRow.getFloat(pos);
        } else if (dataType instanceof DoubleType) {
            return internalRow.getDouble(pos);
        } else if (dataType instanceof StringType) {
            return internalRow.getString(pos).toString();
        } else if (dataType instanceof CharType) {
            CharType charType = (CharType) dataType;
            return internalRow.getChar(pos, charType.getLength()).toString();
        } else if (dataType instanceof BytesType) {
            return ByteBuffer.wrap(internalRow.getBytes(pos));
        } else if (dataType instanceof BinaryType) {
            // Iceberg's Record interface expects ByteBuffer for binary types.
            return ByteBuffer.wrap(internalRow.getBytes(pos));
        } else if (dataType instanceof DecimalType) {
            // Iceberg expects BigDecimal for decimal types.
            DecimalType decimalType = (DecimalType) dataType;
            return internalRow
                    .getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                    .toBigDecimal();
        } else if (dataType instanceof LocalZonedTimestampType) {
            // Iceberg expects OffsetDateTime for timestamp with local timezone.
            return getTimestampLtz(
                    internalRow
                            .getTimestampLtz(
                                    pos, ((LocalZonedTimestampType) dataType).getPrecision())
                            .toInstant());
        } else if (dataType instanceof TimestampType) {
            // Iceberg expects LocalDateType for timestamp without local timezone.
            return internalRow
                    .getTimestampNtz(pos, ((TimestampType) dataType).getPrecision())
                    .toLocalDateTime();
        } else if (dataType instanceof DateType) {
            return DateTimeUtils.toLocalDate(internalRow.getInt(pos));
        } else if (dataType instanceof TimeType) {
            return DateTimeUtils.toLocalTime(internalRow.getInt(pos));
        }
        throw new UnsupportedOperationException(
                "Unsupported data type conversion for Fluss type: "
                        + dataType.getClass().getName());
    }

    private OffsetDateTime getTimestampLtz(long timestamp) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    }

    private OffsetDateTime getTimestampLtz(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
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
        return icebergSchema.asStruct().fields().size();
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
}
