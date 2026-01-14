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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.utils.BytesUtils;

import org.apache.iceberg.data.Record;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;

import static org.apache.fluss.lake.iceberg.IcebergLakeCatalog.SYSTEM_COLUMNS;

/** Adapter for Iceberg Record as fluss row. */
public class IcebergRecordAsFlussRow implements InternalRow {

    private Record icebergRecord;

    public IcebergRecordAsFlussRow() {}

    public IcebergRecordAsFlussRow(Record icebergRecord) {
        this.icebergRecord = icebergRecord;
    }

    public IcebergRecordAsFlussRow replaceIcebergRecord(Record icebergRecord) {
        this.icebergRecord = icebergRecord;
        return this;
    }

    @Override
    public int getFieldCount() {
        return icebergRecord.struct().fields().size() - SYSTEM_COLUMNS.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return icebergRecord.get(pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) icebergRecord.get(pos);
    }

    @Override
    public byte getByte(int pos) {
        Object value = icebergRecord.get(pos);
        // Iceberg stores TINYINT as Integer, need to cast to byte
        return ((Integer) value).byteValue();
    }

    @Override
    public short getShort(int pos) {
        Object value = icebergRecord.get(pos);
        // Iceberg stores SMALLINT as Integer, need to cast to short
        return ((Integer) value).shortValue();
    }

    @Override
    public int getInt(int pos) {
        Object value = icebergRecord.get(pos);
        return (Integer) value;
    }

    @Override
    public long getLong(int pos) {
        Object value = icebergRecord.get(pos);
        return (Long) value;
    }

    @Override
    public float getFloat(int pos) {
        Object value = icebergRecord.get(pos);
        return (float) value;
    }

    @Override
    public double getDouble(int pos) {
        Object value = icebergRecord.get(pos);
        return (double) value;
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        String value = (String) icebergRecord.get(pos);
        return BinaryString.fromBytes(value.getBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        String value = (String) icebergRecord.get(pos);
        return BinaryString.fromBytes(value.getBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        BigDecimal bigDecimal = (BigDecimal) icebergRecord.get(pos);
        return Decimal.fromBigDecimal(bigDecimal, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        Object value = icebergRecord.get(pos);
        if (value == null) {
            throw new IllegalStateException("Value at position " + pos + " is null");
        }
        LocalDateTime localDateTime = (LocalDateTime) value;
        return TimestampNtz.fromLocalDateTime(localDateTime);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        Object value = icebergRecord.get(pos);
        OffsetDateTime offsetDateTime = (OffsetDateTime) value;
        return TimestampLtz.fromInstant(offsetDateTime.toInstant());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        Object value = icebergRecord.get(pos);
        ByteBuffer byteBuffer = (ByteBuffer) value;
        return BytesUtils.toArray(byteBuffer);
    }

    @Override
    public byte[] getBytes(int pos) {
        Object value = icebergRecord.get(pos);
        ByteBuffer byteBuffer = (ByteBuffer) value;
        return BytesUtils.toArray(byteBuffer);
    }

    @Override
    public InternalArray getArray(int pos) {
        Object value = icebergRecord.get(pos);
        if (value == null) {
            return null;
        }
        List<?> icebergList = (List<?>) value;
        return new IcebergArrayAsFlussArray(icebergList);
    }

    @Override
    public InternalMap getMap(int pos) {
        // TODO: Support Map type conversion from Iceberg to Fluss
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        Object value = icebergRecord.get(pos);
        if (value == null) {
            return null;
        }
        if (value instanceof Record) {
            return new IcebergRecordAsFlussRow((Record) value);
        } else {
            throw new IllegalArgumentException(
                    "Expected Iceberg Record for nested row at position "
                            + pos
                            + " but found: "
                            + value.getClass().getName());
        }
    }
}
