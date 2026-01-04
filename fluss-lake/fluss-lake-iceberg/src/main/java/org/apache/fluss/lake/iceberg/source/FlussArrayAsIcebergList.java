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

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.ArrayType;
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
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.fluss.utils.DateTimeUtils;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractList;

/** Adapter class for converting Fluss InternalArray to a Java List for Iceberg. */
public class FlussArrayAsIcebergList extends AbstractList<Object> {

    private final InternalArray flussArray;
    private final DataType elementType;

    public FlussArrayAsIcebergList(InternalArray flussArray, DataType elementType) {
        this.flussArray = flussArray;
        this.elementType = elementType;
    }

    @Override
    public Object get(int index) {
        if (flussArray.isNullAt(index)) {
            return null;
        }

        if (elementType instanceof BooleanType) {
            return flussArray.getBoolean(index);
        } else if (elementType instanceof TinyIntType) {
            return (int) flussArray.getByte(index);
        } else if (elementType instanceof SmallIntType) {
            return (int) flussArray.getShort(index);
        } else if (elementType instanceof IntType) {
            return flussArray.getInt(index);
        } else if (elementType instanceof BigIntType) {
            return flussArray.getLong(index);
        } else if (elementType instanceof FloatType) {
            return flussArray.getFloat(index);
        } else if (elementType instanceof DoubleType) {
            return flussArray.getDouble(index);
        } else if (elementType instanceof StringType) {
            return flussArray.getString(index).toString();
        } else if (elementType instanceof CharType) {
            CharType charType = (CharType) elementType;
            return flussArray.getChar(index, charType.getLength()).toString();
        } else if (elementType instanceof BytesType || elementType instanceof BinaryType) {
            return ByteBuffer.wrap(flussArray.getBytes(index));
        } else if (elementType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) elementType;
            return flussArray
                    .getDecimal(index, decimalType.getPrecision(), decimalType.getScale())
                    .toBigDecimal();
        } else if (elementType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) elementType;
            return toIcebergTimestampLtz(
                    flussArray.getTimestampLtz(index, ltzType.getPrecision()).toInstant());
        } else if (elementType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) elementType;
            return flussArray.getTimestampNtz(index, tsType.getPrecision()).toLocalDateTime();
        } else if (elementType instanceof DateType) {
            return DateTimeUtils.toLocalDate(flussArray.getInt(index));
        } else if (elementType instanceof TimeType) {
            return DateTimeUtils.toLocalTime(flussArray.getInt(index));
        } else if (elementType instanceof ArrayType) {
            InternalArray innerArray = flussArray.getArray(index);
            return innerArray == null
                    ? null
                    : new FlussArrayAsIcebergList(
                            innerArray, ((ArrayType) elementType).getElementType());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported array element type conversion for Fluss type: "
                            + elementType.getClass().getSimpleName());
        }
    }

    @Override
    public int size() {
        return flussArray.size();
    }

    private OffsetDateTime toIcebergTimestampLtz(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
