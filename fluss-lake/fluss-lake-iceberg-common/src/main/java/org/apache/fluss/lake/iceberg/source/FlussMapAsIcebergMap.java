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

import org.apache.fluss.lake.iceberg.FlussDataTypeToIcebergDataType;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
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
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.fluss.utils.DateTimeUtils;

import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/** Adapter class for converting Fluss InternalMap to a Java Map for Iceberg. */
public class FlussMapAsIcebergMap extends AbstractMap<Object, Object> {

    private final InternalMap flussMap;
    private final DataType keyType;
    private final DataType valueType;

    public FlussMapAsIcebergMap(InternalMap flussMap, DataType keyType, DataType valueType) {
        this.flussMap = flussMap;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public int size() {
        return flussMap.size();
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<Object, Object>> iterator() {
                return new Iterator<>() {
                    private final InternalArray keyArray = flussMap.keyArray();
                    private final InternalArray valueArray = flussMap.valueArray();
                    private final int size = flussMap.size();
                    private int currentIndex = 0;

                    @Override
                    public boolean hasNext() {
                        return currentIndex < size;
                    }

                    @Override
                    public Entry<Object, Object> next() {
                        Object key = convertElement(keyArray, currentIndex, keyType);
                        Object value = convertElement(valueArray, currentIndex, valueType);
                        currentIndex++;
                        return new AbstractMap.SimpleEntry<>(key, value);
                    }
                };
            }

            @Override
            public int size() {
                return flussMap.size();
            }
        };
    }

    private Object convertElement(InternalArray array, int index, DataType elementType) {
        if (array.isNullAt(index)) {
            return null;
        }

        if (elementType instanceof BooleanType) {
            return array.getBoolean(index);
        } else if (elementType instanceof TinyIntType) {
            return (int) array.getByte(index);
        } else if (elementType instanceof SmallIntType) {
            return (int) array.getShort(index);
        } else if (elementType instanceof IntType) {
            return array.getInt(index);
        } else if (elementType instanceof BigIntType) {
            return array.getLong(index);
        } else if (elementType instanceof FloatType) {
            return array.getFloat(index);
        } else if (elementType instanceof DoubleType) {
            return array.getDouble(index);
        } else if (elementType instanceof StringType) {
            return array.getString(index).toString();
        } else if (elementType instanceof CharType) {
            CharType charType = (CharType) elementType;
            return array.getChar(index, charType.getLength()).toString();
        } else if (elementType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) elementType;
            return array.getDecimal(index, decimalType.getPrecision(), decimalType.getScale())
                    .toBigDecimal();
        } else if (elementType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) elementType;
            return toIcebergTimestampLtz(
                    array.getTimestampLtz(index, ltzType.getPrecision()).toInstant());
        } else if (elementType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) elementType;
            return array.getTimestampNtz(index, tsType.getPrecision()).toLocalDateTime();
        } else if (elementType instanceof DateType) {
            return DateTimeUtils.toLocalDate(array.getInt(index));
        } else if (elementType instanceof TimeType) {
            return DateTimeUtils.toLocalTime(array.getInt(index));
        } else if (elementType instanceof BytesType || elementType instanceof BinaryType) {
            return ByteBuffer.wrap(array.getBytes(index));
        } else if (elementType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) elementType;
            InternalArray internalArray = array.getArray(index);
            return internalArray == null
                    ? null
                    : new FlussArrayAsIcebergList(internalArray, arrayType.getElementType());
        } else if (elementType instanceof MapType) {
            MapType mapType = (MapType) elementType;
            InternalMap internalMap = array.getMap(index);
            return internalMap == null
                    ? null
                    : new FlussMapAsIcebergMap(
                            internalMap, mapType.getKeyType(), mapType.getValueType());
        } else if (elementType instanceof RowType) {
            RowType rowType = (RowType) elementType;
            Types.StructType nestedStructType =
                    (Types.StructType) rowType.accept(FlussDataTypeToIcebergDataType.INSTANCE);
            InternalRow internalRow = array.getRow(index, rowType.getFieldCount());
            return internalRow == null
                    ? null
                    : new FlussRowAsIcebergRecord(nestedStructType, rowType, internalRow);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported array element type conversion for Fluss type: "
                            + elementType.getClass().getSimpleName());
        }
    }

    private OffsetDateTime toIcebergTimestampLtz(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
