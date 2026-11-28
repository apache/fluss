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

package org.apache.fluss.lake.delta.utils;

import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import java.util.ArrayList;
import java.util.List;

/**
 * Copies a Fluss {@link InternalRow} into plain Java values that {@link DeltaColumnarBatch} can
 * expose to Delta Kernel.
 *
 * <p>The rows of a {@code LogRecord} may be backed by reused buffers, so the values are copied when
 * a record is written instead of keeping a reference to the row. The value of each type is the one
 * Delta Kernel reads through {@code ColumnVector}: dates are days since epoch, timestamps are
 * microseconds since epoch, arrays are {@link List}s, maps are {@link MapEntries} and nested rows
 * are {@code Object[]}.
 */
public class FlussRowToDeltaValues {

    private final ValueGetter[] fieldGetters;

    public FlussRowToDeltaValues(RowType rowType) {
        this.fieldGetters = createFieldGetters(rowType);
    }

    /** Copies all fields of the given row. */
    public Object[] convert(InternalRow row) {
        return convertRow(row, fieldGetters);
    }

    private static Object[] convertRow(DataGetters row, ValueGetter[] getters) {
        Object[] values = new Object[getters.length];
        for (int i = 0; i < getters.length; i++) {
            values[i] = getters[i].get(row, i);
        }
        return values;
    }

    private static List<Object> convertArray(InternalArray array, ValueGetter elementGetter) {
        List<Object> values = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            values.add(elementGetter.get(array, i));
        }
        return values;
    }

    private static ValueGetter[] createFieldGetters(RowType rowType) {
        ValueGetter[] getters = new ValueGetter[rowType.getFieldCount()];
        for (int i = 0; i < getters.length; i++) {
            getters[i] = createValueGetter(rowType.getTypeAt(i));
        }
        return getters;
    }

    private static ValueGetter createValueGetter(DataType type) {
        ValueGetter getter;
        switch (type.getTypeRoot()) {
            case CHAR:
                int charLength = ((CharType) type).getLength();
                getter = (data, pos) -> data.getChar(pos, charLength).toString();
                break;
            case STRING:
                getter = (data, pos) -> data.getString(pos).toString();
                break;
            case BOOLEAN:
                getter = DataGetters::getBoolean;
                break;
            case BINARY:
                int binaryLength = ((BinaryType) type).getLength();
                getter = (data, pos) -> data.getBinary(pos, binaryLength);
                break;
            case BYTES:
                getter = DataGetters::getBytes;
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                getter = (data, pos) -> data.getDecimal(pos, precision, scale).toBigDecimal();
                break;
            case TINYINT:
                getter = DataGetters::getByte;
                break;
            case SMALLINT:
                getter = DataGetters::getShort;
                break;
            case INTEGER:
            case DATE:
                getter = DataGetters::getInt;
                break;
            case BIGINT:
                getter = DataGetters::getLong;
                break;
            case FLOAT:
                getter = DataGetters::getFloat;
                break;
            case DOUBLE:
                getter = DataGetters::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int ntzPrecision = ((TimestampType) type).getPrecision();
                getter = (data, pos) -> data.getTimestampNtz(pos, ntzPrecision).toEpochMicros();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int ltzPrecision = ((LocalZonedTimestampType) type).getPrecision();
                getter = (data, pos) -> data.getTimestampLtz(pos, ltzPrecision).toEpochMicros();
                break;
            case ARRAY:
                ValueGetter elementGetter = createValueGetter(((ArrayType) type).getElementType());
                getter = (data, pos) -> convertArray(data.getArray(pos), elementGetter);
                break;
            case MAP:
                MapType mapType = (MapType) type;
                ValueGetter keyGetter = createValueGetter(mapType.getKeyType());
                ValueGetter valueGetter = createValueGetter(mapType.getValueType());
                getter =
                        (data, pos) -> {
                            InternalMap map = data.getMap(pos);
                            return new MapEntries(
                                    convertArray(map.keyArray(), keyGetter),
                                    convertArray(map.valueArray(), valueGetter));
                        };
                break;
            case ROW:
                RowType rowType = (RowType) type;
                int numFields = rowType.getFieldCount();
                ValueGetter[] nestedGetters = createFieldGetters(rowType);
                getter = (data, pos) -> convertRow(data.getRow(pos, numFields), nestedGetters);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type for Delta Lake: " + type);
        }
        if (!type.isNullable()) {
            return getter;
        }
        ValueGetter nonNullGetter = getter;
        return (data, pos) -> data.isNullAt(pos) ? null : nonNullGetter.get(data, pos);
    }

    /** Reads one value from a row or an array. */
    @FunctionalInterface
    private interface ValueGetter {
        Object get(DataGetters data, int pos);
    }

    /** The keys and values of a map, in the same order. */
    public static final class MapEntries {
        private final List<Object> keys;
        private final List<Object> values;

        public MapEntries(List<Object> keys, List<Object> values) {
            this.keys = keys;
            this.values = values;
        }

        public List<Object> keys() {
            return keys;
        }

        public List<Object> values() {
            return values;
        }
    }
}
