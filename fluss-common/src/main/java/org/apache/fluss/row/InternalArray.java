/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.row.columnar.VectorizedColumnBatch;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/**
 * Base interface of an internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link InternalRow} for more information about internal data structures.
 *
 * @see GenericArray
 * @since 0.9
 */
@PublicEvolving
public interface InternalArray extends DataGetters {
    /** Returns the number of elements in this array. */
    int size();

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

    boolean[] toBooleanArray();

    byte[] toByteArray();

    short[] toShortArray();

    int[] toIntArray();

    long[] toLongArray();

    float[] toFloatArray();

    double[] toDoubleArray();

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * @param fieldType the element type of the array
     */
    static ElementGetter createElementGetter(DataType fieldType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                final int bytesLength = getLength(fieldType);
                elementGetter = (array, pos) -> array.getChar(pos, bytesLength);
                break;
            case STRING:
                elementGetter = InternalArray::getString;
                break;
            case BOOLEAN:
                elementGetter = InternalArray::getBoolean;
                break;
            case BINARY:
                final int binaryLength = getLength(fieldType);
                elementGetter = (array, pos) -> array.getBinary(pos, binaryLength);
                break;
            case BYTES:
                elementGetter = InternalArray::getBytes;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                elementGetter =
                        (array, pos) -> array.getDecimal(pos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                elementGetter = InternalArray::getByte;
                break;
            case SMALLINT:
                elementGetter = InternalArray::getShort;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = InternalArray::getInt;
                break;
            case BIGINT:
                elementGetter = InternalArray::getLong;
                break;
            case FLOAT:
                elementGetter = InternalArray::getFloat;
                break;
            case DOUBLE:
                elementGetter = InternalArray::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                elementGetter = (array, pos) -> array.getTimestampNtz(pos, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                elementGetter = (array, pos) -> array.getTimestampLtz(pos, timestampLtzPrecision);
                break;
            case ARRAY:
                elementGetter = InternalArray::getArray;
                break;
            case MAP:
                elementGetter = InternalArray::getMap;
                break;
            case ROW:
                final int rowFieldCount = ((RowType) fieldType).getFieldCount();
                elementGetter = (array, pos) -> array.getRow(pos, rowFieldCount);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), InternalArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!fieldType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /**
     * Creates a deep accessor for getting elements in an internal array data structure at the given
     * position. It returns new objects (GenericArray/GenericMap/GenericMap) for nested
     * array/map/row types.
     *
     * <p>NOTE: Currently, it is only used for deep copying {@link ColumnarRow} for Arrow which
     * avoid the arrow buffer is released before accessing elements. It doesn't deep copy STRING and
     * BYTES types, because {@link ColumnarRow} already deep copies the bytes, see {@link
     * VectorizedColumnBatch#getString(int, int)}. This can be removed once we supports object reuse
     * for Arrow {@link ColumnarRow}, see {@code CompletedFetch#toScanRecord(LogRecord)}.
     */
    static ElementGetter createDeepElementGetter(DataType fieldType) {
        final ElementGetter elementGetter;
        switch (fieldType.getTypeRoot()) {
            case ARRAY:
                DataType nestedType = ((ArrayType) fieldType).getElementType();
                ElementGetter nestedGetter = createDeepElementGetter(nestedType);
                elementGetter =
                        (array, pos) -> {
                            InternalArray inner = array.getArray(pos);
                            Object[] objs = new Object[inner.size()];
                            for (int i = 0; i < inner.size(); i++) {
                                objs[i] = nestedGetter.getElementOrNull(inner, i);
                            }
                            return new GenericArray(objs);
                        };
                break;
            case MAP:
                DataType keyType = ((MapType) fieldType).getKeyType();
                DataType valueType = ((MapType) fieldType).getValueType();
                ElementGetter keyGetter = createDeepElementGetter(keyType);
                ElementGetter valueGetter = createDeepElementGetter(valueType);
                elementGetter =
                        (array, pos) -> {
                            InternalMap inner = array.getMap(pos);
                            InternalArray keys = inner.keyArray();
                            InternalArray values = inner.valueArray();
                            Map<Object, Object> map = new HashMap<>();
                            for (int i = 0; i < keys.size(); i++) {
                                Object key = keyGetter.getElementOrNull(keys, i);
                                Object value = valueGetter.getElementOrNull(values, i);
                                map.put(key, value);
                            }
                            return new GenericMap(map);
                        };
                break;
            case ROW:
                RowType rowType = (RowType) fieldType;
                int numFields = rowType.getFieldCount();
                InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[numFields];
                for (int i = 0; i < numFields; i++) {
                    fieldGetters[i] = InternalRow.createDeepFieldGetter(rowType.getTypeAt(i), i);
                }
                elementGetter =
                        (array, pos) -> {
                            InternalRow row = array.getRow(pos, numFields);
                            GenericRow genericRow = new GenericRow(numFields);
                            for (int i = 0; i < numFields; i++) {
                                genericRow.setField(i, fieldGetters[i].getFieldOrNull(row));
                            }
                            return genericRow;
                        };
                break;
            default:
                // for primitive types, we can directly return the element getter
                elementGetter = createElementGetter(fieldType);
        }
        if (!fieldType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /** Accessor for getting the elements of an array during runtime. */
    interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(InternalArray array, int pos);
    }
}
