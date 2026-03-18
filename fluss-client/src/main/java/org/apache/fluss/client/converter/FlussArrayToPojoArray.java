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

package org.apache.fluss.client.converter;

import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Adapter class for converting Fluss InternalArray to Pojo Array. */
public class FlussArrayToPojoArray {
    private final InternalArray flussArray;
    private final String fieldName;
    private final Class<?> pojoType;
    /** Pre-compiled per-element converter; avoids repeating the type-switch on every element. */
    private final ElementConverter elementConverter;

    public FlussArrayToPojoArray(
            InternalArray flussArray, DataType elementType, String fieldName, Class<?> pojoType) {
        this.flussArray = flussArray;
        this.fieldName = fieldName;
        this.pojoType = pojoType != null ? pojoType : Object.class;
        this.elementConverter = buildElementConverter(elementType, fieldName, this.pojoType);
    }

    public Object convertArray() {
        if (flussArray == null) {
            return null;
        }

        int size = flussArray.size();
        Object result = Array.newInstance(pojoType, size);
        for (int i = 0; i < size; i++) {
            Object element = convertElementValue(i);
            if (element == null && pojoType.isPrimitive()) {
                throw new NullPointerException(
                        String.format(
                                "Field '%s': cannot store null into a primitive array of type %s[]."
                                        + " Use a boxed type (e.g. Integer[]) or ensure all elements are non-null.",
                                fieldName, pojoType.getName()));
            }
            Array.set(result, i, element);
        }
        return result;
    }

    /** Converts the Fluss array to a {@link List}, preserving null elements. */
    List<Object> convertList() {
        if (flussArray == null) {
            return null;
        }
        int size = flussArray.size();
        List<Object> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(convertElementValue(i));
        }
        return result;
    }

    Object convertElementValue(int index) {
        if (flussArray.isNullAt(index)) {
            return null;
        }
        return elementConverter.convert(flussArray, index);
    }

    /**
     * Resolves the element type once and returns a pre-compiled converter. This avoids repeating
     * the type dispatch (getTypeRoot() switch + casts) on every array element.
     */
    private static ElementConverter buildElementConverter(
            DataType elementType, String fieldName, Class<?> pojoType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return InternalArray::getBoolean;
            case TINYINT:
                return InternalArray::getByte;
            case SMALLINT:
                return InternalArray::getShort;
            case INTEGER:
                return InternalArray::getInt;
            case BIGINT:
                return InternalArray::getLong;
            case FLOAT:
                return InternalArray::getFloat;
            case DOUBLE:
                return InternalArray::getDouble;
            case CHAR:
            case STRING:
                // Default to String when the POJO element type is unspecified (Object[])
                final Class<?> textTarget = (pojoType == Object.class) ? String.class : pojoType;
                return (arr, i) ->
                        FlussTypeToPojoTypeConverter.convertTextValue(
                                elementType, fieldName, textTarget, arr.getString(i));
            case BINARY:
            case BYTES:
                return InternalArray::getBytes;
            case DECIMAL:
                {
                    final DecimalType decimalType = (DecimalType) elementType;
                    final int precision = decimalType.getPrecision();
                    final int scale = decimalType.getScale();
                    return (arr, i) -> {
                        Decimal d = arr.getDecimal(i, precision, scale);
                        return FlussTypeToPojoTypeConverter.convertDecimalValue(d);
                    };
                }
            case DATE:
                return (arr, i) -> FlussTypeToPojoTypeConverter.convertDateValue(arr.getInt(i));
            case TIME_WITHOUT_TIME_ZONE:
                return (arr, i) -> FlussTypeToPojoTypeConverter.convertTimeValue(arr.getInt(i));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return (arr, i) ->
                            FlussTypeToPojoTypeConverter.convertTimestampNtzValue(
                                    arr.getTimestampNtz(i, precision));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    // Default to Instant when the POJO element type is unspecified (Object[])
                    final Class<?> tsTarget = (pojoType == Object.class) ? Instant.class : pojoType;
                    return (arr, i) ->
                            FlussTypeToPojoTypeConverter.convertTimestampLtzValue(
                                    arr.getTimestampLtz(i, precision), fieldName, tsTarget);
                }
            case ARRAY:
                {
                    final ArrayType nestedArrayType = (ArrayType) elementType;
                    final Class<?> componentType = pojoType.getComponentType();
                    return (arr, i) -> {
                        InternalArray innerArray = arr.getArray(i);
                        return innerArray == null
                                ? null
                                : new FlussArrayToPojoArray(
                                                innerArray,
                                                nestedArrayType.getElementType(),
                                                fieldName,
                                                componentType)
                                        .convertArray();
                    };
                }
            case MAP:
                return (arr, i) ->
                        new FlussMapToPojoMap(arr.getMap(i), (MapType) elementType, fieldName)
                                .convertMap();
            case ROW:
                {
                    RowType nestedRowType = (RowType) elementType;
                    int nestedFieldCount = nestedRowType.getFieldCount();
                    if (pojoType == Object.class) {
                        // When the target type is unknown (e.g. ROW values in a Map),
                        // return InternalRow directly
                        return (arr, i) -> arr.getRow(i, nestedFieldCount);
                    }
                    @SuppressWarnings("unchecked")
                    RowToPojoConverter<Object> nestedConverter =
                            RowToPojoConverter.of(
                                    (Class<Object>) pojoType, nestedRowType, nestedRowType);
                    return (arr, i) -> {
                        InternalRow nestedRow = arr.getRow(i, nestedFieldCount);
                        return nestedRow == null ? null : nestedConverter.fromRow(nestedRow);
                    };
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                elementType.getTypeRoot(), fieldName));
        }
    }

    @FunctionalInterface
    private interface ElementConverter {
        Object convert(InternalArray array, int index);
    }
}
