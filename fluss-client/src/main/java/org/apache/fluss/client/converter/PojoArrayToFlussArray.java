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

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.Collection;
import java.util.function.Function;

import static org.apache.fluss.client.converter.PojoTypeToFlussTypeConverter.convertElementValue;

/** Adapter class for converting Pojo Array to Fluss InternalArray. */
public class PojoArrayToFlussArray {
    private final Object obj;
    private final DataType fieldType;
    private final String fieldName;

    /**
     * Pre-compiled element converter for ROW elements. Null when the element type is not ROW (the
     * generic {@link PojoTypeToFlussTypeConverter#convertElementValue} path is used instead).
     */
    private final Function<Object, Object> rowElementConverter;

    public PojoArrayToFlussArray(Object obj, DataType fieldType, String fieldName) {
        this.obj = obj;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.rowElementConverter = buildRowElementConverter(fieldType);
    }

    public GenericArray convertArray() {
        if (obj == null) {
            return null;
        }

        ArrayType arrayType = (ArrayType) fieldType;
        DataType elementType = arrayType.getElementType();

        // Handle primitive arrays
        if (obj instanceof boolean[]) {
            return new GenericArray((boolean[]) obj);
        } else if (obj instanceof long[]) {
            return new GenericArray((long[]) obj);
        } else if (obj instanceof double[]) {
            return new GenericArray((double[]) obj);
        } else if (obj instanceof float[]) {
            return new GenericArray((float[]) obj);
        } else if (obj instanceof short[]) {
            return new GenericArray((short[]) obj);
        } else if (obj instanceof byte[]) {
            return new GenericArray((byte[]) obj);
        } else if (obj instanceof int[]) {
            return new GenericArray((int[]) obj);
        }
        // Handle boxed wrapper arrays
        else if (obj instanceof Boolean[]) {
            return new GenericArray((Boolean[]) obj);
        } else if (obj instanceof Long[]) {
            return new GenericArray((Long[]) obj);
        } else if (obj instanceof Double[]) {
            return new GenericArray((Double[]) obj);
        } else if (obj instanceof Float[]) {
            return new GenericArray((Float[]) obj);
        } else if (obj instanceof Short[]) {
            return new GenericArray((Short[]) obj);
        } else if (obj instanceof Byte[]) {
            return new GenericArray((Byte[]) obj);
        }

        // Handle Object[] and java.util.Collection
        Object[] elements;
        if (obj instanceof Object[]) {
            elements = (Object[]) obj;
        } else if (obj instanceof Collection) {
            elements = ((Collection<?>) obj).toArray();
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s has unsupported array type: %s. Expected array or Collection.",
                            fieldName, obj.getClass().getName()));
        }

        Object[] converted = new Object[elements.length];
        if (rowElementConverter != null) {
            // ROW elements: use the pre-compiled converter to avoid re-creating it per element
            for (int i = 0; i < elements.length; i++) {
                converted[i] = elements[i] == null ? null : rowElementConverter.apply(elements[i]);
            }
        } else {
            for (int i = 0; i < elements.length; i++) {
                converted[i] = convertElementValue(elements[i], elementType, fieldName);
            }
        }
        return new GenericArray(converted);
    }

    /**
     * If the array element type is ROW, pre-compile a {@link PojoToRowConverter} once and return a
     * function that reuses it for every element. Returns {@code null} for non-ROW element types.
     */
    private static Function<Object, Object> buildRowElementConverter(DataType fieldType) {
        if (!(fieldType instanceof ArrayType)) {
            return null;
        }
        DataType elementType = ((ArrayType) fieldType).getElementType();
        if (elementType.getTypeRoot() != org.apache.fluss.types.DataTypeRoot.ROW) {
            return null;
        }
        // We cannot know the concrete POJO class at this point (it depends on the runtime
        // element), so we lazily create and cache the converter on the first non-null element.
        RowType nestedRowType = (RowType) elementType;
        // Use a holder array to cache the converter across lambda invocations
        @SuppressWarnings("unchecked")
        final PojoToRowConverter<Object>[] cache = new PojoToRowConverter[1];
        return (elem) -> {
            PojoToRowConverter<Object> converter = cache[0];
            if (converter == null) {
                @SuppressWarnings("unchecked")
                PojoToRowConverter<Object> c =
                        PojoToRowConverter.of(
                                (Class<Object>) elem.getClass(), nestedRowType, nestedRowType);
                cache[0] = c;
                converter = c;
            }
            return converter.toRow(elem);
        };
    }
}
