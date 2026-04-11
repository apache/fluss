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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.fluss.client.converter.PojoTypeToFlussTypeConverter.convertElementValue;

/** Adapter class for converting Pojo Array to Fluss InternalArray. */
public class PojoArrayToFlussArray {
    private final Object obj;
    private final DataType fieldType;
    private final String fieldName;

    /**
     * Per-element converter for ROW-typed elements, pre-compiled at construction time. {@code null}
     * when the element type is not ROW (the generic {@link
     * PojoTypeToFlussTypeConverter#convertElementValue} path is used instead).
     *
     * <p>When the concrete POJO component class is known at construction time the converter is
     * built eagerly and reused for every element. When the class is unknown ({@code Object.class})
     * it is built lazily from the first non-null element's runtime class and cached in an {@link
     * AtomicReference} to avoid a data race.
     */
    private final Function<Object, Object> rowElementConverter;

    public PojoArrayToFlussArray(Object obj, DataType fieldType, String fieldName) {
        this(obj, fieldType, fieldName, Object.class);
    }

    /**
     * Creates a converter where the concrete Java component type for ROW elements is known up
     * front. Passing the declared component class allows the nested {@link PojoToRowConverter} to
     * be built eagerly (once, at construction time) rather than lazily on the first element.
     */
    public PojoArrayToFlussArray(
            Object obj, DataType fieldType, String fieldName, Class<?> componentClass) {
        this.obj = obj;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.rowElementConverter =
                buildRowElementConverter(
                        fieldType, componentClass != null ? componentClass : Object.class);
    }

    /**
     * Creates an adapter using a pre-built row-element converter, avoiding the cost of calling
     * {@link #buildRowElementConverter} on every row write. The converter must have been
     * constructed once at field-converter setup time (e.g. in {@link PojoToRowConverter}).
     *
     * <p>Passing {@code null} signals that no ROW-element conversion is needed; the generic {@link
     * PojoTypeToFlussTypeConverter#convertElementValue} path is used for every element instead.
     */
    PojoArrayToFlussArray(
            Object obj,
            DataType fieldType,
            String fieldName,
            Function<Object, Object> prebuiltRowElementConverter) {
        this.obj = obj;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.rowElementConverter = prebuiltRowElementConverter;
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
     * If the array element type is ROW, compile a {@link PojoToRowConverter} and return a function
     * that applies it to every element. Returns {@code null} for non-ROW element types.
     *
     * <p>When {@code componentClass} is a concrete POJO class the converter is built eagerly, which
     * also means every element in the array is converted with a consistent converter regardless of
     * its runtime sub-type. When {@code componentClass} is {@code Object.class} (the type is not
     * known statically) the converter is built lazily from the first non-null element's runtime
     * class and stored in an {@link AtomicReference} so the initialisation is thread-safe.
     */
    static Function<Object, Object> buildRowElementConverter(
            DataType fieldType, Class<?> componentClass) {
        if (!(fieldType instanceof ArrayType)) {
            return null;
        }
        DataType elementType = ((ArrayType) fieldType).getElementType();
        if (elementType.getTypeRoot() != org.apache.fluss.types.DataTypeRoot.ROW) {
            return null;
        }
        RowType nestedRowType = (RowType) elementType;

        if (componentClass != Object.class) {
            // Component type is known at construction time — build the converter eagerly.
            @SuppressWarnings("unchecked")
            PojoToRowConverter<Object> converter =
                    PojoToRowConverter.of(
                            (Class<Object>) componentClass, nestedRowType, nestedRowType);
            return converter::toRow;
        }

        // Component type is unknown; lazily build from the first non-null element's runtime class
        // and reuse it for all subsequent elements. AtomicReference makes this thread-safe.
        AtomicReference<PojoToRowConverter<Object>> cacheRef = new AtomicReference<>();
        return (elem) -> {
            PojoToRowConverter<Object> converter = cacheRef.get();
            if (converter == null) {
                @SuppressWarnings("unchecked")
                PojoToRowConverter<Object> c =
                        PojoToRowConverter.of(
                                (Class<Object>) elem.getClass(), nestedRowType, nestedRowType);
                cacheRef.compareAndSet(null, c);
                converter = cacheRef.get();
            }
            return converter.toRow(elem);
        };
    }
}
