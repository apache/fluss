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

import org.apache.fluss.row.GenericMap;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.fluss.client.converter.PojoTypeToFlussTypeConverter.convertElementValue;

/** Adapter class for converting Pojo Map to Fluss InternalMap. */
public class PojoMapToFlussMap {
    private final Map<?, ?> pojoMap;
    private final MapType mapType;
    private final String fieldName;

    /**
     * Pre-compiled converters for ROW-typed keys/values. Null when the key/value type is not ROW.
     */
    private final Function<Object, Object> rowKeyConverter;

    private final Function<Object, Object> rowValueConverter;

    public PojoMapToFlussMap(Map<?, ?> pojoMap, MapType mapType, String fieldName) {
        this(pojoMap, mapType, fieldName, Object.class, Object.class);
    }

    /**
     * Creates a converter where the concrete Java types for ROW-typed keys and/or values are known
     * up front. Providing the declared classes allows the nested {@link PojoToRowConverter}s to be
     * built eagerly (once, at construction time) rather than lazily on the first entry.
     */
    public PojoMapToFlussMap(
            Map<?, ?> pojoMap,
            MapType mapType,
            String fieldName,
            Class<?> keyClass,
            Class<?> valueClass) {
        this.pojoMap = pojoMap;
        this.mapType = mapType;
        this.fieldName = fieldName;
        this.rowKeyConverter =
                buildRowConverter(mapType.getKeyType(), keyClass != null ? keyClass : Object.class);
        this.rowValueConverter =
                buildRowConverter(
                        mapType.getValueType(), valueClass != null ? valueClass : Object.class);
    }

    /**
     * Creates an adapter using pre-built row converters for ROW-typed keys and/or values, avoiding
     * the cost of calling {@link #buildRowConverter} on every row write. Both converters must have
     * been constructed once at field-converter setup time (e.g. in {@link PojoToRowConverter}).
     *
     * <p>A {@code null} converter for key or value means no ROW conversion is required for that
     * side; the generic {@link PojoTypeToFlussTypeConverter#convertElementValue} path is used
     * instead.
     */
    PojoMapToFlussMap(
            Map<?, ?> pojoMap,
            MapType mapType,
            String fieldName,
            Function<Object, Object> prebuiltKeyConverter,
            Function<Object, Object> prebuiltValueConverter) {
        this.pojoMap = pojoMap;
        this.mapType = mapType;
        this.fieldName = fieldName;
        this.rowKeyConverter = prebuiltKeyConverter;
        this.rowValueConverter = prebuiltValueConverter;
    }

    public GenericMap convertMap() {
        if (pojoMap == null) {
            return null;
        }

        Map<Object, Object> converted = new HashMap<>(pojoMap.size() * 2);
        for (Map.Entry<?, ?> entry : pojoMap.entrySet()) {
            Object convertedKey =
                    convertEntry(entry.getKey(), mapType.getKeyType(), rowKeyConverter);
            Object convertedValue =
                    convertEntry(entry.getValue(), mapType.getValueType(), rowValueConverter);
            converted.put(convertedKey, convertedValue);
        }

        // Build the result map
        return new GenericMap(converted);
    }

    private Object convertEntry(
            Object obj, DataType elementType, Function<Object, Object> rowConverter) {
        if (obj == null) {
            return null;
        }
        if (rowConverter != null) {
            return rowConverter.apply(obj);
        }
        return convertElementValue(obj, elementType, fieldName);
    }

    /**
     * If the data type is ROW, compile a {@link PojoToRowConverter} and return a function that
     * applies it to every key/value. Returns {@code null} for non-ROW types.
     *
     * <p>When {@code javaClass} is a concrete POJO class the converter is built eagerly. When
     * {@code javaClass} is {@code Object.class} the converter is built lazily from the first
     * non-null entry's runtime class and stored in an {@link AtomicReference} for thread safety.
     */
    static Function<Object, Object> buildRowConverter(DataType dataType, Class<?> javaClass) {
        if (dataType.getTypeRoot() != DataTypeRoot.ROW) {
            return null;
        }
        RowType nestedRowType = (RowType) dataType;

        if (javaClass != Object.class) {
            @SuppressWarnings("unchecked")
            PojoToRowConverter<Object> converter =
                    PojoToRowConverter.of((Class<Object>) javaClass, nestedRowType, nestedRowType);
            return converter::toRow;
        }

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
