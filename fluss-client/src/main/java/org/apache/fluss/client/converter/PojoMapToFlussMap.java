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
        this.pojoMap = pojoMap;
        this.mapType = mapType;
        this.fieldName = fieldName;
        this.rowKeyConverter = buildRowConverter(mapType.getKeyType());
        this.rowValueConverter = buildRowConverter(mapType.getValueType());
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
     * If the data type is ROW, pre-compile a {@link PojoToRowConverter} once and return a function
     * that reuses it for every element. Returns {@code null} for non-ROW types.
     */
    private static Function<Object, Object> buildRowConverter(DataType dataType) {
        if (dataType.getTypeRoot() != DataTypeRoot.ROW) {
            return null;
        }
        RowType nestedRowType = (RowType) dataType;
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
