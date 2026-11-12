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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Type-aware distinct collection, preserving the first element added for each logical value. */
final class ArrayDistinctCollector implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InternalArray.ElementGetter valueGetter;
    private final InternalArray.ElementGetter keyGetter;

    ArrayDistinctCollector(DataType elementType) {
        valueGetter = InternalArray.createElementGetter(elementType);
        keyGetter = InternalArray.createDeepElementGetter(elementType, false);
    }

    Accumulator newAccumulator() {
        return new Accumulator();
    }

    /**
     * Owns the collection for one aggregation call; the collector retains only type configuration.
     */
    final class Accumulator {
        private final Map<Object, Object> elements = new LinkedHashMap<>();

        void add(InternalArray source, int position) {
            Object key = distinctKey(keyGetter.getElementOrNull(source, position));
            if (!elements.containsKey(key)) {
                elements.put(key, valueGetter.getElementOrNull(source, position));
            }
        }

        int size() {
            return elements.size();
        }

        InternalArray toArray() {
            return new GenericArray(elements.values().toArray());
        }

        InternalArray toReversedArray() {
            Object[] result = new Object[elements.size()];
            int position = result.length;
            for (Object value : elements.values()) {
                result[--position] = value;
            }
            return new GenericArray(result);
        }
    }

    /** Canonical collection keys keep nested equality independent of the input encoding. */
    private static Object distinctKey(Object value) {
        if (value instanceof byte[]) {
            return new ByteArrayWrapper((byte[]) value);
        }
        if (value instanceof GenericMap) {
            GenericMap map = (GenericMap) value;
            Object[] keys = ((GenericArray) map.keyArray()).toObjectArray();
            Object[] values = ((GenericArray) map.valueArray()).toObjectArray();
            Map<Object, Object> key = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                key.put(distinctKey(keys[i]), distinctKey(values[i]));
            }
            return key;
        }
        Object[] fields;
        if (value instanceof GenericArray) {
            fields = ((GenericArray) value).toObjectArray();
        } else if (value instanceof GenericRow) {
            GenericRow row = (GenericRow) value;
            fields = new Object[row.getFieldCount()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = row.getField(i);
            }
        } else {
            return value;
        }
        List<Object> key = new ArrayList<>(fields.length);
        for (Object field : fields) {
            key.add(distinctKey(field));
        }
        return key;
    }
}
