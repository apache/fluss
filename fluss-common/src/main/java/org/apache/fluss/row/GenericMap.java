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

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.types.MapType;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link MapType}.
 *
 * <p>{@link GenericMap} is a generic implementation of {@link InternalMap} which wraps regular Java
 * maps.
 *
 * <p>Note: All keys and values of this data structure must be internal data structures. All keys
 * must be of the same type; same for values.
 *
 * <p>Both keys and values can contain null for representing nullability.
 *
 * @since 0.9
 */
@PublicEvolving
public class GenericMap implements InternalMap, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<?, ?> map;
    private transient InternalArray keyArray;
    private transient InternalArray valueArray;

    public GenericMap(Map<?, ?> map) {
        this.map = map;
    }

    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public InternalArray keyArray() {
        if (keyArray == null) {
            initArrays();
        }
        return keyArray;
    }

    @Override
    public InternalArray valueArray() {
        if (valueArray == null) {
            initArrays();
        }
        return valueArray;
    }

    private void initArrays() {
        Object[] keys = new Object[map.size()];
        Object[] values = new Object[map.size()];
        int i = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            keys[i] = entry.getKey();
            values[i] = entry.getValue();
            i++;
        }
        this.keyArray = new GenericArray(keys);
        this.valueArray = new GenericArray(values);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof GenericMap)) {
            return false;
        }
        return deepEquals(map, ((GenericMap) o).map);
    }

    private static <K, V> boolean deepEquals(Map<K, V> m1, Map<?, ?> m2) {
        if (m1.size() != m2.size()) {
            return false;
        }
        try {
            for (Map.Entry<K, V> e : m1.entrySet()) {
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(m2.get(key) == null && m2.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!Objects.deepEquals(value, m2.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Object key : map.keySet()) {
            result += 31 * Objects.hashCode(key);
        }
        return result;
    }

    public static GenericMap of(Object... values) {
        if (values.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Arguments must be in key-value pairs (even number of elements)");
        }

        Map<Object, Object> javaMap = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            javaMap.put(values[i], values[i + 1]);
        }
        return new GenericMap(javaMap);
    }

    @Override
    public String toString() {
        return "GenericMap{" + "map=" + map + '}';
    }
}
