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

import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

/** Adapter class for converting Fluss InternalMap to Pojo map. */
public class FlussMapToPojoMap {
    private final InternalMap flussMap;
    private final MapType mapType;
    private final String fieldName;

    public FlussMapToPojoMap(InternalMap flussMap, MapType mapType, String fieldName) {
        this.flussMap = flussMap;
        this.mapType = mapType;
        this.fieldName = fieldName;
    }

    public Object convertMap() {
        if (flussMap == null) {
            return null;
        }

        DataType elementKeyType = mapType.getKeyType();
        Object keys =
                new FlussArrayToPojoArray(
                                flussMap.keyArray(), elementKeyType, fieldName, Object.class)
                        .convertArray();

        DataType elementValueType = mapType.getValueType();
        Object values =
                new FlussArrayToPojoArray(
                                flussMap.valueArray(), elementValueType, fieldName, Object.class)
                        .convertArray();

        // Build the result map
        Map<Object, Object> result = new HashMap<>();
        int size = Array.getLength(keys);
        for (int i = 0; i < size; i++) {
            result.put(Array.get(keys, i), Array.get(values, i));
        }
        return result;
    }
}
