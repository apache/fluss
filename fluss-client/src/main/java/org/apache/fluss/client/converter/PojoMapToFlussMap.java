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
import org.apache.fluss.types.MapType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.client.converter.PojoTypeToFlussTypeConverter.convertElementValue;

/** Adapter class for converting Pojo Map to Fluss InternalMap. */
public class PojoMapToFlussMap {
    private final Map<?, ?> pojoMap;
    private final MapType mapType;
    private final String fieldName;

    public PojoMapToFlussMap(Map<?, ?> pojoMap, MapType mapType, String fieldName) {
        this.pojoMap = pojoMap;
        this.mapType = mapType;
        this.fieldName = fieldName;
    }

    public GenericMap convertMap() {
        if (pojoMap == null) {
            return null;
        }

        Map<Object, Object> converted = new HashMap<>(pojoMap.size() * 2);
        for (Map.Entry<?, ?> entry : pojoMap.entrySet()) {
            Object convertedKey =
                    convertElementValue(entry.getKey(), mapType.getKeyType(), fieldName);
            Object convertedValue =
                    convertElementValue(entry.getValue(), mapType.getValueType(), fieldName);
            converted.put(convertedKey, convertedValue);
        }

        // Build the result map
        return new GenericMap(converted);
    }
}
