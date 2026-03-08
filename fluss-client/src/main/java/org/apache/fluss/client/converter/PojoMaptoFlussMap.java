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

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.MapType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Adapter class for converting Pojo Map to Fluss InternalMap. */
public class PojoMaptoFlussMap implements InternalMap {
    private final Map<?, ?> pojoMap;
    private final MapType mapType;
    private final String fieldName;

    public PojoMaptoFlussMap(Map<?, ?> pojoMap, MapType mapType, String fieldName) {
        this.pojoMap = pojoMap;
        this.mapType = mapType;
        this.fieldName = fieldName;
    }

    @Override
    public int size() {
        return pojoMap.size();
    }

    @Override
    public InternalArray keyArray() {
        List<?> pojoArray = new ArrayList<>(pojoMap.keySet());
        ArrayType keyArrayType = new ArrayType(mapType.getKeyType());
        return new PojoArrayToFlussArray(pojoArray, keyArrayType, fieldName).convertArray();
    }

    @Override
    public InternalArray valueArray() {
        List<?> pojoArray = new ArrayList<>(pojoMap.values());
        ArrayType valueArrayType = new ArrayType(mapType.getValueType());
        return new PojoArrayToFlussArray(pojoArray, valueArrayType, fieldName).convertArray();
    }
}
