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

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.MapType;

import java.util.HashMap;
import java.util.Map;

/** Merge two maps. */
public class FieldMergeMapAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private final InternalArray.ElementGetter keyGetter;
    private final InternalArray.ElementGetter valueGetter;

    public FieldMergeMapAgg(MapType dataType) {
        super(dataType);
        this.keyGetter = InternalArray.createElementGetter(dataType.getKeyType());
        this.valueGetter = InternalArray.createElementGetter(dataType.getValueType());
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        InternalMap accumulatorMap = (InternalMap) accumulator;
        InternalMap inputMap = (InternalMap) inputField;
        if (accumulatorMap.size() == 0) {
            return inputField;
        }
        if (inputMap.size() == 0) {
            return accumulator;
        }

        int expectedSize = accumulatorMap.size() + inputMap.size();
        int capacity = (int) (expectedSize / 0.75f) + 1;
        Map<Object, Object> resultMap = new HashMap<>(capacity);
        putToMap(resultMap, accumulatorMap);
        putToMap(resultMap, inputMap);
        return new GenericMap(resultMap);
    }

    private void putToMap(Map<Object, Object> map, InternalMap mapData) {
        InternalArray keyArray = mapData.keyArray();
        InternalArray valueArray = mapData.valueArray();
        for (int i = 0; i < keyArray.size(); i++) {
            Object key = keyGetter.getElementOrNull(keyArray, i);
            Object value = valueGetter.getElementOrNull(valueArray, i);
            map.put(key, value);
        }
    }
}
