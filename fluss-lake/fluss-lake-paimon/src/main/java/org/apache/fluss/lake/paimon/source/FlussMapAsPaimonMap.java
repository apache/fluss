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

package org.apache.fluss.lake.paimon.source;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataType;

/** Adapter class for converting Fluss InternalMap to Paimon InternalMap. */
public class FlussMapAsPaimonMap implements InternalMap {

    private final org.apache.fluss.row.InternalMap flussMap;
    private final DataType keyType;
    private final DataType valueType;

    public FlussMapAsPaimonMap(
            org.apache.fluss.row.InternalMap flussMap, DataType keyType, DataType valueType) {
        this.flussMap = flussMap;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public int size() {
        return flussMap.size();
    }

    @Override
    public InternalArray keyArray() {
        org.apache.fluss.row.InternalArray flussKeyArray = flussMap.keyArray();
        return flussKeyArray == null ? null : new FlussArrayAsPaimonArray(flussKeyArray, keyType);
    }

    @Override
    public InternalArray valueArray() {
        org.apache.fluss.row.InternalArray flussValueArray = flussMap.valueArray();
        return flussValueArray == null
                ? null
                : new FlussArrayAsPaimonArray(flussValueArray, valueType);
    }
}
