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

package org.apache.fluss.lake.hudi.tiering.writer;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;

/** Wraps a Fluss {@link InternalMap} as a Hudi/Flink {@link MapData}. */
public class FlussMapAsHudiMap implements MapData {

    private final InternalMap flussMap;
    private final LogicalType keyType;
    private final LogicalType valueType;

    public FlussMapAsHudiMap(InternalMap flussMap, LogicalType keyType, LogicalType valueType) {
        this.flussMap = flussMap;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public int size() {
        return flussMap.size();
    }

    @Override
    public ArrayData keyArray() {
        InternalArray flussKeyArray = flussMap.keyArray();
        return flussKeyArray == null ? null : new FlussArrayAsHudiArray(flussKeyArray, keyType);
    }

    @Override
    public ArrayData valueArray() {
        InternalArray flussValueArray = flussMap.valueArray();
        return flussValueArray == null
                ? null
                : new FlussArrayAsHudiArray(flussValueArray, valueType);
    }
}
