/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.map;

import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.array.IndexedArray;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;

/**
 * A {@link BinaryMap} that uses {@link org.apache.fluss.row.indexed.IndexedRow} as the binary
 * format for nested row types.
 */
public class IndexedMap extends BinaryMap {

    public IndexedMap(DataType keyType, DataType valueType) {
        super(
                () -> new IndexedArray(keyType),
                () -> new IndexedArray(valueType),
                () -> {
                    if (valueType instanceof MapType) {
                        MapType mapType = (MapType) valueType;
                        return new IndexedMap(mapType.getKeyType(), mapType.getValueType());
                    } else {
                        throw new IllegalArgumentException(
                                "Can not get nested map from Map with value type " + valueType);
                    }
                });
    }
}
