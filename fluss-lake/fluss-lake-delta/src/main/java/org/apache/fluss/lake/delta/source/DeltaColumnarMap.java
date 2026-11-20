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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;

import io.delta.kernel.data.MapValue;

/** A Fluss {@link InternalMap} view of a Delta Kernel map value. */
class DeltaColumnarMap implements InternalMap {

    private final MapValue map;

    DeltaColumnarMap(MapValue map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.getSize();
    }

    @Override
    public InternalArray keyArray() {
        return new DeltaColumnarArray(map.getKeys(), map.getSize());
    }

    @Override
    public InternalArray valueArray() {
        return new DeltaColumnarArray(map.getValues(), map.getSize());
    }
}
