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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;

/** Wraps a Paimon {@link org.apache.paimon.data.InternalMap} as a Fluss {@link InternalMap}. */
public class PaimonMapAsFlussMap implements InternalMap {

    private final org.apache.paimon.data.InternalMap paimonMap;

    public PaimonMapAsFlussMap(org.apache.paimon.data.InternalMap paimonMap) {
        this.paimonMap = paimonMap;
    }

    @Override
    public int size() {
        return paimonMap.size();
    }

    @Override
    public InternalArray keyArray() {
        return new PaimonArrayAsFlussArray(paimonMap.keyArray());
    }

    @Override
    public InternalArray valueArray() {
        return new PaimonArrayAsFlussArray(paimonMap.valueArray());
    }
}
