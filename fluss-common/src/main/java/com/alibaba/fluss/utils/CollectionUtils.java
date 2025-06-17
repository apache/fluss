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

package com.alibaba.fluss.utils;

import java.util.HashMap;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** Simple utility to work with Java collections. */
public class CollectionUtils {
    /** The default load factor for hash maps create with this util class. */
    static final float HASH_MAP_DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Creates a new {@link HashMap} of the expected size, i.e. a hash map that will not rehash if
     * expectedSize many keys are inserted, considering the load factor.
     *
     * @param expectedSize the expected size of the created hash map.
     * @return a new hash map instance with enough capacity for the expected size.
     * @param <K> the type of keys maintained by this map.
     * @param <V> the type of mapped values.
     */
    public static <K, V> HashMap<K, V> newHashMapWithExpectedSize(int expectedSize) {
        return new HashMap<>(
                computeRequiredCapacity(expectedSize, HASH_MAP_DEFAULT_LOAD_FACTOR),
                HASH_MAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Helper method to compute the right capacity for a hash map with load factor
     * HASH_MAP_DEFAULT_LOAD_FACTOR.
     */
    static int computeRequiredCapacity(int expectedSize, float loadFactor) {
        checkArgument(expectedSize >= 0);
        checkArgument(loadFactor > 0f);
        if (expectedSize <= 2) {
            return expectedSize + 1;
        }
        return expectedSize < (Integer.MAX_VALUE / 2 + 1)
                ? (int) Math.ceil(expectedSize / loadFactor)
                : Integer.MAX_VALUE;
    }
}
