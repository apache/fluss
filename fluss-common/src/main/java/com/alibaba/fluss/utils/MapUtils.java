/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import java.util.HashMap;
import java.util.Map;

/** Map utils. */
public class MapUtils {

    private MapUtils() {}

    /**
     * Extract from the original map by key prefix to a new map.
     *
     * @param originMap The original map.
     * @param keyPrefix The key prefix.
     * @param removeKeyPrefix If remote the key prefix when generating the extracted map.
     * @return The map result extracted by key prefix from the original map.
     * @param <V> The value type of the map.
     */
    public static <V> Map<String, V> extractByKeyPrefix(
            Map<String, V> originMap, String keyPrefix, boolean removeKeyPrefix) {
        final Map<String, V> resultMap = new HashMap<>();
        for (Map.Entry<String, V> configEntry : originMap.entrySet()) {
            final String originalKey = configEntry.getKey();
            final V originalVal = configEntry.getValue();
            if (originalKey.startsWith(keyPrefix)) {
                final String newKey =
                        removeKeyPrefix ? originalKey.substring(keyPrefix.length()) : originalKey;
                resultMap.put(newKey, originalVal);
            }
        }
        return resultMap;
    }
}
