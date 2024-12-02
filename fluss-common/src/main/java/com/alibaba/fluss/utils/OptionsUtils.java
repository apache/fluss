/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class for Options related helper functions. */
public class OptionsUtils {

    /** Filter condition for prefix map keys. */
    public static boolean filterPrefixMapKey(String key, String candidate) {
        final String prefixKey = key + ".";
        return candidate.startsWith(prefixKey);
    }

    public static <V> Map<String, V> convertToPropertiesWithPrefix(
            Map<String, V> configData, final String prefixKey, boolean removeKeyPrefix) {
        return convertToPropertiesWithPrefix(configData, prefixKey, removeKeyPrefix, null);
    }

    public static <V> Map<String, V> convertToPropertiesWithPrefix(
            Map<String, V> configData,
            final String prefixKey,
            boolean removePrefixKey,
            @Nullable String newPrefixKey) {
        final Map<String, V> result = new HashMap<>();
        for (Map.Entry<String, V> entry : configData.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith(prefixKey)) {
                String newKey = removePrefixKey ? key.substring(prefixKey.length()) : key;
                if (newPrefixKey != null) {
                    newKey = newPrefixKey + newKey;
                }
                result.put(newKey, entry.getValue());
            }
        }
        return result;
    }

    public static <V> Map<String, V> removePrefixMap(Map<String, V> configData, String key) {
        final List<String> prefixKeys =
                configData.keySet().stream()
                        .filter(candidate -> filterPrefixMapKey(key, candidate))
                        .collect(Collectors.toList());
        prefixKeys.forEach(configData::remove);
        return configData;
    }

    // Make sure that we cannot instantiate this class
    private OptionsUtils() {}
}
