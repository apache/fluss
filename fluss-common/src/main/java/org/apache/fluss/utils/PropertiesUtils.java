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

package org.apache.fluss.utils;

import java.util.Map;
import java.util.stream.Collectors;

/** Utility class for properties related helper functions. */
public class PropertiesUtils {

    /** Returns the properties as a map copy with a prefix key. */
    public static <V> Map<String, V> asPrefixedMap(Map<String, V> properties, String prefix) {
        return properties.entrySet().stream()
                .collect(Collectors.toMap(e -> prefix + e.getKey(), Map.Entry::getValue));
    }

    /**
     * Extracts the properties with the given prefix and removes the prefix from the keys.
     *
     * @param originalMap The original map
     * @param prefix The prefix to filter the keys
     */
    public static <V> Map<String, V> extractAndRemovePrefix(
            Map<String, V> originalMap, String prefix) {
        return originalMap.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().substring(prefix.length()),
                                Map.Entry::getValue));
    }

    /**
     * Extracts the properties with the given prefix.
     *
     * @param originalMap The original map
     * @param prefix The prefix to filter the keys
     */
    public static <V> Map<String, V> extractPrefix(Map<String, V> originalMap, String prefix) {
        return originalMap.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Filter out keys that start with the given prefix from the original map. */
    public static <T> Map<String, T> excludeByPrefix(Map<String, T> originalMap, String prefix) {
        return originalMap.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // Make sure that we cannot instantiate this class
    private PropertiesUtils() {}
}
