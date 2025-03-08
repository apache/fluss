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

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Utilities that expand the usage of {@link Map}. */
public class MapUtils {

    public static <K, V> ConcurrentHashMap<K, V> newConcurrentHashMap() {
        if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
            return new ConcurrentHashMap<>();
        } else {
            return new ConcurrentHashMapForJDK8<>();
        }
    }

    /**
     * For JDK8, there is a bug in {@code ConcurrentHashMap#computeIfAbsent}, checking the key
     * existence to speed up. See details in JDK-8161372.
     */
    private static class ConcurrentHashMapForJDK8<K, V> extends ConcurrentHashMap<K, V> {

        public ConcurrentHashMapForJDK8() {
            super();
        }

        @Override
        public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
            V result;
            if (null == (result = get(key))) {
                result = super.computeIfAbsent(key, mappingFunction);
            }
            return result;
        }
    }
}
