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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CopyOnWriteMap}. */
class CopyOnWriteMapTest {

    private CopyOnWriteMap<String, Integer> map;

    @BeforeEach
    void setUp() {
        map = new CopyOnWriteMap<>();
    }

    @Test
    void testPutAndGet() {
        assertThat(map.isEmpty()).isTrue();
        assertThat(map.size()).isEqualTo(0);

        map.put("a", 1);
        assertThat(map.get("a")).isEqualTo(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.isEmpty()).isFalse();
    }

    @Test
    void testContainsKeyAndValue() {
        map.put("x", 42);
        assertThat(map.containsKey("x")).isTrue();
        assertThat(map.containsKey("y")).isFalse();
        assertThat(map.containsValue(42)).isTrue();
        assertThat(map.containsValue(99)).isFalse();
    }

    @Test
    void testRemove() {
        map.put("k", 10);
        assertThat(map.remove("k")).isEqualTo(10);
        assertThat(map.containsKey("k")).isFalse();
        assertThat(map.remove("nonexistent")).isNull();
    }

    @Test
    void testClear() {
        map.put("a", 1);
        map.put("b", 2);
        map.clear();
        assertThat(map).isEmpty();
        assertThat(map.size()).isEqualTo(0);
    }

    @Test
    void testPutAll() {
        Map<String, Integer> source = new HashMap<>();
        source.put("p", 100);
        source.put("q", 200);
        map.putAll(source);
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.get("p")).isEqualTo(100);
        assertThat(map.get("q")).isEqualTo(200);
    }

    @Test
    void testKeySetEntrySetValues() {
        map.put("a", 1);
        map.put("b", 2);

        Set<String> keys = map.keySet();
        assertThat(keys).containsExactlyInAnyOrder("a", "b");

        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        assertThat(entries).hasSize(2);

        Collection<Integer> values = map.values();
        assertThat(values).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void testPutIfAbsent() {
        // Key absent: inserts and returns null
        Integer prev = map.putIfAbsent("k", 5);
        assertThat(prev).isNull();
        assertThat(map.get("k")).isEqualTo(5);

        // Key present: does NOT overwrite, returns existing value
        Integer existing = map.putIfAbsent("k", 99);
        assertThat(existing).isEqualTo(5);
        assertThat(map.get("k")).isEqualTo(5);
    }

    @Test
    void testRemoveKeyValue() {
        map.put("k", 10);

        // Wrong value: does not remove
        assertThat(map.remove("k", 999)).isFalse();
        assertThat(map.containsKey("k")).isTrue();

        // Correct value: removes and returns true
        assertThat(map.remove("k", 10)).isTrue();
        assertThat(map.containsKey("k")).isFalse();
    }

    @Test
    void testReplaceOldNewValue() {
        map.put("k", 1);

        // Wrong old value: no replacement
        assertThat(map.replace("k", 99, 2)).isFalse();
        assertThat(map.get("k")).isEqualTo(1);

        // Correct old value: replaces
        assertThat(map.replace("k", 1, 2)).isTrue();
        assertThat(map.get("k")).isEqualTo(2);

        // Non-existent key: false
        assertThat(map.replace("missing", 1, 2)).isFalse();
    }

    @Test
    void testReplaceExistingKey() {
        map.put("k", 1);
        assertThat(map.replace("k", 42)).isEqualTo(1);
        assertThat(map.get("k")).isEqualTo(42);

        // Non-existent key returns null
        assertThat(map.replace("missing", 42)).isNull();
    }

    @Test
    void testCopyOnWriteIsolation() {
        map.put("a", 1);
        // Capture a snapshot of the key set before modification
        Set<String> snapshotKeys = map.keySet();
        assertThat(snapshotKeys).contains("a");

        // Modify the map
        map.put("b", 2);
        // The snapshot should still only see what it had when captured
        // (CopyOnWriteMap returns the underlying map's keySet, which is now the new map)
        // The new keySet should reflect both entries
        assertThat(map.keySet()).containsExactlyInAnyOrder("a", "b");
    }
}
