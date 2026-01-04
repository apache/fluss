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

package org.apache.fluss.row;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link GenericMap}. */
public class GenericMapTest {

    @Test
    public void testConstructorAndGet() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("a", 1);
        javaMap.put("b", 2);
        javaMap.put("c", 3);

        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.get("a")).isEqualTo(1);
        assertThat(genericMap.get("b")).isEqualTo(2);
        assertThat(genericMap.get("c")).isEqualTo(3);
        assertThat(genericMap.get("d")).isNull();
    }

    @Test
    public void testSize() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("key1", 100);
        javaMap.put("key2", 200);
        javaMap.put("key3", 300);
        javaMap.put("key4", 400);

        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.size()).isEqualTo(4);
    }

    @Test
    public void testEmptyMap() {
        Map<String, Integer> javaMap = new HashMap<>();
        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.size()).isEqualTo(0);
        assertThat(genericMap.keyArray().size()).isEqualTo(0);
        assertThat(genericMap.valueArray().size()).isEqualTo(0);
    }

    @Test
    public void testKeyArrayAndValueArray() {
        Map<String, Integer> javaMap = new LinkedHashMap<>();
        javaMap.put("first", 1);
        javaMap.put("second", 2);
        javaMap.put("third", 3);

        GenericMap genericMap = new GenericMap(javaMap);

        InternalArray keyArray = genericMap.keyArray();
        InternalArray valueArray = genericMap.valueArray();

        assertThat(keyArray.size()).isEqualTo(3);
        assertThat(valueArray.size()).isEqualTo(3);

        assertThat(keyArray.isNullAt(0)).isFalse();
        assertThat(valueArray.isNullAt(0)).isFalse();
    }

    @Test
    public void testKeyArrayCaching() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("key", 42);

        GenericMap genericMap = new GenericMap(javaMap);

        InternalArray keyArray1 = genericMap.keyArray();
        InternalArray keyArray2 = genericMap.keyArray();

        assertThat(keyArray1).isSameAs(keyArray2);
    }

    @Test
    public void testValueArrayCaching() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("key", 42);

        GenericMap genericMap = new GenericMap(javaMap);

        InternalArray valueArray1 = genericMap.valueArray();
        InternalArray valueArray2 = genericMap.valueArray();

        assertThat(valueArray1).isSameAs(valueArray2);
    }

    @Test
    public void testMapWithNullValues() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("key1", 1);
        javaMap.put("key2", null);
        javaMap.put("key3", 3);

        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.size()).isEqualTo(3);
        assertThat(genericMap.get("key1")).isEqualTo(1);
        assertThat(genericMap.get("key2")).isNull();
        assertThat(genericMap.get("key3")).isEqualTo(3);
    }

    @Test
    public void testOfMethodWithEvenNumberOfArgs() {
        GenericMap genericMap = GenericMap.of("key1", 1, "key2", 2, "key3", 3);

        assertThat(genericMap.size()).isEqualTo(3);
        assertThat(genericMap.get("key1")).isEqualTo(1);
        assertThat(genericMap.get("key2")).isEqualTo(2);
        assertThat(genericMap.get("key3")).isEqualTo(3);
    }

    @Test
    public void testOfMethodWithOddNumberOfArgs() {
        assertThatThrownBy(() -> GenericMap.of("key1", 1, "key2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("even number of elements");
    }

    @Test
    public void testOfMethodEmpty() {
        GenericMap genericMap = GenericMap.of();

        assertThat(genericMap.size()).isEqualTo(0);
    }

    @Test
    public void testOfMethodWithNullValue() {
        GenericMap genericMap = GenericMap.of("key1", 1, "key2", null, "key3", 3);

        assertThat(genericMap.size()).isEqualTo(3);
        assertThat(genericMap.get("key1")).isEqualTo(1);
        assertThat(genericMap.get("key2")).isNull();
        assertThat(genericMap.get("key3")).isEqualTo(3);
    }

    @Test
    public void testOfMethodPreservesOrder() {
        GenericMap genericMap = GenericMap.of("first", 1, "second", 2, "third", 3, "fourth", 4);

        assertThat(genericMap.size()).isEqualTo(4);

        InternalArray keyArray = genericMap.keyArray();
        assertThat(keyArray.size()).isEqualTo(4);
    }

    @Test
    public void testMapWithComplexValues() {
        Map<String, Object> javaMap = new HashMap<>();
        javaMap.put("string", "value");
        javaMap.put("int", 42);
        javaMap.put("long", 100L);
        javaMap.put("double", 3.14);

        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.size()).isEqualTo(4);
        assertThat(genericMap.get("string")).isEqualTo("value");
        assertThat(genericMap.get("int")).isEqualTo(42);
        assertThat(genericMap.get("long")).isEqualTo(100L);
        assertThat(genericMap.get("double")).isEqualTo(3.14);
    }

    @Test
    public void testMapWithBinaryStringKeys() {
        Map<BinaryString, Integer> javaMap = new HashMap<>();
        javaMap.put(BinaryString.fromString("key1"), 1);
        javaMap.put(BinaryString.fromString("key2"), 2);

        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.size()).isEqualTo(2);
        assertThat(genericMap.get(BinaryString.fromString("key1"))).isEqualTo(1);
        assertThat(genericMap.get(BinaryString.fromString("key2"))).isEqualTo(2);
    }

    @Test
    public void testSingletonMap() {
        Map<String, String> javaMap = new HashMap<>();
        javaMap.put("only", "value");

        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.size()).isEqualTo(1);
        assertThat(genericMap.get("only")).isEqualTo("value");
        assertThat(genericMap.keyArray().size()).isEqualTo(1);
        assertThat(genericMap.valueArray().size()).isEqualTo(1);
    }

    @Test
    public void testEqualsSameObject() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("key1", 1);
        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.equals(genericMap)).isTrue();
    }

    @Test
    public void testEqualsWithDifferentType() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put("key1", 1);
        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.equals("not a map")).isFalse();
        assertThat(genericMap.equals(null)).isFalse();
    }

    @Test
    public void testEqualsWithEqualMaps() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", 1);
        javaMap1.put("key2", 2);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key1", 1);
        javaMap2.put("key2", 2);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isTrue();
    }

    @Test
    public void testEqualsWithDifferentSizes() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", 1);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key1", 1);
        javaMap2.put("key2", 2);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isFalse();
    }

    @Test
    public void testEqualsWithNullValue() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", null);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key1", null);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isTrue();
    }

    @Test
    public void testEqualsWithDifferentNullValue() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", null);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key1", 1);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isFalse();
    }

    @Test
    public void testEqualsWithMissingKey() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", 1);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key2", 1);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isFalse();
    }

    @Test
    public void testEqualsWithDifferentValues() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", 1);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key1", 2);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isFalse();
    }

    @Test
    public void testEqualsWithArrayValues() {
        Map<String, int[]> javaMap1 = new HashMap<>();
        javaMap1.put("key1", new int[] {1, 2, 3});

        Map<String, int[]> javaMap2 = new HashMap<>();
        javaMap2.put("key1", new int[] {1, 2, 3});

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.equals(map2)).isTrue();
    }

    @Test
    public void testHashCodeConsistency() {
        Map<String, Integer> javaMap1 = new HashMap<>();
        javaMap1.put("key1", 1);
        javaMap1.put("key2", 2);

        Map<String, Integer> javaMap2 = new HashMap<>();
        javaMap2.put("key1", 1);
        javaMap2.put("key2", 2);

        GenericMap map1 = new GenericMap(javaMap1);
        GenericMap map2 = new GenericMap(javaMap2);

        assertThat(map1.hashCode()).isEqualTo(map2.hashCode());
    }

    @Test
    public void testHashCodeWithNullKeys() {
        Map<String, Integer> javaMap = new HashMap<>();
        javaMap.put(null, 1);
        javaMap.put("key2", 2);

        GenericMap genericMap = new GenericMap(javaMap);

        int hashCode = genericMap.hashCode();
        assertThat(hashCode).isNotZero();
    }

    @Test
    public void testHashCodeWithEmptyMap() {
        Map<String, Integer> javaMap = new HashMap<>();
        GenericMap genericMap = new GenericMap(javaMap);

        assertThat(genericMap.hashCode()).isEqualTo(0);
    }
}
