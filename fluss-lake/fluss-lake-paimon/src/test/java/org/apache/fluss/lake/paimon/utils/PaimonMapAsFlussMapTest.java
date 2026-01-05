/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.InternalArray;

import org.apache.paimon.data.GenericArray;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonMapAsFlussMap}. */
class PaimonMapAsFlussMapTest {

    @Test
    void testSize() {
        org.apache.paimon.data.InternalArray keys = new GenericArray(new Object[] {1, 2, 3});
        org.apache.paimon.data.InternalArray values =
                new GenericArray(new Object[] {"a", "b", "c"});
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        assertThat(flussMap.size()).isEqualTo(3);
    }

    @Test
    void testEmptyMap() {
        org.apache.paimon.data.InternalArray keys = new GenericArray(new Object[] {});
        org.apache.paimon.data.InternalArray values = new GenericArray(new Object[] {});
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        assertThat(flussMap.size()).isEqualTo(0);
    }

    @Test
    void testKeyArray() {
        org.apache.paimon.data.InternalArray keys = new GenericArray(new Object[] {1, 2, 3});
        org.apache.paimon.data.InternalArray values =
                new GenericArray(new Object[] {"a", "b", "c"});
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        InternalArray keyArray = flussMap.keyArray();
        assertThat(keyArray).isInstanceOf(PaimonArrayAsFlussArray.class);
        assertThat(keyArray.size()).isEqualTo(3);
        assertThat(keyArray.getInt(0)).isEqualTo(1);
        assertThat(keyArray.getInt(1)).isEqualTo(2);
        assertThat(keyArray.getInt(2)).isEqualTo(3);
    }

    @Test
    void testValueArray() {
        org.apache.paimon.data.InternalArray keys = new GenericArray(new Object[] {1, 2, 3});
        org.apache.paimon.data.InternalArray values =
                new GenericArray(
                        new Object[] {
                            org.apache.paimon.data.BinaryString.fromString("a"),
                            org.apache.paimon.data.BinaryString.fromString("b"),
                            org.apache.paimon.data.BinaryString.fromString("c")
                        });
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        InternalArray valueArray = flussMap.valueArray();
        assertThat(valueArray).isInstanceOf(PaimonArrayAsFlussArray.class);
        assertThat(valueArray.size()).isEqualTo(3);
        assertThat(valueArray.getString(0).toString()).isEqualTo("a");
        assertThat(valueArray.getString(1).toString()).isEqualTo("b");
        assertThat(valueArray.getString(2).toString()).isEqualTo("c");
    }

    @Test
    void testMapWithComplexTypes() {
        GenericArray innerArray1 = new GenericArray(new Object[] {1, 2});
        GenericArray innerArray2 = new GenericArray(new Object[] {3, 4});

        org.apache.paimon.data.InternalArray keys =
                new GenericArray(
                        new Object[] {
                            org.apache.paimon.data.BinaryString.fromString("key1"),
                            org.apache.paimon.data.BinaryString.fromString("key2")
                        });
        org.apache.paimon.data.InternalArray values =
                new GenericArray(new Object[] {innerArray1, innerArray2});
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        assertThat(flussMap.size()).isEqualTo(2);

        InternalArray keyArray = flussMap.keyArray();
        assertThat(keyArray.getString(0).toString()).isEqualTo("key1");
        assertThat(keyArray.getString(1).toString()).isEqualTo("key2");

        InternalArray valueArray = flussMap.valueArray();
        InternalArray nestedArray1 = valueArray.getArray(0);
        assertThat(nestedArray1.size()).isEqualTo(2);
        assertThat(nestedArray1.getInt(0)).isEqualTo(1);
        assertThat(nestedArray1.getInt(1)).isEqualTo(2);

        InternalArray nestedArray2 = valueArray.getArray(1);
        assertThat(nestedArray2.size()).isEqualTo(2);
        assertThat(nestedArray2.getInt(0)).isEqualTo(3);
        assertThat(nestedArray2.getInt(1)).isEqualTo(4);
    }

    @Test
    void testMapWithNullValues() {
        org.apache.paimon.data.InternalArray keys = new GenericArray(new Object[] {1, 2, 3});
        org.apache.paimon.data.InternalArray values =
                new GenericArray(new Object[] {"a", null, "c"});
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        InternalArray valueArray = flussMap.valueArray();
        assertThat(valueArray.isNullAt(0)).isFalse();
        assertThat(valueArray.isNullAt(1)).isTrue();
        assertThat(valueArray.isNullAt(2)).isFalse();
    }

    @Test
    void testMapWithLongKeys() {
        org.apache.paimon.data.InternalArray keys =
                new GenericArray(new Object[] {100L, 200L, 300L});
        org.apache.paimon.data.InternalArray values =
                new GenericArray(new Object[] {1.1, 2.2, 3.3});
        TestPaimonMap paimonMap = new TestPaimonMap(keys, values);

        PaimonMapAsFlussMap flussMap = new PaimonMapAsFlussMap(paimonMap);

        assertThat(flussMap.size()).isEqualTo(3);

        InternalArray keyArray = flussMap.keyArray();
        assertThat(keyArray.getLong(0)).isEqualTo(100L);
        assertThat(keyArray.getLong(1)).isEqualTo(200L);
        assertThat(keyArray.getLong(2)).isEqualTo(300L);

        InternalArray valueArray = flussMap.valueArray();
        assertThat(valueArray.getDouble(0)).isEqualTo(1.1);
        assertThat(valueArray.getDouble(1)).isEqualTo(2.2);
        assertThat(valueArray.getDouble(2)).isEqualTo(3.3);
    }

    private static class TestPaimonMap implements org.apache.paimon.data.InternalMap {
        private final org.apache.paimon.data.InternalArray keys;
        private final org.apache.paimon.data.InternalArray values;

        TestPaimonMap(
                org.apache.paimon.data.InternalArray keys,
                org.apache.paimon.data.InternalArray values) {
            this.keys = keys;
            this.values = values;
        }

        @Override
        public int size() {
            return keys.size();
        }

        @Override
        public org.apache.paimon.data.InternalArray keyArray() {
            return keys;
        }

        @Override
        public org.apache.paimon.data.InternalArray valueArray() {
            return values;
        }
    }
}
