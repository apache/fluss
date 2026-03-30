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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ByteArrayWrapper}. */
class ByteArrayWrapperTest {

    @Test
    void testEqualsAndHashCode() {
        byte[] data1 = new byte[] {1, 2, 3};
        byte[] data2 = new byte[] {1, 2, 3};
        byte[] data3 = new byte[] {1, 2, 4};

        ByteArrayWrapper wrapper1 = new ByteArrayWrapper(data1);
        ByteArrayWrapper wrapper2 = new ByteArrayWrapper(data2);
        ByteArrayWrapper wrapper3 = new ByteArrayWrapper(data3);

        // Same content should be equal
        assertThat(wrapper1).isEqualTo(wrapper2);
        assertThat(wrapper1.hashCode()).isEqualTo(wrapper2.hashCode());

        // Different content should not be equal
        assertThat(wrapper1).isNotEqualTo(wrapper3);
    }

    @Test
    void testNullDataThrowsException() {
        assertThatThrownBy(() -> new ByteArrayWrapper(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("data cannot be null");
    }

    @Test
    void testAsMapKey() {
        byte[] key1 = new byte[] {1, 2, 3};
        byte[] key2 = new byte[] {1, 2, 3}; // Same content, different array
        byte[] key3 = new byte[] {4, 5, 6};

        Map<ByteArrayWrapper, String> map = new HashMap<>();
        map.put(new ByteArrayWrapper(key1), "value1");

        // Should find with same content
        assertThat(map.get(new ByteArrayWrapper(key2))).isEqualTo("value1");

        // Should not find with different content
        assertThat(map.get(new ByteArrayWrapper(key3))).isNull();

        // Should overwrite with same key
        map.put(new ByteArrayWrapper(key2), "value2");
        assertThat(map).hasSize(1);
        assertThat(map.get(new ByteArrayWrapper(key1))).isEqualTo("value2");
    }

    @Test
    void testGetData() {
        byte[] data = new byte[] {1, 2, 3};
        ByteArrayWrapper wrapper = new ByteArrayWrapper(data);

        assertThat(wrapper.getData()).isSameAs(data);
    }

    @Test
    void testEmptyArray() {
        ByteArrayWrapper wrapper1 = new ByteArrayWrapper(new byte[0]);
        ByteArrayWrapper wrapper2 = new ByteArrayWrapper(new byte[0]);

        assertThat(wrapper1).isEqualTo(wrapper2);
        assertThat(wrapper1.hashCode()).isEqualTo(wrapper2.hashCode());
    }

    @Test
    void testToString() {
        ByteArrayWrapper wrapper = new ByteArrayWrapper(new byte[] {1, 2, 3});
        assertThat(wrapper.toString()).contains("length=3");
    }
}
