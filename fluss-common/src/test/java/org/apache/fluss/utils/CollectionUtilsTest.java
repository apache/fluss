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

import static org.apache.fluss.utils.CollectionUtils.HASH_MAP_DEFAULT_LOAD_FACTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CollectionUtils}. */
class CollectionUtilsTest {

    @Test
    void testComputeRequiredCapacity() {
        // expectedSize <= 2 returns expectedSize + 1
        assertThat(CollectionUtils.computeRequiredCapacity(0, 0.75f)).isEqualTo(1);
        assertThat(CollectionUtils.computeRequiredCapacity(1, 0.75f)).isEqualTo(2);
        assertThat(CollectionUtils.computeRequiredCapacity(2, 0.75f)).isEqualTo(3);

        // expectedSize > 2 uses ceil(expectedSize / loadFactor)
        assertThat(CollectionUtils.computeRequiredCapacity(3, 0.75f)).isEqualTo(4);
        assertThat(CollectionUtils.computeRequiredCapacity(10, 0.75f)).isEqualTo(14);
        assertThat(CollectionUtils.computeRequiredCapacity(100, 0.75f)).isEqualTo(134);

        // Large expectedSize threshold
        int maxThreshold = Integer.MAX_VALUE / 2 + 1;
        assertThat(CollectionUtils.computeRequiredCapacity(maxThreshold, 0.75f))
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(CollectionUtils.computeRequiredCapacity(Integer.MAX_VALUE, 0.75f))
                .isEqualTo(Integer.MAX_VALUE);

        // Invalid arguments
        assertThatThrownBy(() -> CollectionUtils.computeRequiredCapacity(-1, 0.75f))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CollectionUtils.computeRequiredCapacity(5, 0f))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CollectionUtils.computeRequiredCapacity(5, -0.5f))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNewHashMapWithExpectedSize() {
        HashMap<String, Integer> map = CollectionUtils.newHashMapWithExpectedSize(5);
        assertThat(map).isNotNull();
        assertThat(map).isEmpty();

        for (int i = 0; i < 5; i++) {
            map.put("key" + i, i);
        }
        assertThat(map).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(map.get("key" + i)).isEqualTo(i);
        }

        // Test with 0 expected size
        HashMap<String, String> emptyMap = CollectionUtils.newHashMapWithExpectedSize(0);
        assertThat(emptyMap).isNotNull().isEmpty();

        // Test default load factor constant
        assertThat(HASH_MAP_DEFAULT_LOAD_FACTOR).isEqualTo(0.75f);
    }
}
