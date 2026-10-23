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

package org.apache.fluss.metadata;

import org.apache.fluss.utils.serde.PartitionTombstoneBinarySerde;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionTombstoneTest {

    @Test
    void testFloorAndExplicitCombineWithUnionSemantics() {
        Set<Long> set = new HashSet<>();
        set.add(10L);
        PartitionTombstone tombstone = new PartitionTombstone(5L, set, 3L);
        assertThat(tombstone.isTombstoned(3L)).isTrue();
        assertThat(tombstone.isTombstoned(7L)).isFalse();
        assertThat(tombstone.isTombstoned(10L)).isTrue();
    }

    @Test
    void testRejectsExplicitIdsCoveredByFloor() {
        assertThatThrownBy(
                        () -> new PartitionTombstone(5L, new HashSet<>(Arrays.asList(5L, 7L)), 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than floor");
    }

    @Test
    void testBinarySerdeRoundTrip() {
        Set<Long> explicit = new HashSet<>(Arrays.asList(5L, 8L, 12L));
        PartitionTombstone tombstone = new PartitionTombstone(3L, explicit, 7L);

        byte[] bytes = PartitionTombstoneBinarySerde.serialize(tombstone);
        PartitionTombstone deserialized = PartitionTombstoneBinarySerde.deserialize(bytes);

        assertThat(deserialized.getFloor()).isEqualTo(3L);
        assertThat(deserialized.getExplicitSet()).containsExactlyInAnyOrder(5L, 8L, 12L);
        assertThat(deserialized.getVersion()).isEqualTo(7L);
    }

    @Test
    void testBinarySerdeRejectsNonCanonicalExplicitIds() {
        PartitionTombstone tombstone =
                new PartitionTombstone(3L, new HashSet<>(Arrays.asList(5L, 8L)), 1L);
        byte[] bytes = PartitionTombstoneBinarySerde.serialize(tombstone);
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(24, 8L);
        buffer.putLong(32, 5L);

        assertThatThrownBy(() -> PartitionTombstoneBinarySerde.deserialize(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly increasing");

        buffer.putLong(24, 3L);
        buffer.putLong(32, 8L);
        assertThatThrownBy(() -> PartitionTombstoneBinarySerde.deserialize(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than floor");
    }
}
