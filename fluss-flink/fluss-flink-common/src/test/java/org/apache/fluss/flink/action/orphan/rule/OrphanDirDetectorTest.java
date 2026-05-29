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

package org.apache.fluss.flink.action.orphan.rule;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OrphanDirDetector}. */
class OrphanDirDetectorTest {

    // --- Table directory detection ---

    @Test
    void tableOrphanWhenIdLeMaxKnown() {
        assertThat(OrphanDirDetector.isOrphanTable("foo-15", ids(10L, 20L), 30L)).isTrue();
    }

    @Test
    void tableNotOrphanWhenIdGreaterThanMaxKnown() {
        assertThat(OrphanDirDetector.isOrphanTable("foo-99", ids(10L, 20L), 30L)).isFalse();
    }

    @Test
    void tableNotOrphanWhenInActiveSet() {
        assertThat(OrphanDirDetector.isOrphanTable("foo-10", ids(10L, 20L), 30L)).isFalse();
    }

    @Test
    void tableNotOrphanWhenNameFormatBad() {
        assertThat(OrphanDirDetector.isOrphanTable("no_id_here", Collections.<Long>emptySet(), 10L))
                .isFalse();
    }

    // --- Partition directory detection ---

    @Test
    void partitionOrphanWhenIdLeMaxKnown() {
        assertThat(OrphanDirDetector.isOrphanPartition("dt=2024-p150", ids(101L, 102L), 200L))
                .isTrue();
    }

    @Test
    void partitionNotOrphanWhenIdGreaterThanMaxKnown() {
        assertThat(
                        OrphanDirDetector.isOrphanPartition(
                                "dt=2024-p250", Collections.<Long>emptySet(), 200L))
                .isFalse();
    }

    @Test
    void partitionNotOrphanWhenInActiveSet() {
        assertThat(OrphanDirDetector.isOrphanPartition("dt=2024-p150", ids(150L), 200L)).isFalse();
    }

    @Test
    void partitionNotOrphanWhenMissingPPrefix() {
        assertThat(OrphanDirDetector.isOrphanPartition("0", Collections.<Long>emptySet(), 200L))
                .isFalse();
    }

    private static Set<Long> ids(Long... values) {
        return new HashSet<Long>(Arrays.asList(values));
    }
}
