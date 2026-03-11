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

package org.apache.fluss.server.coordinator.remote;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RoundRobinRemoteDirSelector}. */
class RoundRobinRemoteDirSelectorTest {

    private static final String DEFAULT_DIR = "hdfs://default/data";

    @Test
    void testEmptyRemoteDirsShouldReturnDefault() {
        RoundRobinRemoteDirSelector selector =
                new RoundRobinRemoteDirSelector(DEFAULT_DIR, Collections.emptyList());

        // Should always return default when remoteDataDirs is empty
        for (int i = 0; i < 10; i++) {
            assertThat(selector.nextDataDir()).isEqualTo(DEFAULT_DIR);
        }
    }

    @Test
    void testSingleDirShouldAlwaysReturnSame() {
        String dir = "hdfs://cluster/data1";
        RoundRobinRemoteDirSelector selector =
                new RoundRobinRemoteDirSelector(DEFAULT_DIR, Collections.singletonList(dir));

        // Should always return the single directory
        for (int i = 0; i < 10; i++) {
            assertThat(selector.nextDataDir()).isEqualTo(dir);
        }
    }

    @Test
    void testRoundRobinOrder() {
        List<String> dirs =
                Arrays.asList(
                        "hdfs://cluster/data1", "hdfs://cluster/data2", "hdfs://cluster/data3");

        RoundRobinRemoteDirSelector selector = new RoundRobinRemoteDirSelector(DEFAULT_DIR, dirs);

        // Collect selections for multiple cycles
        List<String> selections = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            selections.add(selector.nextDataDir());
        }

        // Verify round-robin pattern: each cycle should contain all dirs in order
        // First cycle
        assertThat(selections.subList(0, 3)).containsExactlyElementsOf(dirs);
        // Second cycle
        assertThat(selections.subList(3, 6)).containsExactlyElementsOf(dirs);
        // Third cycle
        assertThat(selections.subList(6, 9)).containsExactlyElementsOf(dirs);
    }

    @Test
    void testEvenDistribution() {
        List<String> dirs =
                Arrays.asList(
                        "hdfs://cluster/data1", "hdfs://cluster/data2", "hdfs://cluster/data3");

        RoundRobinRemoteDirSelector selector = new RoundRobinRemoteDirSelector(DEFAULT_DIR, dirs);

        Map<String, Integer> counts = new HashMap<>();
        int totalCalls = 30;

        for (int i = 0; i < totalCalls; i++) {
            String selected = selector.nextDataDir();
            counts.merge(selected, 1, Integer::sum);
        }

        // Each directory should be selected equally
        assertThat(counts.get(dirs.get(0))).isEqualTo(10);
        assertThat(counts.get(dirs.get(1))).isEqualTo(10);
        assertThat(counts.get(dirs.get(2))).isEqualTo(10);
    }

    @Test
    void testTwoDirs() {
        List<String> dirs = Arrays.asList("hdfs://cluster/data1", "hdfs://cluster/data2");

        RoundRobinRemoteDirSelector selector = new RoundRobinRemoteDirSelector(DEFAULT_DIR, dirs);

        // Verify alternating pattern
        assertThat(selector.nextDataDir()).isEqualTo(dirs.get(0));
        assertThat(selector.nextDataDir()).isEqualTo(dirs.get(1));
        assertThat(selector.nextDataDir()).isEqualTo(dirs.get(0));
        assertThat(selector.nextDataDir()).isEqualTo(dirs.get(1));
    }

    @Test
    void testCycleWrapsCorrectly() {
        List<String> dirs =
                Arrays.asList(
                        "hdfs://cluster/data1", "hdfs://cluster/data2", "hdfs://cluster/data3");

        RoundRobinRemoteDirSelector selector = new RoundRobinRemoteDirSelector(DEFAULT_DIR, dirs);

        // Collect first cycle
        List<String> firstCycle = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            firstCycle.add(selector.nextDataDir());
        }

        // Collect second cycle
        List<String> secondCycle = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            secondCycle.add(selector.nextDataDir());
        }

        // Both cycles should have same sequence
        assertThat(secondCycle).isEqualTo(firstCycle);
    }

    @Test
    void testAllDirsSelectedInOneCycle() {
        List<String> dirs =
                Arrays.asList(
                        "hdfs://cluster/data1",
                        "hdfs://cluster/data2",
                        "hdfs://cluster/data3",
                        "hdfs://cluster/data4",
                        "hdfs://cluster/data5");

        RoundRobinRemoteDirSelector selector = new RoundRobinRemoteDirSelector(DEFAULT_DIR, dirs);

        Set<String> selectedInCycle = new HashSet<>();
        for (int i = 0; i < dirs.size(); i++) {
            selectedInCycle.add(selector.nextDataDir());
        }

        // All directories should be selected exactly once in one cycle
        assertThat(selectedInCycle).containsExactlyInAnyOrderElementsOf(dirs);
    }
}
