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

import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WeightedRoundRobinRemoteDirSelector}. */
class WeightedRoundRobinRemoteDirSelectorTest {

    private static final FsPath DEFAULT_DIR = new FsPath("hdfs://default/data");

    @Test
    void testEmptyRemoteDirsShouldReturnDefault() {
        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(
                        DEFAULT_DIR, Collections.emptyList(), Collections.emptyList());

        // Should always return default when remoteDataDirs is empty
        for (int i = 0; i < 10; i++) {
            assertThat(selector.nextDataDir()).isEqualTo(DEFAULT_DIR);
        }
    }

    @Test
    void testSingleDirShouldAlwaysReturnSame() {
        FsPath dir = new FsPath("hdfs://cluster/data1");
        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(
                        DEFAULT_DIR, Collections.singletonList(dir), Collections.singletonList(5));

        // Should always return the single directory
        for (int i = 0; i < 10; i++) {
            assertThat(selector.nextDataDir()).isEqualTo(dir);
        }
    }

    @Test
    void testEqualWeightsShouldDistributeEvenly() {
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/data1"),
                        new FsPath("hdfs://cluster/data2"),
                        new FsPath("hdfs://cluster/data3"));
        List<Integer> weights = Arrays.asList(1, 1, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        Map<FsPath, Integer> counts = new HashMap<>();
        int totalCalls = 30;

        for (int i = 0; i < totalCalls; i++) {
            FsPath selected = selector.nextDataDir();
            counts.merge(selected, 1, Integer::sum);
        }

        // Each directory should be selected equally
        assertThat(counts.get(dirs.get(0))).isEqualTo(10);
        assertThat(counts.get(dirs.get(1))).isEqualTo(10);
        assertThat(counts.get(dirs.get(2))).isEqualTo(10);
    }

    @Test
    void testWeightedDistribution() {
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/data1"),
                        new FsPath("hdfs://cluster/data2"),
                        new FsPath("hdfs://cluster/data3"));
        // weights: 5, 1, 1 -> total = 7
        List<Integer> weights = Arrays.asList(5, 1, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        Map<FsPath, Integer> counts = new HashMap<>();
        int totalCalls = 70; // 10 complete cycles

        for (int i = 0; i < totalCalls; i++) {
            FsPath selected = selector.nextDataDir();
            counts.merge(selected, 1, Integer::sum);
        }

        // Distribution should match weights ratio: 5:1:1
        assertThat(counts.get(dirs.get(0))).isEqualTo(50); // 5/7 * 70 = 50
        assertThat(counts.get(dirs.get(1))).isEqualTo(10); // 1/7 * 70 = 10
        assertThat(counts.get(dirs.get(2))).isEqualTo(10); // 1/7 * 70 = 10
    }

    @Test
    void testInterleavedDistribution() {
        // Verify that selections are interleaved, not consecutive
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/A"),
                        new FsPath("hdfs://cluster/B"),
                        new FsPath("hdfs://cluster/C"));
        // weights: 5, 1, 1 -> total = 7
        List<Integer> weights = Arrays.asList(5, 1, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        List<FsPath> sequence = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            sequence.add(selector.nextDataDir());
        }

        // Expected interleaved sequence for weights 5,1,1:
        // The smooth WRR should produce: A, A, B, A, C, A, A (or similar interleaved pattern)
        // Instead of traditional WRR: A, A, A, A, A, B, C

        // Count consecutive same selections - should be less than weight
        int maxConsecutive = 0;
        int currentConsecutive = 1;
        for (int i = 1; i < sequence.size(); i++) {
            if (sequence.get(i).equals(sequence.get(i - 1))) {
                currentConsecutive++;
            } else {
                maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
                currentConsecutive = 1;
            }
        }
        maxConsecutive = Math.max(maxConsecutive, currentConsecutive);

        // With smooth WRR, max consecutive selections should be <= 2 for this weight distribution
        // (In traditional WRR, A would be selected 5 times consecutively)
        assertThat(maxConsecutive).isLessThanOrEqualTo(2);

        // Verify all directories are selected at least once within one cycle
        assertThat(sequence).contains(dirs.get(0), dirs.get(1), dirs.get(2));
    }

    @Test
    void testTwoDirsWithDifferentWeights() {
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/data1"), new FsPath("hdfs://cluster/data2"));
        // weights: 3, 1 -> total = 4
        List<Integer> weights = Arrays.asList(3, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        List<FsPath> sequence = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            sequence.add(selector.nextDataDir());
        }

        // Count selections
        long dir1Count = sequence.stream().filter(d -> d.equals(dirs.get(0))).count();
        long dir2Count = sequence.stream().filter(d -> d.equals(dirs.get(1))).count();

        // Should follow 3:1 ratio
        assertThat(dir1Count).isEqualTo(6); // 3/4 * 8 = 6
        assertThat(dir2Count).isEqualTo(2); // 1/4 * 8 = 2
    }

    @Test
    void testCycleRepeatsCorrectly() {
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/data1"), new FsPath("hdfs://cluster/data2"));
        List<Integer> weights = Arrays.asList(2, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        // Collect first cycle (3 selections)
        List<FsPath> firstCycle = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            firstCycle.add(selector.nextDataDir());
        }

        // Collect second cycle (3 selections)
        List<FsPath> secondCycle = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            secondCycle.add(selector.nextDataDir());
        }

        // Both cycles should have same sequence
        assertThat(secondCycle).isEqualTo(firstCycle);
    }

    @Test
    void testLargeWeights() {
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/data1"), new FsPath("hdfs://cluster/data2"));
        List<Integer> weights = Arrays.asList(100, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        Map<FsPath, Integer> counts = new HashMap<>();
        int totalCalls = 101; // One complete cycle

        for (int i = 0; i < totalCalls; i++) {
            FsPath selected = selector.nextDataDir();
            counts.merge(selected, 1, Integer::sum);
        }

        // Should follow 100:1 ratio
        assertThat(counts.get(dirs.get(0))).isEqualTo(100);
        assertThat(counts.get(dirs.get(1))).isEqualTo(1);
    }

    @Test
    void testZeroWeights() {
        // Test case 1: Some directories have zero weight - they should never be selected
        List<FsPath> dirs =
                Arrays.asList(
                        new FsPath("hdfs://cluster/data1"),
                        new FsPath("hdfs://cluster/data2"),
                        new FsPath("hdfs://cluster/data3"));
        // weight 0 for data2 means it should never be selected
        List<Integer> weights = Arrays.asList(2, 0, 1);

        WeightedRoundRobinRemoteDirSelector selector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, weights);

        Map<FsPath, Integer> counts = new HashMap<>();
        int totalCalls = 30; // 10 complete cycles (total weight = 3)

        for (int i = 0; i < totalCalls; i++) {
            FsPath selected = selector.nextDataDir();
            counts.merge(selected, 1, Integer::sum);
        }

        // data1 should be selected 20 times (2/3 * 30)
        assertThat(counts.get(dirs.get(0))).isEqualTo(20);
        // data2 should never be selected (weight = 0)
        assertThat(counts.get(dirs.get(1))).isNull();
        // data3 should be selected 10 times (1/3 * 30)
        assertThat(counts.get(dirs.get(2))).isEqualTo(10);

        // Test case 2: All weights are zero - should always return default dir
        List<Integer> allZeroWeights = Arrays.asList(0, 0, 0);
        WeightedRoundRobinRemoteDirSelector allZeroSelector =
                new WeightedRoundRobinRemoteDirSelector(DEFAULT_DIR, dirs, allZeroWeights);

        assertThat(allZeroSelector.nextDataDir()).isEqualTo(DEFAULT_DIR);
    }
}
