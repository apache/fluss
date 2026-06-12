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

package org.apache.fluss.lake.lance.source;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link LanceSplitPlanner}. */
class LanceSplitPlannerTest extends LanceSourceTestBase {

    @Test
    void testPlanSplitsWithoutLimit() throws Exception {
        createEmptyDataset();
        long snapshotId = appendRows(Arrays.asList(row(1, "a", 0, 101L, 1001L), row(2, "b", 0, 102L, 1002L)));
        snapshotId = appendRows(Arrays.asList(row(3, "c", 1, 103L, 1003L), row(4, "d", 1, 104L, 1004L), row(5, "e", 1, 105L, 1005L)));

        assertDatasetHasAtLeastFragments(2);

        LanceSplitPlanner planner = new LanceSplitPlanner(config, tablePath, snapshotId, null);
        List<LanceSplit> splits = planner.plan();

        Map<Integer, Integer> fragmentRows = currentFragmentRowCount();
        assertThat(splits).hasSize(fragmentRows.size());

        for (LanceSplit split : splits) {
            assertThat(split.snapshotId()).isEqualTo(snapshotId);
            assertThat(split.bucket()).isEqualTo(-1);
            assertThat(split.partition()).isEmpty();
            assertThat(split.scanLimit()).isEqualTo(-1L);
            assertThat(fragmentRows).containsKey(split.fragmentId());
            assertThat(split.fragmentRows()).isEqualTo(fragmentRows.get(split.fragmentId()));
        }
    }

    @Test
    void testPlanSplitsWithLimit() throws Exception {
        createEmptyDataset();
        long snapshotId = appendRows(Arrays.asList(row(1, "a", 0, 101L, 1001L), row(2, "b", 0, 102L, 1002L)));
        snapshotId = appendRows(Arrays.asList(row(3, "c", 1, 103L, 1003L), row(4, "d", 1, 104L, 1004L), row(5, "e", 1, 105L, 1005L)));
        final long plannedSnapshotId = snapshotId;

        LanceSplitPlanner planner = new LanceSplitPlanner(config, tablePath, plannedSnapshotId, 3);
        List<LanceSplit> splits = planner.plan();

        assertThat(splits).isNotEmpty();
        assertThat(splits.stream().allMatch(s -> s.snapshotId() == plannedSnapshotId)).isTrue();

        long totalAssignedLimit =
                splits.stream()
                        .mapToLong(s -> Math.max(0L, s.scanLimit()))
                        .sum();
        assertThat(totalAssignedLimit).isEqualTo(3L);

        List<LanceSplit> positiveLimitSplits =
                splits.stream().filter(s -> s.scanLimit() > 0).collect(Collectors.toList());
        assertThat(positiveLimitSplits).isNotEmpty();
        for (LanceSplit split : positiveLimitSplits) {
            assertThat(split.scanLimit()).isLessThanOrEqualTo(split.fragmentRows());
        }
    }
}
