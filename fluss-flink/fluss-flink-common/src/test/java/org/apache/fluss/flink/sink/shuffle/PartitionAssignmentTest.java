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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionAssignment}. */
class PartitionAssignmentTest {

    @Test
    void testWeightedRandomAssignment() {
        List<Integer> assignedSubtasks = Arrays.asList(0, 1, 2);
        List<Long> subtaskWeights = Arrays.asList(1L, 3L, 2L);
        WeightedRandomAssignment assignment =
                new WeightedRandomAssignment(assignedSubtasks, subtaskWeights, new MockRandom());

        Map<Integer, Double> subtaskAssignedCounts = new HashMap<>();
        int totalRowNum = 200000;
        for (int i = 0; i < totalRowNum; i++) {
            subtaskAssignedCounts.merge(assignment.select(null), 1.0 / totalRowNum, Double::sum);
        }

        assertThat(subtaskAssignedCounts.get(0)).isCloseTo(1.0 / 6, Percentage.withPercentage(1));
        assertThat(subtaskAssignedCounts.get(1)).isCloseTo(0.5, Percentage.withPercentage(1));
        assertThat(subtaskAssignedCounts.get(2)).isCloseTo(2.0 / 6, Percentage.withPercentage(1));
    }

    @Test
    void testWeightedBucketIdAssignment() {
        List<Integer> assignedSubtasks = Arrays.asList(0, 1, 2);
        List<Long> subtaskWeights = Arrays.asList(1L, 3L, 2L);
        KeyEncoder keyEncoder = KeyEncoder.of(DATA1_ROW_TYPE, Collections.singletonList("a"), null);
        BucketingFunction bucketingFunction = BucketingFunction.of(null);
        int bucketNum = 6;

        WeightedBucketIdAssignment assignment =
                new WeightedBucketIdAssignment(
                        assignedSubtasks,
                        subtaskWeights,
                        bucketNum,
                        keyEncoder,
                        bucketingFunction,
                        new MockRandom());

        for (int i = 0; i < 100; i++) {
            GenericRow row = GenericRow.of(i, "value" + i);
            int bucketId = bucketingFunction.bucketing(keyEncoder.encodeKey(row), bucketNum);
            if (bucketId == 0) {
                assertThat(assignment.select(row)).isEqualTo(0);
            } else if (bucketId >= 1 && bucketId < 4) {
                assertThat(assignment.select(row)).isEqualTo(1);
            } else if (bucketId >= 4 && bucketId < 6) {
                assertThat(assignment.select(row)).isEqualTo(2);
            }
        }
    }
}
