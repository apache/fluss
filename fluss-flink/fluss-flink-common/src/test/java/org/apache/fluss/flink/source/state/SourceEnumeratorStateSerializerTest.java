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

package org.apache.fluss.flink.source.state;

import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;

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

/**
 * Unit tests for {@link org.apache.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer}.
 */
class SourceEnumeratorStateSerializerTest {

    @Test
    void testPendingSplitsCheckpointSerde() throws Exception {
        FlussSourceEnumeratorStateSerializer serializer =
                new FlussSourceEnumeratorStateSerializer(new TestingLakeSource());

        Set<TableBucket> assignedBuckets =
                new HashSet<>(Arrays.asList(new TableBucket(1, 0), new TableBucket(1, 4L, 1)));
        Map<Long, String> assignedPartitions = new HashMap<>();
        assignedPartitions.put(1L, "partition1");
        assignedPartitions.put(2L, "partition2");

        // Create remaining hybrid lake fluss splits with different types
        List<SourceSplitBase> remainingHybridLakeFlussSplits = new ArrayList<>();

        // Add a LogSplit
        TableBucket logSplitBucket = new TableBucket(1, 0);
        LogSplit logSplit = new LogSplit(logSplitBucket, null, 100L);
        remainingHybridLakeFlussSplits.add(logSplit);

        // Add a HybridSnapshotLogSplit
        TableBucket hybridSplitBucket = new TableBucket(1, 1);
        HybridSnapshotLogSplit hybridSplit =
                new HybridSnapshotLogSplit(hybridSplitBucket, null, 200L, 50L);
        remainingHybridLakeFlussSplits.add(hybridSplit);

        // Add a LakeSnapshotAndFlussLogSplit
        TableBucket lakeHybridSplitBucket = new TableBucket(1, 100L, 2);
        List<LakeSplit> lakeSplits =
                Collections.singletonList(
                        new TestingLakeSplit(2, Collections.singletonList("2024-01-01")));
        LakeSnapshotAndFlussLogSplit lakeHybridSplit =
                new LakeSnapshotAndFlussLogSplit(
                        lakeHybridSplitBucket, "2024-01-01", lakeSplits, 300L, Long.MIN_VALUE);
        remainingHybridLakeFlussSplits.add(lakeHybridSplit);

        SourceEnumeratorState sourceEnumeratorState =
                new SourceEnumeratorState(
                        assignedBuckets, assignedPartitions, remainingHybridLakeFlussSplits);

        // serialize state with remaining hybrid lake fluss splits
        byte[] serialized = serializer.serialize(sourceEnumeratorState);
        // deserialize state
        SourceEnumeratorState deserializedSourceEnumeratorState =
                serializer.deserialize(serializer.getVersion(), serialized);

        /* check deserialized is equal to the original */
        assertThat(deserializedSourceEnumeratorState).isEqualTo(sourceEnumeratorState);
    }
}
