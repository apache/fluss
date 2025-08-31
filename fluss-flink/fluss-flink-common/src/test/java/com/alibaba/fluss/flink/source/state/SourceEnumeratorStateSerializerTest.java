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

package com.alibaba.fluss.flink.source.state;

import com.alibaba.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link com.alibaba.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer}.
 */
class SourceEnumeratorStateSerializerTest {

    @Test
    void testPendingSplitsCheckpointSerde() throws Exception {
        FlussSourceEnumeratorStateSerializer serializer =
                FlussSourceEnumeratorStateSerializer.INSTANCE;

        Set<TableBucket> assignedBuckets =
                new HashSet<>(Arrays.asList(new TableBucket(1, 0), new TableBucket(1, 4L, 1)));
        Map<Long, String> assignedPartitions = new HashMap<>();
        assignedPartitions.put(1L, "partition1");
        assignedPartitions.put(2L, "partition2");

        SourceEnumeratorState sourceEnumeratorState =
                new SourceEnumeratorState(assignedBuckets, assignedPartitions);

        // serialize assigned buckets
        byte[] serialized = serializer.serialize(sourceEnumeratorState);
        // deserialize assigned buckets
        SourceEnumeratorState deserializedSourceEnumeratorState =
                serializer.deserialize(serializer.getVersion(), serialized);

        /* check deserialized is equal to the original */
        assertThat(deserializedSourceEnumeratorState).isEqualTo(sourceEnumeratorState);
    }
}
