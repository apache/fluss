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

package org.apache.fluss.client.initializer;

import org.apache.fluss.config.ConsumerOffsetReset;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CommittedOffsetsInitializer}. */
class CommittedOffsetsInitializerTest {

    @Test
    void testEarliestFallback() {
        OffsetsInitializer initializer =
                OffsetsInitializer.committed("group", ConsumerOffsetReset.EARLIEST);
        Map<Integer, Long> offsets =
                initializer.getBucketOffsets(null, Arrays.asList(0, 1, 2), new TestingRetriever());

        assertThat(offsets)
                .containsEntry(0, 5L)
                .containsEntry(1, EARLIEST_OFFSET)
                .containsEntry(2, EARLIEST_OFFSET);
    }

    @Test
    void testLatestFallback() {
        OffsetsInitializer initializer =
                OffsetsInitializer.committed("group", ConsumerOffsetReset.LATEST);
        Map<Integer, Long> offsets =
                initializer.getBucketOffsets(null, Arrays.asList(0, 1, 2), new TestingRetriever());

        assertThat(offsets).containsEntry(0, 5L).containsEntry(1, 10L).containsEntry(2, 10L);
    }

    @Test
    void testNoneFallbackThrowsWhenOffsetMissing() {
        OffsetsInitializer initializer =
                OffsetsInitializer.committed("group", ConsumerOffsetReset.NONE);

        assertThatThrownBy(
                        () ->
                                initializer.getBucketOffsets(
                                        null, Arrays.asList(0, 1, 2), new TestingRetriever()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reset policy is none");
    }

    private static final class TestingRetriever
            implements OffsetsInitializer.BucketOffsetsRetriever {

        @Override
        public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
            return fixedOffsets(buckets, 10L);
        }

        @Override
        public Map<Integer, Long> earliestOffsets(
                String partitionName, Collection<Integer> buckets) {
            return fixedOffsets(buckets, 0L);
        }

        @Override
        public Map<Integer, Long> offsetsFromTimestamp(
                String partitionName, Collection<Integer> buckets, long timestamp) {
            return fixedOffsets(buckets, 0L);
        }

        @Override
        public Map<Integer, Long> committedOffsets(
                String groupId, String partitionName, Collection<Integer> buckets) {
            Map<Integer, Long> offsets = new HashMap<>();
            offsets.put(0, 5L);
            offsets.put(1, -1L);
            return offsets;
        }

        private static Map<Integer, Long> fixedOffsets(Collection<Integer> buckets, long offset) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, offset);
            }
            return offsets;
        }
    }
}
