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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link LakeSplitGenerator}. */
class LakeSplitGeneratorTest {

    private static final TablePath TABLE_PATH = TablePath.of("fluss", "test_table");
    private static final long TABLE_ID = 1L;

    @Test
    void testLogTableHybridSplitsStartFromMaxSnapshotAndTimestampOffset() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.getReadableLakeSnapshot(TABLE_PATH))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new LakeSnapshot(
                                        1L,
                                        Collections.singletonMap(
                                                new TableBucket(TABLE_ID, 0), 50L))));

        LakeSource<LakeSplit> lakeSource =
                new TestingLakeSource(
                        2,
                        Collections.singletonList(
                                new PartitionInfo(
                                        -1L,
                                        new ResolvedPartitionSpec(
                                                Collections.emptyList(), Collections.emptyList()),
                                        DEFAULT_REMOTE_DATA_DIR)));
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        createLogTableInfo(),
                        admin,
                        lakeSource,
                        new TestingBucketOffsetsRetriever(),
                        OffsetsInitializer.timestamp(1_000L),
                        OffsetsInitializer.latest(),
                        2,
                        Collections::emptySet);

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).filteredOn(split -> split instanceof LakeSnapshotSplit).hasSize(2);
        assertThat(
                        splits.stream()
                                .filter(split -> split instanceof LogSplit)
                                .map(split -> (LogSplit) split)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        new LogSplit(new TableBucket(TABLE_ID, 0), null, 70L, 100L),
                        new LogSplit(new TableBucket(TABLE_ID, 1), null, 30L, 100L));
    }

    private static TableInfo createLogTableInfo() {
        return new TableInfo(
                TABLE_PATH,
                TABLE_ID,
                1,
                Schema.newBuilder().column("id", DataTypes.INT()).build(),
                Collections.emptyList(),
                Collections.emptyList(),
                2,
                new Configuration(),
                new Configuration(),
                null,
                null,
                0L,
                0L);
    }

    private static class TestingBucketOffsetsRetriever
            implements OffsetsInitializer.BucketOffsetsRetriever {

        @Override
        public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
            return offsets(buckets, 100L);
        }

        @Override
        public Map<Integer, Long> earliestOffsets(
                String partitionName, Collection<Integer> buckets) {
            return offsets(buckets, 0L);
        }

        @Override
        public Map<Integer, Long> offsetsFromTimestamp(
                String partitionName, Collection<Integer> buckets, long timestamp) {
            Map<Integer, Long> offsets = new HashMap<>();
            offsets.put(0, 70L);
            offsets.put(1, 30L);
            return offsets;
        }

        private Map<Integer, Long> offsets(Collection<Integer> buckets, long offset) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, offset);
            }
            return offsets;
        }
    }
}
