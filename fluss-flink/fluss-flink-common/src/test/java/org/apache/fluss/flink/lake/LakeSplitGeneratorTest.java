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

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for the fail-loud guard in {@link LakeSplitGenerator}: for a primary-key table, if the
 * lake snapshot of a partition contains a bucket id outside the partition's enumerated bucket range
 * (which can only happen if the per-partition bucket count is inconsistent with the tiered data),
 * union-read split generation must refuse rather than silently drop the out-of-range lake data.
 */
class LakeSplitGeneratorTest {

    /** Table-level bucket count, kept different from the per-partition counts used below. */
    private static final int TABLE_LEVEL_BUCKET_COUNT = 3;

    /**
     * Builds a {@link LakeSplitGenerator} for a partitioned primary-key table (schema: a INT, b
     * STRING, c STRING; PK a+c) whose single partition "p" has {@code partitionBucketCount}
     * enumerated buckets and a single lake split landing in {@code lakeSplitBucket}.
     */
    private static LakeSplitGenerator createGenerator(int partitionBucketCount, int lakeSplitBucket)
            throws Exception {
        TablePath tablePath = TablePath.of("db", "pk_table");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .primaryKey("a", "c")
                                        .build())
                        .distributedBy(TABLE_LEVEL_BUCKET_COUNT, "a")
                        .partitionedBy("c")
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, null, 1L, 1L);

        LakeSplit lakeSplit = new TestingLakeSplit(lakeSplitBucket, Collections.singletonList("p"));
        LakeSource<LakeSplit> lakeSource =
                TestingLakeSource.fromSplits(Collections.singletonList(lakeSplit));
        TestAdminAdapter admin =
                new TestAdminAdapter() {
                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath ignored) {
                        return CompletableFuture.completedFuture(
                                new LakeSnapshot(1L, new HashMap<>()));
                    }
                };
        OffsetsInitializer.BucketOffsetsRetriever retriever = new ZeroOffsetsRetriever();

        // the partition "p" carries its own bucket count (so an out-of-range lake bucket can be
        // detected against the enumerated range [0, partitionBucketCount))
        PartitionInfo partitionInfo =
                new PartitionInfo(
                        7L,
                        ResolvedPartitionSpec.fromPartitionName(tableInfo.getPartitionKeys(), "p"),
                        null,
                        partitionBucketCount);

        return new LakeSplitGenerator(
                tableInfo,
                admin,
                lakeSource,
                retriever,
                OffsetsInitializer.latest(),
                partitionBucketCount,
                () -> Collections.singleton(partitionInfo));
    }

    private static final class ZeroOffsetsRetriever
            implements OffsetsInitializer.BucketOffsetsRetriever {

        @Override
        public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
            return zeroOffsets(buckets);
        }

        @Override
        public Map<Integer, Long> earliestOffsets(
                String partitionName, Collection<Integer> buckets) {
            return zeroOffsets(buckets);
        }

        @Override
        public Map<Integer, Long> offsetsFromTimestamp(
                String partitionName, Collection<Integer> buckets, long timestamp) {
            return zeroOffsets(buckets);
        }

        private static Map<Integer, Long> zeroOffsets(Collection<Integer> buckets) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, 0L);
            }
            return offsets;
        }
    }

    @Test
    void testPrimaryKeyOutOfRangeLakeBucketFailsLoud() throws Exception {
        // lake split lands in bucket 5, which is outside [0, 2)
        LakeSplitGenerator generator = createGenerator(2, 5);
        assertThatThrownBy(generator::generateHybridLakeFlussSplits)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside the enumerated range")
                .hasMessageContaining("refusing to generate union-read splits");
    }

    @Test
    void testPrimaryKeyInRangeLakeBucketSucceeds() throws Exception {
        // lake split lands in bucket 1, which is within [0, 4)
        int partitionBucketCount = 4;
        LakeSplitGenerator generator = createGenerator(partitionBucketCount, 1);

        // no out-of-range bucket: generation succeeds and produces one hybrid lake+log split per
        // bucket of the partition's enumerated range [0, partitionBucketCount)
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();
        assertThat(partitionBucketCount).isNotEqualTo(TABLE_LEVEL_BUCKET_COUNT);
        assertThat(splits).isNotNull().hasSize(partitionBucketCount);
        for (int bucket = 0; bucket < partitionBucketCount; bucket++) {
            SourceSplitBase split = splits.get(bucket);
            assertThat(split).isInstanceOf(LakeSnapshotAndFlussLogSplit.class);
            assertThat(split.getPartitionName()).isEqualTo("p");
            assertThat(split.getTableBucket().getPartitionId()).isEqualTo(7L);
            assertThat(split.getTableBucket().getBucket()).isEqualTo(bucket);
        }
    }
}
