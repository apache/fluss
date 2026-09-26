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
import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests lake and log split planning for partitioned tables. */
class LakeSplitGeneratorTest {

    /** Table-level bucket count, kept different from the per-partition counts used below. */
    private static final int TABLE_LEVEL_BUCKET_COUNT = 3;

    private static final long TABLE_ID = 1L;
    private static final PartitionInfo ACTIVE = partition(10L, "20260916");
    private static final PartitionInfo HISTORICAL = partition(20L, HISTORICAL_PARTITION_VALUE);

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

    private static class ZeroOffsetsRetriever implements OffsetsInitializer.BucketOffsetsRetriever {

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPrunedLakePartitionRetainsCommittedLogOffsets(boolean primaryKey) throws Exception {
        TablePath path = TablePath.of("db", "pruned_partition");
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).column("day", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("id", "day");
        }
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("day")
                        .distributedBy(2, "id")
                        .build();
        TableInfo info = TableInfo.of(path, 1L, 1, descriptor, null, 1L, 1L);
        PartitionInfo partition =
                new PartitionInfo(
                        7L,
                        ResolvedPartitionSpec.fromPartitionName(info.getPartitionKeys(), "p"),
                        null,
                        2);
        TestAdminAdapter admin =
                new TestAdminAdapter() {
                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath ignored) {
                        // Bucket 0 is tiered through offset 100. Bucket 1 has not been tiered.
                        return CompletableFuture.completedFuture(
                                new LakeSnapshot(
                                        1L,
                                        Collections.singletonMap(
                                                new TableBucket(1L, 7L, 0), 100L)));
                    }
                };
        OffsetsInitializer.BucketOffsetsRetriever retriever =
                new ZeroOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        Map<Integer, Long> offsets = new HashMap<>();
                        offsets.put(0, 110L);
                        offsets.put(1, 20L);
                        return offsets;
                    }
                };
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        info,
                        admin,
                        // A pushed-down predicate has pruned every lake split of this partition.
                        TestingLakeSource.fromSplits(Collections.emptyList()),
                        retriever,
                        OffsetsInitializer.latest(),
                        2,
                        () -> Collections.singleton(partition));

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getTableBucket()).isEqualTo(new TableBucket(1L, 7L, 0));
        assertThat(splits.get(1).getTableBucket()).isEqualTo(new TableBucket(1L, 7L, 1));
        assertThat(splits)
                .extracting(
                        split ->
                                primaryKey
                                        ? ((LakeSnapshotAndFlussLogSplit) split).getStartingOffset()
                                        : split.asLogSplit().getStartingOffset())
                .containsExactly(100L, EARLIEST_OFFSET);
        assertThat(splits)
                .extracting(
                        split ->
                                primaryKey
                                        ? ((LakeSnapshotAndFlussLogSplit) split)
                                                .getStoppingOffset()
                                                .get()
                                        : split.asLogSplit().getStoppingOffset().get())
                .containsExactly(110L, 20L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHistoricalBaselineGroupedByBucket(boolean hasHistoricalOffset) throws Exception {
        Map<TableBucket, Long> offsets = new HashMap<>();
        for (int bucket = 0; bucket < 2; bucket++) {
            offsets.put(new TableBucket(TABLE_ID, ACTIVE.getPartitionId(), bucket), 100L);
            if (hasHistoricalOffset) {
                offsets.put(new TableBucket(TABLE_ID, HISTORICAL.getPartitionId(), bucket), 50L);
            }
        }
        List<SourceSplitBase> splits =
                generateSplits(
                        Arrays.asList(ACTIVE, partition(1L, "20240101"), partition(2L, "20240102")),
                        offsets);

        assertThat(splits).hasSize(4);
        for (SourceSplitBase split : splits) {
            assertThat(split).isInstanceOf(LakeSnapshotAndFlussLogSplit.class);
            LakeSnapshotAndFlussLogSplit hybrid = (LakeSnapshotAndFlussLogSplit) split;
            assertThat(hybrid.isLakeSplitFinished()).isFalse();
            assertThat(hybrid.getLakeSplits())
                    .allSatisfy(
                            lakeSplit ->
                                    assertThat(lakeSplit.bucket())
                                            .isEqualTo(split.getTableBucket().getBucket()));
            if (HISTORICAL_PARTITION_VALUE.equals(split.getPartitionName())) {
                assertThat(split.getTableBucket().getPartitionId())
                        .isEqualTo(HISTORICAL.getPartitionId());
                assertThat(hybrid.getStartingOffset())
                        .isEqualTo(hasHistoricalOffset ? 50L : EARLIEST_OFFSET);
                assertThat(hybrid.getLakeSplits())
                        .extracting(LakeSplit::partition)
                        .containsExactlyInAnyOrder(
                                Collections.singletonList("20240101"),
                                Collections.singletonList("20240102"));
            } else {
                assertThat(hybrid.getStartingOffset()).isEqualTo(100L);
                assertThat(hybrid.getLakeSplits())
                        .extracting(LakeSplit::partition)
                        .containsExactly(Collections.singletonList(ACTIVE.getPartitionName()));
            }
        }
    }

    @Test
    void testHistoricalOffsetWithoutMatchingLakeSplits() throws Exception {
        TableBucket historicalBucket = new TableBucket(TABLE_ID, HISTORICAL.getPartitionId(), 0);
        // Partition predicates can prune every lake split without changing the shared log offset.
        List<SourceSplitBase> splits =
                generateSplits(
                        Collections.emptyList(), Collections.singletonMap(historicalBucket, 50L));
        assertThat(splits)
                .filteredOn(split -> HISTORICAL_PARTITION_VALUE.equals(split.getPartitionName()))
                .hasSize(2)
                .allSatisfy(
                        split -> {
                            assertThat(split).isInstanceOf(LakeSnapshotAndFlussLogSplit.class);
                            LakeSnapshotAndFlussLogSplit hybrid =
                                    (LakeSnapshotAndFlussLogSplit) split;
                            assertThat(hybrid.getLakeSplits()).isNull();
                            assertThat(hybrid.isLakeSplitFinished()).isTrue();
                            assertThat(hybrid.getStartingOffset())
                                    .isEqualTo(
                                            split.getTableBucket().equals(historicalBucket)
                                                    ? 50L
                                                    : EARLIEST_OFFSET);
                        });
    }

    private List<SourceSplitBase> generateSplits(
            List<PartitionInfo> lakePartitions, Map<TableBucket, Long> offsets) throws Exception {
        TablePath tablePath = TablePath.of("db", "historical");
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        TABLE_ID,
                        0,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("dt", DataTypes.STRING())
                                                .primaryKey("id", "dt")
                                                .build())
                                .distributedBy(2, "id")
                                .partitionedBy("dt")
                                .build(),
                        null,
                        0,
                        0);
        Admin admin = mock(Admin.class);
        when(admin.getReadableLakeSnapshot(tablePath))
                .thenReturn(CompletableFuture.completedFuture(new LakeSnapshot(1L, offsets)));
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        tableInfo,
                        admin,
                        new TestingLakeSource(2, lakePartitions),
                        mock(OffsetsInitializer.BucketOffsetsRetriever.class),
                        new NoStoppingOffsetsInitializer(),
                        2,
                        () -> new HashSet<>(Arrays.asList(ACTIVE, HISTORICAL)));
        return generator.generateHybridLakeFlussSplits();
    }

    private static PartitionInfo partition(long id, String value) {
        return new PartitionInfo(
                id, ResolvedPartitionSpec.fromPartitionValue("dt", value), null, 2);
    }
}
