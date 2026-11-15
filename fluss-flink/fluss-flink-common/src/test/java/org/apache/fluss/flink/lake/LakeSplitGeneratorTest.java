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
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
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
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.PartitionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests lake and log split planning for partitioned tables. */
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
                () -> Collections.singleton(partitionInfo),
                null);
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
                        () -> Collections.singleton(partition),
                        null);

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

    @Test
    void testPartitionFilterKeepsMatchingLakeOnlyPartition() throws Exception {
        // d1, d2 and d3 have all expired from Fluss and exist only in lake
        TableInfo info = createPartitionedLogTable("day");
        Predicate partitionFilter =
                new PredicateBuilder(PartitionUtils.partitionRowType(info))
                        .greaterOrEqual(0, BinaryString.fromString("d2"));

        LakeSplitGenerator generator =
                createGenerator(
                        info,
                        Arrays.asList(
                                Collections.singletonList("d1"),
                                Collections.singletonList("d2"),
                                Collections.singletonList("d3")),
                        partitionFilter);

        assertThat(generator.generateHybridLakeFlussSplits())
                .allMatch(split -> split instanceof LakeSnapshotSplit)
                .extracting(SourceSplitBase::getPartitionName)
                .containsExactlyInAnyOrder("d2", "d3");
    }

    @Test
    void testNoPartitionFilterReadsAllLakeOnlyPartitions() throws Exception {
        TableInfo info = createPartitionedLogTable("day");

        LakeSplitGenerator generator =
                createGenerator(
                        info,
                        Arrays.asList(
                                Collections.singletonList("d1"),
                                Collections.singletonList("d2"),
                                Collections.singletonList("d3")),
                        null);

        assertThat(generator.generateHybridLakeFlussSplits())
                .extracting(SourceSplitBase::getPartitionName)
                .containsExactlyInAnyOrder("d1", "d2", "d3");
    }

    @Test
    void testPartitionFilterOnMultiplePartitionKeys() throws Exception {
        // the partition keys are declared as (region, day), the reverse of the column order, so
        // the filter on "day" must be evaluated against the second partition value
        TableInfo info = createPartitionedLogTable("region", "day");
        Predicate partitionFilter =
                new PredicateBuilder(PartitionUtils.partitionRowType(info))
                        .equal(1, BinaryString.fromString("d2"));

        LakeSplitGenerator generator =
                createGenerator(
                        info,
                        Arrays.asList(
                                Arrays.asList("eu", "d1"),
                                Arrays.asList("eu", "d2"),
                                Arrays.asList("us", "d2")),
                        partitionFilter);

        assertThat(generator.generateHybridLakeFlussSplits())
                .extracting(SourceSplitBase::getPartitionName)
                .containsExactlyInAnyOrder("eu$d2", "us$d2");
    }

    /**
     * Creates a log table (schema: id INT, day STRING, region STRING) with a single bucket,
     * partitioned by the given keys.
     */
    private static TableInfo createPartitionedLogTable(String... partitionKeys) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("day", DataTypes.STRING())
                                        .column("region", DataTypes.STRING())
                                        .build())
                        .partitionedBy(partitionKeys)
                        .distributedBy(1, "id")
                        .build();
        return TableInfo.of(
                TablePath.of("db", "partitioned_log_table"), 1L, 1, descriptor, null, 1L, 1L);
    }

    /**
     * Creates a generator whose lake planner returns one split in bucket 0 for each of the given
     * lake partitions, none of which exist in Fluss anymore.
     */
    private static LakeSplitGenerator createGenerator(
            TableInfo info,
            List<List<String>> lakePartitions,
            @Nullable Predicate partitionFilter) {
        List<LakeSplit> lakeSplits = new ArrayList<>();
        for (List<String> partition : lakePartitions) {
            lakeSplits.add(new TestingLakeSplit(0, partition));
        }
        TestAdminAdapter admin =
                new TestAdminAdapter() {
                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath ignored) {
                        return CompletableFuture.completedFuture(
                                new LakeSnapshot(1L, new HashMap<>()));
                    }
                };
        return new LakeSplitGenerator(
                info,
                admin,
                TestingLakeSource.fromSplits(lakeSplits),
                new ZeroOffsetsRetriever(),
                OffsetsInitializer.latest(),
                1,
                Collections::emptySet,
                partitionFilter);
    }
}
