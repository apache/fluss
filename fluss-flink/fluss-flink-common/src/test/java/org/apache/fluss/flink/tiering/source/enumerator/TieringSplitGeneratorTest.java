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

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.client.tiering.TieringSplitGenerator;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests conditional KV snapshot lookup when generating tiering splits. */
class TieringSplitGeneratorTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "table");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSkipKvSnapshotsForCommittedAndEmptyBuckets(boolean partitioned) throws Exception {
        TestingTieringAdmin admin = new TestingTieringAdmin(partitioned, 10, 10, 10, 10, 0, -1);
        Map<TableBucket, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(admin.bucket(0), 8L);
        committedOffsets.put(admin.bucket(1), 10L);
        committedOffsets.put(admin.bucket(2), 12L);
        committedOffsets.put(admin.bucket(3), 0L);
        admin.lakeSnapshot = new LakeSnapshot(1, committedOffsets);

        assertThat(admin.generateSplits())
                .containsExactly(
                        new TieringLogSplit(
                                TABLE_PATH, admin.bucket(0), admin.partitionName, 8, 10, 0),
                        new TieringLogSplit(
                                TABLE_PATH, admin.bucket(3), admin.partitionName, 0, 10, 0));
        assertThat(admin.kvSnapshotRequests).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmptyTableWithoutLakeSnapshotSkipsKvSnapshots(boolean partitioned) throws Exception {
        TestingTieringAdmin admin = new TestingTieringAdmin(partitioned, 0, -1);

        assertThat(admin.generateSplits()).isEmpty();
        assertThat(admin.kvSnapshotRequests).isZero();
    }

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void testInitializeUntieredBuckets(boolean partitioned, boolean hasLakeSnapshot)
            throws Exception {
        TestingTieringAdmin admin = new TestingTieringAdmin(partitioned, 10, 10, 10);
        if (hasLakeSnapshot) {
            admin.lakeSnapshot = new LakeSnapshot(1, Collections.singletonMap(admin.bucket(0), 8L));
        }
        admin.kvSnapshots.complete(
                new KvSnapshots(
                        1,
                        admin.partitionId,
                        Collections.singletonMap(1, 2L),
                        Collections.singletonMap(1, 6L)));

        assertThat(admin.generateSplits())
                .containsExactly(
                        new TieringLogSplit(
                                TABLE_PATH,
                                admin.bucket(0),
                                admin.partitionName,
                                hasLakeSnapshot ? 8 : EARLIEST_OFFSET,
                                10,
                                0),
                        new TieringSnapshotSplit(
                                TABLE_PATH, admin.bucket(1), admin.partitionName, 2, 6, 0),
                        new TieringLogSplit(
                                TABLE_PATH,
                                admin.bucket(2),
                                admin.partitionName,
                                EARLIEST_OFFSET,
                                10,
                                0));
        assertThat(admin.kvSnapshotRequests).isEqualTo(1);
    }

    @Test
    void testHistoricalPartitionSkipsKvSnapshots() throws Exception {
        TestingTieringAdmin admin = new TestingTieringAdmin(true, 10);
        admin.partitionName = HISTORICAL_PARTITION_VALUE;

        assertThat(admin.generateSplits())
                .containsExactly(
                        new TieringLogSplit(
                                TABLE_PATH,
                                admin.bucket(0),
                                admin.partitionName,
                                EARLIEST_OFFSET,
                                10,
                                0));
        assertThat(admin.kvSnapshotRequests).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testKvSnapshotFailureIsPropagated(boolean partitioned) {
        TestingTieringAdmin admin = new TestingTieringAdmin(partitioned, 10);
        RuntimeException failure = new RuntimeException("KV snapshot lookup failed");
        admin.kvSnapshots.completeExceptionally(failure);

        assertThatThrownBy(admin::generateSplits)
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage(
                        partitioned
                                ? "Failed to get table snapshot for table db.table and partition p1"
                                : "Failed to get table snapshot for table db.table")
                .hasRootCause(failure);
    }

    private static class TestingTieringAdmin extends TestAdminAdapter {
        @Nullable private final Long partitionId;
        @Nullable private String partitionName;
        private final long[] latestOffsets;
        @Nullable private LakeSnapshot lakeSnapshot;
        private final CompletableFuture<KvSnapshots> kvSnapshots = new CompletableFuture<>();
        private int kvSnapshotRequests;

        private TestingTieringAdmin(boolean partitioned, long... latestOffsets) {
            this.partitionId = partitioned ? 2L : null;
            this.partitionName = partitioned ? "p1" : null;
            this.latestOffsets = latestOffsets;
        }

        private TableBucket bucket(int bucketId) {
            return new TableBucket(1, partitionId, bucketId);
        }

        private List<TieringSplit> generateSplits() throws Exception {
            TableDescriptor.Builder descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.INT())
                                            .column("p", DataTypes.STRING())
                                            .primaryKey("id", "p")
                                            .build())
                            .distributedBy(latestOffsets.length);
            List<PartitionInfo> partitions = Collections.emptyList();
            if (partitionId != null) {
                descriptor.partitionedBy("p");
                partitions =
                        Collections.singletonList(
                                new PartitionInfo(
                                        partitionId,
                                        ResolvedPartitionSpec.fromPartitionValue(
                                                "p", partitionName),
                                        null,
                                        latestOffsets.length));
            }
            TableInfo tableInfo = TableInfo.of(TABLE_PATH, 1, 1, descriptor.build(), null, 0, 0);
            return new TieringSplitGenerator(this).generateTableSplits(tableInfo, partitions);
        }

        @Override
        public CompletableFuture<LakeSnapshot> getLatestLakeSnapshot(TablePath tablePath) {
            if (lakeSnapshot != null) {
                return CompletableFuture.completedFuture(lakeSnapshot);
            }
            CompletableFuture<LakeSnapshot> result = new CompletableFuture<>();
            result.completeExceptionally(
                    new LakeTableSnapshotNotExistException("No lake snapshot"));
            return result;
        }

        @Override
        public ListOffsetsResult listOffsets(
                TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec) {
            return listOffsets(tablePath, null, buckets, offsetSpec);
        }

        @Override
        public ListOffsetsResult listOffsets(
                TablePath tablePath,
                @Nullable String partitionName,
                Collection<Integer> buckets,
                OffsetSpec offsetSpec) {
            assertThat(partitionName).isEqualTo(this.partitionName);
            return new ListOffsetsResult(
                    buckets.stream()
                            .collect(
                                    Collectors.toMap(
                                            bucket -> bucket,
                                            bucket ->
                                                    CompletableFuture.completedFuture(
                                                            latestOffsets[bucket]))));
        }

        @Override
        public CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath) {
            return getLatestKvSnapshots(tablePath, null);
        }

        @Override
        public CompletableFuture<KvSnapshots> getLatestKvSnapshots(
                TablePath tablePath, @Nullable String partitionName) {
            assertThat(partitionName).isEqualTo(this.partitionName);
            kvSnapshotRequests++;
            assertThat(kvSnapshots).isDone();
            return kvSnapshots;
        }
    }
}
