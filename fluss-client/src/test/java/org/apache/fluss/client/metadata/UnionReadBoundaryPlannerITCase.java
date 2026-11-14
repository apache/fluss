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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.rpc.messages.PbUnionReadContext;
import org.apache.fluss.rpc.messages.PbUnionReadLogRange;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the public capture entry against the ordinary Java client test cluster. */
class UnionReadBoundaryPlannerITCase extends ClientToServerITCaseBase {

    @Test
    void testCapturePartitionScope() throws Exception {
        TablePath path = TablePath.of("union_read_boundary", "orders");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("region", DataTypes.STRING())
                                                .primaryKey("id", "region")
                                                .build())
                                .distributedBy(2, "id")
                                .partitionedBy("region")
                                .build(),
                        false);
        try {
            for (String region : Arrays.asList("US", "EU")) {
                admin.createPartition(
                                path,
                                new PartitionSpec(Collections.singletonMap("region", region)),
                                false)
                        .get();
            }
            for (PartitionInfo partition : admin.listPartitionInfos(path).get()) {
                for (int bucket = 0; bucket < partition.getBucketCount(); bucket++) {
                    FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                            new TableBucket(tableId, partition.getPartitionId(), bucket));
                }
            }
            PbUnionReadContext all = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            PbUnionReadContext scoped =
                    UnionReadBoundaryPlanner.captureReadContext(
                            admin,
                            path,
                            new PartitionSpec(Collections.singletonMap("region", "US")));
            assertThat(all.getLogRangesList()).hasSize(4);
            assertThat(scoped.hasLake()).isFalse();
            assertThat(scoped.getTableId()).isEqualTo(tableId);
            assertThat(scoped.getLogRangesList()).hasSize(2);
            assertThat(
                            PredicateMessageUtils.toPredicate(
                                    scoped.getPartitionScope(),
                                    admin.getTableInfo(path).get().getRowType()))
                    .isEqualTo(
                            PredicateBuilder.partition(
                                    Collections.singletonMap("region", "US"),
                                    admin.getTableInfo(path).get().getRowType()));
            for (PbUnionReadLogRange range : scoped.getLogRangesList()) {
                assertThat(range.getPartitionValuesList()).containsExactly("US");
                assertThat(range.getStartOffset()).isEqualTo(range.getEarliestOffset());
                assertThat(range.getStopOffset()).isZero();
            }
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @Test
    void testCaptureReadableSnapshotAndLogTail() throws Exception {
        TablePath path = TablePath.of("union_read_boundary_lake", "events");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .build(),
                        false);
        TableBucket bucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        try (Table table = conn.getTable(path);
                FlussTableLakeSnapshotCommitter committer =
                        new FlussTableLakeSnapshotCommitter(clientConf)) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(row(1)).get();
            writer.append(row(2)).get();
            PbUnionReadContext untiered = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            assertThat(untiered.getLake().hasSnapshotId()).isFalse();
            assertThat(untiered.getLogRangeAt(0).getBaseline()).isZero();
            assertThat(untiered.getLogRangeAt(0).getStopOffset()).isEqualTo(2);

            committer.open();
            Map<TableBucket, Long> tiered = Collections.singletonMap(bucket, 2L);
            String tieredPath = committer.prepareLakeSnapshot(tableId, path, tiered);
            String readablePath =
                    committer.prepareLakeSnapshot(
                            tableId, path, Collections.singletonMap(bucket, 1L));
            committer.commit(
                    tableId, 42L, tieredPath, readablePath, tiered, Collections.emptyMap(), null);
            PbUnionReadContext captured = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            assertThat(captured.getLake().getSnapshotId()).isEqualTo(42);
            assertThat(captured.getLogRangeAt(0).getBaseline()).isEqualTo(1);
            assertThat(captured.getLogRangeAt(0).getStartOffset()).isEqualTo(1);
            assertThat(captured.getLogRangeAt(0).getStopOffset()).isEqualTo(2);
            writer.append(row(3)).get();
            assertThat(
                            UnionReadBoundaryPlanner.captureReadContext(admin, path)
                                    .getLogRangeAt(0)
                                    .getStopOffset())
                    .isEqualTo(3);
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @Test
    void testPkRequiresBaselineWhenLogIsTruncated() throws Exception {
        TablePath path = TablePath.of("union_read_boundary", "pk_baseline");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .build(),
                        false);
        AtomicReference<LakeSnapshot> snapshot =
                new AtomicReference<>(new LakeSnapshot(42L, Collections.emptyMap()));
        try (FlussAdmin captureAdmin =
                new FlussAdmin(
                        FLUSS_CLUSTER_EXTENSION.getRpcClient(),
                        ((FlussConnection) conn).getMetadataUpdater()) {
                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath tablePath) {
                        return CompletableFuture.completedFuture(snapshot.get());
                    }

                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath tablePath, Collection<Integer> buckets, OffsetSpec spec) {
                        return new ListOffsetsResult(
                                Collections.singletonMap(
                                        0,
                                        CompletableFuture.completedFuture(
                                                spec instanceof OffsetSpec.EarliestSpec
                                                        ? 1L
                                                        : 2L)));
                    }
                }) {
            assertThatThrownBy(
                            () -> UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("KV snapshot fallback is required");
            snapshot.set(
                    new LakeSnapshot(
                            42L, Collections.singletonMap(new TableBucket(tableId, 0), 1L)));
            PbUnionReadLogRange range =
                    UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path)
                            .getLogRangeAt(0);
            assertThat(range.getBaseline()).isEqualTo(1);
            assertThat(range.getStartOffset()).isEqualTo(1);
            assertThat(range.getStopOffset()).isEqualTo(2);
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecreatedPartitionIsExcludedWithoutHidingLivePartitionErrors(boolean lookupFails)
            throws Exception {
        TablePath path = TablePath.of("union_read_boundary", "partition_recreation");
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("region", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .partitionedBy("region")
                        .build(),
                false);
        PartitionSpec replaced = new PartitionSpec(Collections.singletonMap("region", "US"));
        PartitionSpec retained = new PartitionSpec(Collections.singletonMap("region", "EU"));
        RuntimeException lookupFailure = new IllegalStateException("offset lookup failed");
        AtomicReference<String> failingPartition = new AtomicReference<>();
        try {
            admin.createPartition(path, replaced, false).get();
            admin.createPartition(path, retained, false).get();
            PartitionInfo oldPartition = admin.listPartitionInfos(path, replaced).get().get(0);
            PartitionInfo livePartition = admin.listPartitionInfos(path, retained).get().get(0);
            if (lookupFails) {
                failingPartition.set(oldPartition.getPartitionName());
            }
            try (FlussAdmin captureAdmin =
                    new FlussAdmin(
                            FLUSS_CLUSTER_EXTENSION.getRpcClient(),
                            ((FlussConnection) conn).getMetadataUpdater()) {
                        private boolean discovered;
                        private boolean recreated;

                        @Override
                        public CompletableFuture<List<PartitionInfo>> listPartitionInfos(
                                TablePath tablePath, PartitionSpec scope) {
                            if (discovered && !recreated) {
                                recreated = true;
                                // Change identity after offset capture, before the ID recheck.
                                return admin.dropPartition(path, replaced, false)
                                        .thenCompose(
                                                ignored ->
                                                        admin.createPartition(
                                                                path, replaced, false))
                                        .thenCompose(
                                                ignored ->
                                                        admin.listPartitionInfos(tablePath, scope));
                            }
                            discovered = true;
                            return admin.listPartitionInfos(tablePath, scope);
                        }

                        @Override
                        public ListOffsetsResult listOffsets(
                                TablePath tablePath,
                                String partitionName,
                                Collection<Integer> buckets,
                                OffsetSpec spec) {
                            CompletableFuture<Long> offset = new CompletableFuture<>();
                            if (partitionName.equals(failingPartition.get())) {
                                offset.completeExceptionally(lookupFailure);
                            } else {
                                offset.complete(0L);
                            }
                            return new ListOffsetsResult(
                                    buckets.stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            bucket -> bucket, bucket -> offset)));
                        }
                    }) {
                PbUnionReadContext context =
                        UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path);
                assertThat(admin.listPartitionInfos(path, replaced).get().get(0).getPartitionId())
                        .isNotEqualTo(oldPartition.getPartitionId());
                assertThat(context.getLogRangesList())
                        .extracting(PbUnionReadLogRange::getPartitionId)
                        .containsExactly(livePartition.getPartitionId());
                failingPartition.set(livePartition.getPartitionName());
                assertThatThrownBy(
                                () ->
                                        UnionReadBoundaryPlanner.captureReadContext(
                                                captureAdmin, path))
                        .hasRootCause(lookupFailure);
            }
        } finally {
            admin.dropTable(path, false).get();
        }
    }
}
