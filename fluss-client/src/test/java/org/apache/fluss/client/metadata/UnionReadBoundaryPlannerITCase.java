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
import org.apache.fluss.exception.UnionReadDataUnavailableException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.rpc.messages.PbUnionReadContext;
import org.apache.fluss.rpc.messages.PbUnionReadLogRange;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.testutils.TestTableNames;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the public capture entry against the ordinary Java client test cluster. */
class UnionReadBoundaryPlannerITCase extends ClientToServerITCaseBase {

    private final TestTableNames tableNames = new TestTableNames();

    @Test
    void testPartitionScopeAndConcurrentCapture() throws Exception {
        TablePath path = tableNames.tablePath("union_read_boundary", "double_partitions");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("value", DataTypes.DOUBLE())
                                                .build())
                                .distributedBy(2, "id")
                                .partitionedBy("value")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .build(),
                        false);
        CountDownLatch ninthPartitionStarted = new CountDownLatch(1);
        Map<String, CompletableFuture<Long>> earliest = new ConcurrentHashMap<>();
        try {
            for (String value :
                    Arrays.asList(
                            "1_5", "2_5", "3_5", "4_5", "5_5", "6_5", "7_5", "8_5", "9_5",
                            "10_5")) {
                admin.createPartition(
                                path,
                                new PartitionSpec(Collections.singletonMap("value", value)),
                                false)
                        .get();
            }
            PartitionSpec spec = new PartitionSpec(Collections.singletonMap("value", "1_5"));
            PartitionInfo partition = admin.listPartitionInfos(path, spec).get().get(0);
            for (int bucket = 0; bucket < partition.getBucketCount(); bucket++) {
                FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                        new TableBucket(tableId, partition.getPartitionId(), bucket));
            }
            RowType schema = admin.getTableInfo(path).get().getRowType();
            PredicateBuilder builder = new PredicateBuilder(schema);
            PbUnionReadContext exact =
                    UnionReadBoundaryPlanner.captureReadContext(admin, path, spec);
            assertThat(exact.getLogRangesList()).hasSize(2);
            assertThat(PredicateMessageUtils.toPredicate(exact.getPartitionScope(), schema))
                    .isEqualTo(builder.equal(1, 1.5));
            try (FlussAdmin captureAdmin =
                    new FlussAdmin(
                            FLUSS_CLUSTER_EXTENSION.getRpcClient(),
                            ((FlussConnection) conn).getMetadataUpdater()) {
                        @Override
                        public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                                TablePath tablePath) {
                            return CompletableFuture.completedFuture(
                                    new LakeSnapshot(42L, Collections.emptyMap()));
                        }

                        @Override
                        public ListOffsetsResult listOffsets(
                                TablePath tablePath,
                                String partitionName,
                                Collection<Integer> buckets,
                                OffsetSpec offsetSpec) {
                            assertThat(partitionName).doesNotContain("10_5");
                            CompletableFuture<Long> begin =
                                    earliest.computeIfAbsent(
                                            partitionName, ignored -> new CompletableFuture<>());
                            if (offsetSpec instanceof OffsetSpec.LatestSpec) {
                                if (partitionName.equals("9_5")) {
                                    ninthPartitionStarted.countDown();
                                }
                                try {
                                    if (partitionName.equals("1_5")) {
                                        assertThat(
                                                        ninthPartitionStarted.await(
                                                                30, TimeUnit.SECONDS))
                                                .isTrue();
                                    }
                                    begin.complete(0L);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(e);
                                }
                            }
                            CompletableFuture<Long> offset =
                                    offsetSpec instanceof OffsetSpec.EarliestSpec
                                            ? begin
                                            : CompletableFuture.completedFuture(5L);
                            return new ListOffsetsResult(
                                    buckets.stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            bucket -> bucket, bucket -> offset)));
                        }
                    }) {
                PbUnionReadContext scoped =
                        UnionReadBoundaryPlanner.captureReadContext(
                                captureAdmin, path, builder.lessThan(1, 10.0));
                assertThat(scoped.getLogRangesList())
                        .extracting(range -> range.getPartitionValuesList().get(0))
                        .containsExactly(
                                "1_5", "1_5", "2_5", "2_5", "3_5", "3_5", "4_5", "4_5", "5_5",
                                "5_5", "6_5", "6_5", "7_5", "7_5", "8_5", "8_5", "9_5", "9_5");
                PbUnionReadContext task =
                        UnionReadBoundaryPlanner.forBucket(
                                scoped,
                                new TableBucket(
                                        tableId, scoped.getLogRangeAt(2).getPartitionId(), 1));
                assertThat(task.getCoverage()).isEqualTo(1);
                assertThat(task.getLogRangesList()).hasSize(1);
                PbUnionReadLogRange range = task.getLogRangeAt(0);
                assertThat(range.getPartitionValuesList()).containsExactly("2_5");
                assertThat(range.getPartitionId())
                        .isEqualTo(scoped.getLogRangeAt(2).getPartitionId());
                assertThat(range.getBucketId()).isEqualTo(1);
                assertThat(range.getStartOffset()).isZero();
                assertThat(range.getStopOffset()).isEqualTo(5);
                assertThat(PredicateMessageUtils.toPredicate(task.getPartitionScope(), schema))
                        .isEqualTo(builder.lessThan(1, 10.0));
                admin.alterTable(
                                path,
                                Collections.singletonList(
                                        TableChange.addColumn(
                                                "note",
                                                DataTypes.STRING(),
                                                null,
                                                TableChange.ColumnPosition.last())),
                                false)
                        .get();
                assertThatThrownBy(
                                () ->
                                        UnionReadBoundaryPlanner.forRemainingLakePartitions(
                                                scoped, admin.getTableSchema(path).get()))
                        .isInstanceOf(IllegalArgumentException.class);
                PbUnionReadContext remaining =
                        UnionReadBoundaryPlanner.forRemainingLakePartitions(
                                        scoped,
                                        admin.getTableSchema(path, scoped.getSchemaId()).get())
                                .get();
                Predicate remainingScope =
                        PredicateMessageUtils.toPredicate(remaining.getPartitionScope(), schema);
                assertThat(remainingScope.test(row(1, 0.5))).isTrue();
                assertThat(remainingScope.test(row(1, 1.5))).isFalse();
                assertThat(remainingScope.test(row(1, 9.5))).isFalse();
                assertThat(remainingScope.test(row(1, 10.5))).isFalse();
                assertThat(remaining.getLogRangesList()).isEmpty();
                assertThat(remaining.getLake().getSnapshotId()).isEqualTo(42);
                assertThat(scoped.getLogRangesList()).hasSize(18);
            }
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @Test
    void testInterruptedCaptureStopsOffsetRequests() throws Exception {
        TablePath path = tableNames.tablePath("union_read_boundary", "interrupted_capture");
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build(),
                false);
        try (FlussAdmin captureAdmin =
                new FlussAdmin(
                        FLUSS_CLUSTER_EXTENSION.getRpcClient(),
                        ((FlussConnection) conn).getMetadataUpdater()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath tablePath, Collection<Integer> buckets, OffsetSpec spec) {
                        assertThat(spec).isInstanceOf(OffsetSpec.EarliestSpec.class);
                        // A synchronous metadata refresh can return with interruption still
                        // pending.
                        Thread.currentThread().interrupt();
                        return new ListOffsetsResult(
                                Collections.singletonMap(0, CompletableFuture.completedFuture(0L)));
                    }
                }) {
            assertThatThrownBy(
                            () -> UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
            admin.dropTable(path, false).get();
        }
    }

    @Test
    void testCaptureReadableSnapshotAndLogTail() throws Exception {
        TablePath path = tableNames.tablePath("union_read_boundary_lake", "events");
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
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @Test
    void testPkBaselineSelection() throws Exception {
        TablePath path = tableNames.tablePath("union_read_boundary", "pk_baseline");
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
        TableInfo originalTable = admin.getTableInfo(path).get();
        AtomicReference<TableInfo> metadata = new AtomicReference<>(originalTable);
        AtomicBoolean recreateTable = new AtomicBoolean();
        AtomicLong earliestOffset = new AtomicLong(0);
        AtomicReference<LakeSnapshot> snapshot =
                new AtomicReference<>(new LakeSnapshot(42L, Collections.emptyMap()));
        AtomicReference<KvSnapshots> kv =
                new AtomicReference<>(
                        new KvSnapshots(
                                tableId, null, Collections.emptyMap(), Collections.emptyMap()));
        try (FlussAdmin captureAdmin =
                new FlussAdmin(
                        FLUSS_CLUSTER_EXTENSION.getRpcClient(),
                        ((FlussConnection) conn).getMetadataUpdater()) {
                    @Override
                    public CompletableFuture<TableInfo> getTableInfo(TablePath tablePath) {
                        return CompletableFuture.completedFuture(metadata.get());
                    }

                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath tablePath) {
                        return CompletableFuture.completedFuture(snapshot.get());
                    }

                    @Override
                    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(
                            TablePath tablePath) {
                        return CompletableFuture.completedFuture(kv.get());
                    }

                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath tablePath, Collection<Integer> buckets, OffsetSpec spec) {
                        if (recreateTable.get()) {
                            metadata.set(
                                    TableInfo.of(
                                            path,
                                            tableId + 1,
                                            originalTable.getSchemaId(),
                                            originalTable.toTableDescriptor(),
                                            null,
                                            0,
                                            0));
                        }
                        return new ListOffsetsResult(
                                Collections.singletonMap(
                                        0,
                                        CompletableFuture.completedFuture(
                                                spec instanceof OffsetSpec.EarliestSpec
                                                        ? earliestOffset.get()
                                                        : 2L)));
                    }
                }) {
            PbUnionReadLogRange completeLog =
                    UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path)
                            .getLogRangeAt(0);
            assertThat(completeLog.getBaseline()).isZero();
            assertThat(completeLog.getStartOffset()).isZero();
            earliestOffset.set(1);
            assertThatThrownBy(
                            () -> UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path))
                    .isInstanceOf(UnionReadDataUnavailableException.class)
                    .hasMessageContaining("has no lake or KV snapshot baseline");
            kv.set(
                    new KvSnapshots(
                            tableId,
                            null,
                            Collections.singletonMap(0, 7L),
                            Collections.singletonMap(0, 1L)));
            PbUnionReadLogRange kvRange =
                    UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path)
                            .getLogRangeAt(0);
            assertThat(kvRange.getBaseline()).isEqualTo(2);
            assertThat(kvRange.getKvSnapshotId()).isEqualTo(7);
            assertThat(kvRange.getStartOffset()).isEqualTo(1);
            assertThat(kvRange.getStopOffset()).isEqualTo(2);
            kv.set(
                    new KvSnapshots(
                            tableId + 1, null, Collections.emptyMap(), Collections.emptyMap()));
            assertThatThrownBy(
                            () -> UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path))
                    .isInstanceOf(UnionReadDataUnavailableException.class)
                    .hasMessageContaining("KV snapshot identity changed");
            snapshot.set(
                    new LakeSnapshot(
                            42L, Collections.singletonMap(new TableBucket(tableId, 0), 1L)));
            PbUnionReadLogRange range =
                    UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path)
                            .getLogRangeAt(0);
            assertThat(range.getBaseline()).isEqualTo(1);
            assertThat(range.hasKvSnapshotId()).isFalse();
            assertThat(range.getStartOffset()).isEqualTo(1);
            assertThat(range.getStopOffset()).isEqualTo(2);
            recreateTable.set(true);
            assertThatThrownBy(
                            () -> UnionReadBoundaryPlanner.captureReadContext(captureAdmin, path))
                    .isInstanceOf(UnionReadDataUnavailableException.class)
                    .hasMessageContaining("Table identity changed");
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecreatedPartitionIsExcludedWithoutHidingLivePartitionErrors(boolean lookupFails)
            throws Exception {
        TablePath path = tableNames.tablePath("union_read_boundary", "partition_recreation");
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
