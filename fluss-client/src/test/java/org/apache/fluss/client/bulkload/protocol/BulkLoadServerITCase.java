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

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;
import org.apache.fluss.rpc.messages.GetInProgressBulkLoadRequest;
import org.apache.fluss.rpc.messages.ListKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.server.kv.snapshot.PeriodicSnapshotManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromPhysicalTablePath;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Shared BulkLoad transactions through Coordinator RPCs and ordinary client operations. */
final class BulkLoadServerITCase {
    private static final Duration TIMEOUT = Duration.ofMinutes(3);
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final Schema PARTITIONED =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .column("pt", DataTypes.STRING())
                    .primaryKey("id", "pt")
                    .build();
    private final FlussClusterExtension cluster =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(configuration())
                    .build();
    private Connection connection;
    private FlussAdmin admin;

    @BeforeEach
    void start() throws Exception {
        cluster.start();
        connection = ConnectionFactory.createConnection(cluster.getClientConfig());
        admin = (FlussAdmin) connection.getAdmin();
    }

    @AfterEach
    void close() throws Exception {
        IOUtils.closeAll(admin, connection, cluster::close);
    }

    @ParameterizedTest
    @EnumSource(ChangelogImage.class)
    void testSharedPartitionsImportOneDecisionAndPreserveOnlineWrites(ChangelogImage image)
            throws Exception {
        TablePath path = create("shared_commit", true, image);
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1"),
                second = PhysicalTablePath.of(path, "p2");
        BeginBulkLoadResponse begun = begin(second, first);
        assertThat(begun.getTargetInfosCount()).isEqualTo(2);
        assertThat(owner(first).getHandle().getBulkLoadId())
                .isEqualTo(begun.getHandle().getBulkLoadId());
        assertThat(owner(second).getHandle().getBulkLoadId())
                .isEqualTo(begun.getHandle().getBulkLoadId());
        Map<PhysicalTablePath, List<Object[]>> rows = new HashMap<>();
        rows.put(
                first, Arrays.asList(new Object[] {1, "one", "p1"}, new Object[] {2, "two", "p1"}));
        rows.put(second, Collections.singletonList(new Object[] {3, "three", "p2"}));
        CommitBulkLoadRequest input = new TestingBulkLoadInputBuilder().build(begun, rows);
        PbBulkLoadStatus committed = commit(input);
        assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED.getCode());
        try (Table table = connection.getTable(path)) {
            verify(table, PARTITIONED, new Object[] {1, "one", "p1"});
            verify(table, PARTITIONED, new Object[] {3, "three", "p2"});
            write(table, row(1, "updated", "p1"));
            verify(table, PARTITIONED, new Object[] {1, "updated", "p1"});
            assertThat(commit(new CommitBulkLoadRequest().setHandle(begun.getHandle())).getState())
                    .isEqualTo(BulkLoadState.COMMITTED.getCode());
            verify(table, PARTITIONED, new Object[] {1, "updated", "p1"});
        }
        assertUnowned(first);
        assertUnowned(second);
    }

    @Test
    void testConflictingMissingAndDisjointPartitionSets() throws Exception {
        TablePath path = create("shared_owners", true);
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1"),
                second = PhysicalTablePath.of(path, "p2"),
                third = PhysicalTablePath.of(path, "p3");
        BeginBulkLoadResponse original = begin(second);
        assertThatThrownBy(() -> begin(first, second))
                .hasRootCauseInstanceOf(
                        org.apache.fluss.exception.InvalidBulkLoadRequestException.class);
        assertUnowned(first);
        assertThatThrownBy(() -> begin(first, PhysicalTablePath.of(path, "missing")))
                .isInstanceOf(Exception.class);
        assertUnowned(first);
        BeginBulkLoadResponse disjoint = begin(first, third);
        assertThat(disjoint.getHandle().getBulkLoadId())
                .isNotEqualTo(original.getHandle().getBulkLoadId());
        abort(disjoint.getHandle());
        assertUnowned(first);
        assertUnowned(third);
        assertThat(owner(second).getHandle().getBulkLoadId())
                .isEqualTo(original.getHandle().getBulkLoadId());
        abort(original.getHandle());
        assertUnowned(second);
    }

    @Test
    void testMetadataOnlyBeginAndAbortLeaveOrdinaryAccessAvailable() throws Exception {
        TablePath path = create("ordinary_access", false);
        BeginBulkLoadResponse begun = begin(PhysicalTablePath.of(path));
        assertThat(
                        await(
                                        admin.getAdminGateway()
                                                .listKvSnapshots(
                                                        new ListKvSnapshotsRequest()
                                                                .setTableId(
                                                                        begun.getHandle()
                                                                                .getTableId())))
                                .getActiveSnapshotsList())
                .isEmpty();
        try (Table table = connection.getTable(path)) {
            write(table, row(1, "during-begun"));
            verify(table, SCHEMA, new Object[] {1, "during-begun"});
            assertThat(abort(begun.getHandle()).getState())
                    .isEqualTo(BulkLoadState.ABORTED.getCode());
            verify(table, SCHEMA, new Object[] {1, "during-begun"});
        }
    }

    @Test
    void testCommitRecoversSharedOwnersAfterCoordinatorAndLeaderLoss() throws Exception {
        TablePath path = create("shared_recovery", true);
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1"),
                second = PhysicalTablePath.of(path, "p2");
        BeginBulkLoadResponse begun = begin(first, second);
        Map<PhysicalTablePath, List<Object[]>> rows = new HashMap<>();
        rows.put(first, Collections.singletonList(new Object[] {11, "one", "p1"}));
        rows.put(second, Collections.singletonList(new Object[] {22, "two", "p2"}));
        CommitBulkLoadRequest input = new TestingBulkLoadInputBuilder().build(begun, rows);
        long partitionId = begun.getTargetInfoAt(0).getPartitionId();
        TableBucket bucket = new TableBucket(begun.getHandle().getTableId(), partitionId, 0);
        int original = cluster.waitAndGetLeader(bucket);
        cluster.stopCoordinatorServer();
        cluster.startCoordinatorServer();
        IOUtils.closeAll(admin, connection);
        connection = ConnectionFactory.createConnection(cluster.getClientConfig());
        admin = (FlussAdmin) connection.getAdmin();
        cluster.stopTabletServer(original);
        commit(input);
        assertNoOrdinaryPublication(begun, true);
        retry(
                TIMEOUT,
                () -> {
                    try (Table table = connection.getTable(path)) {
                        Lookuper lookuper = table.newLookup().createLookuper();
                        assertThatRow(
                                        lookuper.lookup(row(11, "p1"))
                                                .get(10, TimeUnit.SECONDS)
                                                .getSingletonRow())
                                .withSchema(PARTITIONED.getRowType())
                                .isEqualTo(row(PARTITIONED.getRowType(), 11, "one", "p1"));
                        assertThatRow(
                                        lookuper.lookup(row(22, "p2"))
                                                .get(10, TimeUnit.SECONDS)
                                                .getSingletonRow())
                                .withSchema(PARTITIONED.getRowType())
                                .isEqualTo(row(PARTITIONED.getRowType(), 22, "two", "p2"));
                    } catch (Exception failure) {
                        throw new AssertionError("Recovered public table is not ready", failure);
                    }
                });
        assertUnowned(first);
        assertUnowned(second);
    }

    @Test
    void testInvalidCommonDigestCannotInstallAnySnapshotAndCanAbortWholeSet() throws Exception {
        TablePath path = create("shared_digest", true);
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1"),
                second = PhysicalTablePath.of(path, "p2");
        BeginBulkLoadResponse begun = begin(first, second);
        CommitBulkLoadRequest input =
                new TestingBulkLoadInputBuilder().build(begun, Collections.emptyMap());
        input.setInputSha256(String.join("", Collections.nCopies(64, "f")));
        assertThatThrownBy(
                        () ->
                                admin.getAdminGateway()
                                        .commitBulkLoad(input)
                                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(
                        org.apache.fluss.exception.InvalidBulkLoadRequestException.class);
        assertThat(owner(first).getState()).isEqualTo(BulkLoadState.BEGUN.getCode());
        assertThat(owner(second).getState()).isEqualTo(BulkLoadState.BEGUN.getCode());
        abort(begun.getHandle());
        assertUnowned(first);
        assertUnowned(second);
    }

    @Test
    void testPartitionsKeepIndependentFrozenBucketLayouts() throws Exception {
        TablePath path = create("partition_layouts", true);
        await(
                admin.alterTable(
                        path, Collections.singletonList(TableChange.modifyBucketCount(3)), false));
        await(
                admin.createPartition(
                        path, new PartitionSpec(Collections.singletonMap("pt", "p4")), false));
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1");
        PhysicalTablePath second = PhysicalTablePath.of(path, "p4");
        BeginBulkLoadResponse begun = begin(first, second);
        assertThat(begun.getTargetInfoAt(0).getSnapshotIds().length).isEqualTo(2);
        assertThat(begun.getTargetInfoAt(1).getSnapshotIds().length).isEqualTo(3);
        Map<PhysicalTablePath, List<Object[]>> rows = new HashMap<>();
        rows.put(first, Collections.singletonList(new Object[] {1, "first", "p1"}));
        rows.put(second, Collections.singletonList(new Object[] {2, "second", "p4"}));
        commit(new TestingBulkLoadInputBuilder().build(begun, rows));
        try (Table table = connection.getTable(path)) {
            verify(table, PARTITIONED, new Object[] {1, "first", "p1"});
            verify(table, PARTITIONED, new Object[] {2, "second", "p4"});
        }
    }

    @ParameterizedTest
    @EnumSource(ChangelogImage.class)
    void testExplicitLogTailSurvivesLeaderRecoveryAndSubsequentWrites(ChangelogImage image)
            throws Exception {
        TablePath path = create("snapshot_tail", false, image);
        BeginBulkLoadResponse begun = begin(PhysicalTablePath.of(path));
        Map<Integer, Long> offsets = new HashMap<>();
        offsets.put(0, 10_017L);
        offsets.put(1, 20_017L);
        assertNoOrdinaryPublication(begun, false);
        CommitBulkLoadRequest input =
                new TestingBulkLoadInputBuilder()
                        .build(
                                begun,
                                Arrays.asList(new Object[] {1, "one"}, new Object[] {2, "two"}),
                                offsets);
        assertNoOrdinaryPublication(begun, false);
        commit(input);
        assertNoOrdinaryPublication(begun, true);
        TableBucket bucket = new TableBucket(begun.getHandle().getTableId(), 0);
        int leader = cluster.waitAndGetLeader(bucket);
        try (Table table = connection.getTable(path)) {
            write(table, row(3, "online"));
            cluster.stopTabletServer(leader);
            verify(table, SCHEMA, new Object[] {1, "one"});
            verify(table, SCHEMA, new Object[] {2, "two"});
            verify(table, SCHEMA, new Object[] {3, "online"});
        }
    }

    @Test
    void testImportedFullSnapshotBatchScanCrossesPageBoundaries() throws Exception {
        TablePath path = create("snapshot_batch_scan", false);
        PhysicalTablePath target = PhysicalTablePath.of(path);
        BeginBulkLoadResponse begun = begin(target);
        List<Object[]> expected = new java.util.ArrayList<>();
        for (int id = 0; id < 4200; id++) {
            expected.add(new Object[] {id, "payload-" + id});
        }
        commit(
                new TestingBulkLoadInputBuilder()
                        .build(begun, Collections.singletonMap(target, expected)));
        try (Table table = connection.getTable(path)) {
            retry(
                    TIMEOUT,
                    () -> {
                        try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                            List<InternalRow> actual = BatchScanUtils.collectRows(scanner);
                            assertThat(actual).hasSize(expected.size());
                            java.util.Set<Integer> keys = new java.util.HashSet<>();
                            for (InternalRow row : actual) {
                                keys.add(row.getInt(0));
                                assertThat(row.getString(1).toString())
                                        .isEqualTo("payload-" + row.getInt(0));
                            }
                            assertThat(keys).hasSize(expected.size());
                        } catch (Exception failure) {
                            throw new AssertionError("Imported batch scan is not ready", failure);
                        }
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartiallyInstalledSharedDecisionRollsForward(boolean coordinatorTakeover)
            throws Exception {
        TablePath path = create("partial_takeover", true);
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1");
        PhysicalTablePath second = PhysicalTablePath.of(path, "p2");
        BeginBulkLoadResponse begun = begin(first, second);
        Map<PhysicalTablePath, List<Object[]>> rows = new HashMap<>();
        rows.put(first, Collections.singletonList(new Object[] {31, "first", "p1"}));
        rows.put(second, Collections.singletonList(new Object[] {32, "second", "p2"}));
        CommitBulkLoadRequest input = new TestingBulkLoadInputBuilder().build(begun, rows);
        org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo last = begun.getTargetInfoAt(1);
        TableBucket blocked =
                new TableBucket(begun.getHandle().getTableId(), last.getPartitionId(), 1);
        long blockedSnapshot = last.getSnapshotIds()[1];
        org.apache.fluss.fs.FsPath metadata =
                new org.apache.fluss.fs.FsPath(
                        org.apache.fluss.utils.FlussPaths.remoteKvSnapshotDir(
                                org.apache.fluss.utils.FlussPaths.remoteKvTabletDir(
                                        new org.apache.fluss.fs.FsPath(
                                                last.getTableInfo().getRemoteDataDir(),
                                                org.apache.fluss.utils.FlussPaths
                                                        .REMOTE_KV_DIR_NAME),
                                        second,
                                        blocked),
                                blockedSnapshot),
                        "_METADATA");
        ZooKeeperClient zk = cluster.getZooKeeperClient();
        String blockedPath = ZkData.BucketSnapshotIdZNode.path(blocked, blockedSnapshot);
        zk.getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        blockedPath,
                        ZkData.BucketSnapshotIdZNode.encode(
                                new org.apache.fluss.server.zk.data.BucketSnapshot(
                                        blockedSnapshot, 999L, metadata.toString())));
        assertThatThrownBy(() -> await(admin.getAdminGateway().commitBulkLoad(input)))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        BulkLoadTransaction decision =
                ZkData.BulkLoadTransactionsZNode.decode(
                        zk.getDataWithStat(
                                        ZkData.BulkLoadTableTransactionZNode.path(
                                                begun.getHandle().getTableId(),
                                                begun.getHandle().getBulkLoadId()))
                                .getData());
        assertThat(decision.getState()).isEqualTo(BulkLoadState.COMMITTING);
        TableBucket adopted =
                new TableBucket(
                        begun.getHandle().getTableId(),
                        begun.getTargetInfoAt(0).getPartitionId(),
                        0);
        assertThat(zk.listBucketSnapshotIds(adopted))
                .contains(begun.getTargetInfoAt(0).getSnapshotIds()[0]);
        assertThat(
                        await(
                                        admin.getAdminGateway()
                                                .listKvSnapshots(
                                                        new ListKvSnapshotsRequest()
                                                                .setTableId(adopted.getTableId())
                                                                .setPartitionId(
                                                                        adopted.getPartitionId())))
                                .getActiveSnapshotsList())
                .anySatisfy(
                        snapshot -> {
                            assertThat(snapshot.getBucketId()).isEqualTo(adopted.getBucket());
                            assertThat(snapshot.getSnapshotId())
                                    .isEqualTo(begun.getTargetInfoAt(0).getSnapshotIds()[0]);
                        });
        assertThat(owner(first).getState()).isEqualTo(BulkLoadState.COMMITTING.getCode());
        assertThat(owner(second).getState()).isEqualTo(BulkLoadState.COMMITTING.getCode());
        CompletableFuture<CommitBulkLoadResponse> resumed;
        CommitBulkLoadRequest retryRequest =
                new CommitBulkLoadRequest().setHandle(begun.getHandle());
        if (coordinatorTakeover) {
            cluster.stopCoordinatorServer();
            zk.getCuratorClient().delete().forPath(blockedPath);
            cluster.startCoordinatorServer();
            IOUtils.closeAll(admin, connection);
            connection = ConnectionFactory.createConnection(cluster.getClientConfig());
            admin = (FlussAdmin) connection.getAdmin();
            resumed = admin.getAdminGateway().commitBulkLoad(retryRequest);
        } else {
            String adoptedPath =
                    ZkData.BucketSnapshotIdZNode.path(
                            adopted, begun.getTargetInfoAt(0).getSnapshotIds()[0]);
            Path validatedMetadata =
                    Paths.get(
                            new FsPath(
                                            ZkData.BucketSnapshotIdZNode.decode(
                                                            zk.getDataWithStat(adoptedPath)
                                                                    .getData())
                                                    .getMetadataPath())
                                    .toUri());
            Path unavailable = validatedMetadata.resolveSibling("_METADATA.unavailable");
            Files.move(validatedMetadata, unavailable);
            try {
                zk.getCuratorClient().delete().forPath(blockedPath);
                resumed = admin.getAdminGateway().commitBulkLoad(retryRequest);
                waitUntil(
                        () -> zk.pathExists(blockedPath),
                        Duration.ofSeconds(20),
                        "remaining bucket is adopted without rereading validated metadata");
            } finally {
                Files.move(unavailable, validatedMetadata);
            }
        }
        assertThat(await(resumed).getStatus().getState())
                .isEqualTo(BulkLoadState.COMMITTED.getCode());
        retry(
                TIMEOUT,
                () -> {
                    try (Table table = connection.getTable(path)) {
                        Lookuper lookuper = table.newLookup().createLookuper();
                        assertThatRow(
                                        lookuper.lookup(row(31, "p1"))
                                                .get(10, TimeUnit.SECONDS)
                                                .getSingletonRow())
                                .withSchema(PARTITIONED.getRowType())
                                .isEqualTo(row(PARTITIONED.getRowType(), 31, "first", "p1"));
                        assertThatRow(
                                        lookuper.lookup(row(32, "p2"))
                                                .get(10, TimeUnit.SECONDS)
                                                .getSingletonRow())
                                .withSchema(PARTITIONED.getRowType())
                                .isEqualTo(row(PARTITIONED.getRowType(), 32, "second", "p2"));
                    } catch (Exception failure) {
                        throw new AssertionError("Recovered shared table is not ready", failure);
                    }
                });
        assertUnowned(first);
        assertUnowned(second);
    }

    @Test
    void testOrdinarySnapshotDeleteChangelogAndTieringAfterImport() throws Exception {
        TablePath path = create("ordinary_after_import", false);
        PhysicalTablePath target = PhysicalTablePath.of(path);
        BeginBulkLoadResponse begun = begin(target);
        commit(
                new TestingBulkLoadInputBuilder()
                        .build(
                                begun,
                                Collections.singletonMap(
                                        target,
                                        Collections.singletonList(new Object[] {1, "imported"}))));
        long tableId = begun.getHandle().getTableId();
        try (Table table = connection.getTable(path);
                org.apache.fluss.client.table.scanner.log.LogScanner scanner =
                        table.newScan().createLogScanner()) {
            for (int bucket = 0; bucket < 2; bucket++) {
                scanner.subscribe(
                        bucket,
                        cluster.waitAndGetLeaderReplica(new TableBucket(tableId, bucket))
                                .getLocalLogEndOffset());
            }
            write(table, row(1, "updated"));
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.delete(row(1, (String) null));
            writer.flush();
            for (int round = 0; round < 5; round++) {
                for (int id = 10; id < 110; id++) {
                    writer.upsert(
                            row(
                                    id,
                                    String.join(
                                            "",
                                            Collections.nCopies(
                                                    100, "payload-" + id + "-" + round))));
                }
                writer.flush();
            }
            boolean sawDelete = false;
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (!sawDelete && System.nanoTime() < deadline) {
                org.apache.fluss.client.table.scanner.log.ScanRecords records =
                        scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets()) {
                    for (org.apache.fluss.client.table.scanner.ScanRecord record :
                            records.records(bucket)) {
                        if (record.getChangeType() == org.apache.fluss.record.ChangeType.DELETE
                                && record.getRow().getInt(0) == 1) {
                            assertThat(record.getRow().getString(1).toString())
                                    .isEqualTo("updated");
                            sawDelete = true;
                        }
                    }
                }
            }
            assertThat(sawDelete).isTrue();
        }
        await(
                admin.alterTable(
                        path,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key(), "5")),
                        false));
        for (int bucket = 0; bucket < 2; bucket++) {
            TableBucket physical = new TableBucket(tableId, bucket);
            org.apache.fluss.server.kv.snapshot.CompletedSnapshot snapshot =
                    cluster.triggerAndWaitSnapshot(physical);
            assertThat(snapshot.getSnapshotID())
                    .isGreaterThan(begun.getTargetInfoAt(0).getSnapshotIds()[bucket]);
            waitUntil(
                    () -> {
                        try {
                            return cluster.getZooKeeperClient()
                                    .getRemoteLogManifestHandle(physical)
                                    .isPresent();
                        } catch (Exception failure) {
                            throw new RuntimeException(failure);
                        }
                    },
                    TIMEOUT,
                    "ordinary remote log publication");
        }
        int leader = cluster.waitAndGetLeader(new TableBucket(tableId, 0));
        cluster.stopTabletServer(leader);
        retry(
                TIMEOUT,
                () -> {
                    try (Table table = connection.getTable(path)) {
                        Lookuper lookuper = table.newLookup().createLookuper();
                        assertThat(
                                        lookuper.lookup(row(1))
                                                .get(10, TimeUnit.SECONDS)
                                                .getSingletonRow())
                                .isNull();
                        assertThat(
                                        lookuper.lookup(row(40))
                                                .get(10, TimeUnit.SECONDS)
                                                .getSingletonRow())
                                .isNotNull();
                    } catch (Exception failure) {
                        throw new AssertionError(
                                "Ordinary snapshot recovery is not ready", failure);
                    }
                });
    }

    private void assertNoOrdinaryPublication(BeginBulkLoadResponse begun, boolean adopted)
            throws Exception {
        ZooKeeperClient zk = cluster.getZooKeeperClient();
        for (org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo target :
                begun.getTargetInfosList()) {
            for (int id = 0; id < target.getSnapshotIds().length; id++) {
                TableBucket bucket =
                        new TableBucket(
                                begun.getHandle().getTableId(),
                                target.hasPartitionId() ? target.getPartitionId() : null,
                                id);
                Replica replica = cluster.waitAndGetLeaderReplica(bucket);
                PeriodicSnapshotManager snapshots = replica.getKvSnapshotManager();
                assertThat(snapshots).isNotNull();
                long counter = snapshots.currentSnapshotId();
                snapshots.triggerSnapshot();
                assertThat(snapshots.currentSnapshotId()).isEqualTo(counter);
                if (adopted) {
                    assertThat(zk.listBucketSnapshotIds(bucket))
                            .containsExactly(target.getSnapshotIds()[id]);
                } else {
                    assertThat(zk.listBucketSnapshotIds(bucket)).isEmpty();
                }
                assertThat(zk.getRemoteLogManifestHandle(bucket)).isNotPresent();
            }
        }
    }

    private TablePath create(String name, boolean partitioned) throws Exception {
        return create(name, partitioned, ChangelogImage.FULL);
    }

    private TablePath create(String name, boolean partitioned, ChangelogImage image)
            throws Exception {
        TablePath path = TablePath.of("bulkload_shared", name);
        await(admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true));
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(partitioned ? PARTITIONED : SCHEMA)
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, image)
                        .distributedBy(2, "id");
        if (partitioned) {
            builder.partitionedBy("pt");
        }
        await(admin.createTable(path, builder.build(), false));
        if (partitioned) {
            for (String part : Arrays.asList("p1", "p2", "p3")) {
                await(
                        admin.createPartition(
                                path,
                                new PartitionSpec(Collections.singletonMap("pt", part)),
                                false));
            }
        }
        return path;
    }

    private BeginBulkLoadResponse begin(PhysicalTablePath... targets) throws Exception {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest();
        for (PhysicalTablePath target : targets) {
            request.addAllTargets(Collections.singletonList(fromPhysicalTablePath(target)));
        }
        return await(admin.getAdminGateway().beginBulkLoad(request));
    }

    private PbBulkLoadStatus owner(PhysicalTablePath target) throws Exception {
        return await(
                        admin.getAdminGateway()
                                .getInProgressBulkLoad(
                                        new GetInProgressBulkLoadRequest()
                                                .setTarget(fromPhysicalTablePath(target))))
                .getStatus();
    }

    private void assertUnowned(PhysicalTablePath target) throws Exception {
        assertThat(
                        await(
                                        admin.getAdminGateway()
                                                .getInProgressBulkLoad(
                                                        new GetInProgressBulkLoadRequest()
                                                                .setTarget(
                                                                        fromPhysicalTablePath(
                                                                                target))))
                                .hasStatus())
                .isFalse();
    }

    private PbBulkLoadStatus abort(PbBulkLoadHandle handle) throws Exception {
        return await(
                        admin.getAdminGateway()
                                .abortBulkLoad(new AbortBulkLoadRequest().setHandle(handle)))
                .getStatus();
    }

    private PbBulkLoadStatus commit(CommitBulkLoadRequest input) throws Exception {
        return await(admin.getAdminGateway().commitBulkLoad(input)).getStatus();
    }

    private static void write(Table table, InternalRow row) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row);
        writer.flush();
    }

    private static void verify(Table table, Schema schema, Object[] expected) {
        retry(
                TIMEOUT,
                () -> {
                    try {
                        Lookuper lookuper = table.newLookup().createLookuper();
                        InternalRow key =
                                expected.length == 2
                                        ? row((Integer) expected[0])
                                        : row((Integer) expected[0], expected[2]);
                        InternalRow actual =
                                lookuper.lookup(key).get(10, TimeUnit.SECONDS).getSingletonRow();
                        assertThatRow(actual)
                                .withSchema(schema.getRowType())
                                .isEqualTo(row(schema.getRowType(), expected));
                    } catch (Exception failure) {
                        throw new AssertionError("Imported row is not ready", failure);
                    }
                });
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofMillis(100));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("2kb"));
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        return conf;
    }
}
