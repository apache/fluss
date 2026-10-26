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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.GetInProgressBulkLoadRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.tablet.bulkload.BulkLoadTargetMetadata;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.client.bulkload.protocol.TestingBulkLoadInputBuilder.bucketOf;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromPhysicalTablePath;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toPhysicalTablePath;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end server transaction tests using internal RPC and ordinary client reads and writes. */
final class BulkLoadServerITCase {

    private static final Duration E2E_TIMEOUT = Duration.ofMinutes(5);
    private static final int BUCKET_COUNT = 5;
    private static final int INDEXED_ROWS_PER_BUCKET = 2050;
    private static final int STRESS_ROWS_PER_BUCKET = 1101;
    private static final String WAL_TARGET_PARTITION = "target";
    private static final String WAL_LIVE_PARTITION = "live";

    private static final Schema FULL_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema PARTITIONED_WAL_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .column("pt", DataTypes.STRING())
                    .primaryKey("id", "pt")
                    .build();

    private final FlussClusterExtension cluster =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(featureClusterConfiguration())
                    .build();

    private Connection connection;
    private FlussAdmin admin;

    @BeforeEach
    void startCluster() throws Exception {
        cluster.start();
        connection = ConnectionFactory.createConnection(cluster.getClientConfig());
        admin = (FlussAdmin) connection.getAdmin();
    }

    @AfterEach
    void closeCluster() throws Exception {
        IOUtils.closeAll(admin, connection, cluster::close);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitRecoversAfterCoordinatorAndOriginalLeaderLoss(boolean resizeTable)
            throws Exception {
        TablePath tablePath = TablePath.of("bulkload_e2e", "partitioned_wal_import");
        await(admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false));
        await(
                admin.createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(PARTITIONED_WAL_SCHEMA)
                                .distributedBy(BUCKET_COUNT, "id")
                                .partitionedBy("pt")
                                .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, ChangelogImage.WAL)
                                .build(),
                        false));
        await(
                admin.createPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("pt", WAL_TARGET_PARTITION)),
                        false));
        await(
                admin.createPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("pt", WAL_LIVE_PARTITION)),
                        false));
        TableInfo tableInfo = await(admin.getTableInfo(tablePath));
        if (resizeTable) {
            await(
                    admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.modifyBucketCount(BUCKET_COUNT + 1)),
                            false));
        }
        Map<String, Long> partitionIds =
                await(admin.listPartitionInfos(tablePath)).stream()
                        .collect(
                                Collectors.toMap(
                                        PartitionInfo::getPartitionName,
                                        PartitionInfo::getPartitionId));
        assertThat(partitionIds).containsKeys(WAL_TARGET_PARTITION, WAL_LIVE_PARTITION);

        try (Table table = connection.getTable(tablePath)) {
            runPartitionedWalRecoveryScenario(table, tableInfo, partitionIds);
        }
    }

    @Test
    void testImportFinalStandardFilesAndContinueOnlineWrites() throws Exception {
        Configuration clientConfiguration = cluster.getClientConfig();
        Configuration accessProbeConfiguration = new Configuration(clientConfiguration);
        accessProbeConfiguration.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofSeconds(1));
        accessProbeConfiguration.setInt(ConfigOptions.CLIENT_LOOKUP_MAX_RETRIES, 0);
        accessProbeConfiguration.setInt(ConfigOptions.CLIENT_WRITER_RETRIES, 0);
        accessProbeConfiguration.setBoolean(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
        try (Connection accessProbeConnection =
                ConnectionFactory.createConnection(accessProbeConfiguration)) {
            TablePath tablePath = TablePath.of("bulkload_e2e", "full_import");
            createDatabaseAndTable(admin, tablePath, BUCKET_COUNT);

            try (Table table = connection.getTable(tablePath);
                    Table accessProbeTable = accessProbeConnection.getTable(tablePath)) {
                runFullImportScenario(table, accessProbeTable);
            }
        }
    }

    @Test
    void testSnapshotAtProvidedLogEndOffsetsPersistsAcrossRecovery() throws Exception {
        Configuration clientConfiguration = cluster.getClientConfig();
        TablePath tablePath = TablePath.of("bulkload_e2e", "snapshot_only_import");
        PhysicalTablePath target = PhysicalTablePath.of(tablePath);
        List<Object[]> expectedRows = new ArrayList<>();
        List<Object[]> firstTailRows = new ArrayList<>();
        Map<Integer, Long> expectedOffsets = new LinkedHashMap<>();
        BucketRows rowGenerator;
        TableInfo tableInfo;

        try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
                FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
            tableInfo = createDatabaseAndTable(admin, tablePath, BUCKET_COUNT);
            rowGenerator = new BucketRows(tableInfo, 700_000);
            cluster.waitUntilTableReady(tableInfo.getTableId());

            for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                expectedOffsets.put(bucket, 10_000L + bucket * 1_000L + 17L);
                for (int index = 0; index < 3; index++) {
                    Object[] values =
                            rowGenerator.row(bucket, "snapshot-only-" + bucket + '-' + index);
                    expectedRows.add(values);
                }
            }
            PbBulkLoadTargetInfo context = await(begin(admin, target));
            CommitBulkLoadRequest input =
                    new TestingBulkLoadInputBuilder().build(context, expectedRows, expectedOffsets);
            PbBulkLoadStatus committed = commitUntilReady(admin, input);
            assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED.getCode());
            assertThat(latestOffsets(admin, target, expectedOffsets.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(expectedOffsets);
            try (Table table = connection.getTable(tablePath)) {
                verifyPublicRows(table, tableInfo.getRowType(), expectedRows);
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                    Object[] tail = rowGenerator.row(bucket, "first-tail-" + bucket);
                    firstTailRows.add(tail);
                    await(writer.upsert(row(tableInfo.getRowType(), tail)));
                }
                writer.flush();
                expectedRows.addAll(firstTailRows);
                verifyPublicRows(table, tableInfo.getRowType(), expectedRows);
                Map<Integer, Long> afterWriteOffsets = offsetsAfter(expectedOffsets, 1L);
                assertThat(latestOffsets(admin, target, expectedOffsets.keySet()))
                        .containsExactlyInAnyOrderEntriesOf(afterWriteOffsets);
                try (LogScanner scanner = table.newScan().createLogScanner()) {
                    for (Map.Entry<Integer, Long> entry : expectedOffsets.entrySet()) {
                        scanner.subscribe(entry.getKey(), entry.getValue());
                    }
                    Map<Integer, ScanRecord> tails = new LinkedHashMap<>();
                    long deadline = System.nanoTime() + E2E_TIMEOUT.toNanos();
                    while (tails.size() < BUCKET_COUNT && System.nanoTime() < deadline) {
                        ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                        for (TableBucket bucket : records.buckets()) {
                            List<ScanRecord> recordsForBucket = records.records(bucket);
                            if (!recordsForBucket.isEmpty()) {
                                tails.put(bucket.getBucket(), recordsForBucket.get(0));
                            }
                        }
                    }
                    assertThat(tails).hasSize(BUCKET_COUNT);
                    for (Map.Entry<Integer, ScanRecord> entry : tails.entrySet()) {
                        assertThat(entry.getValue().logOffset())
                                .isEqualTo(expectedOffsets.get(entry.getKey()));
                    }
                }
            }

            assertNoBulkLoadRemoteLogArtifacts(cluster, tableInfo, target);

            int restartedTabletServer =
                    cluster.waitAndGetLeader(new TableBucket(tableInfo.getTableId(), 0));
            cluster.stopCoordinatorServer();
            cluster.startCoordinatorServer();
            cluster.stopTabletServer(restartedTabletServer);
            cluster.startTabletServer(restartedTabletServer);
        }

        try (Connection recoveredConnection =
                        ConnectionFactory.createConnection(cluster.getClientConfig());
                Admin recoveredAdmin = recoveredConnection.getAdmin();
                Table recoveredTable = recoveredConnection.getTable(tablePath)) {
            Map<Integer, Long> recoveredOffsets = offsetsAfter(expectedOffsets, 1L);
            verifyPublicRows(recoveredTable, tableInfo.getRowType(), expectedRows);
            assertThat(latestOffsets(recoveredAdmin, target, expectedOffsets.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(recoveredOffsets);

            UpsertWriter writer = recoveredTable.newUpsert().createWriter();
            for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                Object[] tail = rowGenerator.row(bucket, "recovered-tail-" + bucket);
                expectedRows.add(tail);
                await(writer.upsert(row(tableInfo.getRowType(), tail)));
            }
            writer.flush();
            verifyPublicRows(recoveredTable, tableInfo.getRowType(), expectedRows);
            Map<Integer, Long> finalOffsets = offsetsAfter(expectedOffsets, 2L);
            assertThat(latestOffsets(recoveredAdmin, target, expectedOffsets.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(finalOffsets);
        }
    }

    @Test
    void testOrdinarySnapshotCommitAfterActiveTableRegistrationChanges() throws Exception {
        TablePath tablePath = TablePath.of("bulkload_e2e", "active_identity_refresh");
        PhysicalTablePath target = PhysicalTablePath.of(tablePath);
        TableInfo tableInfo = createDatabaseAndTable(admin, tablePath, 1);
        cluster.waitUntilTableReady(tableInfo.getTableId());
        PbBulkLoadTargetInfo context = await(begin(admin, target));
        CommitBulkLoadRequest input =
                buildInput(context, Collections.singletonList(new Object[] {1, "bulkload"}));
        PbBulkLoadStatus committed = commitUntilReady(admin, input);
        assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED.getCode());

        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
        Replica leader = cluster.waitAndGetLeaderReplica(tableBucket);
        Integer leaderId = leader.getLeaderId();
        await(
                admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key(), "5")),
                        false));
        waitValue(
                () ->
                        leader.getLogTablet().getTieredLogLocalSegments() == 5
                                ? Optional.of(Boolean.TRUE)
                                : Optional.empty(),
                E2E_TIMEOUT,
                "table config metadata dispatch reaches the active leader");
        assertThat(cluster.waitAndGetLeaderReplica(tableBucket).getLeaderId()).isEqualTo(leaderId);

        try (Table table = connection.getTable(tablePath)) {
            writeAndAwaitOrdinaryRow(
                    table, tableInfo, new Object[] {2, "ordinary"}, "ordinary row before snapshot");
        }
        assertThat(cluster.triggerAndWaitSnapshot(tableBucket)).isNotNull();
    }

    @Test
    void testAbortAfterBuildRestoresAccessAndLeavesStandardFilesForOrdinaryOrphanCleanup()
            throws Exception {
        TablePath tablePath = TablePath.of("bulkload_e2e", "built_abort");
        createDatabaseAndTable(admin, tablePath, BUCKET_COUNT);

        try (Table table = connection.getTable(tablePath)) {
            runAbortAfterBuildScenario(table);
        }
    }

    @Test
    void testAbortConvergesWhenAssignedHolderIsAbsentBeforeFence() throws Exception {
        TablePath tablePath = TablePath.of("bulkload_e2e", "pre_fence_abort");
        TableInfo tableInfo = createDatabaseAndTable(admin, tablePath, 1);
        cluster.waitUntilTableReady(tableInfo.getTableId());

        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        int leader = cluster.waitAndGetLeader(bucket);
        TableAssignment assignment =
                cluster.getZooKeeperClient()
                        .getTableAssignment(tableInfo.getTableId())
                        .orElseThrow(() -> new AssertionError("Missing table assignment."));
        List<Integer> replicas = assignment.getBucketAssignment(0).getReplicas();
        assertThat(replicas).hasSize(3);
        int absentHolder =
                replicas.stream()
                        .filter(holder -> holder != leader)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing follower holder."));
        cluster.stopTabletServer(absentHolder);

        ZooKeeperClient zkClient = cluster.getZooKeeperClient();
        String holderPath = ZkData.ServerIdZNode.path(absentHolder);
        waitValue(
                () ->
                        zkClient.pathExists(holderPath)
                                ? Optional.empty()
                                : Optional.of(Boolean.TRUE),
                Duration.ofSeconds(30),
                "stopped holder registration disappears before Begin");

        begin(admin, PhysicalTablePath.of(tablePath));
        TableRegistration registration =
                waitValue(
                        () -> {
                            try {
                                TableRegistration value =
                                        ZkData.TableZNode.decode(
                                                zkClient.getDataWithStat(
                                                                ZkData.TableZNode.path(tablePath))
                                                        .getData());
                                return value.dataState == BulkLoadDataState.LOADING
                                                && value.bulkLoadId != null
                                        ? Optional.of(value)
                                        : Optional.empty();
                            } catch (Exception notLoadingYet) {
                                return Optional.empty();
                            }
                        },
                        Duration.ofSeconds(30),
                        "pre-fence registration owns the BulkLoad transaction");
        String transactionPath =
                ZkData.BulkLoadTableTransactionZNode.path(
                        tableInfo.getTableId(), registration.bulkLoadId);
        BulkLoadTransaction transaction =
                ZkData.BulkLoadTransactionsZNode.decode(
                        zkClient.getDataWithStat(transactionPath).getData());
        assertThat(transaction.getState()).isEqualTo(BulkLoadState.BEGUN);
        assertThat(transaction.isFenceReady()).isFalse();
        assertThat(transaction.getBulkLoadId()).isEqualTo(registration.bulkLoadId);
        assertThat(registration.dataState).isEqualTo(BulkLoadDataState.LOADING);
        assertThat(registration.bulkLoadId).isEqualTo(transaction.getBulkLoadId());

        PbBulkLoadStatus aborted =
                abort(
                                admin,
                                new PbBulkLoadHandle()
                                        .setTarget(
                                                fromPhysicalTablePath(
                                                        transaction.getHandle().getTarget()))
                                        .setTableId(transaction.getTableId())
                                        .setBulkLoadId(transaction.getBulkLoadId()))
                        .get(30, TimeUnit.SECONDS);
        assertThat(aborted.getState()).isEqualTo(BulkLoadState.ABORTED.getCode());
        BulkLoadTransaction terminal =
                ZkData.BulkLoadTransactionsZNode.decode(
                        zkClient.getDataWithStat(transactionPath).getData());
        TableRegistration active =
                ZkData.TableZNode.decode(
                        zkClient.getDataWithStat(ZkData.TableZNode.path(tablePath)).getData());
        assertThat(terminal.getState()).isEqualTo(BulkLoadState.ABORTED);
        assertThat(active.dataState).isEqualTo(BulkLoadDataState.ACTIVE);
        assertThat(active.bulkLoadId).isNull();

        try (Table table = connection.getTable(tablePath)) {
            writeAndAwaitOrdinaryRow(
                    table,
                    tableInfo,
                    new Object[] {1, "ordinary-after-pre-fence-abort"},
                    "ordinary lookup succeeds after pre-fence Abort");
        }
    }

    @Test
    void testIdempotencyAndNonEmptyTargetFence() throws Exception {
        TablePath idempotent = TablePath.of("bulkload_e2e", "idempotent");
        TablePath localNonEmpty = TablePath.of("bulkload_e2e", "local_non_empty");
        TablePath remoteNonEmpty = TablePath.of("bulkload_e2e", "remote_non_empty");
        await(admin.createDatabase(idempotent.getDatabaseName(), DatabaseDescriptor.EMPTY, false));
        Map<TablePath, TableInfo> tables = new LinkedHashMap<>();
        for (TablePath tablePath : Arrays.asList(idempotent, localNonEmpty, remoteNonEmpty)) {
            createTable(admin, tablePath, 1);
            TableInfo tableInfo = await(admin.getTableInfo(tablePath));
            cluster.waitUntilTableReady(tableInfo.getTableId());
            tables.put(tablePath, tableInfo);
        }

        PhysicalTablePath idempotentTarget = PhysicalTablePath.of(idempotent);
        PbBulkLoadTargetInfo firstBegin = await(begin(admin, idempotentTarget));
        assertThat(await(getInProgress((FlussAdmin) connection.getAdmin(), idempotentTarget)))
                .hasValueSatisfying(
                        status -> {
                            assertThat(status.getHandle().toByteArray())
                                    .isEqualTo(firstBegin.getHandle().toByteArray());
                            assertThat(status.getState()).isEqualTo(BulkLoadState.BEGUN.getCode());
                        });
        assertThatThrownBy(() -> await(begin(admin, idempotentTarget)))
                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageNotContaining(firstBegin.getHandle().getBulkLoadId());
        CommitBulkLoadRequest input =
                buildInput(
                        firstBegin,
                        Collections.<Object[]>singletonList(new Object[] {1, "idempotent"}));
        PbBulkLoadStatus firstCommit = await(commit(admin, input));
        PbBulkLoadStatus repeatedCommit = await(commit(admin, input));
        assertThat(repeatedCommit.toByteArray()).isEqualTo(firstCommit.toByteArray());
        assertThatThrownBy(
                        () ->
                                await(
                                        commit(
                                                admin,
                                                new CommitBulkLoadRequest()
                                                        .setHandle(input.getHandle())
                                                        .setInputSha256(
                                                                String.join(
                                                                        "",
                                                                        Collections.nCopies(
                                                                                64, "0"))))))
                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("conflicts with the frozen input");
        assertThat(firstCommit.getState()).isEqualTo(BulkLoadState.COMMITTED.getCode());
        assertThat(await(getInProgress((FlussAdmin) connection.getAdmin(), idempotentTarget)))
                .isEmpty();

        TableInfo localTableInfo = tables.get(localNonEmpty);
        try (Table localTable = connection.getTable(localNonEmpty)) {
            writeAndAwaitOrdinaryRow(
                    localTable,
                    localTableInfo,
                    new Object[] {2, "ordinary-local"},
                    "ordinary local content before rejected Begin");
        }
        assertTargetNotEmpty(admin, PhysicalTablePath.of(localNonEmpty), "contains local data");

        TableInfo remoteTableInfo = tables.get(remoteNonEmpty);
        TableBucket remoteBucket = new TableBucket(remoteTableInfo.getTableId(), 0);
        FsPath remoteManifestPath =
                new FsPath(cluster.getRemoteDataDir(), "registered-remote-manifest.json");
        Files.createDirectories(Paths.get(remoteManifestPath.toUri()).getParent());
        Files.write(
                Paths.get(remoteManifestPath.toUri()),
                new RemoteLogManifest(
                                PhysicalTablePath.of(remoteNonEmpty),
                                remoteBucket,
                                Collections.emptyList())
                        .toJsonBytes());
        cluster.getZooKeeperClient()
                .upsertRemoteLogManifestHandle(
                        remoteBucket, new RemoteLogManifestHandle(remoteManifestPath, -1L));
        assertTargetNotEmpty(admin, PhysicalTablePath.of(remoteNonEmpty), "ordinary remote data");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRetainedTerminalHandleIsIdempotentAcrossConnections(boolean committed)
            throws Exception {
        Configuration clientConfiguration = cluster.getClientConfig();
        TablePath tablePath = TablePath.of("bulkload_e2e", "retained_handle");
        PhysicalTablePath target = PhysicalTablePath.of(tablePath);
        TableInfo tableInfo;
        PbBulkLoadTargetInfo targetInfo;
        Object[] ordinaryRow = new Object[] {Integer.MAX_VALUE - 1, "ordinary-after-terminal"};
        try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
                FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
            tableInfo = createDatabaseAndTable(admin, tablePath, BUCKET_COUNT);
            cluster.waitUntilTableReady(tableInfo.getTableId());
            targetInfo = await(begin(admin, target));
        }

        BucketRows rowGenerator = new BucketRows(tableInfo, 0);
        List<Object[]> rows = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            rows.add(rowGenerator.row(bucket, "retained"));
        }
        CommitBulkLoadRequest input = committed ? buildInput(targetInfo, rows) : null;
        PbBulkLoadHandle handle = targetInfo.getHandle();
        PbBulkLoadStatus status;
        try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
                FlussAdmin admin = (FlussAdmin) connection.getAdmin();
                Table table = connection.getTable(tablePath)) {
            status = await((committed ? commit(admin, input) : abort(admin, handle)));
            assertThat(status.getState())
                    .isEqualTo(
                            (committed ? BulkLoadState.COMMITTED : BulkLoadState.ABORTED)
                                    .getCode());
            if (!committed) {
                PbBulkLoadHandle freshHandle = await(begin(admin, target)).getHandle();
                assertThat(freshHandle.toByteArray()).isNotEqualTo(handle.toByteArray());
                assertThat(await(abort(admin, freshHandle)).getState())
                        .isEqualTo(BulkLoadState.ABORTED.getCode());
            }
            writeAndAwaitOrdinaryRow(
                    table, tableInfo, ordinaryRow, "ordinary lookup after terminal");
        }

        try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
                FlussAdmin admin = (FlussAdmin) connection.getAdmin();
                Table table = connection.getTable(tablePath)) {
            PbBulkLoadStatus repeated =
                    await((committed ? commit(admin, input) : abort(admin, handle)));
            assertThat(repeated.toByteArray()).isEqualTo(status.toByteArray());
            assertThat(await(getInProgress(admin, target))).isEmpty();
            awaitPublicRow(
                    table,
                    tableInfo.getRowType(),
                    ordinaryRow,
                    "ordinary lookup after repeated terminal request");
        }

        if (!committed) {
            try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
                    FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                await(admin.dropTable(tablePath, false));
                PbBulkLoadStatus afterDrop = await(abort(admin, handle));
                assertThat(afterDrop.toByteArray()).isEqualTo(status.toByteArray());
            }
        }
    }

    @Test
    void testReRegisteredAssignedHolderCannotEscapeLoadingFence() throws Exception {
        TablePath tablePath = TablePath.of("bulkload_e2e", "re_registered_holder");
        TableInfo tableInfo = createDatabaseAndTable(admin, tablePath, 1);
        cluster.waitUntilTableReady(tableInfo.getTableId());

        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        int leader = cluster.waitAndGetLeader(bucket);
        TableAssignment assignment =
                cluster.getZooKeeperClient()
                        .getTableAssignment(tableInfo.getTableId())
                        .orElseThrow(() -> new AssertionError("Missing table assignment."));
        List<Integer> followers =
                assignment.getBucketAssignment(0).getReplicas().stream()
                        .filter(holder -> holder != leader)
                        .collect(Collectors.toList());
        assertThat(followers).hasSize(2);
        int staleHolder = followers.get(0);
        int unavailableHolder = followers.get(1);

        Replica staleReplica =
                cluster.getTabletServerById(staleHolder)
                        .getReplicaManager()
                        .getReplicaOrException(bucket);
        staleReplica.appendRecordsToFollower(genMemoryLogRecordsByObject(DATA1));
        assertThat(staleReplica.getLocalLogEndOffset()).isEqualTo(DATA1.size());
        cluster.stopTabletServer(staleHolder);
        cluster.stopTabletServer(unavailableHolder);
        cluster.assertHasTabletServerNumber(1);

        CompletableFuture<PbBulkLoadTargetInfo> beginFuture =
                begin(admin, PhysicalTablePath.of(tablePath));
        assertThatThrownBy(() -> beginFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);

        cluster.startTabletServer(staleHolder);
        Replica reRegisteredReplica = awaitLoadingReplica(staleHolder, bucket);
        assertThat(reRegisteredReplica.getLocalLogEndOffset()).isEqualTo(DATA1.size());
        assertLoadingFetchRejected(staleHolder, bucket);

        cluster.startTabletServer(unavailableHolder);
        assertThatThrownBy(() -> await(beginFuture))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining(BulkLoadAbortReason.TARGET_NOT_EMPTY.name());
        assertThat(
                        await(
                                getInProgress(
                                        (FlussAdmin) connection.getAdmin(),
                                        PhysicalTablePath.of(tablePath))))
                .isEmpty();
    }

    private static void createTable(FlussAdmin admin, TablePath tablePath, int buckets)
            throws Exception {
        await(
                admin.createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(FULL_SCHEMA)
                                .distributedBy(buckets, "id")
                                .build(),
                        false));
    }

    private static TableInfo createDatabaseAndTable(
            FlussAdmin admin, TablePath tablePath, int buckets) throws Exception {
        await(admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false));
        createTable(admin, tablePath, buckets);
        return await(admin.getTableInfo(tablePath));
    }

    private static void assertTargetNotEmpty(
            FlussAdmin admin, PhysicalTablePath target, String expectedEvidence) throws Exception {
        assertThatThrownBy(() -> await(begin(admin, target)))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining(BulkLoadAbortReason.TARGET_NOT_EMPTY.name())
                .hasMessageContaining(expectedEvidence);
        assertThat(getInProgress(admin, target).get()).isEmpty();
    }

    private static void writeRow(Table table, Object[] values, Duration timeout) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row(table.getTableInfo().getRowType(), values))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        writer.flush();
    }

    private static void writeAndAwaitOrdinaryRow(
            Table table, TableInfo tableInfo, Object[] values, String description)
            throws Exception {
        writeRow(table, values, E2E_TIMEOUT);
        awaitPublicRow(table, tableInfo.getRowType(), values, description);
    }

    private void runFullImportScenario(Table table, Table accessProbeTable) throws Exception {
        TableInfo tableInfo = table.getTableInfo();
        TablePath tablePath = tableInfo.getTablePath();
        int seedBase =
                (System.getProperty("test.randomization.seed", "bulkload-e2e-default").hashCode()
                                & Integer.MAX_VALUE)
                        % 100_000;
        BucketRows rowGenerator = new BucketRows(tableInfo, seedBase);
        List<Object[]> inputRows = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT - 1; bucket++) {
            int records = bucket == 0 ? INDEXED_ROWS_PER_BUCKET : 6;
            for (int record = 0; record < records; record++) {
                inputRows.add(rowGenerator.row(bucket, "bucket-" + bucket + "-record-" + record));
            }
        }
        Object[] first = inputRows.get(0);
        Object[] overwrittenFirst = Arrays.copyOf(first, first.length);
        overwrittenFirst[1] = "overwritten-" + first[0];
        List<Object[]> readinessProbes = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            readinessProbes.add(rowGenerator.row(bucket, "readiness-probe-" + bucket));
        }
        awaitPublicBucketReadiness(table, readinessProbes);

        PhysicalTablePath target = PhysicalTablePath.of(tablePath);
        PbBulkLoadTargetInfo context = await(begin(admin, target));
        assertThat(await(getInProgress(admin, target)))
                .hasValueSatisfying(
                        status -> {
                            assertThat(status.getHandle().toByteArray())
                                    .isEqualTo(context.getHandle().toByteArray());
                            assertThat(status.getState()).isEqualTo(BulkLoadState.BEGUN.getCode());
                        });
        assertLoadingAccessRejected(accessProbeTable, tableInfo, first);

        List<Object[]> buildRows = new ArrayList<>();
        buildRows.add(overwrittenFirst);
        buildRows.addAll(inputRows);
        CommitBulkLoadRequest input = buildInput(context, buildRows);
        Map<Integer, Long> expectedInitialOffsets = new LinkedHashMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            Set<Integer> finalKeys = new HashSet<>();
            for (Object[] values : inputRows) {
                if (bucketOf(tableInfo, values) == bucket) {
                    finalKeys.add((Integer) values[0]);
                }
            }
            expectedInitialOffsets.put(bucket, (long) finalKeys.size());
        }

        int controlledBucket = 0;
        TableBucket controlledTableBucket =
                new TableBucket(tableInfo.getTableId(), controlledBucket);
        int stoppedServer = cluster.waitAndGetLeader(controlledTableBucket);
        PbBulkLoadStatus committed = commitUntilReady(admin, input);
        assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED.getCode());
        assertThat(latestOffsets(admin, target, expectedInitialOffsets.keySet()))
                .containsExactlyInAnyOrderEntriesOf(expectedInitialOffsets);
        verifyPublicRows(table, tableInfo.getRowType(), inputRows);
        assertNoBulkLoadRemoteLogArtifacts(cluster, tableInfo, target);

        cluster.stopTabletServer(stoppedServer);
        try (Connection recoveredConnection =
                        ConnectionFactory.createConnection(cluster.getClientConfig());
                Admin recoveredAdmin = recoveredConnection.getAdmin();
                Table recoveredTable = recoveredConnection.getTable(tablePath)) {
            verifyPublicRows(recoveredTable, tableInfo.getRowType(), inputRows);

            Object[] ordinaryRow = rowGenerator.row(controlledBucket, "ordinary-tail");
            writeRow(recoveredTable, ordinaryRow, E2E_TIMEOUT);

            List<Object[]> rowsAfterWrite = new ArrayList<>(inputRows);
            rowsAfterWrite.add(Arrays.copyOf(ordinaryRow, ordinaryRow.length));
            verifyPublicRows(recoveredTable, tableInfo.getRowType(), rowsAfterWrite);
            Map<Integer, Long> expectedOffsetsAfterWrite =
                    new LinkedHashMap<>(expectedInitialOffsets);
            expectedOffsetsAfterWrite.put(
                    controlledBucket, expectedInitialOffsets.get(controlledBucket) + 1L);
            assertThat(latestOffsets(recoveredAdmin, target, expectedInitialOffsets.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(expectedOffsetsAfterWrite);
        }
    }

    private void runAbortAfterBuildScenario(Table table) throws Exception {
        TableInfo tableInfo = table.getTableInfo();
        TablePath tablePath = tableInfo.getTablePath();
        int seedBase =
                (System.getProperty("test.randomization.seed", "bulkload-abort-e2e-default")
                                                .hashCode()
                                        & Integer.MAX_VALUE)
                                % 100_000
                        + 600_000;
        BucketRows rowGenerator = new BucketRows(tableInfo, seedBase);
        List<Object[]> inputRows = new ArrayList<>();
        List<Object[]> readinessProbes = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            inputRows.add(rowGenerator.row(bucket, "aborted-input-bucket-" + bucket));
            readinessProbes.add(rowGenerator.row(bucket, "abort-readiness-bucket-" + bucket));
        }
        awaitPublicBucketReadiness(table, readinessProbes);

        PbBulkLoadTargetInfo targetInfo = await(begin(admin, PhysicalTablePath.of(tablePath)));

        CommitBulkLoadRequest input = buildInput(targetInfo, inputRows);
        assertThatThrownBy(() -> await(admin.dropTable(tablePath, false)))
                .hasCauseInstanceOf(InvalidAlterTableException.class);

        PbBulkLoadStatus aborted = await(abort(admin, input.getHandle()));
        assertThat(aborted.getState()).isEqualTo(BulkLoadState.ABORTED.getCode());

        verifyEmptyPublicTarget(admin, table, tablePath);

        Object[] ordinaryRow = rowGenerator.row(0, "ordinary-after-abort");
        writeRow(table, ordinaryRow, E2E_TIMEOUT);

        awaitPublicRow(
                table,
                tableInfo.getRowType(),
                ordinaryRow,
                "ordinary lookup succeeds after aborting built output");
        assertThat(
                        await(table.newLookup().createLookuper().lookup(row(Integer.MAX_VALUE)))
                                .getSingletonRow())
                .isNull();
        awaitExactPublicBatchScan(table, Collections.<Object[]>singletonList(ordinaryRow));
        verifySingleOrdinaryLogAfterAbort(admin, table, tablePath, ordinaryRow);
    }

    private void runPartitionedWalRecoveryScenario(
            Table table, TableInfo tableInfo, Map<String, Long> partitionIds) throws Exception {
        TablePath tablePath = tableInfo.getTablePath();
        long targetPartitionId = partitionIds.get(WAL_TARGET_PARTITION);
        int seedBase =
                (System.getProperty("test.randomization.seed", "bulkload-wal-e2e-default")
                                                .hashCode()
                                        & Integer.MAX_VALUE)
                                % 100_000
                        + 300_000;
        BucketRows rowGenerator = new BucketRows(tableInfo, seedBase);
        List<Object[]> targetRows = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            targetRows.addAll(
                    rowGenerator.partitionRows(
                            bucket, STRESS_ROWS_PER_BUCKET, WAL_TARGET_PARTITION));
        }
        List<Object[]> liveRows = new ArrayList<>();
        writeAndAwaitPartitionRow(
                table,
                tableInfo.getRowType(),
                liveRows,
                rowGenerator.row(0, "live-before-begin", WAL_LIVE_PARTITION),
                "non-target partition is live before Begin");

        PbBulkLoadTargetInfo targetInfo =
                await(begin(admin, PhysicalTablePath.of(tablePath, WAL_TARGET_PARTITION)));
        assertThat(targetInfo.getHandle().getPartitionId()).isEqualTo(targetPartitionId);
        assertThat(
                        TableDescriptor.fromJsonBytes(targetInfo.getTableJson())
                                .getTableDistribution()
                                .get()
                                .getBucketCount()
                                .get())
                .isEqualTo(BUCKET_COUNT);
        writeAndAwaitPartitionRow(
                table,
                tableInfo.getRowType(),
                liveRows,
                rowGenerator.row(1, "live-after-begin", WAL_LIVE_PARTITION),
                "non-target partition is live while target is LOADING");

        CommitBulkLoadRequest input = buildInput(targetInfo, targetRows);
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            FsPath snapshotDir =
                    FlussPaths.remoteKvSnapshotDir(
                            FlussPaths.remoteKvTabletDir(
                                    new FsPath(
                                            targetInfo.getRemoteDataDir(),
                                            FlussPaths.REMOTE_KV_DIR_NAME),
                                    toPhysicalTablePath(targetInfo.getHandle().getTarget()),
                                    new TableBucket(
                                            targetInfo.getHandle().getTableId(),
                                            targetInfo.getHandle().getPartitionId(),
                                            bucket)),
                            targetInfo.getSnapshotIds()[bucket]);
            CompletedSnapshot snapshot =
                    new CompletedSnapshotHandle(0, new FsPath(snapshotDir, "_METADATA"), 0)
                            .retrieveCompleteSnapshot();
            assertThat(snapshot.getRowCount()).isNull();
            assertThat(snapshot.getLogOffset()).isEqualTo(STRESS_ROWS_PER_BUCKET);
        }

        writeAndAwaitPartitionRow(
                table,
                tableInfo.getRowType(),
                liveRows,
                rowGenerator.row(2, "live-before-commit", WAL_LIVE_PARTITION),
                "non-target partition is live before target Commit");

        int controlledBucket = 0;
        TableBucket controlledTableBucket =
                new TableBucket(tableInfo.getTableId(), targetPartitionId, controlledBucket);
        int originalLeader = cluster.waitAndGetLeader(controlledTableBucket);
        cluster.stopTabletServer(originalLeader);
        int replacementLeader =
                awaitReplacementLeader(cluster, controlledTableBucket, originalLeader);
        assertThat(replacementLeader).isNotEqualTo(originalLeader);

        cluster.stopCoordinatorServer();
        cluster.startCoordinatorServer();
        try (Connection recoveredConnection =
                        ConnectionFactory.createConnection(cluster.getClientConfig());
                FlussAdmin recoveredAdmin = (FlussAdmin) recoveredConnection.getAdmin();
                Table recoveredTable = recoveredConnection.getTable(tablePath)) {
            CompletableFuture<Void> recoveredCommit =
                    publicOperation(() -> commitUntilReady(recoveredAdmin, input));
            await(recoveredCommit);
            verifyPartitionedPublicState(
                    recoveredAdmin, recoveredTable, tableInfo, targetRows, null, liveRows);

            Object[] ordinaryTail =
                    rowGenerator.row(
                            controlledBucket, "ordinary-target-tail", WAL_TARGET_PARTITION);
            writeAndAwaitPartitionRow(
                    recoveredTable,
                    tableInfo.getRowType(),
                    null,
                    ordinaryTail,
                    "ordinary target write continues from E",
                    Duration.ofSeconds(15));
            assertThat(cluster.triggerAndWaitSnapshot(controlledTableBucket)).isNotNull();
            writeAndAwaitPartitionRow(
                    recoveredTable,
                    tableInfo.getRowType(),
                    liveRows,
                    rowGenerator.row(1, "live-after-recovery", WAL_LIVE_PARTITION),
                    "non-target partition remains live after both process restarts");
            verifyPartitionedPublicState(
                    recoveredAdmin, recoveredTable, tableInfo, targetRows, ordinaryTail, liveRows);
        }
    }

    /** Restarts one holder inside the build window and reasserts the LOADING fence. */
    @Test
    void testLoadingFenceSurvivesHolderRestartWhileBuilding() throws Exception {
        TablePath tablePath = TablePath.of("bulkload_e2e", "fence_survives_holder_restart");
        TableInfo tableInfo = createDatabaseAndTable(admin, tablePath, 1);
        cluster.waitUntilTableReady(tableInfo.getTableId());

        PhysicalTablePath target = PhysicalTablePath.of(tablePath);
        PbBulkLoadTargetInfo context = await(begin(admin, target));

        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        int restartedHolder = cluster.waitAndGetLeader(bucket);
        cluster.stopTabletServer(restartedHolder);
        cluster.startTabletServer(restartedHolder);
        awaitLoadingReplica(restartedHolder, bucket);
        assertLoadingFetchRejected(restartedHolder, bucket);

        PbBulkLoadStatus aborted = await(abort(admin, context.getHandle()));
        assertThat(aborted.getState()).isEqualTo(BulkLoadState.ABORTED.getCode());
        assertThat(await(getInProgress((FlussAdmin) connection.getAdmin(), target))).isEmpty();
    }

    private Replica awaitLoadingReplica(int serverId, TableBucket bucket) {
        return waitValue(
                () -> {
                    try {
                        Replica replica =
                                cluster.getTabletServerById(serverId)
                                        .getReplicaManager()
                                        .getReplicaOrException(bucket);
                        BulkLoadTargetMetadata target = replica.getBulkLoadTargetMetadata();
                        return target != null && target.isLoading()
                                ? Optional.of(replica)
                                : Optional.empty();
                    } catch (RuntimeException notReady) {
                        return Optional.empty();
                    }
                },
                E2E_TIMEOUT,
                "restarted holder reapplies LOADING before access");
    }

    private void assertLoadingFetchRejected(int serverId, TableBucket bucket) throws Exception {
        FetchLogResponse response =
                await(
                        cluster.newTabletServerClientForNode(serverId)
                                .fetchLog(
                                        newFetchLogRequest(
                                                -1, bucket.getTableId(), bucket.getBucket(), 0L)));
        assertThat(response.getTablesRespsCount()).isEqualTo(1);
        assertThat(response.getTablesRespAt(0).getBucketsRespsCount()).isEqualTo(1);
        assertThat(response.getTablesRespAt(0).getBucketsRespAt(0).getErrorCode())
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        assertThat(response.getTablesRespAt(0).getBucketsRespAt(0).getErrorMessage())
                .contains("BulkLoad target is LOADING and rejects external access")
                .contains(bucket.toString());
    }

    private static void assertLoadingAccessRejected(
            Table table, TableInfo tableInfo, Object[] existingRow) {
        awaitLoadingAccessRejection(
                () -> table.newLookup().createLookuper().lookup(row((Integer) existingRow[0])),
                E2E_TIMEOUT,
                "point lookup");
        awaitLoadingAccessRejection(
                () ->
                        publicOperation(
                                () -> {
                                    try (BatchScanner scanner =
                                            table.newScan()
                                                    .createBatchScanner(
                                                            new TableBucket(
                                                                    tableInfo.getTableId(), 0))) {
                                        scanner.pollBatch(Duration.ofSeconds(1));
                                    }
                                }),
                E2E_TIMEOUT,
                "batch scan");
        awaitLoadingAccessRejection(
                () ->
                        publicOperation(
                                () -> {
                                    try (LogScanner scanner = table.newScan().createLogScanner()) {
                                        scanner.subscribeFromBeginning(0);
                                        scanner.poll(Duration.ofSeconds(1));
                                    }
                                }),
                E2E_TIMEOUT,
                "log scan");
        awaitLoadingAccessRejection(
                () -> {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    return writer.upsert(
                            row(
                                    tableInfo.getRowType(),
                                    new Object[] {
                                        (Integer) existingRow[0] + 1_000_000, "fenced-write"
                                    }));
                },
                E2E_TIMEOUT,
                "ordinary write");
    }

    private static void writeAndAwaitPartitionRow(
            Table table,
            RowType rowType,
            List<Object[]> retainedRows,
            Object[] values,
            String description)
            throws Exception {
        writeAndAwaitPartitionRow(table, rowType, retainedRows, values, description, E2E_TIMEOUT);
    }

    private static void writeAndAwaitPartitionRow(
            Table table,
            RowType rowType,
            List<Object[]> retainedRows,
            Object[] values,
            String description,
            Duration writeTimeout)
            throws Exception {
        writeRow(table, values, writeTimeout);
        awaitPublicRow(table, rowType, values, description);
        if (retainedRows != null) {
            retainedRows.add(Arrays.copyOf(values, values.length));
        }
    }

    private static void verifyPartitionedPublicState(
            Admin admin,
            Table table,
            TableInfo tableInfo,
            List<Object[]> targetRows,
            Object[] targetTail,
            List<Object[]> liveRows)
            throws Exception {
        List<Object[]> expectedRows = new ArrayList<>(targetRows);
        if (targetTail != null) {
            expectedRows.add(Arrays.copyOf(targetTail, targetTail.length));
        }
        expectedRows.addAll(liveRows);
        List<Object[]> lookupRows = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            final int bucketId = bucket;
            List<Object[]> bucketRows =
                    targetRows.stream()
                            .filter(values -> bucketOf(tableInfo, values) == bucketId)
                            .collect(Collectors.toList());
            lookupRows.add(bucketRows.get(0));
            lookupRows.add(bucketRows.get(bucketRows.size() / 2));
            lookupRows.add(bucketRows.get(bucketRows.size() - 1));
        }
        if (targetTail != null) {
            lookupRows.add(targetTail);
        }
        lookupRows.addAll(liveRows);
        for (Object[] expected : lookupRows) {
            awaitPublicRow(table, tableInfo.getRowType(), expected, "exact partitioned KV");
        }
        assertThat(
                        await(
                                        table.newLookup()
                                                .createLookuper()
                                                .lookup(
                                                        row(
                                                                Integer.MAX_VALUE,
                                                                WAL_TARGET_PARTITION)))
                                .getSingletonRow())
                .isNull();
        awaitExactPublicBatchScan(table, expectedRows);

        Map<Integer, List<Object[]>> targetLogs =
                groupByBucket(
                        tableInfo,
                        targetTail == null
                                ? Collections.emptyList()
                                : Collections.singletonList(targetTail));
        assertExactPublicLogs(
                admin,
                table,
                PhysicalTablePath.of(tableInfo.getTablePath(), WAL_TARGET_PARTITION),
                targetLogs,
                logEndOffsets(targetLogs, STRESS_ROWS_PER_BUCKET),
                ChangeType.UPDATE_AFTER);

        Map<Integer, List<Object[]>> liveLogs = groupByBucket(tableInfo, liveRows);
        assertExactPublicLogs(
                admin,
                table,
                PhysicalTablePath.of(tableInfo.getTablePath(), WAL_LIVE_PARTITION),
                liveLogs,
                logEndOffsets(liveLogs, 0L),
                ChangeType.UPDATE_AFTER);
    }

    private static void awaitExactPublicBatchScan(Table table, List<Object[]> expectedRows) {
        List<Object[]> actual =
                waitValue(
                        () -> {
                            try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                                List<Object[]> observed =
                                        toLogicalRows(BatchScanUtils.collectRows(scanner));
                                return observed.size() == expectedRows.size()
                                        ? Optional.of(observed)
                                        : Optional.empty();
                            } catch (Exception failure) {
                                rethrowUnlessTransient(failure);
                                return Optional.empty();
                            }
                        },
                        E2E_TIMEOUT,
                        "exact public batch scan");
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    private static void assertExactPublicLogs(
            Admin admin,
            Table table,
            PhysicalTablePath target,
            Map<Integer, List<Object[]>> expectedByBucket,
            Map<Integer, Long> expectedEndOffsets,
            ChangeType expectedChangeType)
            throws Exception {
        int expectedRecordCount = 0;
        for (Map.Entry<Integer, List<Object[]>> entry : expectedByBucket.entrySet()) {
            expectedRecordCount += entry.getValue().size();
        }
        retry(
                E2E_TIMEOUT,
                () -> {
                    try {
                        assertThat(latestOffsets(admin, target, expectedByBucket.keySet()))
                                .as("BulkLoad offsets for %s", target)
                                .containsExactlyInAnyOrderEntriesOf(expectedEndOffsets);
                    } catch (Exception failure) {
                        rethrowUnlessTransient(failure);
                        throw new AssertionError("Public access has not converged", failure);
                    }
                });

        Map<Integer, List<ScanRecord>> actualByBucket = new LinkedHashMap<>();
        for (Integer bucket : expectedByBucket.keySet()) {
            actualByBucket.put(bucket, new ArrayList<>());
        }
        Long partitionId = partitionId(admin, target);
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (Integer bucket : expectedByBucket.keySet()) {
                if (partitionId == null) {
                    scanner.subscribeFromBeginning(bucket);
                } else {
                    scanner.subscribeFromBeginning(partitionId, bucket);
                }
            }
            long deadlineNanos = System.nanoTime() + E2E_TIMEOUT.toNanos();
            int actualCount = 0;
            while (actualCount < expectedRecordCount && System.nanoTime() < deadlineNanos) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets()) {
                    assertThat(bucket.getPartitionId()).isEqualTo(partitionId);
                    List<ScanRecord> bucketRecords = records.records(bucket);
                    actualByBucket.get(bucket.getBucket()).addAll(bucketRecords);
                    actualCount += bucketRecords.size();
                }
            }
            assertThat(actualCount).isEqualTo(expectedRecordCount);
        }
        for (Map.Entry<Integer, List<Object[]>> entry : expectedByBucket.entrySet()) {
            List<ScanRecord> actual = actualByBucket.get(entry.getKey());
            assertThat(actual).hasSameSizeAs(entry.getValue());
            long firstExpectedOffset =
                    expectedEndOffsets.get(entry.getKey()) - entry.getValue().size();
            for (int offset = 0; offset < entry.getValue().size(); offset++) {
                ScanRecord record = actual.get(offset);
                assertThat(record.logOffset()).isEqualTo(firstExpectedOffset + offset);
                assertThat(record.getChangeType()).isEqualTo(expectedChangeType);
                assertThatRow(record.getRow())
                        .withSchema(table.getTableInfo().getRowType())
                        .isEqualTo(
                                row(
                                        table.getTableInfo().getRowType(),
                                        entry.getValue().get(offset)));
            }
        }
    }

    private static Map<Integer, Long> latestOffsets(
            Admin admin, PhysicalTablePath target, Set<Integer> buckets) throws Exception {
        List<Integer> bucketList = new ArrayList<>(buckets);
        return target.getPartitionName() == null
                ? await(
                        admin.listOffsets(
                                        target.getTablePath(),
                                        bucketList,
                                        new OffsetSpec.LatestSpec())
                                .all())
                : await(
                        admin.listOffsets(
                                        target.getTablePath(),
                                        target.getPartitionName(),
                                        bucketList,
                                        new OffsetSpec.LatestSpec())
                                .all());
    }

    private static void assertNoBulkLoadRemoteLogArtifacts(
            FlussClusterExtension cluster, TableInfo tableInfo, PhysicalTablePath target)
            throws Exception {
        ZooKeeperClient zkClient = cluster.getZooKeeperClient();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucket);
            assertThat(zkClient.pathExists(ZkData.BucketRemoteLogsZNode.path(tableBucket)))
                    .isFalse();
            FsPath remoteTabletDirectory =
                    FlussPaths.remoteLogTabletDir(
                            new FsPath(
                                    tableInfo.getRemoteDataDir(), FlussPaths.REMOTE_LOG_DIR_NAME),
                            target,
                            tableBucket);
            Path localRemoteTabletDirectory = Paths.get(remoteTabletDirectory.toUri());
            if (Files.exists(localRemoteTabletDirectory)) {
                try (Stream<Path> paths = Files.walk(localRemoteTabletDirectory)) {
                    assertThat(paths.filter(Files::isRegularFile)).isEmpty();
                }
            }
        }
    }

    private static Long partitionId(Admin admin, PhysicalTablePath target) throws Exception {
        if (target.getPartitionName() == null) {
            return null;
        }
        return await(admin.listPartitionInfos(target.getTablePath())).stream()
                .filter(info -> info.getPartitionName().equals(target.getPartitionName()))
                .map(PartitionInfo::getPartitionId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing partition metadata for " + target));
    }

    private static int awaitReplacementLeader(
            FlussClusterExtension cluster, TableBucket tableBucket, int originalLeader) {
        return waitValue(
                () -> {
                    Optional<LeaderAndIsr> role =
                            cluster.getZooKeeperClient().getLeaderAndIsr(tableBucket);
                    if (role.isPresent() && role.get().leader() != originalLeader) {
                        return Optional.of(role.get().leader());
                    }
                    return Optional.empty();
                },
                E2E_TIMEOUT,
                "replacement leader after the original leader is permanently stopped");
    }

    private static void awaitPublicRow(
            Table table, RowType rowType, Object[] expected, String description) {
        InternalRow lookupKey =
                expected.length == 2
                        ? row((Integer) expected[0])
                        : row((Integer) expected[0], expected[2]);
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow actual =
                waitValue(
                        () -> {
                            try {
                                return Optional.ofNullable(
                                        lookuper.lookup(lookupKey)
                                                .get(5, TimeUnit.SECONDS)
                                                .getSingletonRow());
                            } catch (Exception failure) {
                                rethrowUnlessTransient(failure);
                                return Optional.empty();
                            }
                        },
                        E2E_TIMEOUT,
                        "public lookup: " + description);
        assertThatRow(actual).withSchema(rowType).isEqualTo(row(rowType, expected));
    }

    private static void awaitPublicBucketReadiness(Table table, List<Object[]> probes) {
        Lookuper lookuper = table.newLookup().createLookuper();
        for (Object[] probe : probes) {
            retry(
                    E2E_TIMEOUT,
                    () -> {
                        try {
                            assertThat(
                                            lookuper.lookup(row((Integer) probe[0]))
                                                    .get(5, TimeUnit.SECONDS)
                                                    .getSingletonRow())
                                    .as("public lookup routing for key %s", probe[0])
                                    .isNull();
                        } catch (Exception failure) {
                            rethrowUnlessTransient(failure);
                            throw new AssertionError("Public access has not converged", failure);
                        }
                    });
        }
    }

    private static void verifyPublicRows(Table table, RowType rowType, List<Object[]> expectedRows)
            throws Exception {
        Lookuper lookuper = table.newLookup().createLookuper();
        List<CompletableFuture<LookupResult>> lookupFutures = new ArrayList<>(expectedRows.size());
        for (Object[] expected : expectedRows) {
            lookupFutures.add(lookuper.lookup(row((Integer) expected[0])));
        }
        CompletableFuture<LookupResult> absentLookup = lookuper.lookup(row(Integer.MAX_VALUE));
        lookupFutures.add(absentLookup);
        await(FutureUtils.completeAll(lookupFutures));

        for (int rowIndex = 0; rowIndex < expectedRows.size(); rowIndex++) {
            Object[] expected = expectedRows.get(rowIndex);
            int key = (Integer) expected[0];
            InternalRow actual = lookupFutures.get(rowIndex).get().getSingletonRow();
            assertThatRow(actual)
                    .as("public lookup result for key %s", key)
                    .withSchema(rowType)
                    .isEqualTo(row(rowType, Arrays.copyOf(expected, expected.length)));
        }

        assertThat(absentLookup.get().getSingletonRow())
                .as("public lookup result for absent key %s", Integer.MAX_VALUE)
                .isNull();

        awaitExactPublicBatchScan(table, expectedRows);
    }

    private static Map<Integer, Long> offsetsAfter(Map<Integer, Long> offsets, long increment) {
        Map<Integer, Long> result = new LinkedHashMap<>(offsets);
        result.replaceAll((bucket, offset) -> offset + increment);
        return result;
    }

    private static Map<Integer, List<Object[]>> groupByBucket(
            TableInfo tableInfo, List<Object[]> rows) {
        Map<Integer, List<Object[]>> result = new LinkedHashMap<>();
        for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
            result.put(bucket, new ArrayList<>());
        }
        for (Object[] row : rows) {
            result.get(bucketOf(tableInfo, row)).add(Arrays.copyOf(row, row.length));
        }
        return result;
    }

    private static Map<Integer, Long> logEndOffsets(
            Map<Integer, List<Object[]>> rows, long baseOffset) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        rows.forEach((bucket, values) -> result.put(bucket, baseOffset + values.size()));
        return result;
    }

    private static void verifyEmptyPublicTarget(Admin admin, Table table, TablePath tablePath)
            throws Exception {
        awaitExactPublicBatchScan(table, Collections.<Object[]>emptyList());
        Map<Integer, Long> expectedEndOffsets = new LinkedHashMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            expectedEndOffsets.put(bucket, 0L);
        }
        assertThat(
                        await(
                                admin.listOffsets(
                                                tablePath,
                                                new ArrayList<>(expectedEndOffsets.keySet()),
                                                new OffsetSpec.LatestSpec())
                                        .all()))
                .containsExactlyInAnyOrderEntriesOf(expectedEndOffsets);
    }

    private static void verifySingleOrdinaryLogAfterAbort(
            Admin admin, Table table, TablePath tablePath, Object[] ordinaryRow) throws Exception {
        Map<Integer, List<Object[]>> rows =
                groupByBucket(table.getTableInfo(), Collections.singletonList(ordinaryRow));
        assertExactPublicLogs(
                admin,
                table,
                PhysicalTablePath.of(tablePath),
                rows,
                logEndOffsets(rows, 0L),
                ChangeType.INSERT);
    }

    private static List<Object[]> toLogicalRows(List<InternalRow> rows) {
        List<Object[]> logicalRows = new ArrayList<>(rows.size());
        for (InternalRow row : rows) {
            logicalRows.add(toLogicalRow(row));
        }
        return logicalRows;
    }

    private static Object[] toLogicalRow(InternalRow row) {
        if (row.getFieldCount() == 2) {
            return new Object[] {
                row.getInt(0), row.isNullAt(1) ? null : row.getString(1).toString()
            };
        }
        return new Object[] {
            row.getInt(0),
            row.isNullAt(1) ? null : row.getString(1).toString(),
            row.isNullAt(2) ? null : row.getString(2).toString()
        };
    }

    private static final class BucketRows {
        private final TableInfo tableInfo;
        private int nextId;

        private BucketRows(TableInfo tableInfo, int firstId) {
            this.tableInfo = tableInfo;
            this.nextId = firstId;
        }

        private Object[] row(int targetBucket, String payloadPrefix, String... partition) {
            for (int attempts = 0; attempts < 100_000; attempts++) {
                int candidate = nextId++;
                String payload = payloadPrefix + '-' + candidate;
                Object[] values =
                        partition.length == 0
                                ? new Object[] {candidate, payload}
                                : new Object[] {candidate, payload, partition[0]};
                if (bucketOf(tableInfo, values) == targetBucket) {
                    return values;
                }
            }
            throw new AssertionError("Unable to find deterministic row for bucket " + targetBucket);
        }

        private List<Object[]> partitionRows(int targetBucket, int count, String partition) {
            List<Object[]> rows = new ArrayList<>(count);
            while (rows.size() < count) {
                int candidate = nextId++;
                String payload;
                if (candidate % 101 == 0) {
                    payload = null;
                } else {
                    StringBuilder value =
                            new StringBuilder(candidate % 97 == 0 ? "生产数据-" : "target-");
                    int suffixLength = candidate % 257;
                    for (int index = 0; index < suffixLength; index++) {
                        value.append((char) ('a' + index % 26));
                    }
                    payload = value.append('-').append(candidate).toString();
                }
                Object[] values = new Object[] {candidate, payload, partition};
                if (bucketOf(tableInfo, values) == targetBucket) {
                    rows.add(values);
                }
            }
            return rows;
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static CompletableFuture<Void> publicOperation(CheckedRunnable operation) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        operation.run();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private static Configuration featureClusterConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        configuration.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofHours(1));
        configuration.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        configuration.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        configuration.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        return configuration;
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static CompletableFuture<PbBulkLoadTargetInfo> begin(
            FlussAdmin admin, PhysicalTablePath target) {
        return admin.getAdminGateway()
                .beginBulkLoad(new BeginBulkLoadRequest().setTarget(fromPhysicalTablePath(target)))
                .thenApply(response -> response.getTargetInfo());
    }

    private static CompletableFuture<PbBulkLoadStatus> abort(
            FlussAdmin admin, PbBulkLoadHandle handle) {
        return admin.getAdminGateway()
                .abortBulkLoad(new AbortBulkLoadRequest().setHandle(handle))
                .thenApply(response -> assertValidStatus(response.getStatus()));
    }

    private static CompletableFuture<Optional<PbBulkLoadStatus>> getInProgress(
            FlussAdmin admin, PhysicalTablePath target) {
        return admin.getAdminGateway()
                .getInProgressBulkLoad(
                        new GetInProgressBulkLoadRequest().setTarget(fromPhysicalTablePath(target)))
                .thenApply(
                        response ->
                                response.hasStatus()
                                        ? Optional.of(assertValidStatus(response.getStatus()))
                                        : Optional.empty());
    }

    private static CompletableFuture<PbBulkLoadStatus> commit(
            FlussAdmin admin, CommitBulkLoadRequest input) {
        return admin.getAdminGateway()
                .commitBulkLoad(input)
                .thenApply(response -> assertValidStatus(response.getStatus()));
    }

    private static PbBulkLoadStatus assertValidStatus(PbBulkLoadStatus status) {
        boolean aborted = BulkLoadState.fromCode(status.getState()) == BulkLoadState.ABORTED;
        assertThat(status.hasAbortReason()).isEqualTo(aborted);
        if (aborted) {
            assertThat(BulkLoadAbortReason.fromCode(status.getAbortReason())).isNotNull();
        } else {
            assertThat(status.hasAbortMessage()).isFalse();
        }
        return status;
    }

    private static PbBulkLoadStatus commitUntilReady(FlussAdmin admin, CommitBulkLoadRequest input)
            throws Exception {
        long deadline = System.nanoTime() + E2E_TIMEOUT.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException("Commit was not confirmed within " + E2E_TIMEOUT);
            }
            try {
                return commit(admin, input).get(remaining, TimeUnit.NANOSECONDS);
            } catch (ExecutionException failure) {
                if (!(rootCause(failure) instanceof RetriableException)) {
                    throw failure;
                }
                Thread.sleep(200);
            }
        }
    }

    private static CommitBulkLoadRequest buildInput(
            PbBulkLoadTargetInfo targetInfo, List<Object[]> logicalRows) throws Exception {
        return new TestingBulkLoadInputBuilder().build(targetInfo, logicalRows);
    }

    private static void awaitLoadingAccessRejection(
            Supplier<? extends CompletableFuture<?>> publicAccess,
            Duration timeout,
            String description) {
        waitUntil(
                () -> {
                    try {
                        publicAccess.get().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                        throw new AssertionError("LOADING access was allowed: " + description);
                    } catch (ExecutionException | CompletionException failure) {
                        if (isLoadingAccessRejection(rootCause(failure))) {
                            return true;
                        }
                        rethrowUnlessTransient(failure);
                        return false;
                    }
                },
                timeout,
                "LOADING access rejection for " + description);
    }

    private static boolean isLoadingAccessRejection(Throwable failure) {
        return failure instanceof ApiException
                && failure.getMessage() != null
                && failure.getMessage()
                        .contains("BulkLoad target is LOADING and rejects external access");
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static void rethrowUnlessTransient(Throwable failure) {
        Throwable cause = rootCause(failure);
        if (!(cause instanceof TimeoutException) && !(cause instanceof RetriableException)) {
            ExceptionUtils.rethrow(cause);
        }
    }
}
