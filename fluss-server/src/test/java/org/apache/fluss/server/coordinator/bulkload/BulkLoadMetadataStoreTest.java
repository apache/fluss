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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaRound;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadMaintenanceEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hashing;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.apache.fluss.server.testutils.KvTestUtils.writeSnapshotMetadata;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable checked-multi tests for ordinary metadata adoption and two-step Abort. */
class BulkLoadMetadataStoreTest {

    private static final String ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final String SNAPSHOT_SHA =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final AtomicLong NEXT_TABLE_ID = new AtomicLong(9000L);

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient zkClient;
    private BulkLoadMetadataStore store;
    private Configuration configuration;
    private String remoteDataDir;
    private LakeCatalogDynamicLoader catalogLoader;
    private TestCoordinatorChannelManager channelManager;
    private MetadataManager metadataManager;
    private CoordinatorContext context;
    private final TestingEventManager eventManager = new TestingEventManager();
    private final ManuallyTriggeredScheduledExecutorService ioExecutor =
            new ManuallyTriggeredScheduledExecutorService();

    @TempDir private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        zkClient = ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
        store = new BulkLoadMetadataStore(zkClient);
        remoteDataDir = tempDir.toUri().toString();
        configuration = new Configuration();
        configuration.setString(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        catalogLoader = new LakeCatalogDynamicLoader(configuration, null, true);
        channelManager = new TestCoordinatorChannelManager();
        metadataManager = new MetadataManager(zkClient, configuration, catalogLoader);
        context = new CoordinatorContext(zkClient.fenceBecomeCoordinatorLeader("bulkload-test"));
        context.setCoordinatorServerInfo(
                new ServerInfo(
                        0,
                        null,
                        Collections.singletonList(new Endpoint("localhost", 10000, "INTERNAL")),
                        ServerType.COORDINATOR));
        zkClient.ensureBulkLoadMetadataPaths();
        ensureServerIdsPath();
    }

    @AfterEach
    void tearDown() throws Exception {
        channelManager.close();
        catalogLoader.close();
        zkClient.close();
    }

    @Test
    void testPartitionDeletionCannotOvertakeBulkLoadBegin() throws Exception {
        TablePath tablePath = TablePath.of("bulkload_store", "partition_race");
        ResolvedPartitionSpec partition = ResolvedPartitionSpec.fromPartitionValue("part", "p0");
        String path = ZkData.PartitionZNode.path(tablePath, partition.getPartitionName());
        PartitionRegistration registration = new PartitionRegistration(41L, 71L, remoteDataDir, 1);
        create(path, ZkData.PartitionZNode.encode(registration));
        PartitionRegistration loading = registration.withDataState(BulkLoadDataState.LOADING, ID);
        ZooKeeperClient racingClient =
                new ZooKeeperClient(
                        new CuratorFrameworkWithUnhandledErrorListener(
                                zkClient.getCuratorClient(), (message, failure) -> {}),
                        configuration) {
                    @Override
                    public void deletePartition(TablePath table, String name, int expectedVersion)
                            throws Exception {
                        getCuratorClient()
                                .setData()
                                .forPath(path, ZkData.PartitionZNode.encode(loading));
                        super.deletePartition(table, name, expectedVersion);
                    }
                };
        MetadataManager racingManager =
                new MetadataManager(racingClient, configuration, catalogLoader);

        assertThatThrownBy(() -> racingManager.dropPartition(tablePath, partition, false))
                .hasCauseInstanceOf(KeeperException.BadVersionException.class);
        assertThat(zkClient.getPartition(tablePath, partition.getPartitionName()))
                .contains(loading);
    }

    @Test
    void testAdoptsOrdinaryMetadataIdempotentlyAndRejectsConflict() throws Exception {
        Fixture fixture = fixture(BulkLoadState.COMMITTING, new long[] {7L});
        BucketSnapshot snapshot = new BucketSnapshot(7L, 17L, fixture.snapshotPath);

        store.adoptBucketMetadata(
                fixture.transaction(),
                fixture.registration(),
                0,
                snapshot,
                fixture.coordinatorEpochVersion);
        store.adoptBucketMetadata(
                fixture.transaction(),
                fixture.registration(),
                0,
                snapshot,
                fixture.coordinatorEpochVersion);

        assertThat(zkClient.getTableBucketSnapshot(fixture.tableBucket, 7L)).contains(snapshot);

        String snapshotPath = ZkData.BucketSnapshotIdZNode.path(fixture.tableBucket, 7L);
        zkClient.getCuratorClient()
                .setData()
                .forPath(
                        snapshotPath,
                        ZkData.BucketSnapshotIdZNode.encode(
                                new BucketSnapshot(7L, 18L, fixture.snapshotPath)));
        assertThatThrownBy(
                        () ->
                                store.adoptBucketMetadata(
                                        fixture.transaction(),
                                        fixture.registration(),
                                        0,
                                        snapshot,
                                        fixture.coordinatorEpochVersion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting ordinary Snapshot");
    }

    @Test
    void testCommitReleasesTargetOwnership() throws Exception {
        Fixture fixture = fixture(BulkLoadState.COMMITTING, new long[] {7L});
        store.adoptBucketMetadata(
                fixture.transaction(),
                fixture.registration(),
                0,
                new BucketSnapshot(7L, 17L, fixture.snapshotPath),
                fixture.coordinatorEpochVersion);

        Versioned<BulkLoadTransaction> published =
                fixture.update(
                        value -> value.activated(value.getMetadataVersion() + 1, null, null));
        assertThat(published.getValue().getState()).isEqualTo(BulkLoadState.COMMITTING);
        assertThat(fixture.registration().getValue().bulkLoadId).isEqualTo(ID);

        Versioned<BulkLoadTransaction> committed =
                fixture.update(value -> value.released(value.getMetadataVersion() + 1, 1202L));
        assertThat(committed.getValue().getState()).isEqualTo(BulkLoadState.COMMITTED);
        assertThat(fixture.registration().getValue().bulkLoadId).isNull();

        assertThat(fixture.transaction().getValue()).isEqualTo(committed.getValue());
    }

    @Test
    void testStaleAssignmentRejectsFenceReadyCas() throws Exception {
        Fixture fixture = fixture(BulkLoadState.BEGUN, null);
        Versioned<TableAssignment> staleAssignment = fixture.assignment();
        zkClient.getCuratorClient()
                .setData()
                .forPath(
                        staleAssignment.getPath(),
                        ZkData.TableIdZNode.encode(staleAssignment.getValue()));
        assertThatThrownBy(
                        () ->
                                store.update(
                                        fixture.transaction(),
                                        fixture.transaction()
                                                .getValue()
                                                .withSnapshotIds(new long[] {7L}),
                                        fixture.registration(),
                                        new ReplicaRound(
                                                staleAssignment,
                                                Collections.emptyList(),
                                                Collections.emptyMap()),
                                        fixture.coordinatorEpochVersion))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(fixture.transaction().getValue().isFenceReady()).isFalse();
    }

    @Test
    void testAbortActivationRetainsOwnershipUntilTerminalCas() throws Exception {
        Fixture fixture = fixture(BulkLoadState.BEGUN, new long[] {7L});
        fixture.update(
                value ->
                        value.activated(
                                value.getMetadataVersion() + 1,
                                BulkLoadAbortReason.TARGET_NOT_EMPTY,
                                "Target contains local data."));

        Versioned<BulkLoadTransaction> activeTransaction = fixture.transaction();
        assertThat(activeTransaction.getValue().getState()).isEqualTo(BulkLoadState.BEGUN);
        assertThat(activeTransaction.getValue().getAbortReason())
                .isEqualTo(BulkLoadAbortReason.TARGET_NOT_EMPTY);
        assertThat(activeTransaction.getValue().getAbortMessage())
                .isEqualTo("Target contains local data.");
        assertThat(fixture.registration().getValue().dataState).isEqualTo(BulkLoadDataState.ACTIVE);
        assertThat(fixture.registration().getValue().bulkLoadId).isEqualTo(ID);

        Versioned<BulkLoadTransaction> terminal =
                fixture.update(value -> value.released(value.getMetadataVersion() + 1, 1202L));

        assertThat(terminal.getValue().getState()).isEqualTo(BulkLoadState.ABORTED);
        assertThat(terminal.getValue().getAbortReason())
                .isEqualTo(BulkLoadAbortReason.TARGET_NOT_EMPTY);
        assertThat(terminal.getValue().getAbortMessage()).isEqualTo("Target contains local data.");
        assertThat(fixture.registration().getValue().bulkLoadId).isNull();
    }

    @Test
    void testTargetNotEmptyAbortIgnoresReRegisteredHolder() throws Exception {
        Fixture fixture = fixture(BulkLoadState.BEGUN, new long[] {7L});
        TabletServerRegistration holderRegistration =
                new TabletServerRegistration(
                        "rack1", Endpoint.fromListenersString("CLIENT://host1:1234"), 100L);
        ZooKeeperClient originalHolder =
                ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
        ZooKeeperClient replacementHolder = null;
        try {
            assertThat(originalHolder.getCuratorClient().blockUntilConnected(30, TimeUnit.SECONDS))
                    .isTrue();
            originalHolder.registerTabletServer(1, holderRegistration);
            Versioned<TabletServerRegistration> observedHolder =
                    observed(ZkData.ServerIdZNode.path(1), ZkData.ServerIdZNode::decode);

            originalHolder.close();
            waitUntil(
                    () -> !zkClient.pathExists(observedHolder.getPath()),
                    Duration.ofSeconds(30),
                    "Original TabletServer registration was not removed.");
            replacementHolder =
                    ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
            assertThat(
                            replacementHolder
                                    .getCuratorClient()
                                    .blockUntilConnected(30, TimeUnit.SECONDS))
                    .isTrue();
            replacementHolder.registerTabletServer(1, holderRegistration);
            Versioned<TabletServerRegistration> replacementRegistration =
                    observed(ZkData.ServerIdZNode.path(1), ZkData.ServerIdZNode::decode);
            assertThat(replacementRegistration.getEphemeralOwner())
                    .isNotEqualTo(observedHolder.getEphemeralOwner());

            store.update(
                    fixture.transaction(),
                    fixture.transaction()
                            .getValue()
                            .activated(
                                    fixture.registration().getVersion() + 1,
                                    BulkLoadAbortReason.TARGET_NOT_EMPTY,
                                    "Target contains local data."),
                    fixture.registration(),
                    null,
                    fixture.coordinatorEpochVersion);

            Versioned<BulkLoadTransaction> aborting = fixture.transaction();
            assertThat(aborting.getValue().getState()).isEqualTo(BulkLoadState.BEGUN);
            assertThat(aborting.getValue().getAbortReason())
                    .isEqualTo(BulkLoadAbortReason.TARGET_NOT_EMPTY);
        } finally {
            if (replacementHolder != null) {
                replacementHolder.close();
            }
            originalHolder.close();
        }
    }

    @Test
    void testAbortWinsWhileFrozenInputValidationIsInFlight() throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        Fixture fixture = fixture(BulkLoadState.BEGUN, new long[] {7L}, deadline);
        BulkLoadHandle handle = fixture.handle;
        String inputSha256 = writeSnapshots(remoteDataDir, handle, fixture.tableBucket, 7L);
        fixture.update(value -> value.withInput(inputSha256, deadline));
        BulkLoadManager manager = manager(zkClient);

        CompletableFuture<CommitBulkLoadResponse> commitFuture =
                manager.commit(handle, inputSha256);
        assertThat(ioExecutor.numQueuedRunnables()).isEqualTo(1);

        CompletableFuture<AbortBulkLoadResponse> abortFuture = manager.abort(handle);
        ioExecutor.trigger();
        ((AccessContextEvent<?>) eventManager.getEvents().get(0))
                .getAccessFunction()
                .apply(context);

        Versioned<BulkLoadTransaction> aborting = fixture.transaction();
        assertThat(aborting.getValue().getState()).isEqualTo(BulkLoadState.BEGUN);
        assertThat(aborting.getValue().getAbortReason())
                .isEqualTo(BulkLoadAbortReason.ABORTED_BY_CALLER);

        ((AccessContextEvent<?>) eventManager.getEvents().get(1))
                .getAccessFunction()
                .apply(context);
        assertThat(abortFuture.get(10, TimeUnit.SECONDS).getStatus().getState())
                .isEqualTo(BulkLoadState.ABORTED.getCode());
        assertThat(commitFuture).isCompletedExceptionally();
    }

    @Test
    void testUncertainCheckedMultiOutcomesReconcileActiveInventory() throws Exception {
        configuration.set(ConfigOptions.BULKLOAD_MAX_ACTIVE_TRANSACTIONS, 1);
        int serverId = Math.toIntExact(NEXT_TABLE_ID.getAndIncrement());
        String coordinatorId = "bulkload-uncertain-" + serverId;
        zkClient.registerCoordinatorServer(
                new CoordinatorAddress(
                        coordinatorId,
                        Collections.singletonList(
                                new Endpoint("localhost", 10000 + serverId, "INTERNAL")),
                        ServerProtocolVersion.CURRENT));
        zkClient.registerTabletServer(
                serverId,
                new TabletServerRegistration(
                        "rack",
                        Collections.singletonList(
                                new Endpoint("localhost", 20000 + serverId, "INTERNAL")),
                        1L,
                        TabletServerResource.unknown(),
                        ServerProtocolVersion.CURRENT));

        String database = "bulkload_uncertain_" + serverId;
        TablePath firstPath = TablePath.of(database, "first");
        TablePath secondPath = TablePath.of(database, "second");
        long firstTableId = createTable(firstPath, serverId).getTableId();
        createTable(secondPath, serverId);

        UncertainMultiZooKeeperClient uncertainZkClient =
                new UncertainMultiZooKeeperClient(zkClient, configuration);
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        gateways.put(
                serverId,
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<UpdateMetadataResponse> updateMetadata(
                            UpdateMetadataRequest request) {
                        return CompletableFuture.completedFuture(new UpdateMetadataResponse());
                    }
                });
        channelManager.setGateways(gateways);
        ServerInfo liveServer =
                new ServerInfo(
                        serverId,
                        "rack",
                        Collections.singletonList(
                                new Endpoint("localhost", 20000 + serverId, "INTERNAL")),
                        ServerType.TABLET_SERVER);
        context.setLiveTabletServers(Collections.singletonList(liveServer));
        BulkLoadManager manager = manager(uncertainZkClient);
        manager.bindTabletServerGateway(liveServer);

        uncertainZkClient.failNextCheckedMultiAndReadAfterCommit();
        CompletableFuture<BeginBulkLoadResponse> uncertainBegin = begin(manager, firstPath);
        assertThat(uncertainBegin).isCompletedExceptionally();
        Versioned<TableRegistration> firstOwned =
                observed(ZkData.TableZNode.path(firstPath), ZkData.TableZNode::decode);
        assertThat(firstOwned.getValue().dataState).isEqualTo(BulkLoadDataState.LOADING);
        assertThat(firstOwned.getValue().bulkLoadId).isNotNull();

        begin(manager, secondPath);
        Versioned<TableRegistration> blocked =
                observed(ZkData.TableZNode.path(secondPath), ZkData.TableZNode::decode);
        assertThat(blocked.getValue().dataState).isEqualTo(BulkLoadDataState.ACTIVE);
        assertThat(blocked.getValue().bulkLoadId).isNull();

        BulkLoadHandle firstHandle =
                new BulkLoadHandle(
                        PhysicalTablePath.of(firstPath),
                        firstTableId,
                        null,
                        firstOwned.getValue().bulkLoadId);
        CompletableFuture<AbortBulkLoadResponse> abortResult = manager.abort(firstHandle);
        uncertainZkClient.failNextCheckedMultiAfterCommit();
        ((AccessContextEvent<?>) eventManager.getEvents().get(0))
                .getAccessFunction()
                .apply(context);
        assertThat(abortResult).isCompletedExceptionally();
        manager.process(new BulkLoadMaintenanceEvent());
        assertThat(manager.abort(firstHandle).get(10, TimeUnit.SECONDS).getStatus().getState())
                .isEqualTo(BulkLoadState.ABORTED.getCode());
        assertThat(
                        observed(
                                        BulkLoadMetadataStore.transactionPath(firstHandle),
                                        ZkData.BulkLoadTransactionsZNode::decode)
                                .getValue()
                                .getState())
                .isEqualTo(BulkLoadState.ABORTED);
        assertThat(
                        observed(ZkData.TableZNode.path(firstPath), ZkData.TableZNode::decode)
                                .getValue()
                                .bulkLoadId)
                .isNull();

        manager.process(new BulkLoadMaintenanceEvent());
        begin(manager, secondPath);
        Versioned<TableRegistration> admitted =
                observed(ZkData.TableZNode.path(secondPath), ZkData.TableZNode::decode);
        assertThat(admitted.getValue().dataState).isEqualTo(BulkLoadDataState.LOADING);
        assertThat(admitted.getValue().bulkLoadId).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMaintenanceContinuesPastInconsistentOwnedTransaction(boolean inputFrozen)
            throws Exception {
        Fixture damaged = fixture(BulkLoadState.BEGUN, null, Long.MAX_VALUE);
        Fixture healthy = fixture(BulkLoadState.BEGUN, new long[] {7L}, 0L);
        if (inputFrozen) {
            healthy.update(value -> value.withInput(SNAPSHOT_SHA, Long.MAX_VALUE));
            assertThat(BulkLoadManager.deadlineReason(healthy.transaction().getValue(), 0L))
                    .isNull();
            healthy.update(value -> value.withInput(SNAPSHOT_SHA, 0L));
        }
        BulkLoadManager manager = manager(zkClient);
        manager.restoreTransactions(Arrays.asList(damaged.handle, healthy.handle));
        zkClient.getCuratorClient()
                .setData()
                .forPath(
                        damaged.registrationPath,
                        ZkData.TableZNode.encode(damaged.registration().getValue()));

        manager.process(new BulkLoadMaintenanceEvent());

        assertThat(healthy.transaction().getValue().getAbortReason())
                .isEqualTo(
                        inputFrozen
                                ? BulkLoadAbortReason.COMMIT_DECISION_DEADLINE_EXCEEDED
                                : BulkLoadAbortReason.BUILD_DEADLINE_EXCEEDED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRetainedTerminalResultDoesNotRequireAvailableReplicas(boolean committed)
            throws Exception {
        Fixture fixture =
                fixture(
                        committed ? BulkLoadState.COMMITTING : BulkLoadState.BEGUN,
                        new long[] {7L});
        fixture.update(
                value ->
                        value.activated(
                                value.getMetadataVersion() + 1,
                                committed ? null : BulkLoadAbortReason.ABORTED_BY_CALLER,
                                null));
        BulkLoadTransaction terminal =
                fixture.update(
                                value ->
                                        value.released(
                                                value.getMetadataVersion() + 1,
                                                System.currentTimeMillis()
                                                        + Duration.ofDays(1).toMillis()))
                        .getValue();
        ZooKeeperClient.DataWithStat registration =
                zkClient.getDataWithStat(fixture.registrationPath);
        ZooKeeperClient.DataWithStat assignment = zkClient.getDataWithStat(fixture.assignmentPath);
        context =
                new CoordinatorContext(
                        zkClient.fenceBecomeCoordinatorLeader("terminal-result-test"));
        BulkLoadManager manager = manager(zkClient);
        CompletableFuture<PbBulkLoadStatus> result;
        if (committed) {
            CompletableFuture<CommitBulkLoadResponse> response =
                    manager.commit(fixture.handle, null);
            result = response.thenApply(CommitBulkLoadResponse::getStatus);
        } else {
            CompletableFuture<AbortBulkLoadResponse> response = manager.abort(fixture.handle);
            result = response.thenApply(AbortBulkLoadResponse::getStatus);
        }
        PbBulkLoadStatus status = result.get(10, TimeUnit.SECONDS);
        assertThat(status.getState()).isEqualTo(terminal.getState().getCode());
        assertThat(status.getHandle().getBulkLoadId()).isEqualTo(fixture.handle.getBulkLoadId());
        assertThat(zkClient.getDataWithStat(fixture.registrationPath).getData())
                .isEqualTo(registration.getData());
        assertThat(zkClient.getDataWithStat(fixture.assignmentPath).getData())
                .isEqualTo(assignment.getData());
    }

    private BulkLoadManager manager(ZooKeeperClient client) {
        return new BulkLoadManager(
                client,
                configuration,
                context,
                new MetadataManager(client, configuration, catalogLoader),
                channelManager,
                ioExecutor,
                eventManager,
                new CompletedSnapshotStoreManager(
                        1,
                        ioExecutor,
                        client,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        bucket -> false));
    }

    private TableInfo createTable(TablePath path, int serverId) throws Exception {
        metadataManager.createDatabase(
                path.getDatabaseName(), DatabaseDescriptor.builder().build(), true);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .build()
                        .withReplicationFactor(1);
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(serverId)).build();
        long tableId =
                metadataManager.createTable(path, remoteDataDir, descriptor, assignment, false);
        TableInfo info = metadataManager.getTable(path);
        TableBucket bucket = new TableBucket(tableId, 0);
        context.putTableInfo(info);
        context.putTablePath(tableId, path);
        context.updateBucketReplicaAssignment(
                bucket, assignment.getBucketAssignment(0).getReplicas());
        context.putBucketLeaderAndIsr(
                bucket, new LeaderAndIsr(serverId, context.getCoordinatorEpoch()));
        return info;
    }

    private Fixture fixture(BulkLoadState state, long[] snapshotIds) throws Exception {
        return fixture(state, snapshotIds, 300L);
    }

    private Fixture fixture(BulkLoadState state, long[] snapshotIds, long buildDeadlineMs)
            throws Exception {
        TableInfo info =
                createTable(
                        TablePath.of("bulkload_store", "table_" + NEXT_TABLE_ID.getAndIncrement()),
                        1);
        Fixture fixture = new Fixture(info);
        Versioned<TableRegistration> registration = fixture.registration();
        store.createTransactionAndFence(
                registration,
                fixture.assignment(),
                new BulkLoadTransaction(
                        fixture.handle,
                        state,
                        "alice",
                        "USER",
                        remoteDataDir,
                        info.getSchemaId(),
                        registration.getVersion() + 1,
                        snapshotIds,
                        state == BulkLoadState.BEGUN ? buildDeadlineMs : 400L,
                        state == BulkLoadState.BEGUN ? null : SNAPSHOT_SHA,
                        null,
                        null),
                fixture.coordinatorEpochVersion);
        create(ZkData.BucketIdZNode.path(fixture.tableBucket), new byte[0]);
        return fixture;
    }

    private void create(String path, byte[] data) throws Exception {
        zkClient.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }

    private void ensureServerIdsPath() throws Exception {
        try {
            create(ZkData.ServerIdsZNode.path(), new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // The extension is shared by concurrently executing test methods.
        }
    }

    private static CompletableFuture<BeginBulkLoadResponse> begin(
            BulkLoadManager manager, TablePath tablePath) {
        return manager.begin(
                PhysicalTablePath.of(tablePath), null, new FlussPrincipal("alice", "USER"));
    }

    private <T> Versioned<T> observed(String path, Function<byte[], T> decoder) throws Exception {
        ZooKeeperClient.DataWithStat data = zkClient.getDataWithStat(path);
        return new Versioned<>(
                decoder.apply(data.getData()),
                path,
                data.getStat().getVersion(),
                data.getStat().getEphemeralOwner());
    }

    private static String writeSnapshots(
            String remoteDataDir, BulkLoadHandle handle, TableBucket tableBucket, long snapshotId)
            throws Exception {
        FsPath metadataPath =
                writeSnapshotMetadata(remoteDataDir, handle, tableBucket, snapshotId, 0L, 0L);
        byte[] metadataBytes = Files.readAllBytes(java.nio.file.Paths.get(metadataPath.toUri()));
        return Hashing.sha256()
                .hashBytes(Hashing.sha256().hashBytes(metadataBytes).asBytes())
                .toString();
    }

    private final class Fixture {
        private final int coordinatorEpochVersion;
        private final BulkLoadHandle handle;
        private final TableBucket tableBucket;
        private final String registrationPath;
        private final String assignmentPath;
        private final String transactionPath;
        private final String snapshotPath;

        private Fixture(TableInfo tableInfo) {
            this.coordinatorEpochVersion = context.getCoordinatorZkVersion();
            this.handle =
                    new BulkLoadHandle(
                            PhysicalTablePath.of(tableInfo.getTablePath()),
                            tableInfo.getTableId(),
                            null,
                            ID);
            this.tableBucket = new TableBucket(handle.getTableId(), 0);
            this.registrationPath = ZkData.TableZNode.path(tableInfo.getTablePath());
            this.assignmentPath = ZkData.TableIdZNode.path(handle.getTableId());
            this.transactionPath = BulkLoadMetadataStore.transactionPath(handle);
            this.snapshotPath =
                    new FsPath(
                                    FlussPaths.remoteKvSnapshotDir(
                                            FlussPaths.remoteKvTabletDir(
                                                    new FsPath(
                                                            remoteDataDir,
                                                            FlussPaths.REMOTE_KV_DIR_NAME),
                                                    handle.getTarget(),
                                                    tableBucket),
                                            7L),
                                    "_METADATA")
                            .toString();
        }

        private Versioned<BulkLoadTransaction> update(UnaryOperator<BulkLoadTransaction> transition)
                throws Exception {
            Versioned<BulkLoadTransaction> current = transaction();
            return store.update(
                    current,
                    transition.apply(current.getValue()),
                    registration(),
                    new ReplicaRound(assignment(), Collections.emptyList(), Collections.emptyMap()),
                    coordinatorEpochVersion);
        }

        private Versioned<TableRegistration> registration() throws Exception {
            return observed(registrationPath, ZkData.TableZNode::decode);
        }

        private Versioned<TableAssignment> assignment() throws Exception {
            return observed(assignmentPath, ZkData.TableIdZNode::decode);
        }

        private Versioned<BulkLoadTransaction> transaction() throws Exception {
            return observed(transactionPath, ZkData.BulkLoadTransactionsZNode::decode);
        }
    }

    private static final class UncertainMultiZooKeeperClient extends ZooKeeperClient {
        private boolean failNextCheckedMulti;
        private boolean failNextRead;
        private boolean failReadAfterCheckedMulti;

        private UncertainMultiZooKeeperClient(
                ZooKeeperClient connectedClient, Configuration configuration) {
            super(
                    new CuratorFrameworkWithUnhandledErrorListener(
                            connectedClient.getCuratorClient(), (message, failure) -> {}),
                    configuration);
        }

        private void failNextCheckedMultiAfterCommit() {
            failNextCheckedMulti = true;
        }

        private void failNextCheckedMultiAndReadAfterCommit() {
            failNextCheckedMulti = true;
            failReadAfterCheckedMulti = true;
        }

        @Override
        public Optional<ZooKeeperClient.DataWithStat> getDataWithStatIfExists(String path)
                throws Exception {
            if (failNextRead) {
                failNextRead = false;
                throw new KeeperException.ConnectionLossException();
            }
            return super.getDataWithStatIfExists(path);
        }

        @Override
        public Map<String, Stat> submitCheckedMulti(List<CuratorOp> operations) throws Exception {
            Map<String, Stat> result = super.submitCheckedMulti(operations);
            if (failNextCheckedMulti) {
                failNextCheckedMulti = false;
                failNextRead = failReadAfterCheckedMulti;
                failReadAfterCheckedMulti = false;
                throw new KeeperException.ConnectionLossException();
            }
            return result;
        }
    }
}
