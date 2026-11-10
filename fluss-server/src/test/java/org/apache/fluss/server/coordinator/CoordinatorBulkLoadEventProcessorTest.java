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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadManager;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hashing;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.fluss.server.testutils.KvTestUtils.writeSnapshotMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/** Event and process-capacity ownership across asynchronous BulkLoad admission and cancellation. */
class CoordinatorBulkLoadEventProcessorTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @TempDir private Path directory;
    private ZooKeeperClient client;
    private LakeCatalogDynamicLoader catalogs;
    private TestCoordinatorChannelManager channels;
    private MetadataManager metadata;
    private CoordinatorContext context;
    private Configuration conf;
    private BulkLoadManager manager;
    private final Semaphore capacity = new Semaphore(1);
    private final ManuallyTriggeredScheduledExecutorService executor =
            new ManuallyTriggeredScheduledExecutorService();
    private final TestingEventManager events = new TestingEventManager();
    private TablePath table;

    @BeforeEach
    void setUp() throws Exception {
        client = ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
        context = new CoordinatorContext(client.fenceBecomeCoordinatorLeader("bulkload-events"));
        conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, directory.toUri().toString());
        catalogs = new LakeCatalogDynamicLoader(conf, null, true);
        metadata = new MetadataManager(client, conf, catalogs);
        channels = new TestCoordinatorChannelManager();
        client.ensureBulkLoadMetadataPaths();
        ensure(ZkData.ServerIdsZNode.path());
        ensure(ZkData.CoordinatorIdsZNode.path());
        java.util.List<org.apache.fluss.cluster.Endpoint> endpoints =
                Collections.singletonList(
                        new org.apache.fluss.cluster.Endpoint("localhost", 20000, "INTERNAL"));
        client.registerCoordinatorServer(
                new org.apache.fluss.server.zk.data.CoordinatorAddress(
                        "bulkload-events",
                        endpoints,
                        org.apache.fluss.server.zk.data.ServerProtocolVersion.CURRENT));
        client.registerTabletServer(
                1,
                new org.apache.fluss.server.zk.data.TabletServerRegistration(
                        "rack",
                        endpoints,
                        1L,
                        org.apache.fluss.server.metadata.TabletServerResource.unknown(),
                        org.apache.fluss.server.zk.data.ServerProtocolVersion.CURRENT));

        table =
                TablePath.of(
                        "bulkload_events",
                        "t" + Math.abs(UUID.randomUUID().getLeastSignificantBits()));
        metadata.createDatabase(table.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
        metadata.createTable(
                table,
                directory.toUri().toString(),
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .build()
                        .withReplicationFactor(1),
                TableAssignment.builder().add(0, BucketAssignment.of(1)).build(),
                false);
        manager = newManager();
    }

    @AfterEach
    void tearDown() throws Exception {
        manager.close();
        channels.close();
        catalogs.close();
        client.close();
    }

    @Test
    void testBeginDoesNotWaitForReplicaAcknowledgement() throws Exception {
        CompletableFuture<BeginBulkLoadResponse> result = begin();
        assertThat(result).isNotDone();
        executor.triggerAll();
        drainEvents();
        assertThat(result).isCompleted();
        assertThat(result.get().getTargetInfosCount()).isEqualTo(1);
        assertThat(client.getTable(table).get().bulkLoadId)
                .isEqualTo(result.get().getHandle().getBulkLoadId());
        assertThat(capacity.availablePermits()).isZero();
    }

    @Test
    void testCloseDuringAdmissionRetainsCapacityUntilActualWorkerExit() {
        CompletableFuture<BeginBulkLoadResponse> result = begin();
        manager.close();
        assertThat(result).isCompletedExceptionally();
        assertThat(capacity.availablePermits()).isZero();
        executor.triggerAll();
        assertThat(capacity.availablePermits()).isEqualTo(1);
        assertThat(events.getEvents()).isEmpty();
    }

    @Test
    void testCloseRetainsCapacityUntilOutstandingRestorationResponseExits() throws Exception {
        manager.close();
        channels.close();
        AtomicReference<BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>> response =
                new AtomicReference<>();
        channels =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int server,
                            NotifyLeaderAndIsrRequest request,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> callback) {
                        response.set(callback);
                    }
                };
        manager = newManager();
        CompletableFuture<BeginBulkLoadResponse> begun = begin();
        executor.triggerAll();
        drainEvents();
        BeginBulkLoadResponse target = begun.get();
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        table, target.getHandle().getTableId(), target.getHandle().getBulkLoadId());
        TableBucket bucket = new TableBucket(handle.getTableId(), 0);
        ensure(ZkData.BucketIdZNode.path(bucket));
        client.getCuratorClient()
                .create()
                .forPath(
                        ZkData.LeaderAndIsrZNode.path(bucket),
                        ZkData.LeaderAndIsrZNode.encode(
                                new LeaderAndIsr(1, context.getCoordinatorEpoch())));
        Long rowCount =
                metadata.getTable(table).getTableConfig().getChangelogImage() == ChangelogImage.FULL
                        ? 0L
                        : null;
        FsPath input =
                writeSnapshotMetadata(
                        directory.toUri().toString(),
                        PhysicalTablePath.of(table),
                        bucket,
                        target.getTargetInfoAt(0).getSnapshotIds()[0],
                        0L,
                        rowCount);
        byte[] bucketDigest =
                Hashing.sha256().hashBytes(Files.readAllBytes(Paths.get(input.toUri()))).asBytes();
        CompletableFuture<?> commit =
                manager.commit(handle, Hashing.sha256().hashBytes(bucketDigest).toString());
        try {
            executor.triggerAll();
            drainEvents();
            executor.triggerAll();
            drainEvents();
            assertThat(response.get()).isNotNull();
            assertThat(commit).isNotDone();
            manager.close();
            assertThat(commit).isCompletedExceptionally();
            assertThat(capacity.availablePermits()).isZero();
        } finally {
            manager.close();
            if (response.get() != null) {
                response.get().accept(null, new java.util.concurrent.TimeoutException());
            }
        }
        assertThat(capacity.availablePermits()).isEqualTo(1);
        assertThat(events.getEvents()).isEmpty();
    }

    @Test
    void testSaturatedRequestsFailWithoutQueuingWork() {
        CompletableFuture<BeginBulkLoadResponse> first = begin();
        assertThat(begin()).isCompletedExceptionally();
        BulkLoadHandle unknown = new BulkLoadHandle(table, 99L, UUID.randomUUID().toString());
        assertThat(manager.commit(unknown, null)).isCompletedExceptionally();
        assertThat(executor.numQueuedRunnables()).isEqualTo(1);
        manager.close();
        executor.triggerAll();
        assertThat(first).isCompletedExceptionally();
        assertThat(capacity.availablePermits()).isEqualTo(1);
    }

    @Test
    void testAbortWhileValidationIsQueuedRejectsLateResultAndRetainsCapacity() throws Exception {
        CompletableFuture<BeginBulkLoadResponse> begun = begin();
        executor.triggerAll();
        drainEvents();
        BeginBulkLoadResponse response = begun.get();
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        table,
                        response.getHandle().getTableId(),
                        response.getHandle().getBulkLoadId());
        CompletableFuture<?> commit =
                manager.commit(handle, String.join("", Collections.nCopies(64, "a")));
        CompletableFuture<?> abort = manager.abort(handle);
        assertThat(executor.numQueuedRunnables()).isEqualTo(2);
        // Run validation's worker first, but leave its continuation queued on the event lane.
        executor.trigger();
        executor.trigger();
        assertThat(capacity.availablePermits()).isZero();
        drainEvents();
        assertThat(abort).isCompleted();
        assertThat(commit).isCompletedExceptionally();
        assertThat(client.getTable(table).get().bulkLoadId).isNull();
        assertThat(capacity.availablePermits()).isEqualTo(1);
        assertThat(executor.numQueuedRunnables()).isZero();
    }

    @Test
    void testUnknownHandleReleasesItsProvisionalCapacity() {
        BulkLoadHandle unknown = new BulkLoadHandle(table, 99L, UUID.randomUUID().toString());
        CompletableFuture<?> result = manager.commit(unknown, null);
        executor.triggerAll();
        drainEvents();
        assertThat(result).isCompletedExceptionally();
        assertThat(capacity.availablePermits()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCallerAndDeadlineAbortPersistWhileReadOnlyWorkerIsBlocked(boolean expired)
            throws Exception {
        CompletableFuture<BeginBulkLoadResponse> begun = begin();
        executor.triggerAll();
        drainEvents();
        BeginBulkLoadResponse response = begun.get();
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        table,
                        response.getHandle().getTableId(),
                        response.getHandle().getBulkLoadId());
        if (expired) {
            org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction current =
                    BulkLoadManager.readTransaction(client, handle);
            org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction deadline =
                    new org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction(
                            handle,
                            current.getState(),
                            current.getCreatorName(),
                            current.getCreatorType(),
                            current.getTargets(),
                            0L,
                            null,
                            null,
                            null);
            client.getCuratorClient()
                    .setData()
                    .forPath(
                            ZkData.BulkLoadTableTransactionZNode.path(
                                    handle.getTableId(), handle.getBulkLoadId()),
                            ZkData.BulkLoadTransactionsZNode.encode(deadline));
        }
        manager.close();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ZooKeeperClient blocked =
                new ZooKeeperClient(
                        new org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener(
                                client.getCuratorClient(), (message, failure) -> {}),
                        conf) {
                    @Override
                    public java.util.Optional<DataWithStat> getDataWithStatIfExists(String path)
                            throws Exception {
                        if (Thread.currentThread().getName().equals("bulkload-validation")
                                && path.equals(ZkData.TableZNode.path(table))) {
                            entered.countDown();
                            release.await();
                        }
                        return super.getDataWithStatIfExists(path);
                    }
                };
        manager = newManager(blocked);
        manager.restoreTransactions(Collections.singletonList(handle));
        CompletableFuture<?> commit =
                manager.commit(handle, String.join("", Collections.nCopies(64, "a")));
        Thread validation = new Thread(executor::trigger, "bulkload-validation");
        validation.start();
        assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        try {
            CompletableFuture<?> abort = expired ? null : manager.abort(handle);
            if (expired) {
                manager.process(
                        new org.apache.fluss.server.coordinator.event.BulkLoadMaintenanceEvent());
            }
            executor.trigger();
            drainEvents();
            if (abort != null) {
                assertThat(abort).isCompleted();
            }
            assertThat(BulkLoadManager.readTransaction(client, handle).getAbortReason())
                    .isEqualTo(
                            expired
                                    ? org.apache.fluss.metadata.BulkLoadAbortReason
                                            .DEADLINE_EXCEEDED
                                    : org.apache.fluss.metadata.BulkLoadAbortReason
                                            .ABORTED_BY_CALLER);
            assertThat(commit).isCompletedExceptionally();
            assertThat(client.getTable(table).get().bulkLoadId).isNull();
            assertThat(capacity.availablePermits()).isZero();
        } finally {
            release.countDown();
            validation.join(10_000);
        }
        assertThat(validation.isAlive()).isFalse();
        executor.triggerAll();
        drainEvents();
        assertThat(capacity.availablePermits()).isEqualTo(1);
        assertThat(executor.numQueuedRunnables()).isZero();
    }

    @Test
    void testUncertainBeginRetainsRecoveryOwnership() throws Exception {
        manager.close();
        ZooKeeperClient uncertain =
                new ZooKeeperClient(
                        new org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener(
                                client.getCuratorClient(), (message, failure) -> {}),
                        conf) {
                    private boolean loseResponse = true;

                    @Override
                    public java.util.Map<
                                    String,
                                    org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data
                                            .Stat>
                            submitCheckedMulti(
                                    java.util.List<
                                                    org.apache.fluss.shaded.curator5.org.apache
                                                            .curator.framework.api.transaction
                                                            .CuratorOp>
                                            operations)
                                    throws Exception {
                        java.util.Map<
                                        String,
                                        org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data
                                                .Stat>
                                result = super.submitCheckedMulti(operations);
                        if (loseResponse) {
                            loseResponse = false;
                            throw new org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper
                                    .KeeperException.ConnectionLossException();
                        }
                        return result;
                    }
                };
        manager = newManager(uncertain);
        CompletableFuture<BeginBulkLoadResponse> result = begin();
        executor.triggerAll();
        drainEvents();
        executor.triggerAll();
        drainEvents();
        assertThat(result).isCompletedExceptionally();
        assertThat(client.getTable(table).get().bulkLoadId).isNotNull();
        assertThat(capacity.availablePermits()).isZero();
        CompletableFuture<org.apache.fluss.rpc.messages.GetInProgressBulkLoadResponse> discovered =
                manager.getInProgress(
                        PhysicalTablePath.of(table), new FlussPrincipal("alice", "USER"), false);
        executor.triggerAll();
        org.apache.fluss.rpc.messages.PbBulkLoadHandle handle =
                discovered.get().getStatus().getHandle();
        CompletableFuture<?> aborted =
                manager.abort(
                        new BulkLoadHandle(table, handle.getTableId(), handle.getBulkLoadId()));
        executor.triggerAll();
        drainEvents();
        assertThat(aborted).isCompleted();
        assertThat(capacity.availablePermits()).isEqualTo(1);
    }

    @Test
    void testQueuedBeginCannotRestartItsPreparationBudget() throws Exception {
        CompletableFuture<BeginBulkLoadResponse> result =
                manager.begin(
                        Collections.singletonList(PhysicalTablePath.of(table)),
                        1L,
                        new FlussPrincipal("alice", "USER"));
        long pastAdmissionDeadline = System.currentTimeMillis() + 2L;
        org.apache.fluss.testutils.common.CommonTestUtils.waitUntil(
                () -> System.currentTimeMillis() >= pastAdmissionDeadline,
                java.time.Duration.ofSeconds(5),
                "admitted Begin budget expires before worker execution");
        executor.triggerAll();
        drainEvents();
        executor.triggerAll();
        drainEvents();
        assertThat(result).isCompletedExceptionally();
        assertThat(client.getTable(table).get().bulkLoadId).isNull();
        assertThat(capacity.availablePermits()).isEqualTo(1);
    }

    private BulkLoadManager newManager() {
        return newManager(client);
    }

    private BulkLoadManager newManager(ZooKeeperClient client) {
        return new BulkLoadManager(
                client,
                conf,
                context,
                metadata,
                channels,
                executor,
                events,
                new CompletedSnapshotStoreManager(
                        1,
                        executor,
                        client,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        bucket -> false),
                capacity);
    }

    private CompletableFuture<BeginBulkLoadResponse> begin() {
        return manager.begin(
                Collections.singletonList(PhysicalTablePath.of(table)),
                null,
                new FlussPrincipal("alice", "USER"));
    }

    private void drainEvents() {
        while (!events.getEvents().isEmpty()) {
            AccessContextEvent<?> event = (AccessContextEvent<?>) events.getEvents().remove(0);
            event.getAccessFunction().apply(context);
        }
    }

    private void ensure(String path) throws Exception {
        if (!client.pathExists(path)) {
            client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path);
        }
    }
}
