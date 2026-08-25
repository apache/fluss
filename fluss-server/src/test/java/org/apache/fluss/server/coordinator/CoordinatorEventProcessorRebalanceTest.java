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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrRequestContext;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.rebalance.RebalanceExecutionKey;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdsZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests rebalance execution and recovery in {@link CoordinatorEventProcessor}. */
class CoordinatorEventProcessorRebalanceTest {

    private static final int REPLICATION_FACTOR = 3;

    private static final TableDescriptor TEST_TABLE =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .primaryKey("a")
                                    .build())
                    .distributedBy(3, "a")
                    .property(ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED.key(), "true")
                    .build()
                    .withReplicationFactor(REPLICATION_FACTOR);

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;
    private static ZkEpoch zkEpoch;

    private final String defaultDatabase = "db";

    private CoordinatorEventProcessor eventProcessor;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private AutoPartitionManager autoPartitionManager;
    private LakeTableTieringManager lakeTableTieringManager;
    private CoordinatorMetadataCache serverMetadataCache;
    private ReplicaCapacityController replicaCapacityController;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;
    private Scheduler scheduler;
    private String remoteDataDir;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        zookeeperClient.registerCoordinatorLeader(
                new CoordinatorAddress(
                        "2", Endpoint.fromListenersString("CLIENT://localhost:10012")));

        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("2");
        for (int i = 0; i < 3; i++) {
            zookeeperClient.registerTabletServer(
                    i,
                    new TabletServerRegistration(
                            "rack" + i,
                            Collections.singletonList(
                                    new Endpoint("host" + i, 1000, DEFAULT_LISTENER_NAME)),
                            System.currentTimeMillis()));
        }
    }

    @BeforeEach
    void beforeEach() {
        serverMetadataCache = new CoordinatorMetadataCache();
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        lakeTableTieringManager =
                new LakeTableTieringManager(TestingMetricGroups.LAKE_TIERING_METRICS);
        remoteDataDir = zookeeperClient.getDefaultRemoteDataDir();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        replicaCapacityController = new ReplicaCapacityController(conf, serverMetadataCache);
        autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        metadataManager,
                        new RemoteDirDynamicLoader(conf),
                        conf,
                        replicaCapacityController);
        kvSnapshotLeaseManager =
                new KvSnapshotLeaseManager(
                        Duration.ofMinutes(10).toMillis(),
                        zookeeperClient,
                        remoteDataDir,
                        SystemClock.getInstance(),
                        TestingMetricGroups.COORDINATOR_METRICS);
        kvSnapshotLeaseManager.start();

        scheduler = new FlussScheduler(1);
        scheduler.startup();

        eventProcessor = buildCoordinatorEventProcessor();
        eventProcessor.startup();
        metadataManager.createDatabase(
                defaultDatabase, DatabaseDescriptor.builder().build(), false);
    }

    @AfterEach
    void afterEach() throws Exception {
        if (eventProcessor != null) {
            eventProcessor.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        metadataManager.dropDatabase(defaultDatabase, false, true);
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(TableIdsZNode.path());
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(PartitionIdsZNode.path());
    }

    @Test
    void testApplyRebalanceConcurrencyChangeOnCoordinatorEventThread() {
        Configuration newConfig = new Configuration();
        newConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 3);

        eventProcessor.getRebalanceManager().reconfigure(newConfig);

        retry(
                Duration.ofSeconds(10),
                () ->
                        assertThat(
                                        eventProcessor
                                                .getRebalanceManager()
                                                .getMaxInflightRebalanceTasks())
                                .isEqualTo(3));
    }

    @Test
    void testDoBucketReassignment() throws Exception {
        registerTabletServer(3);

        initCoordinatorChannel();
        TablePath t1 = TablePath.of(defaultDatabase, "test_bucket_reassignment_table");
        // Mock un-balanced table assignment.
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(0, 1, 3));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);
        long t1Id =
                metadataManager.createTable(t1, remoteDataDir, TEST_TABLE, tableAssignment, false);
        TableBucket tb0 = new TableBucket(t1Id, 0);
        verifyIsr(tb0, 0, Arrays.asList(0, 1, 3));

        // trigger bucket reassignment for tb0:
        // bucket0 -> (0, 1, 2)
        Map<TableBucket, RebalancePlanForBucket> rebalancePlan = new HashMap<>();
        RebalancePlanForBucket planForBucket0 =
                new RebalancePlanForBucket(
                        tb0, 0, 0, Arrays.asList(0, 1, 3), Arrays.asList(0, 1, 2));

        rebalancePlan.put(tb0, planForBucket0);
        // try to execute.
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "rebalance-task-jdsds1", rebalancePlan, RebalanceStatus.NOT_STARTED);

        // Mock to finish rebalance tasks, in production case, this need to be trigged by receiving
        // AdjustIsrRequest.
        Map<TableBucket, LeaderAndIsr> leaderAndIsrMap = new HashMap<>();
        CompletableFuture<AdjustIsrResponse> respCallback = new CompletableFuture<>();

        // This isr list equals originReplicas + addingReplicas. the bucket epoch is 1.
        leaderAndIsrMap.put(
                tb0,
                new LeaderAndIsr(0, 0, Arrays.asList(0, 1, 2, 3), Collections.emptyList(), 0, 1));
        eventProcessor
                .getCoordinatorEventManager()
                .put(new AdjustIsrReceivedEvent(leaderAndIsrMap, respCallback));
        respCallback.get();
        verifyIsr(tb0, 0, Arrays.asList(0, 1, 2));

        // clean up the tablet server 3
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(ZkData.ServerIdZNode.path(3));
    }

    @Test
    void testTimedOutReassignmentRetriesPhaseAIdempotently() throws Exception {
        registerTabletServer(3);

        try {
            initCoordinatorChannel();
            ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers =
                    new ConcurrentLinkedDeque<>();

            TablePath tablePath =
                    TablePath.of(defaultDatabase, "test_timed_out_reassignment_retry");
            Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
            bucketAssignments.put(0, BucketAssignment.of(0, 1, 3));
            long tableId =
                    metadataManager.createTable(
                            tablePath,
                            remoteDataDir,
                            TEST_TABLE,
                            new TableAssignment(bucketAssignments),
                            false);
            TableBucket tableBucket = new TableBucket(tableId, 0);
            verifyIsr(tableBucket, 0, Arrays.asList(0, 1, 3));
            installBlockingNotifyGateways(pendingTriggers);

            RebalancePlanForBucket plan =
                    new RebalancePlanForBucket(
                            tableBucket, 0, 0, Arrays.asList(0, 1, 3), Arrays.asList(0, 1, 2));
            eventProcessor
                    .getRebalanceManager()
                    .registerRebalance(
                            "timed-out-retry-test",
                            Collections.singletonMap(tableBucket, plan),
                            RebalanceStatus.NOT_STARTED);
            RebalanceExecutionKey executionKey =
                    eventProcessor.getRebalanceManager().getExecutionKey(tableBucket);
            retry(Duration.ofMinutes(1), () -> assertThat(pendingTriggers).isNotEmpty());
            int requestsAfterInitialPhaseA = pendingTriggers.size();
            int epochAfterInitialPhaseA =
                    fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());

            eventProcessor
                    .getCoordinatorEventManager()
                    .put(new RebalanceTaskTimeoutEvent(executionKey));
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(rebalanceStatus(tableBucket))
                                    .isEqualTo(RebalanceStatus.TIMEOUT));
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(pendingTriggers.size())
                                    .isGreaterThan(requestsAfterInitialPhaseA));
            int requestsAfterFirstRetry = pendingTriggers.size();
            int epochAfterFirstRetry =
                    fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());
            assertThat(epochAfterFirstRetry).isEqualTo(epochAfterInitialPhaseA);
            List<Integer> assignmentAfterFirstRetry =
                    fromCtx(ctx -> ctx.getAssignment(tableBucket));
            assertThat(assignmentAfterFirstRetry).containsExactly(0, 1, 2, 3);

            eventProcessor
                    .getCoordinatorEventManager()
                    .put(new ReconcileRebalanceTaskEvent(executionKey));
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(pendingTriggers.size())
                                    .isGreaterThan(requestsAfterFirstRetry));
            assertThat(eventProcessor.getRebalanceManager().getExecutionKey(tableBucket))
                    .isEqualTo(executionKey);
            int epochAfterDuplicateRetry =
                    fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());
            assertThat(epochAfterDuplicateRetry).isEqualTo(epochAfterInitialPhaseA);
            List<Integer> assignmentAfterDuplicateRetry =
                    fromCtx(ctx -> ctx.getAssignment(tableBucket));
            assertThat(assignmentAfterDuplicateRetry).containsExactly(0, 1, 2, 3);

            drainPendingNotifyTriggers(pendingTriggers);
            fromCtx(ctx -> null);
            LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
            CompletableFuture<AdjustIsrResponse> responseFuture = new CompletableFuture<>();
            eventProcessor
                    .getCoordinatorEventManager()
                    .put(
                            new AdjustIsrReceivedEvent(
                                    Collections.singletonMap(
                                            tableBucket,
                                            new LeaderAndIsr(
                                                    current.leader(),
                                                    current.leaderEpoch(),
                                                    Arrays.asList(0, 1, 2, 3),
                                                    Collections.emptyList(),
                                                    current.coordinatorEpoch(),
                                                    current.bucketEpoch())),
                                    responseFuture));
            responseFuture.get();
            fromCtx(ctx -> null);
            assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

            ZOO_KEEPER_EXTENSION_WRAPPER
                    .getCustomExtension()
                    .cleanupPath(ZkData.ServerIdZNode.path(2));
            retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).doesNotContain(2));
            drainPendingNotifyTriggers(pendingTriggers);
            fromCtx(ctx -> null);
            assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();
            assertThat(pendingTriggers).isEmpty();
            List<Integer> assignmentWhileTargetOffline =
                    fromCtx(ctx -> ctx.getAssignment(tableBucket));
            assertThat(assignmentWhileTargetOffline).containsExactly(0, 1, 2);

            registerTabletServer(2);
            retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).contains(2));
            drainPendingNotifyTriggers(pendingTriggers);
            fromCtx(ctx -> null);
            current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
            responseFuture = new CompletableFuture<>();
            eventProcessor
                    .getCoordinatorEventManager()
                    .put(
                            new AdjustIsrReceivedEvent(
                                    Collections.singletonMap(
                                            tableBucket,
                                            new LeaderAndIsr(
                                                    current.leader(),
                                                    current.leaderEpoch(),
                                                    Arrays.asList(0, 1, 2),
                                                    Collections.emptyList(),
                                                    current.coordinatorEpoch(),
                                                    current.bucketEpoch())),
                                    responseFuture));
            responseFuture.get();
            drainPendingNotifyTriggers(pendingTriggers);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            eventProcessor
                                                    .getRebalanceManager()
                                                    .hasInProgressRebalance())
                                    .isFalse());
            fromCtx(ctx -> null);
            List<Integer> finalAssignment = fromCtx(ctx -> ctx.getAssignment(tableBucket));
            assertThat(finalAssignment).containsExactly(0, 1, 2);
            verifyIsr(tableBucket, 0, Arrays.asList(0, 1, 2));
        } finally {
            if (Arrays.stream(zookeeperClient.getSortedTabletServerList())
                    .noneMatch(id -> id == 2)) {
                registerTabletServer(2);
            }
            ZOO_KEEPER_EXTENSION_WRAPPER
                    .getCustomExtension()
                    .cleanupPath(ZkData.ServerIdZNode.path(3));
        }
    }

    @Test
    void testTimedOutRebalanceCompletesWhenTableIsBeingDeleted() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        TablePath tablePath = TablePath.of(defaultDatabase, "test_rebalance_during_delete");
        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        assignments.put(0, BucketAssignment.of(0, 1, 2));
        long tableId =
                metadataManager.createTable(
                        tablePath,
                        remoteDataDir,
                        TEST_TABLE,
                        new TableAssignment(assignments),
                        false);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers =
                new ConcurrentLinkedDeque<>();
        installBlockingNotifyGateways(pendingTriggers);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 3));
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "delete-during-rebalance",
                        Collections.singletonMap(tableBucket, plan),
                        RebalanceStatus.NOT_STARTED);
        RebalanceExecutionKey executionKey =
                eventProcessor.getRebalanceManager().getExecutionKey(tableBucket);

        retry(Duration.ofMinutes(1), () -> assertThat(pendingTriggers).isNotEmpty());
        fromCtx(
                ctx -> {
                    ctx.queueTableDeletion(Collections.singleton(tableId));
                    return null;
                });
        eventProcessor
                .getCoordinatorEventManager()
                .put(new RebalanceTaskTimeoutEvent(executionKey));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
        assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.COMPLETED);
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(ZkData.ServerIdZNode.path(3));
    }

    @Test
    void testRebalanceCompletesWhenPartitionIsBeingDeleted() throws Exception {
        TableBucket tableBucket = new TableBucket(123L, 456L, 0);
        fromCtx(
                ctx -> {
                    ctx.updateBucketReplicaAssignment(tableBucket, Arrays.asList(0, 1, 2));
                    ctx.queuePartitionDeletion(
                            Collections.singleton(new TablePartition(123L, 456L)));
                    return null;
                });
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 0));
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "partition-delete-during-rebalance",
                        Collections.singletonMap(tableBucket, plan),
                        RebalanceStatus.NOT_STARTED);

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
        assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.COMPLETED);
    }

    @Test
    void testLeaderOnlyRebalanceExecutesSequentially() throws Exception {
        // Set up controlled gateways that capture NotifyLeaderAndIsr calls.
        // Gateways start in pass-through mode for table creation, then switch
        // to controlled mode to verify sequential leader migration.
        ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers =
                new ConcurrentLinkedDeque<>();
        int[] servers = zookeeperClient.getSortedTabletServerList();
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        ControlledNotifyGateway[] controlledGateways = new ControlledNotifyGateway[servers.length];
        for (int i = 0; i < servers.length; i++) {
            ControlledNotifyGateway gw = new ControlledNotifyGateway(servers[i], pendingTriggers);
            gateways.put(servers[i], gw);
            controlledGateways[i] = gw;
        }
        testCoordinatorChannelManager.setGateways(gateways);

        // Create a table with 3 buckets, each assigned to replicas [0, 1, 2] with leader 0.
        TablePath t1 = TablePath.of(defaultDatabase, "test_leader_rebalance_sequential");
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(0, 1, 2));
        bucketAssignments.put(1, BucketAssignment.of(0, 1, 2));
        bucketAssignments.put(2, BucketAssignment.of(0, 1, 2));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);
        long t1Id =
                metadataManager.createTable(t1, remoteDataDir, TEST_TABLE, tableAssignment, false);

        TableBucket tb0 = new TableBucket(t1Id, 0);
        TableBucket tb1 = new TableBucket(t1Id, 1);
        TableBucket tb2 = new TableBucket(t1Id, 2);

        // Wait for initial leaders to be elected (all should be leader 0).
        verifyIsr(tb0, 0, Arrays.asList(0, 1, 2));
        verifyIsr(tb1, 0, Arrays.asList(0, 1, 2));
        verifyIsr(tb2, 0, Arrays.asList(0, 1, 2));

        // Switch to controlled mode: from now on, NotifyLeaderAndIsr responses
        // are held until the test explicitly releases them.
        for (ControlledNotifyGateway gw : controlledGateways) {
            gw.enableControlMode();
        }
        pendingTriggers.clear();

        // Create leader-only rebalance plan (replicas stay the same, only leaders change):
        // tb0: leader 0 -> 1 (newReplicas=[1,0,2] puts target leader first)
        // tb1: leader 0 -> 2 (newReplicas=[2,0,1] puts target leader first)
        // tb2: leader 0 -> 1 (newReplicas=[1,2,0] puts target leader first)
        Map<TableBucket, RebalancePlanForBucket> rebalancePlan = new HashMap<>();
        rebalancePlan.put(
                tb0,
                new RebalancePlanForBucket(
                        tb0, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2)));
        rebalancePlan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 2, Arrays.asList(0, 1, 2), Arrays.asList(2, 0, 1)));
        rebalancePlan.put(
                tb2,
                new RebalancePlanForBucket(
                        tb2, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 0)));

        // Register the rebalance. Only the FIRST task should trigger a leader election
        // because subsequent tasks must wait for the NotifyLeaderAndIsr response.
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "rebalance-leader-sequential", rebalancePlan, RebalanceStatus.NOT_STARTED);

        // === Step 1: Verify only the first task started ===
        // registerRebalance() is synchronous, so after it returns, the first task's
        // leader election has triggered NotifyLeaderAndIsr to replica servers.
        // Other tasks must NOT have started because the first response is held.
        assertThat(pendingTriggers).isNotEmpty();
        // All 3 tasks are still in progress (first executing, two waiting).
        assertThat(countInProgressRebalanceTasks(tb0, tb1, tb2)).isEqualTo(3);

        // Release the first batch - this allows the event processor to complete
        // the first task and start the second.
        drainPendingNotifyTriggers(pendingTriggers);

        // === Step 2: Wait for the second task to start ===
        // The event processor completes the first task via the response callback,
        // then starts the second task which produces new pending triggers.
        retry(Duration.ofMinutes(1), () -> assertThat(pendingTriggers).isNotEmpty());
        // First task completed, 2 tasks remaining.
        assertThat(countInProgressRebalanceTasks(tb0, tb1, tb2)).isEqualTo(2);
        drainPendingNotifyTriggers(pendingTriggers);

        // === Step 3: Wait for the third task to start ===
        retry(Duration.ofMinutes(1), () -> assertThat(pendingTriggers).isNotEmpty());
        // Two tasks completed, 1 task remaining.
        assertThat(countInProgressRebalanceTasks(tb0, tb1, tb2)).isEqualTo(1);
        drainPendingNotifyTriggers(pendingTriggers);

        // === Step 4: Wait for the rebalance to complete ===
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());

        // Verify all leaders changed correctly.
        verifyIsr(tb0, 1, Arrays.asList(0, 1, 2));
        verifyIsr(tb1, 2, Arrays.asList(0, 1, 2));
        verifyIsr(tb2, 1, Arrays.asList(0, 1, 2));
    }

    @Test
    void testRebalanceRecoveryStateClassification() throws Exception {
        // The classification only reads the coordinator state that recovery has just loaded.
        TableBucket tableBucket = new TableBucket(987L, 0);
        putBucketState(tableBucket, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2));

        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 3));
        assertThat(eventProcessor.isRebalanceTaskComplete(plan)).isFalse();
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isTrue();

        // Origin assignment with a leftover adding replica in ISR is not clean.
        putBucketState(tableBucket, 0, Arrays.asList(0, 1, 2, 3), Arrays.asList(0, 1, 2));
        assertThat(eventProcessor.isRebalanceTaskComplete(plan)).isFalse();
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isFalse();

        // The leader has not moved to the new leader of the plan yet, so recovery replays it.
        putBucketState(tableBucket, 0, Arrays.asList(0, 1, 3), Arrays.asList(1, 0, 3));
        assertThat(eventProcessor.isRebalanceTaskComplete(plan)).isFalse();

        // A plan whose target state is already in place must not be replayed, replaying it would
        // elect the very same leader again and bump the epoch of an already migrated bucket.
        RebalancePlanForBucket appliedPlan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2));
        putBucketState(tableBucket, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2));
        assertThat(eventProcessor.isRebalanceTaskComplete(appliedPlan)).isTrue();

        // The plan targets replica 3, which is not hosted by a live server, so it stays replayable.
        putBucketState(tableBucket, 1, Arrays.asList(0, 1, 3), Arrays.asList(1, 0, 3));
        assertThat(eventProcessor.isRebalanceTaskComplete(plan)).isFalse();
    }

    private void putBucketState(
            TableBucket tableBucket, int leader, List<Integer> isr, List<Integer> assignment)
            throws Exception {
        fromCtx(
                ctx -> {
                    ctx.updateBucketReplicaAssignment(tableBucket, assignment);
                    ctx.putBucketLeaderAndIsr(
                            tableBucket,
                            new LeaderAndIsr(
                                    leader,
                                    1,
                                    isr,
                                    Collections.emptyList(),
                                    ctx.getCoordinatorEpoch(),
                                    1));
                    return null;
                });
    }

    @Test
    void testStaleNotifyLeaderAndIsrResponseCannotCompleteRebalance() throws Exception {
        ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers =
                new ConcurrentLinkedDeque<>();
        int[] servers = zookeeperClient.getSortedTabletServerList();
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int server : servers) {
            ControlledNotifyGateway gateway = new ControlledNotifyGateway(server, pendingTriggers);
            gateways.put(server, gateway);
        }
        testCoordinatorChannelManager.setGateways(gateways);

        TablePath tablePath = TablePath.of(defaultDatabase, "test_stale_rebalance_response");
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(0, 1, 2));
        long tableId =
                metadataManager.createTable(
                        tablePath,
                        remoteDataDir,
                        TEST_TABLE,
                        new TableAssignment(bucketAssignments),
                        false);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        verifyIsr(tableBucket, 0, Arrays.asList(0, 1, 2));

        for (TabletServerGateway gateway : gateways.values()) {
            ((ControlledNotifyGateway) gateway).enableControlMode();
        }
        pendingTriggers.clear();
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2));
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "stale-response-test",
                        Collections.singletonMap(tableBucket, plan),
                        RebalanceStatus.NOT_STARTED);
        retry(Duration.ofMinutes(1), () -> assertThat(pendingTriggers).isNotEmpty());

        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        NotifyLeaderAndIsrResultForBucket success =
                new NotifyLeaderAndIsrResultForBucket(tableBucket);
        NotifyLeaderAndIsrRequestContext staleContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch() - 1,
                        eventProcessor.getRebalanceManager().getExecutionKey(tableBucket));
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new NotifyLeaderAndIsrResponseReceivedEvent(
                                Collections.singletonList(success),
                                current.leader(),
                                Collections.singletonMap(tableBucket, staleContext)));
        fromCtx(ctx -> null);
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        NotifyLeaderAndIsrRequestContext oldAttemptContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        new RebalanceExecutionKey("old-rebalance", tableBucket, 1));
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new NotifyLeaderAndIsrResponseReceivedEvent(
                                Collections.singletonList(success),
                                current.leader(),
                                Collections.singletonMap(tableBucket, oldAttemptContext)));
        fromCtx(ctx -> null);
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        NotifyLeaderAndIsrRequestContext currentContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        eventProcessor.getRebalanceManager().getExecutionKey(tableBucket));
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new NotifyLeaderAndIsrResponseReceivedEvent(
                                Collections.singletonList(success),
                                current.leader(),
                                Collections.singletonMap(tableBucket, currentContext)));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
        drainPendingNotifyTriggers(pendingTriggers);
    }

    @Test
    void testLeaderOnlyRebalanceIgnoresSuccessResponseFromOldLeader() throws Exception {
        ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers =
                new ConcurrentLinkedDeque<>();
        int[] servers = zookeeperClient.getSortedTabletServerList();
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        ControlledNotifyGateway[] controlledGateways = new ControlledNotifyGateway[servers.length];
        for (int i = 0; i < servers.length; i++) {
            ControlledNotifyGateway gw = new ControlledNotifyGateway(servers[i], pendingTriggers);
            gateways.put(servers[i], gw);
            controlledGateways[i] = gw;
        }
        testCoordinatorChannelManager.setGateways(gateways);

        TablePath t1 = TablePath.of(defaultDatabase, "test_leader_rebalance_wait_new_leader");
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(0, 1, 2));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);
        long t1Id =
                metadataManager.createTable(t1, remoteDataDir, TEST_TABLE, tableAssignment, false);

        TableBucket tb0 = new TableBucket(t1Id, 0);

        verifyIsr(tb0, 0, Arrays.asList(0, 1, 2));

        for (ControlledNotifyGateway gw : controlledGateways) {
            gw.enableControlMode();
        }
        pendingTriggers.clear();

        Map<TableBucket, RebalancePlanForBucket> rebalancePlan = new HashMap<>();
        rebalancePlan.put(
                tb0,
                new RebalancePlanForBucket(
                        tb0, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2)));

        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "rebalance-wait-new-leader-response",
                        rebalancePlan,
                        RebalanceStatus.NOT_STARTED);

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(hasPendingNotifyTrigger(pendingTriggers, 0)).isTrue());
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(hasPendingNotifyTrigger(pendingTriggers, 1)).isTrue());
        assertThat(countInProgressRebalanceTasks(tb0)).isEqualTo(1);

        completePendingNotifyTrigger(pendingTriggers, 0);
        fromCtx(ctx -> null);

        assertThat(countInProgressRebalanceTasks(tb0)).isEqualTo(1);
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        completePendingNotifyTrigger(pendingTriggers, 1);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
        verifyIsr(tb0, 1, Arrays.asList(0, 1, 2));
    }

    private void verifyIsr(TableBucket tb, int expectedLeader, List<Integer> expectedIsr)
            throws Exception {
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> fromCtx((ctx) -> ctx.getBucketLeaderAndIsr(tb)),
                        Duration.ofMinutes(1),
                        "leader not elected");
        LeaderAndIsr newLeaderAndIsrOfZk = zookeeperClient.getLeaderAndIsr(tb).get();
        assertThat(leaderAndIsr.leader())
                .isEqualTo(newLeaderAndIsrOfZk.leader())
                .isEqualTo(expectedLeader);
        assertThat(leaderAndIsr.isr())
                .isEqualTo(newLeaderAndIsrOfZk.isr())
                .hasSameElementsAs(expectedIsr);
    }

    private CoordinatorEventProcessor buildCoordinatorEventProcessor() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        conf.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_RETRY_DELAY, Duration.ofDays(1));
        return new CoordinatorEventProcessor(
                zookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(zkEpoch),
                replicaCapacityController,
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                conf,
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")),
                metadataManager,
                kvSnapshotLeaseManager,
                scheduler,
                SystemClock.getInstance());
    }

    private void initCoordinatorChannel() throws Exception {
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                Arrays.stream(zookeeperClient.getSortedTabletServerList())
                        .boxed()
                        .collect(Collectors.toSet()),
                Collections.emptySet());
    }

    private void registerTabletServer(int serverId) throws Exception {
        zookeeperClient.registerTabletServer(
                serverId,
                new TabletServerRegistration(
                        "rack" + serverId,
                        Collections.singletonList(
                                new Endpoint("host" + serverId, 1001, DEFAULT_LISTENER_NAME)),
                        System.currentTimeMillis()));
    }

    private RebalanceStatus rebalanceStatus(TableBucket tableBucket) {
        return eventProcessor
                .getRebalanceManager()
                .listRebalanceProgress(null)
                .progressForBucketMap()
                .get(tableBucket)
                .status();
    }

    private void installBlockingNotifyGateways(
            ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers) throws Exception {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int server : zookeeperClient.getSortedTabletServerList()) {
            ControlledNotifyGateway gateway = new ControlledNotifyGateway(server, pendingTriggers);
            gateway.enableControlMode();
            gateways.put(server, gateway);
        }
        testCoordinatorChannelManager.setGateways(gateways);
    }

    private void retryVerifyContext(Consumer<CoordinatorContext> verifyFunction) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    AccessContextEvent<Void> event =
                            new AccessContextEvent<>(
                                    ctx -> {
                                        verifyFunction.accept(ctx);
                                        return null;
                                    });
                    eventProcessor.getCoordinatorEventManager().put(event);
                    try {
                        event.getResultFuture().get(30, TimeUnit.SECONDS);
                    } catch (Throwable t) {
                        throw ExceptionUtils.stripExecutionException(t);
                    }
                });
    }

    private <T> T fromCtx(Function<CoordinatorContext, T> retrieveFunction) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(retrieveFunction);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private static void drainPendingNotifyTriggers(
            ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers) {
        ControlledNotifyTrigger trigger;
        while ((trigger = pendingTriggers.poll()) != null) {
            trigger.complete(null);
        }
    }

    private static boolean hasPendingNotifyTrigger(
            ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers, int responseServerId) {
        for (ControlledNotifyTrigger trigger : pendingTriggers) {
            if (trigger.getResponseServerId() == responseServerId) {
                return true;
            }
        }
        return false;
    }

    private static void completePendingNotifyTrigger(
            ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers, int responseServerId) {
        for (ControlledNotifyTrigger trigger : pendingTriggers) {
            if (trigger.getResponseServerId() == responseServerId) {
                assertThat(pendingTriggers.remove(trigger)).isTrue();
                trigger.complete(null);
                return;
            }
        }
        throw new AssertionError(
                "No pending NotifyLeaderAndIsr response for server " + responseServerId);
    }

    private int countInProgressRebalanceTasks(TableBucket... buckets) {
        int count = 0;
        for (TableBucket tableBucket : buckets) {
            if (!RebalanceStatus.FINAL_STATUSES.contains(
                    eventProcessor
                            .getRebalanceManager()
                            .listRebalanceProgress(null)
                            .progressForBucketMap()
                            .get(tableBucket)
                            .status())) {
                count++;
            }
        }
        return count;
    }
}
