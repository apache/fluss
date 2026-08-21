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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.RebalanceInfo;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.AutoPartitionManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.LakeTableTieringManager;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.ReplicaCapacityController;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.ZkData.RebalanceHistoryTaskZNode;
import org.apache.fluss.server.zk.data.ZkData.RebalanceHistoryZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.ManualClock;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link RebalanceManager}. */
public class RebalanceManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;
    private static ZkEpoch zkEpoch;

    private CoordinatorMetadataCache serverMetadataCache;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private AutoPartitionManager autoPartitionManager;
    private ReplicaCapacityController replicaCapacityController;
    private LakeTableTieringManager lakeTableTieringManager;
    private RebalanceManager rebalanceManager;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;
    private Scheduler scheduler;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("1");
    }

    @BeforeEach
    void beforeEach() {
        serverMetadataCache = new CoordinatorMetadataCache();
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        String remoteDataDir = "/tmp/fluss/remote-data";
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);

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

        replicaCapacityController =
                new ReplicaCapacityController(
                        conf, serverMetadataCache, TestingMetricGroups.COORDINATOR_METRICS);
        autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        metadataManager,
                        new RemoteDirDynamicLoader(conf),
                        conf,
                        replicaCapacityController);
        lakeTableTieringManager =
                new LakeTableTieringManager(TestingMetricGroups.LAKE_TIERING_METRICS);
        CoordinatorEventProcessor eventProcessor = buildCoordinatorEventProcessor(conf);
        RecordingEventManager recordingEventManager = new RecordingEventManager();
        rebalanceManager =
                new RebalanceManager(
                        eventProcessor,
                        zookeeperClient,
                        recordingEventManager,
                        SystemClock.getInstance());
        rebalanceManager.startup();
    }

    @AfterEach
    void afterEach() throws Exception {
        rebalanceManager.close();
        if (scheduler != null) {
            scheduler.shutdown();
        }
        zookeeperClient.deleteRebalanceTask();
        for (String rebalanceId : zookeeperClient.getChildren(RebalanceHistoryZNode.path())) {
            zookeeperClient.deletePath(RebalanceHistoryTaskZNode.path(rebalanceId));
        }
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
    }

    @Test
    void testRebalanceWithoutTask() throws Exception {
        assertThat(rebalanceManager.getRebalanceId()).isNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isNull();

        String rebalanceId = "test-rebalance-id";
        RebalanceTask rebalanceTask =
                new RebalanceTask(rebalanceId, NOT_STARTED, new HashMap<>(), -1, -1);
        zookeeperClient.registerRebalanceTask(rebalanceTask);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(rebalanceTask);

        // register a rebalance task with empty plan.
        long beforeRegister = System.currentTimeMillis();
        rebalanceManager.registerRebalance(rebalanceId, new HashMap<>(), NOT_STARTED);
        long afterRegister = System.currentTimeMillis();

        assertThat(rebalanceManager.getRebalanceId()).isEqualTo(rebalanceId);
        RebalanceStatus status = rebalanceManager.getRebalanceStatus();
        assertThat(status).isNotNull();
        assertThat(status).isEqualTo(COMPLETED);

        // An empty plan completes immediately, so started/completed are both stamped with
        // "now" (real clock, since this test's rebalanceManager uses SystemClock).
        RebalanceTask finalTask = zookeeperClient.getRebalanceTask().get();
        assertThat(finalTask.getRebalanceId()).isEqualTo(rebalanceId);
        assertThat(finalTask.getRebalanceStatus()).isEqualTo(COMPLETED);
        assertThat(finalTask.getExecutePlan()).isEmpty();
        assertThat(finalTask.getStartedAtMs()).isBetween(beforeRegister, afterRegister);
        assertThat(finalTask.getCompletedAtMs()).isBetween(beforeRegister, afterRegister);
    }

    @Test
    void testGenerateRebalanceTaskStampsStartedAtMs() throws Exception {
        ManualClock clock = new ManualClock(12_345L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        RebalanceTask task = manager.generateRebalanceTask(Collections.emptyList());

        assertThat(task.getStartedAtMs()).isEqualTo(12_345L);
        assertThat(task.getCompletedAtMs()).isEqualTo(-1L);

        manager.close();
    }

    @Test
    void testInitializeRestoresTimestampsOnFailover() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        // Simulate a task left behind by a previous coordinator, with a real startedAtMs.
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("failover-test", NOT_STARTED, plan, 5_000L, -1));

        ManualClock clock = new ManualClock(20_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);

        // startup() -> initialize() restores the task and its timestamps from ZooKeeper.
        manager.startup();

        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(5_000L);
        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(-1L);

        // Completing the restored task must keep the original startedAtMs.
        manager.finishRebalanceTask(tb1, COMPLETED);

        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(5_000L);
        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(20_000L);
        assertThat(zookeeperClient.getRebalanceTask())
                .hasValue(new RebalanceTask("failover-test", COMPLETED, plan, 5_000L, 20_000L));

        manager.close();
    }

    @Test
    void testInitializeRestoresTaskWithoutTimestamps() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        // A version-1 znode has no timestamp fields, so the deserializer yields -1. Completing
        // such a restored task must leave startedAtMs unset rather than back-date it to the
        // failover clock.
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("failover-v1-test", NOT_STARTED, plan, -1, -1));

        ManualClock clock = new ManualClock(20_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);

        manager.startup();
        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(-1L);

        manager.finishRebalanceTask(tb1, COMPLETED);

        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(-1L);
        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(20_000L);
        assertThat(zookeeperClient.getRebalanceTask())
                .hasValue(new RebalanceTask("failover-v1-test", COMPLETED, plan, -1, 20_000L));

        manager.close();
    }

    @Test
    void testInitializeDoesNotReCompleteRestoredFinalEmptyPlanTask() throws Exception {
        // Simulate an empty-plan rebalance that already reached COMPLETED before the coordinator
        // failed over, including its already-written history entry.
        Map<TableBucket, RebalancePlanForBucket> emptyPlan = new HashMap<>();
        RebalanceTask alreadyCompletedTask =
                new RebalanceTask("failover-empty-plan-test", COMPLETED, emptyPlan, 5_000L, 8_000L);
        zookeeperClient.registerRebalanceTask(alreadyCompletedTask);
        zookeeperClient.registerRebalanceHistory(alreadyCompletedTask, 10);

        ManualClock clock = new ManualClock(50_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);

        // startup() -> initialize() restores the already-final task; it must not be re-completed
        // with the (much later) failover clock time.
        manager.startup();

        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(5_000L);
        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(8_000L);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(alreadyCompletedTask);
        assertThat(zookeeperClient.getRebalanceHistory()).containsExactly(alreadyCompletedTask);
        assertThat(manager.listRebalances())
                .containsExactly(
                        new RebalanceInfo("failover-empty-plan-test", COMPLETED, 5_000L, 8_000L));

        manager.close();
    }

    @Test
    void testInitializeKeepsStatusOfRestoredCanceledEmptyPlanTask() throws Exception {
        // A restored final empty-plan task must keep its own status: re-running completion on
        // failover would coerce CANCELED to COMPLETED and rewrite the znode and history.
        Map<TableBucket, RebalancePlanForBucket> emptyPlan = new HashMap<>();
        RebalanceTask canceledTask =
                new RebalanceTask(
                        "failover-canceled-empty-plan-test",
                        RebalanceStatus.CANCELED,
                        emptyPlan,
                        5_000L,
                        8_000L);
        zookeeperClient.registerRebalanceTask(canceledTask);
        zookeeperClient.registerRebalanceHistory(canceledTask, 10);

        ManualClock clock = new ManualClock(50_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);

        manager.startup();

        assertThat(manager.getRebalanceStatus()).isEqualTo(RebalanceStatus.CANCELED);
        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(5_000L);
        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(8_000L);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(canceledTask);
        assertThat(zookeeperClient.getRebalanceHistory()).containsExactly(canceledTask);
        assertThat(manager.listRebalances())
                .containsExactly(
                        new RebalanceInfo(
                                "failover-canceled-empty-plan-test",
                                RebalanceStatus.CANCELED,
                                5_000L,
                                8_000L));

        manager.close();
    }

    @Test
    void testCompleteRebalanceStampsCompletedAtMsAndWritesHistory() throws Exception {
        ManualClock clock = new ManualClock(1_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("complete-ts-test", NOT_STARTED, plan, 1_000L, -1));
        manager.registerRebalance("complete-ts-test", plan, NOT_STARTED, 1_000L, -1);

        clock.advanceTime(Duration.ofMillis(5_000));
        manager.finishRebalanceTask(tb1, COMPLETED);

        assertThat(manager.getCurrentStartedAtMs()).isEqualTo(1_000L);
        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(6_000L);
        assertThat(zookeeperClient.getRebalanceTask())
                .hasValue(new RebalanceTask("complete-ts-test", COMPLETED, plan, 1_000L, 6_000L));
        assertThat(zookeeperClient.getRebalanceHistory())
                .contains(new RebalanceTask("complete-ts-test", COMPLETED, plan, 1_000L, 6_000L));

        manager.close();
    }

    @Test
    void testCancelRebalanceStampsCompletedAtMsAndWritesHistory() throws Exception {
        ManualClock clock = new ManualClock(1_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("cancel-ts-test", NOT_STARTED, plan, 1_000L, -1));
        manager.registerRebalance("cancel-ts-test", plan, NOT_STARTED, 1_000L, -1);

        clock.advanceTime(Duration.ofMillis(3_000));
        manager.cancelRebalance("cancel-ts-test");

        assertThat(manager.getCurrentCompletedAtMs()).isEqualTo(4_000L);
        RebalanceTask canceledTask =
                new RebalanceTask("cancel-ts-test", RebalanceStatus.CANCELED, plan, 1_000L, 4_000L);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(canceledTask);
        assertThat(zookeeperClient.getRebalanceHistory()).contains(canceledTask);

        manager.close();
    }

    @Test
    void testTimeoutEnqueuesEvent() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        plan.put(
                tb2,
                new RebalancePlanForBucket(
                        tb2, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("timeout-test", NOT_STARTED, plan, -1, -1));
        manager.registerRebalance("timeout-test", plan, NOT_STARTED);

        // Not yet timed out.
        clock.advanceTime(Duration.ofMillis(100_000));
        manager.checkTimeout();
        assertThat(eventManager.events).isEmpty();

        // Cross the 2-minute boundary.
        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();

        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(RebalanceTaskTimeoutEvent.class);
        RebalanceTaskTimeoutEvent timeoutEvent =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        assertThat(timeoutEvent.getTableBucket()).isEqualTo(tb1);

        // A second checkTimeout() should NOT enqueue another event because the
        // inflight state was cleared after the first timeout.
        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();
        assertThat(eventManager.events).hasSize(1);

        manager.close();
    }

    @Test
    void testTimeoutAfterCompletionIsNoOp() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("completion-test", NOT_STARTED, plan, -1, -1));
        manager.registerRebalance("completion-test", plan, NOT_STARTED);

        // The task completes normally before timeout.
        manager.finishRebalanceTask(tb1, COMPLETED);

        // Now the timeout fires, but the task is already done.
        clock.advanceTime(Duration.ofMillis(130_000));
        manager.checkTimeout();

        // No timeout event should be enqueued because inflightTaskStartMs was cleared.
        assertThat(eventManager.events).isEmpty();

        manager.close();
    }

    @Test
    void testTimeoutTreatsTaskAsCompleted() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        plan.put(
                tb2,
                new RebalancePlanForBucket(
                        tb2, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("completed-test", NOT_STARTED, plan, -1, -1));
        manager.registerRebalance("completed-test", plan, NOT_STARTED);

        // Timeout fires.
        clock.advanceTime(Duration.ofMillis(130_000));
        manager.checkTimeout();

        // Simulate the coordinator event thread processing the timeout event.
        assertThat(eventManager.events).hasSize(1);
        RebalanceTaskTimeoutEvent timeoutEvent =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        manager.finishRebalanceTask(timeoutEvent.getTableBucket(), TIMEOUT);

        // The timed-out task should be in finishedRebalanceTasks as TIMEOUT.
        assertThat(manager.hasInProgressRebalance()).isTrue();
        RebalanceResultForBucket result =
                manager.listRebalanceProgress(null).progressForBucketMap().get(tb1);
        assertThat(result.status()).isEqualTo(TIMEOUT);

        manager.close();
    }

    @Test
    void testListRebalancesEmptyWhenNoRebalanceEverRun() {
        assertThat(rebalanceManager.listRebalances()).isEmpty();
    }

    @Test
    void testListRebalancesPropagatesZooKeeperReadFailure() throws Exception {
        ZooKeeperClient failingZkClient = mock(ZooKeeperClient.class);
        when(failingZkClient.getRebalanceHistory())
                .thenThrow(new RuntimeException("zk read failed"));
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor,
                        failingZkClient,
                        eventManager,
                        SystemClock.getInstance(),
                        executor);
        manager.startup();

        assertThatThrownBy(manager::listRebalances).isInstanceOf(FlussRuntimeException.class);

        manager.close();
    }

    @Test
    void testListRebalancesCurrentFirstThenHistoryNewestFirstDeduped() throws Exception {
        ManualClock clock = new ManualClock(1_000L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        // First rebalance completes and lands in history.
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("rebalance-1", NOT_STARTED, plan, 1_000L, -1));
        manager.registerRebalance("rebalance-1", plan, NOT_STARTED, 1_000L, -1);
        clock.advanceTime(Duration.ofMillis(1_000));
        manager.finishRebalanceTask(tb1, COMPLETED);

        // Second rebalance completes later, also lands in history.
        clock.advanceTime(Duration.ofMillis(1_000));
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("rebalance-2", NOT_STARTED, plan, clock.milliseconds(), -1));
        manager.registerRebalance("rebalance-2", plan, NOT_STARTED, clock.milliseconds(), -1);
        clock.advanceTime(Duration.ofMillis(1_000));
        manager.finishRebalanceTask(tb1, COMPLETED);

        // Third rebalance is still in progress (current).
        clock.advanceTime(Duration.ofMillis(1_000));
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("rebalance-3", NOT_STARTED, plan, clock.milliseconds(), -1));
        manager.registerRebalance("rebalance-3", plan, NOT_STARTED, clock.milliseconds(), -1);

        List<RebalanceInfo> rebalanceInfos = manager.listRebalances();

        // Current rebalance first, then history newest first; the current one must not be
        // duplicated even though it will eventually also be written to history.
        assertThat(rebalanceInfos).hasSize(3);
        assertThat(rebalanceInfos.get(0).rebalanceId()).isEqualTo("rebalance-3");
        assertThat(rebalanceInfos.get(1).rebalanceId()).isEqualTo("rebalance-2");
        assertThat(rebalanceInfos.get(2).rebalanceId()).isEqualTo("rebalance-1");

        manager.close();
    }

    private CoordinatorEventProcessor buildCoordinatorEventProcessor(Configuration conf) {
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

    /** Records events put into the coordinator event queue. */
    private static final class RecordingEventManager implements EventManager {
        final List<CoordinatorEvent> events = new ArrayList<>();

        @Override
        public void put(CoordinatorEvent event) {
            events.add(event);
        }
    }

    /**
     * A scheduled executor that never actually runs scheduled tasks, so tests retain full control
     * over when {@link RebalanceManager#checkTimeout()} is invoked.
     */
    private static final class NoOpScheduledExecutor extends ScheduledThreadPoolExecutor {

        NoOpScheduledExecutor() {
            super(0);
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command,
                long initialDelay,
                long delay,
                java.util.concurrent.TimeUnit unit) {
            return null;
        }
    }
}
