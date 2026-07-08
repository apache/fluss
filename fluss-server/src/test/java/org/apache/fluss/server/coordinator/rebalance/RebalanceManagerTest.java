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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.AutoPartitionManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.LakeTableTieringManager;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceMaxInflightTasksChangedEvent;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    private LakeTableTieringManager lakeTableTieringManager;
    private RebalanceManager rebalanceManager;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;

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

        autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        metadataManager,
                        new RemoteDirDynamicLoader(conf),
                        conf);
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
        zookeeperClient.deleteRebalanceTask();
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
        RebalanceTask rebalanceTask = new RebalanceTask(rebalanceId, NOT_STARTED, new HashMap<>());
        zookeeperClient.registerRebalanceTask(rebalanceTask);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(rebalanceTask);

        // register a rebalance task with empty plan.
        rebalanceManager.registerRebalance(rebalanceId, new HashMap<>(), NOT_STARTED);

        assertThat(rebalanceManager.getRebalanceId()).isEqualTo(rebalanceId);
        RebalanceStatus status = rebalanceManager.getRebalanceStatus();
        assertThat(status).isNotNull();
        assertThat(status).isEqualTo(COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask())
                .hasValue(new RebalanceTask(rebalanceId, COMPLETED, new HashMap<>()));
    }

    @Test
    void testStartupQueuesRecoverRebalanceEvent() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(new Configuration());
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(2);
        RebalanceTask rebalanceTask = new RebalanceTask("recover-test", NOT_STARTED, plan);
        zookeeperClient.registerRebalanceTask(rebalanceTask);

        manager.startup();

        assertThat(eventProcessor.executedPlans).isEmpty();
        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(RecoverRebalanceEvent.class);

        RecoverRebalanceEvent recoverEvent = (RecoverRebalanceEvent) eventManager.events.get(0);
        assertThat(recoverEvent.getRebalanceTask()).isEqualTo(rebalanceTask);

        manager.registerRebalance(
                rebalanceTask.getRebalanceId(),
                rebalanceTask.getExecutePlan(),
                rebalanceTask.getRebalanceStatus());
        assertThat(eventProcessor.executedPlans).hasSize(1);

        manager.close();
    }

    @Test
    void testTimeoutEnqueuesEvent() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        plan.put(
                tb2,
                new RebalancePlanForBucket(
                        tb2, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(new RebalanceTask("timeout-test", NOT_STARTED, plan));
        manager.registerRebalance("timeout-test", plan, NOT_STARTED);
        assertThat(eventProcessor.executedPlans).hasSize(1);

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

        manager.finishRebalanceTask(tb1, TIMEOUT);
        assertThat(eventProcessor.executedPlans).hasSize(2);

        // A second checkTimeout() should NOT enqueue another event because the timeout event
        // has been handled on the coordinator event thread.
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
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("completion-test", NOT_STARTED, plan));
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
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(
                tb1,
                new RebalancePlanForBucket(
                        tb1, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        plan.put(
                tb2,
                new RebalancePlanForBucket(
                        tb2, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));

        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("completed-test", NOT_STARTED, plan));
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
    void testRegisterRebalanceRespectsMaxInflightTasks() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 2);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(5);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("inflight-test", NOT_STARTED, plan));

        manager.registerRebalance("inflight-test", plan, NOT_STARTED);

        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(3);

        manager.finishRebalanceTask(buckets.get(0), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(3);
        assertThat(eventProcessor.executedPlans.get(2).getTableBucket()).isEqualTo(buckets.get(2));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(2);

        manager.close();
    }

    @Test
    void testRebalanceRoundLimitsActivatedBuckets() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 2);
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 2);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(5);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(new RebalanceTask("round-test", NOT_STARTED, plan));

        manager.registerRebalance("round-test", plan, NOT_STARTED);

        assertThat(manager.getMaxBucketsPerRound()).isEqualTo(2);
        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(3);

        manager.finishRebalanceTask(buckets.get(0), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(3);

        manager.finishRebalanceTask(buckets.get(1), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(4);
        assertThat(eventProcessor.executedPlans.get(2).getTableBucket()).isEqualTo(buckets.get(2));
        assertThat(eventProcessor.executedPlans.get(3).getTableBucket()).isEqualTo(buckets.get(3));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(1);

        manager.close();
    }

    @Test
    void testRebalanceRoundWorksWithLowerMaxInflightTasks() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 1);
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 2);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("round-lower-inflight-test", NOT_STARTED, plan));

        manager.registerRebalance("round-lower-inflight-test", plan, NOT_STARTED);

        assertThat(eventProcessor.executedPlans).hasSize(1);
        assertThat(eventProcessor.executedPlans.get(0).getTableBucket()).isEqualTo(buckets.get(0));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(2);

        manager.finishRebalanceTask(buckets.get(0), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(eventProcessor.executedPlans.get(1).getTableBucket()).isEqualTo(buckets.get(1));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(1);

        manager.finishRebalanceTask(buckets.get(1), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(3);
        assertThat(eventProcessor.executedPlans.get(2).getTableBucket()).isEqualTo(buckets.get(2));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isZero();

        manager.close();
    }

    @Test
    void testZeroMaxBucketsPerRoundKeepsMaxInflightBehavior() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 2);
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 0);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(5);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("unlimited-round-test", NOT_STARTED, plan));

        manager.registerRebalance("unlimited-round-test", plan, NOT_STARTED);

        assertThat(manager.getMaxBucketsPerRound()).isZero();
        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(3);

        manager.finishRebalanceTask(buckets.get(0), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(3);
        assertThat(eventProcessor.executedPlans.get(2).getTableBucket()).isEqualTo(buckets.get(2));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(2);

        manager.close();
    }

    @Test
    void testMaxBucketsPerRoundSmallerThanMaxInflightCapsScheduling() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 3);
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 1);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("round-smaller-than-inflight-test", NOT_STARTED, plan));

        manager.registerRebalance("round-smaller-than-inflight-test", plan, NOT_STARTED);

        assertThat(eventProcessor.executedPlans).hasSize(1);
        assertThat(eventProcessor.executedPlans.get(0).getTableBucket()).isEqualTo(buckets.get(0));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(2);

        manager.finishRebalanceTask(buckets.get(0), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(eventProcessor.executedPlans.get(1).getTableBucket()).isEqualTo(buckets.get(1));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(1);

        manager.close();
    }

    @Test
    void testNegativeMaxBucketsPerRoundIsRejected() {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, -1);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(new Configuration());

        assertThatThrownBy(
                        () ->
                                new RebalanceManager(
                                        eventProcessor,
                                        zookeeperClient,
                                        eventManager,
                                        clock,
                                        conf,
                                        executor))
                .hasMessageContaining(
                        ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND.key());
    }

    @Test
    void testIncreaseMaxInflightRebalanceTasksStartsMoreTasks() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 1);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(4);
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("increase-inflight-test", NOT_STARTED, plan));
        manager.registerRebalance("increase-inflight-test", plan, NOT_STARTED);
        assertThat(eventProcessor.executedPlans).hasSize(1);

        Configuration newConfig = new Configuration(conf);
        newConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 3);
        manager.reconfigure(newConfig);

        assertThat(manager.getMaxInflightRebalanceTasks()).isEqualTo(1);
        assertThat(eventProcessor.executedPlans).hasSize(1);
        applyLatestMaxInflightTasksChangedEvent(eventManager, manager, 3);

        assertThat(manager.getMaxInflightRebalanceTasks()).isEqualTo(3);
        assertThat(eventProcessor.executedPlans).hasSize(3);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(3);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(1);

        manager.close();
    }

    @Test
    void testZeroMaxInflightRebalanceTasksPausesAndResumesScheduling() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 0);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(4);
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("paused-inflight-test", NOT_STARTED, plan));
        manager.registerRebalance("paused-inflight-test", plan, NOT_STARTED);

        assertThat(manager.getMaxInflightRebalanceTasks()).isZero();
        assertThat(eventProcessor.executedPlans).isEmpty();
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(4);
        assertThat(manager.hasInProgressRebalance()).isTrue();

        Configuration newConfig = new Configuration(conf);
        newConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 2);
        manager.reconfigure(newConfig);

        assertThat(manager.getMaxInflightRebalanceTasks()).isZero();
        assertThat(eventProcessor.executedPlans).isEmpty();
        applyLatestMaxInflightTasksChangedEvent(eventManager, manager, 2);

        assertThat(manager.getMaxInflightRebalanceTasks()).isEqualTo(2);
        assertThat(eventProcessor.executedPlans).hasSize(2);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(2);

        manager.close();
    }

    @Test
    void testDecreaseMaxInflightRebalanceTasksToZeroDoesNotCancelRunningTasks() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 3);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(5);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("decrease-inflight-test", NOT_STARTED, plan));
        manager.registerRebalance("decrease-inflight-test", plan, NOT_STARTED);
        assertThat(eventProcessor.executedPlans).hasSize(3);

        Configuration newConfig = new Configuration(conf);
        newConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 0);
        manager.reconfigure(newConfig);

        assertThat(manager.getMaxInflightRebalanceTasks()).isEqualTo(3);
        applyLatestMaxInflightTasksChangedEvent(eventManager, manager, 0);

        assertThat(manager.getMaxInflightRebalanceTasks()).isZero();
        assertThat(eventProcessor.executedPlans).hasSize(3);
        manager.finishRebalanceTask(buckets.get(0), COMPLETED);
        manager.finishRebalanceTask(buckets.get(1), COMPLETED);
        assertThat(eventProcessor.executedPlans).hasSize(3);

        manager.finishRebalanceTask(buckets.get(2), COMPLETED);

        assertThat(eventProcessor.executedPlans).hasSize(3);
        assertThat(countStatus(manager, RebalanceStatus.NOT_STARTED)).isEqualTo(2);
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isZero();

        newConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 1);
        manager.reconfigure(newConfig);

        assertThat(eventProcessor.executedPlans).hasSize(3);
        applyLatestMaxInflightTasksChangedEvent(eventManager, manager, 1);

        assertThat(eventProcessor.executedPlans).hasSize(4);
        assertThat(eventProcessor.executedPlans.get(3).getTableBucket()).isEqualTo(buckets.get(3));
        assertThat(countStatus(manager, RebalanceStatus.REBALANCING)).isEqualTo(1);

        manager.close();
    }

    @Test
    void testTimeoutEnqueuesEventsForAllInflightTasks() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        NoOpScheduledExecutor executor = new NoOpScheduledExecutor();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 2);
        RecordingCoordinatorEventProcessor eventProcessor =
                buildRecordingCoordinatorEventProcessor(conf);
        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor, zookeeperClient, eventManager, clock, conf, executor);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("timeout-all-inflight-test", NOT_STARTED, plan));
        manager.registerRebalance("timeout-all-inflight-test", plan, NOT_STARTED);
        assertThat(eventProcessor.executedPlans).hasSize(2);

        clock.advanceTime(Duration.ofMillis(130_000));
        manager.checkTimeout();

        assertThat(eventManager.events).hasSize(2);
        assertThat(((RebalanceTaskTimeoutEvent) eventManager.events.get(0)).getTableBucket())
                .isEqualTo(buckets.get(0));
        assertThat(((RebalanceTaskTimeoutEvent) eventManager.events.get(1)).getTableBucket())
                .isEqualTo(buckets.get(1));

        manager.finishRebalanceTask(buckets.get(0), TIMEOUT);
        manager.finishRebalanceTask(buckets.get(1), TIMEOUT);
        assertThat(eventProcessor.executedPlans).hasSize(3);

        manager.checkTimeout();
        assertThat(eventManager.events).hasSize(2);

        manager.close();
    }

    private CoordinatorEventProcessor buildCoordinatorEventProcessor(Configuration conf) {
        return new CoordinatorEventProcessor(
                zookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(zkEpoch),
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                conf,
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")),
                metadataManager,
                kvSnapshotLeaseManager,
                SystemClock.getInstance());
    }

    private RecordingCoordinatorEventProcessor buildRecordingCoordinatorEventProcessor(
            Configuration conf) {
        return new RecordingCoordinatorEventProcessor(
                zookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(zkEpoch),
                autoPartitionManager,
                lakeTableTieringManager,
                conf,
                metadataManager,
                kvSnapshotLeaseManager);
    }

    private static Map<TableBucket, RebalancePlanForBucket> createRebalancePlan(int bucketCount) {
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            TableBucket tableBucket = new TableBucket(1L, bucketId);
            plan.put(
                    tableBucket,
                    new RebalancePlanForBucket(
                            tableBucket, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        }
        return plan;
    }

    private static int countStatus(RebalanceManager manager, RebalanceStatus status) {
        int count = 0;
        for (RebalanceResultForBucket result :
                manager.listRebalanceProgress(null).progressForBucketMap().values()) {
            if (result.status() == status) {
                count++;
            }
        }
        return count;
    }

    private static void applyLatestMaxInflightTasksChangedEvent(
            RecordingEventManager eventManager, RebalanceManager manager, int expectedMaxInflight) {
        CoordinatorEvent event = eventManager.events.get(eventManager.events.size() - 1);
        assertThat(event).isInstanceOf(RebalanceMaxInflightTasksChangedEvent.class);
        RebalanceMaxInflightTasksChangedEvent changedEvent =
                (RebalanceMaxInflightTasksChangedEvent) event;
        assertThat(changedEvent.getMaxInflightTasks()).isEqualTo(expectedMaxInflight);
        manager.updateMaxInflightRebalanceTasks(changedEvent.getMaxInflightTasks());
    }

    private static final class RecordingCoordinatorEventProcessor
            extends CoordinatorEventProcessor {
        private final List<RebalancePlanForBucket> executedPlans = new ArrayList<>();

        private RecordingCoordinatorEventProcessor(
                ZooKeeperClient zooKeeperClient,
                CoordinatorMetadataCache serverMetadataCache,
                TestCoordinatorChannelManager testCoordinatorChannelManager,
                CoordinatorContext coordinatorContext,
                AutoPartitionManager autoPartitionManager,
                LakeTableTieringManager lakeTableTieringManager,
                Configuration conf,
                MetadataManager metadataManager,
                KvSnapshotLeaseManager kvSnapshotLeaseManager) {
            super(
                    zooKeeperClient,
                    serverMetadataCache,
                    testCoordinatorChannelManager,
                    coordinatorContext,
                    autoPartitionManager,
                    lakeTableTieringManager,
                    TestingMetricGroups.COORDINATOR_METRICS,
                    conf,
                    Executors.newFixedThreadPool(
                            1, new ExecutorThreadFactory("recording-coordinator-io")),
                    metadataManager,
                    kvSnapshotLeaseManager,
                    SystemClock.getInstance());
        }

        @Override
        public void tryToExecuteRebalanceTask(RebalancePlanForBucket planForBucket) {
            executedPlans.add(planForBucket);
        }
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
