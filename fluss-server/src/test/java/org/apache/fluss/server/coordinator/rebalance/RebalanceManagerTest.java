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
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
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
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
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
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
    private CoordinatorMetricGroup coordinatorMetricGroup;
    private AbstractMetricGroup rebalanceMetricGroup;
    private MetricRegistry metricRegistry;
    private ManualClock metricClock;
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
        metricRegistry = mock(MetricRegistry.class);
        coordinatorMetricGroup =
                new CoordinatorMetricGroup(metricRegistry, "cluster", "host", "coordinator");
        metricClock = new ManualClock(0L);
        rebalanceManager =
                new RebalanceManager(
                        eventProcessor,
                        zookeeperClient,
                        recordingEventManager,
                        metricClock,
                        coordinatorMetricGroup);
        rebalanceMetricGroup = coordinatorMetricGroup.getOrAddRebalanceMetricGroup();
        rebalanceManager.startup();
    }

    @AfterEach
    void afterEach() throws Exception {
        rebalanceManager.close();
        coordinatorMetricGroup.close();
        if (scheduler != null) {
            scheduler.shutdown();
        }
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
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(2);
        RebalanceTask rebalanceTask = new RebalanceTask("recover-test", NOT_STARTED, plan);
        zookeeperClient.registerRebalanceTask(rebalanceTask);

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        createCoordinatorMetricGroup(),
                        executor);
        // If startup() finds a pending rebalance task in ZooKeeper, it should enqueue a
        // RecoverRebalanceEvent to be processed by the coordinator event thread, instead of
        // calling registerRebalance() directly on the startup thread.
        manager.startup();

        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(RecoverRebalanceEvent.class);

        RecoverRebalanceEvent recoverEvent = (RecoverRebalanceEvent) eventManager.events.get(0);
        assertThat(recoverEvent.getRebalanceTask()).isEqualTo(rebalanceTask);

        manager.close();
    }

    private Map<TableBucket, RebalancePlanForBucket> createRebalancePlan(int taskCount) {
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        for (int i = 0; i < taskCount; i++) {
            TableBucket tb = new TableBucket(1L, i);
            plan.put(
                    tb,
                    new RebalancePlanForBucket(
                            tb, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        }
        return plan;
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
                        eventProcessor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        createCoordinatorMetricGroup(),
                        executor);
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

        zookeeperClient.registerRebalanceTask(new RebalanceTask("timeout-test", NOT_STARTED, plan));
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
                        eventProcessor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        createCoordinatorMetricGroup(),
                        executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
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
        CoordinatorEventProcessor eventProcessor =
                buildCoordinatorEventProcessor(new Configuration());

        RebalanceManager manager =
                new RebalanceManager(
                        eventProcessor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        createCoordinatorMetricGroup(),
                        executor);
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
    void testRebalanceMetricsLifecycle() {
        assertThat(rebalanceMetricGroup.getLogicalScope(value -> value, '_'))
                .isEqualTo("coordinator");
        assertThat(rebalanceMetricGroup.getScopeComponents())
                .containsExactly(coordinatorMetricGroup.getScopeComponents());
        assertThat(rebalanceMetricGroup.getAllVariables())
                .isEqualTo(coordinatorMetricGroup.getAllVariables());
        assertThat(rebalanceMetricGroup.getMetrics()).hasSize(10);
        rebalanceMetricGroup
                .getMetrics()
                .values()
                .forEach(
                        metric -> {
                            if (metric instanceof Gauge) {
                                assertThat(((Number) ((Gauge<?>) metric).getValue()).longValue())
                                        .isZero();
                            } else {
                                assertThat(((Counter) metric).getCount()).isZero();
                            }
                        });

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(2);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        rebalanceManager.registerRebalance("success", plan, NOT_STARTED);
        assertThat(gaugeValue(MetricNames.REBALANCE_IN_PROGRESS)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isEqualTo(2);

        metricClock.advanceTime(Duration.ofMillis(500));
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isEqualTo(500);
        assertThat(gaugeValue(MetricNames.INFLIGHT_BUCKET_DURATION_MS)).isEqualTo(500);

        rebalanceManager.finishRebalanceTask(buckets.get(0), COMPLETED);
        metricClock.advanceTime(Duration.ofMillis(200));
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isEqualTo(700);
        assertThat(gaugeValue(MetricNames.INFLIGHT_BUCKET_DURATION_MS)).isEqualTo(200);
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();

        rebalanceManager.finishRebalanceTask(buckets.get(1), COMPLETED);
        rebalanceManager.finishRebalanceTask(buckets.get(1), COMPLETED);
        assertThat(gaugeValue(MetricNames.REBALANCE_IN_PROGRESS)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isEqualTo(2);
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isZero();
        assertThat(gaugeValue(MetricNames.INFLIGHT_BUCKET_DURATION_MS)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isEqualTo(1);

        rebalanceManager.registerRebalance("empty", Collections.emptyMap(), NOT_STARTED);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isEqualTo(2);
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_CANCELED_TOTAL)).isZero();
    }

    @Test
    void testRebalanceFailureMetrics() {
        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        rebalanceManager.registerRebalance("mixed-results", plan, NOT_STARTED);
        rebalanceManager.finishRebalanceTask(buckets.get(0), FAILED);
        rebalanceManager.finishRebalanceTask(buckets.get(1), TIMEOUT);

        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_FAILED)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isEqualTo(1);
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isZero();

        rebalanceManager.finishRebalanceTask(buckets.get(2), COMPLETED);
        rebalanceManager.finishRebalanceTask(buckets.get(0), FAILED);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_FAILED)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_IN_PROGRESS)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isEqualTo(1);
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(COMPLETED);

        rebalanceManager.registerRebalance("next", createRebalancePlan(1), NOT_STARTED);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_FAILED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isEqualTo(1);
    }

    @Test
    void testRebalanceTimeoutMetrics() {
        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(1);
        TableBucket bucket = plan.keySet().iterator().next();
        rebalanceManager.registerRebalance("timeout", plan, NOT_STARTED);
        metricClock.advanceTime(Duration.ofMinutes(3));
        rebalanceManager.checkTimeout();

        assertThat(gaugeValue(MetricNames.INFLIGHT_BUCKET_DURATION_MS)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isEqualTo(180_000);
        rebalanceManager.finishRebalanceTask(bucket, TIMEOUT);

        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isEqualTo(1);
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
    }

    @Test
    void testRebalanceCancellationMetrics() {
        rebalanceManager.cancelRebalance(null);
        assertThat(counterValue(MetricNames.REBALANCES_CANCELED_TOTAL)).isZero();

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(2);
        rebalanceManager.registerRebalance("cancel", plan, NOT_STARTED);
        rebalanceManager.finishRebalanceTask(plan.keySet().iterator().next(), COMPLETED);
        rebalanceManager.cancelRebalance("cancel");
        rebalanceManager.cancelRebalance("cancel");
        assertThat(counterValue(MetricNames.REBALANCES_CANCELED_TOTAL)).isEqualTo(1);
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_IN_PROGRESS)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isEqualTo(1);
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isZero();
        assertThat(gaugeValue(MetricNames.INFLIGHT_BUCKET_DURATION_MS)).isZero();
    }

    @ParameterizedTest
    @EnumSource(
            value = RebalanceStatus.class,
            names = {"COMPLETED", "CANCELED", "FAILED", "TIMEOUT"})
    void testRecoveringFinishedRebalanceDoesNotRestoreOutcomeMetrics(RebalanceStatus status) {
        rebalanceManager.registerRebalance("recovered", createRebalancePlan(1), status);
        assertThat(gaugeValue(MetricNames.REBALANCE_IN_PROGRESS)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_FAILED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_DURATION_MS)).isZero();
        assertThat(gaugeValue(MetricNames.INFLIGHT_BUCKET_DURATION_MS)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_CANCELED_TOTAL)).isZero();

        rebalanceManager.registerRebalance("recovered-empty", Collections.emptyMap(), status);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_FAILED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_CANCELED_TOTAL)).isZero();
    }

    @ParameterizedTest
    @EnumSource(
            value = RebalanceStatus.class,
            names = {"FAILED", "TIMEOUT"})
    void testRecoveringMixedResultsDoesNotReportSuccessfulBuckets(RebalanceStatus failedStatus)
            throws Exception {
        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(2);
        List<TableBucket> buckets = new ArrayList<>(plan.keySet());
        String failureMetric =
                failedStatus == FAILED
                        ? MetricNames.REBALANCE_BUCKETS_FAILED
                        : MetricNames.REBALANCE_BUCKETS_TIMED_OUT;
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("mixed-results", NOT_STARTED, plan));
        rebalanceManager.registerRebalance("mixed-results", plan, NOT_STARTED);
        rebalanceManager.finishRebalanceTask(buckets.get(0), failedStatus);
        rebalanceManager.finishRebalanceTask(buckets.get(1), COMPLETED);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isEqualTo(1);
        assertThat(gaugeValue(failureMetric)).isEqualTo(1);
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isEqualTo(1);

        rebalanceManager.close();
        RecordingEventManager eventManager = new RecordingEventManager();
        rebalanceManager =
                new RebalanceManager(
                        mock(CoordinatorEventProcessor.class),
                        zookeeperClient,
                        eventManager,
                        metricClock,
                        coordinatorMetricGroup,
                        new NoOpScheduledExecutor());
        rebalanceMetricGroup = coordinatorMetricGroup.getOrAddRebalanceMetricGroup();
        rebalanceManager.startup();
        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(RecoverRebalanceEvent.class);
        RebalanceTask recoveredTask =
                ((RecoverRebalanceEvent) eventManager.events.get(0)).getRebalanceTask();
        assertThat(recoveredTask).isEqualTo(new RebalanceTask("mixed-results", COMPLETED, plan));
        rebalanceManager.registerRebalance(
                recoveredTask.getRebalanceId(),
                recoveredTask.getExecutePlan(),
                recoveredTask.getRebalanceStatus());

        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_FAILED)).isZero();
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_TIMED_OUT)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_CANCELED_TOTAL)).isZero();
        assertThat(rebalanceManager.listRebalanceProgress(null).progressForBucketMap().values())
                .hasSize(2)
                .extracting(RebalanceResultForBucket::status)
                .containsOnly(COMPLETED);

        zookeeperClient.registerRebalanceTask(new RebalanceTask("next", NOT_STARTED, plan));
        rebalanceManager.registerRebalance("next", plan, NOT_STARTED);
        rebalanceManager.finishRebalanceTask(buckets.get(0), failedStatus);
        rebalanceManager.finishRebalanceTask(buckets.get(1), COMPLETED);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_COMPLETED)).isEqualTo(1);
        assertThat(gaugeValue(failureMetric)).isEqualTo(1);
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();
        assertThat(counterValue(MetricNames.REBALANCES_FAILED_TOTAL)).isEqualTo(1);
    }

    @Test
    void testRebalanceMetricsResetOnLeadershipChange() {
        rebalanceManager.registerRebalance("first-term", Collections.emptyMap(), NOT_STARTED);
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isEqualTo(1);
        AbstractMetricGroup previousMetricGroup = rebalanceMetricGroup;
        rebalanceManager.close();
        assertThat(previousMetricGroup.isClosed()).isTrue();
        assertThat(coordinatorMetricGroup.isClosed()).isFalse();
        verify(metricRegistry, times(10)).unregister(any(), anyString(), eq(previousMetricGroup));

        rebalanceManager =
                new RebalanceManager(
                        mock(CoordinatorEventProcessor.class),
                        zookeeperClient,
                        new RecordingEventManager(),
                        metricClock,
                        coordinatorMetricGroup,
                        new NoOpScheduledExecutor());
        rebalanceMetricGroup = coordinatorMetricGroup.getOrAddRebalanceMetricGroup();
        assertThat(rebalanceMetricGroup).isNotSameAs(previousMetricGroup);
        verify(metricRegistry, times(10)).register(any(), anyString(), eq(rebalanceMetricGroup));
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isZero();

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(1);
        rebalanceManager.registerRebalance("recovered-active", plan, NOT_STARTED);
        assertThat(gaugeValue(MetricNames.REBALANCE_BUCKETS_PENDING)).isEqualTo(1);
        rebalanceManager.finishRebalanceTask(plan.keySet().iterator().next(), COMPLETED);
        assertThat(counterValue(MetricNames.REBALANCES_COMPLETED_TOTAL)).isEqualTo(1);

        coordinatorMetricGroup.close();
        assertThat(rebalanceMetricGroup.isClosed()).isTrue();
        assertThat(coordinatorMetricGroup.getOrAddRebalanceMetricGroup().isClosed()).isTrue();
    }

    private long gaugeValue(String metricName) {
        return ((Number) ((Gauge<?>) rebalanceMetricGroup.getMetrics().get(metricName)).getValue())
                .longValue();
    }

    private long counterValue(String metricName) {
        return ((Counter) rebalanceMetricGroup.getMetrics().get(metricName)).getCount();
    }

    private static CoordinatorMetricGroup createCoordinatorMetricGroup() {
        return new CoordinatorMetricGroup(NOPMetricRegistry.INSTANCE, "cluster", "host", "0");
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
                createCoordinatorMetricGroup(),
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
