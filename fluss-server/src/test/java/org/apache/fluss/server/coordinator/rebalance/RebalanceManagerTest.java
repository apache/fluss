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
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
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
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RebalanceExecution;
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
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testRegisterGeneratedRebalanceTaskSplitsIntoRoundsAndCompletes() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 2);
        RebalanceManager manager = createTestingRebalanceManager(conf);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(5);
        RebalanceTask firstRoundTask =
                manager.registerGeneratedRebalanceTask(
                        new RebalanceTask("round-test", NOT_STARTED, plan));

        assertThat(firstRoundTask.getExecutePlan()).hasSize(2);
        assertThat(zookeeperClient.getRebalanceTask().get().getExecutePlan()).hasSize(2);
        assertThat(zookeeperClient.getRebalanceRounds()).hasSize(3);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("round-test", REBALANCING, 2, 0, 3));
        assertThat(manager.listRebalanceProgress(null).progressForBucketMap()).hasSize(5);

        finishCurrentRound(manager, COMPLETED);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("round-test", REBALANCING, 2, 1, 3));
        assertThat(zookeeperClient.getRebalanceTask().get().getExecutePlan()).hasSize(2);

        finishCurrentRound(manager, COMPLETED);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("round-test", REBALANCING, 2, 2, 3));
        assertThat(zookeeperClient.getRebalanceTask().get().getExecutePlan()).hasSize(1);

        finishCurrentRound(manager, COMPLETED);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("round-test", COMPLETED, 2, 2, 3));
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(COMPLETED);
        assertThat(manager.hasInProgressRebalance()).isFalse();
        RebalanceProgress progress = manager.listRebalanceProgress(null);
        assertThat(progress.status()).isEqualTo(COMPLETED);
        assertThat(progress.progressForBucketMap()).hasSize(5);
        assertThat(progress.progressForBucketMap().values())
                .allSatisfy(result -> assertThat(result.status()).isEqualTo(COMPLETED));

        manager.close();
    }

    @Test
    void testRegisterGeneratedRebalanceTaskKeepsLegacyTaskWhenRoundLimitDisabled()
            throws Exception {
        RebalanceManager manager = createTestingRebalanceManager(new Configuration());

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        RebalanceTask rebalanceTask =
                manager.registerGeneratedRebalanceTask(
                        new RebalanceTask("legacy-test", NOT_STARTED, plan));

        assertThat(rebalanceTask.getExecutePlan()).hasSize(3);
        assertThat(zookeeperClient.getRebalanceTask()).isPresent();
        RebalanceTask persistedTask = zookeeperClient.getRebalanceTask().get();
        assertThat(persistedTask.getRebalanceId()).isEqualTo("legacy-test");
        assertThat(persistedTask.getRebalanceStatus()).isEqualTo(NOT_STARTED);
        assertThat(persistedTask.getExecutePlan()).isEqualTo(plan);
        assertThat(zookeeperClient.getRebalanceExecution()).isEmpty();
        assertThat(manager.listRebalanceProgress(null).progressForBucketMap()).hasSize(3);

        manager.close();
    }

    @Test
    void testRegisterGeneratedRebalanceTaskKeepsLegacyTaskWhenPlanFitsRoundLimit()
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 3);
        RebalanceManager manager = createTestingRebalanceManager(conf);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        RebalanceTask rebalanceTask =
                manager.registerGeneratedRebalanceTask(
                        new RebalanceTask("small-plan-test", NOT_STARTED, plan));

        assertThat(rebalanceTask.getExecutePlan()).hasSize(3);
        assertThat(zookeeperClient.getRebalanceTask()).isPresent();
        RebalanceTask persistedTask = zookeeperClient.getRebalanceTask().get();
        assertThat(persistedTask.getRebalanceId()).isEqualTo("small-plan-test");
        assertThat(persistedTask.getRebalanceStatus()).isEqualTo(NOT_STARTED);
        assertThat(persistedTask.getExecutePlan()).isEqualTo(plan);
        assertThat(zookeeperClient.getRebalanceExecution()).isEmpty();
        assertThat(manager.listRebalanceProgress(null).progressForBucketMap()).hasSize(3);

        manager.close();
    }

    @Test
    void testRoundBasedRebalanceHandlesPartitionedBuckets() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 2);
        RebalanceManager manager = createTestingRebalanceManager(conf);

        Map<TableBucket, RebalancePlanForBucket> plan = createPartitionedRebalancePlan(3);
        manager.registerGeneratedRebalanceTask(
                new RebalanceTask("partitioned-round-test", NOT_STARTED, plan));

        assertThat(zookeeperClient.getRebalanceRounds()).hasSize(2);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("partitioned-round-test", REBALANCING, 2, 0, 2));
        assertThat(manager.listRebalanceProgress(null).progressForBucketMap())
                .containsOnlyKeys(plan.keySet());

        finishCurrentRound(manager, COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask().get().getExecutePlan())
                .containsOnlyKeys(new TableBucket(1L, 10L, 2));

        finishCurrentRound(manager, COMPLETED);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("partitioned-round-test", COMPLETED, 2, 1, 2));
        assertThat(manager.listRebalanceProgress(null).progressForBucketMap().values())
                .allSatisfy(result -> assertThat(result.status()).isEqualTo(COMPLETED));

        manager.close();
    }

    @Test
    void testRoundBasedRebalanceRestoresFromZooKeeper() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 2);
        RebalanceManager manager = createTestingRebalanceManager(conf);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        manager.registerGeneratedRebalanceTask(
                new RebalanceTask("restore-test", NOT_STARTED, plan));
        finishCurrentRound(manager, COMPLETED);
        manager.close();

        RebalanceManager restoredManager = createTestingRebalanceManager(conf);
        assertThat(restoredManager.hasInProgressRebalance()).isTrue();
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("restore-test", REBALANCING, 2, 1, 2));
        assertThat(restoredManager.listRebalanceProgress(null).progressForBucketMap()).hasSize(3);

        finishCurrentRound(restoredManager, COMPLETED);
        assertThat(zookeeperClient.getRebalanceExecution())
                .hasValue(new RebalanceExecution("restore-test", COMPLETED, 2, 1, 2));
        assertThat(restoredManager.listRebalanceProgress(null).progressForBucketMap()).hasSize(3);

        restoredManager.close();
    }

    @Test
    void testCancelRoundBasedRebalanceMarksAllUnfinishedBuckets() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND, 2);
        RebalanceManager manager = createTestingRebalanceManager(conf);

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(3);
        manager.registerGeneratedRebalanceTask(new RebalanceTask("cancel-test", NOT_STARTED, plan));
        TableBucket firstBucket =
                zookeeperClient
                        .getRebalanceTask()
                        .get()
                        .getExecutePlan()
                        .keySet()
                        .iterator()
                        .next();
        manager.finishRebalanceTask(firstBucket, COMPLETED);
        manager.cancelRebalance("cancel-test");

        Optional<RebalanceExecution> rebalanceExecution = zookeeperClient.getRebalanceExecution();
        assertThat(rebalanceExecution)
                .hasValue(new RebalanceExecution("cancel-test", CANCELED, 2, 0, 2));
        RebalanceProgress progress = manager.listRebalanceProgress(null);
        assertThat(progress.status()).isEqualTo(CANCELED);
        assertThat(progress.progressForBucketMap()).hasSize(3);
        assertThat(progress.progressForBucketMap().get(firstBucket).status()).isEqualTo(COMPLETED);
        assertThat(progress.progressForBucketMap().values())
                .allSatisfy(result -> assertThat(result.status()).isIn(COMPLETED, CANCELED));

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
                        eventProcessor, zookeeperClient, eventManager, clock, executor);
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

    private RebalanceManager createTestingRebalanceManager(Configuration conf) {
        return createTestingRebalanceManager(conf, SystemClock.getInstance());
    }

    private RebalanceManager createTestingRebalanceManager(
            Configuration conf, org.apache.fluss.utils.clock.Clock clock) {
        RebalanceManager manager =
                new RebalanceManager(
                        buildCoordinatorEventProcessor(conf),
                        zookeeperClient,
                        new RecordingEventManager(),
                        clock,
                        new NoOpScheduledExecutor(),
                        conf);
        manager.startup();
        return manager;
    }

    private Map<TableBucket, RebalancePlanForBucket> createRebalancePlan(int bucketCount) {
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

    private Map<TableBucket, RebalancePlanForBucket> createPartitionedRebalancePlan(
            int bucketCount) {
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            TableBucket tableBucket = new TableBucket(1L, 10L, bucketId);
            plan.put(
                    tableBucket,
                    new RebalancePlanForBucket(
                            tableBucket, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
        }
        return plan;
    }

    private void finishCurrentRound(RebalanceManager manager, RebalanceStatus status)
            throws Exception {
        List<TableBucket> currentRoundBuckets =
                new ArrayList<>(zookeeperClient.getRebalanceTask().get().getExecutePlan().keySet());
        for (TableBucket tableBucket : currentRoundBuckets) {
            manager.finishRebalanceTask(tableBucket, status);
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
