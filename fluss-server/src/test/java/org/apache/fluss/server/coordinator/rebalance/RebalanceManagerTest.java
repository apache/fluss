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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.coordinator.rebalance.goal.ReplicaDistributionGoal;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Stream;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RebalanceManager}. */
public class RebalanceManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static ZkEpoch zkEpoch;

    private TestingRebalanceExecutor rebalanceExecutor;
    private RecordingEventManager eventManager;
    private RebalanceManager rebalanceManager;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("1");
    }

    @BeforeEach
    void beforeEach() throws Exception {
        zookeeperClient.deleteRebalanceTask();
        rebalanceExecutor = new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch));
        eventManager = new RecordingEventManager();
        rebalanceManager =
                new RebalanceManager(
                        rebalanceExecutor,
                        zookeeperClient,
                        eventManager,
                        new ManualClock(),
                        new Configuration(),
                        new NoOpScheduledExecutor());
        rebalanceManager.startup();
    }

    @AfterEach
    void afterEach() throws Exception {
        rebalanceManager.close();
        zookeeperClient.deleteRebalanceTask();
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

        Map<TableBucket, RebalancePlanForBucket> plan = createRebalancePlan(2);
        RebalanceTask rebalanceTask = new RebalanceTask("recover-test", NOT_STARTED, plan);
        zookeeperClient.registerRebalanceTask(rebalanceTask);

        RebalanceManager manager =
                new RebalanceManager(
                        new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch)),
                        zookeeperClient,
                        eventManager,
                        clock,
                        new Configuration(),
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
        RebalanceManager manager =
                new RebalanceManager(
                        new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch)),
                        zookeeperClient,
                        eventManager,
                        clock,
                        new Configuration(),
                        executor);
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = plans(tb1, tb2);
        manager.registerRebalance("timeout-test", plan, NOT_STARTED);
        RebalanceExecutionKey executionKey = manager.getExecutionKey(tb1);

        clock.advanceTime(Duration.ofMillis(100_000));
        manager.checkTimeout();
        assertThat(eventManager.events).isEmpty();

        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();

        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(RebalanceTaskTimeoutEvent.class);
        RebalanceTaskTimeoutEvent timeoutEvent =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        assertThat(timeoutEvent.getExecutionKey()).isEqualTo(executionKey);

        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();
        assertThat(eventManager.events).hasSize(1);

        manager.close();
    }

    @Test
    void testSoftTimeoutAdmitsNextTaskAndTracksLateCompletion() throws Exception {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        TestingRebalanceExecutor executor =
                new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch));
        RebalanceManager manager =
                new RebalanceManager(
                        executor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        new Configuration(),
                        new NoOpScheduledExecutor());
        manager.startup();

        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        manager.registerRebalance("soft-timeout-test", plans(tb1, tb2), NOT_STARTED);
        RebalanceExecutionKey firstAttempt = manager.getExecutionKey(tb1);
        assertThat(executor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1);

        clock.advanceTime(Duration.ofMillis(130_000));
        manager.checkTimeout();
        RebalanceTaskTimeoutEvent timeoutEvent =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        assertThat(manager.timeoutRebalanceTask(timeoutEvent.getExecutionKey())).isTrue();

        RebalanceExecutionKey secondAttempt = manager.getExecutionKey(tb2);
        assertThat(secondAttempt).isNotNull();
        assertThat(executor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2);
        assertThat(manager.listRebalanceProgress(null).status()).isEqualTo(REBALANCING);
        assertThat(manager.listRebalanceProgress(null).progressForBucketMap().get(tb1).status())
                .isEqualTo(TIMEOUT);
        assertThat(eventManager.events.get(1)).isInstanceOf(ReconcileRebalanceTaskEvent.class);

        RebalancePlanForBucket retryPlan = manager.getPlanForReconciliation(firstAttempt);
        assertThat(retryPlan).isNotNull();
        assertThat(retryPlan.getTableBucket()).isEqualTo(tb1);
        // the dispatched reconciliation backs off, so no event is enqueued right away.
        manager.checkTimeout();
        assertThat(eventManager.events).hasSize(2);

        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();
        assertThat(eventManager.events).hasSize(3);
        assertThat(eventManager.events.get(2)).isInstanceOf(ReconcileRebalanceTaskEvent.class);
        assertThat(((ReconcileRebalanceTaskEvent) eventManager.events.get(2)).getExecutionKey())
                .isEqualTo(firstAttempt);

        assertThat(manager.timeoutRebalanceTask(firstAttempt)).isFalse();
        assertThat(manager.finishRebalanceTask(firstAttempt, COMPLETED)).isTrue();
        assertThat(manager.finishRebalanceTask(firstAttempt, COMPLETED)).isFalse();
        assertThat(manager.finishRebalanceTask(secondAttempt, COMPLETED)).isTrue();

        assertThat(manager.getRebalanceStatus()).isEqualTo(COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(COMPLETED);

        manager.close();
    }

    @Test
    void testFailureIsAggregatedIntoOverallStatus() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        rebalanceManager.registerRebalance("failed-test", plans(tb1, tb2), NOT_STARTED);
        rebalanceManager.finishRebalanceTask(tb1, FAILED);
        rebalanceManager.finishRebalanceTask(tb2, COMPLETED);

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);
    }

    @Test
    void testCancelPersistsIntentAndDrainsOnlyAdmittedTasks() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        rebalanceManager.registerRebalance("cancel-test", plans(tb1, tb2), NOT_STARTED);
        RebalanceExecutionKey runningAttempt = rebalanceManager.getExecutionKey(tb1);

        rebalanceManager.cancelRebalance("cancel-test");

        RebalanceTask storedTask = zookeeperClient.getRebalanceTask().get();
        assertThat(storedTask.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(storedTask.isCancelRequested()).isTrue();
        assertThat(rebalanceManager.isCancelRequested()).isTrue();
        assertThat(
                        rebalanceManager
                                .listRebalanceProgress(null)
                                .progressForBucketMap()
                                .get(tb2)
                                .status())
                .isEqualTo(CANCELED);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1);

        rebalanceManager.finishRebalanceTask(runningAttempt, COMPLETED);

        storedTask = zookeeperClient.getRebalanceTask().get();
        assertThat(storedTask.getRebalanceStatus()).isEqualTo(CANCELED);
        assertThat(storedTask.isCancelRequested()).isTrue();
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
    }

    @Test
    void testRecoverReconcilesCompletedAndIntermediateBuckets() {
        TableBucket completedBucket = new TableBucket(1L, 0);
        TableBucket intermediateBucket = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plans = plans(completedBucket, intermediateBucket);
        rebalanceExecutor.completedBuckets.add(completedBucket);

        rebalanceManager.recoverRebalance(new RebalanceTask("recover-test", REBALANCING, plans));

        Map<TableBucket, RebalanceStatus> statuses = statuses(rebalanceManager);
        assertThat(statuses.get(completedBucket)).isEqualTo(COMPLETED);
        assertThat(statuses.get(intermediateBucket)).isEqualTo(REBALANCING);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(intermediateBucket);
    }

    @Test
    void testRecoverCancellationKeepsIntermediateBucketTracked() throws Exception {
        TableBucket originBucket = new TableBucket(1L, 0);
        TableBucket intermediateBucket = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plans = plans(originBucket, intermediateBucket);
        rebalanceExecutor.originBuckets.add(originBucket);

        rebalanceManager.recoverRebalance(
                new RebalanceTask("recover-cancel-test", REBALANCING, plans, true));

        Map<TableBucket, RebalanceStatus> statuses = statuses(rebalanceManager);
        assertThat(statuses.get(originBucket)).isEqualTo(CANCELED);
        assertThat(statuses.get(intermediateBucket)).isEqualTo(REBALANCING);
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(intermediateBucket);
        rebalanceManager.finishRebalanceTask(attempt, COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(CANCELED);
    }

    @Test
    void testRecoverFinalTaskDoesNotExecuteAgain() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.recoverRebalance(
                new RebalanceTask("final-test", COMPLETED, plans(tableBucket)));

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(COMPLETED);
        assertThat(rebalanceExecutor.executedPlans).isEmpty();
    }

    @Test
    void testReconciliationBacksOffBetweenRetries() {
        ManualClock clock = new ManualClock(0L);
        RecordingEventManager eventManager = new RecordingEventManager();
        RebalanceManager manager = newManager(clock, eventManager, rebalanceExecutor);

        TableBucket tableBucket = new TableBucket(1L, 0);
        manager.registerRebalance("backoff-test", plans(tableBucket), NOT_STARTED);
        RebalanceExecutionKey attempt = manager.getExecutionKey(tableBucket);
        assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(1);

        // first retry is dispatched at the base interval, the next one only after twice that.
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(2);

        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(2);

        clock.advanceTime(Duration.ofMillis(30_000));
        manager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(3);

        manager.close();
    }

    @ParameterizedTest
    @MethodSource("trackedTimedOutTaskLimits")
    void testTrackedTimedOutTasksAreCapped(Configuration conf, int limit) {
        ManualClock clock = new ManualClock(0L);
        TestingRebalanceExecutor executor =
                new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch));
        RebalanceManager manager = newManager(clock, new RecordingEventManager(), executor, conf);

        TableBucket[] tableBuckets = new TableBucket[limit + 2];
        for (int i = 0; i < tableBuckets.length; i++) {
            tableBuckets[i] = new TableBucket(1L, i);
        }
        manager.registerRebalance("cap-test", plans(tableBuckets), NOT_STARTED);

        // every timed-out task keeps being tracked, so admitting new work has to stop at the cap.
        List<RebalanceExecutionKey> timedOut = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            TableBucket running =
                    executor.executedPlans.get(executor.executedPlans.size() - 1).getTableBucket();
            RebalanceExecutionKey attempt = manager.getExecutionKey(running);
            assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();
            timedOut.add(attempt);
        }
        assertThat(executor.executedPlans).hasSize(limit);

        // once a tracked task reaches a final status the next pending task is admitted again.
        assertThat(manager.finishRebalanceTask(timedOut.get(0), COMPLETED)).isTrue();
        assertThat(executor.executedPlans).hasSize(limit + 1);

        manager.close();
    }

    @ParameterizedTest
    @MethodSource("targetUnavailableTimeouts")
    void testTimedOutTaskFailsWhenTargetReplicasStayUnavailable(
            Configuration conf, Duration timeout) throws Exception {
        ManualClock clock = new ManualClock(0L);
        // no tablet server is live, so the target replicas can never catch up.
        RebalanceManager manager =
                newManager(clock, new RecordingEventManager(), rebalanceExecutor, conf);

        TableBucket tableBucket = new TableBucket(1L, 0);
        manager.registerRebalance("give-up-test", plans(tableBucket), NOT_STARTED);
        RebalanceExecutionKey attempt = manager.getExecutionKey(tableBucket);
        assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();

        clock.advanceTime(timeout);
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(manager.getPlanForReconciliation(attempt)).isNull();

        // the rebalance reaches a final status, so later rebalance requests are not blocked.
        assertThat(manager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(manager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);

        manager.close();
    }

    @Test
    void testTimedOutTaskKeepsRetryingWhileTargetReplicasAreLive() {
        ManualClock clock = new ManualClock(0L);
        CoordinatorContext coordinatorContext = new CoordinatorContext(zkEpoch);
        // the plans target replicas 1, 2 and 3, so the migration can still make progress.
        for (int serverId : new int[] {1, 2, 3}) {
            coordinatorContext.addLiveTabletServer(tabletServer(serverId));
        }
        RebalanceManager manager =
                newManager(
                        clock,
                        new RecordingEventManager(),
                        new TestingRebalanceExecutor(coordinatorContext));

        TableBucket tableBucket = new TableBucket(1L, 0);
        manager.registerRebalance("keep-retrying-test", plans(tableBucket), NOT_STARTED);
        RebalanceExecutionKey attempt = manager.getExecutionKey(tableBucket);
        assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();

        clock.advanceTime(Duration.ofMinutes(31));
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
        assertThat(manager.getRebalanceStatus()).isEqualTo(REBALANCING);

        manager.close();
    }

    @ParameterizedTest
    @MethodSource("noProgressTimeouts")
    void testTimedOutTaskFailsWithoutProgressWhileTargetsAreLive(
            Configuration conf, Duration timeout) throws Exception {
        ManualClock clock = new ManualClock(0L);
        CoordinatorContext context = new CoordinatorContext(zkEpoch);
        for (int serverId : new int[] {1, 2, 3}) {
            context.addLiveTabletServer(tabletServer(serverId));
        }
        TestingRebalanceExecutor executor = new TestingRebalanceExecutor(context);
        RebalanceManager manager = newManager(clock, new RecordingEventManager(), executor, conf);
        TableBucket tableBucket = new TableBucket(1L, 0);
        manager.registerRebalance("no-progress-test", plans(tableBucket), NOT_STARTED);
        RebalanceExecutionKey attempt = manager.getExecutionKey(tableBucket);
        assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();

        clock.advanceTime(timeout.minusMillis(1));
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
        assertThat(manager.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(manager.hasInProgressRebalance()).isTrue();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(manager.getPlanForReconciliation(attempt)).isNull();
        assertThat(manager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(manager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);

        TableBucket nextBucket = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> nextPlan = plans(nextBucket);
        manager.registerRebalance("after-no-progress-timeout", nextPlan, NOT_STARTED);
        assertThat(manager.getRebalanceId()).isEqualTo("after-no-progress-timeout");
        assertThat(manager.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(manager.hasInProgressRebalance()).isTrue();
        assertThat(executor.executedPlans).hasSize(2).last().isEqualTo(nextPlan.get(nextBucket));
        manager.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGenerateRebalanceAfterFailureAndRecovery(boolean targetUnavailable) throws Exception {
        ManualClock clock = new ManualClock(0L);
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        for (int serverId : new int[] {0, 1, 2, 3, 4}) {
            if (!targetUnavailable || serverId != 3) {
                context.addLiveTabletServer(tabletServer(serverId));
            }
        }
        context.putServerTag(0, ServerTag.PERMANENT_OFFLINE);
        TableBucket bucket = new TableBucket(1L, 0);
        putBucketForPlanning(context, bucket, Arrays.asList(0, 1, 2, 3));
        RebalanceManager manager =
                newManager(clock, new RecordingEventManager(), rebalanceExecutor);
        try {
            manager.registerRebalance("failed-migration", plans(bucket), NOT_STARTED);
            RebalanceExecutionKey attempt = manager.getExecutionKey(bucket);
            manager.timeoutRebalanceTask(attempt);
            assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();
            clock.advanceTime(targetUnavailable ? Duration.ofMinutes(31) : Duration.ofHours(25));
            assertThat(manager.getPlanForReconciliation(attempt)).isNull();
            assertThat(manager.hasInProgressRebalance()).isFalse();

            RebalanceTask persistedTask = zookeeperClient.getRebalanceTask().get();
            assertThat(persistedTask.getRebalanceStatus()).isEqualTo(FAILED);
            manager.close();
            manager = newManager(clock, new RecordingEventManager(), rebalanceExecutor);
            manager.recoverRebalance(persistedTask);
            RebalanceTask next =
                    manager.generateRebalanceTask(
                            Collections.singletonList(new ReplicaDistributionGoal()));
            RebalancePlanForBucket nextPlan = next.getExecutePlan().get(bucket);
            assertThat(nextPlan).isNotNull();
            assertThat(nextPlan.getOriginReplicas()).containsExactly(0, 1, 2, 3);
            assertThat(nextPlan.getNewReplicas()).hasSize(3).contains(1, 2).doesNotContain(0);
            assertThat(context.liveTabletServerSet()).containsAll(nextPlan.getNewReplicas());
            // Planning must leave the real assignment intact until the replacement task executes.
            assertThat(context.getAssignment(bucket)).containsExactly(0, 1, 2, 3);
            manager.registerRebalance(next.getRebalanceId(), next.getExecutePlan(), NOT_STARTED);
            assertThat(rebalanceExecutor.executedPlans).hasSize(2).last().isEqualTo(nextPlan);
        } finally {
            manager.close();
        }
    }

    @Test
    void testGenerateRebalanceReplacesUnavailableReplica() {
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        for (int serverId : new int[] {0, 1, 4}) {
            context.addLiveTabletServer(tabletServer(serverId));
        }
        TableBucket bucket = new TableBucket(1L, 0);
        putBucketForPlanning(context, bucket, Arrays.asList(0, 1, 2));

        RebalanceTask task =
                rebalanceManager.generateRebalanceTask(
                        Collections.singletonList(new ReplicaDistributionGoal()));
        assertThat(task.getExecutePlan().get(bucket).getNewReplicas())
                .containsExactlyInAnyOrder(0, 1, 4);
    }

    @Test
    void testGenerateCleanupPlanWithoutOptimizationChanges() {
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        for (int serverId : new int[] {0, 1, 2, 3}) {
            context.addLiveTabletServer(tabletServer(serverId));
        }
        // The replica count comes from table metadata, even when the previous plan is gone.
        TableBucket bucket = new TableBucket(1L, 10L, 0);
        putBucketForPlanning(context, bucket, Arrays.asList(0, 1, 2, 3));

        RebalanceTask task =
                rebalanceManager.generateRebalanceTask(
                        Collections.singletonList(new ReplicaDistributionGoal()));
        assertThat(task.getExecutePlan()).containsKey(bucket);
        assertThat(task.getExecutePlan().get(bucket).getOriginReplicas())
                .containsExactly(0, 1, 2, 3);
        assertThat(task.getExecutePlan().get(bucket).getNewReplicas()).containsExactly(0, 1, 2);
    }

    private static void putBucketForPlanning(
            CoordinatorContext context, TableBucket bucket, List<Integer> assignment) {
        TableDescriptor descriptor = DATA1_TABLE_DESCRIPTOR.withReplicationFactor(3);
        context.putTableInfo(
                TableInfo.of(
                        TablePath.of("db", "table"),
                        bucket.getTableId(),
                        1,
                        descriptor,
                        "file:///tmp/rebalance-planning",
                        0L,
                        0L));
        context.updateBucketReplicaAssignment(bucket, assignment);
        context.putBucketLeaderAndIsr(
                bucket,
                new LeaderAndIsr(
                        0,
                        1,
                        Arrays.asList(0, 1, 2),
                        Collections.emptyList(),
                        context.getCoordinatorEpoch(),
                        1));
    }

    @ParameterizedTest
    @MethodSource("invalidTimeouts")
    void testRejectsInvalidTimeout(ConfigOption<Duration> option, Duration timeout) {
        Configuration conf = new Configuration().set(option, timeout);
        assertThatThrownBy(
                        () -> newManager(new ManualClock(), eventManager, rebalanceExecutor, conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(option.key())
                .hasMessageContaining("must be between 1 ms");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectsInvalidTrackedTimedOutTaskLimit(int limit) {
        Configuration conf =
                new Configuration().set(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS, limit);
        assertThatThrownBy(
                        () -> newManager(new ManualClock(), eventManager, rebalanceExecutor, conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS.key())
                .hasMessageContaining("must be at least 1");
    }

    @ParameterizedTest
    @ValueSource(longs = {1, Long.MAX_VALUE})
    void testAcceptsTimeoutConfigurationBounds(long timeoutMs) {
        Configuration conf =
                new Configuration()
                        .set(
                                COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT,
                                Duration.ofMillis(timeoutMs))
                        .set(
                                COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT,
                                Duration.ofMillis(timeoutMs));
        RebalanceManager manager =
                newManager(new ManualClock(), eventManager, rebalanceExecutor, conf);
        manager.close();
    }

    private static Stream<Arguments> targetUnavailableTimeouts() {
        return Stream.of(
                Arguments.of(new Configuration(), Duration.ofMinutes(30)),
                Arguments.of(
                        new Configuration()
                                .set(
                                        COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT,
                                        Duration.ofMinutes(5)),
                        Duration.ofMinutes(5)),
                Arguments.of(
                        new Configuration()
                                .set(
                                        COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT,
                                        Duration.ofHours(1)),
                        Duration.ofHours(1)));
    }

    private static Stream<Arguments> noProgressTimeouts() {
        return Stream.of(
                Arguments.of(new Configuration(), Duration.ofHours(24)),
                Arguments.of(
                        new Configuration()
                                .set(
                                        COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT,
                                        Duration.ofMinutes(5)),
                        Duration.ofMinutes(5)),
                Arguments.of(
                        new Configuration()
                                .set(
                                        COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT,
                                        Duration.ofHours(48)),
                        Duration.ofHours(48)));
    }

    private static Stream<Arguments> trackedTimedOutTaskLimits() {
        return Stream.of(
                Arguments.of(new Configuration(), 8),
                Arguments.of(
                        new Configuration()
                                .set(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS, 1),
                        1),
                Arguments.of(
                        new Configuration()
                                .set(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS, 3),
                        3));
    }

    private static Stream<Arguments> invalidTimeouts() {
        return Stream.of(
                        COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT,
                        COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT)
                .flatMap(
                        option ->
                                Stream.of(
                                                Duration.ZERO,
                                                Duration.ofMillis(-1),
                                                Duration.ofNanos(999_999),
                                                Duration.ofMillis(Long.MAX_VALUE).plusMillis(1))
                                        .map(timeout -> Arguments.of(option, timeout)));
    }

    @Test
    void testCancelGivesUpImmediatelyOnAdmittedTaskStillAtOrigin() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        rebalanceExecutor.originBuckets.add(tb1);
        rebalanceManager.registerRebalance("cancel-at-origin-test", plans(tb1, tb2), NOT_STARTED);

        rebalanceManager.cancelRebalance("cancel-at-origin-test");

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(CANCELED);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(CANCELED);
    }

    private RebalanceManager newManager(
            ManualClock clock,
            RecordingEventManager eventManager,
            TestingRebalanceExecutor executor) {
        return newManager(clock, eventManager, executor, new Configuration());
    }

    private RebalanceManager newManager(
            ManualClock clock,
            RecordingEventManager eventManager,
            TestingRebalanceExecutor executor,
            Configuration conf) {
        RebalanceManager manager =
                new RebalanceManager(
                        executor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        conf,
                        new NoOpScheduledExecutor());
        manager.startup();
        return manager;
    }

    private static int reconciliationsFor(
            RecordingEventManager eventManager, RebalanceExecutionKey executionKey) {
        int reconciliations = 0;
        for (CoordinatorEvent event : eventManager.events) {
            if (event instanceof ReconcileRebalanceTaskEvent
                    && ((ReconcileRebalanceTaskEvent) event)
                            .getExecutionKey()
                            .equals(executionKey)) {
                reconciliations++;
            }
        }
        return reconciliations;
    }

    private static ServerInfo tabletServer(int serverId) {
        return new ServerInfo(
                serverId,
                "RACK" + serverId,
                Endpoint.fromListenersString("CLIENT://host" + serverId + ":9124"),
                ServerType.TABLET_SERVER);
    }

    @Test
    void testStartupFencesNewRebalanceUntilRecoveryEventRuns() throws Exception {
        TableBucket tableBucket = new TableBucket(1L, 0);
        RebalanceTask storedTask =
                new RebalanceTask("startup-recovery-test", REBALANCING, plans(tableBucket));
        zookeeperClient.registerRebalanceTask(storedTask);
        TestingRebalanceExecutor executor =
                new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch));
        RecordingEventManager recordingEventManager = new RecordingEventManager();
        RebalanceManager recoveringManager =
                new RebalanceManager(
                        executor,
                        zookeeperClient,
                        recordingEventManager,
                        new ManualClock(),
                        new Configuration(),
                        new NoOpScheduledExecutor());

        recoveringManager.startup();

        assertThat(recoveringManager.hasInProgressRebalance()).isTrue();
        assertThat(recoveringManager.getRebalanceId()).isNull();
        assertThat(recordingEventManager.events).hasSize(1);
        RecoverRebalanceEvent recoveryEvent =
                (RecoverRebalanceEvent) recordingEventManager.events.get(0);
        assertThat(recoveryEvent.getRebalanceTask()).isEqualTo(storedTask);

        recoveringManager.recoverRebalance(recoveryEvent.getRebalanceTask());
        assertThat(recoveringManager.getRebalanceId()).isEqualTo("startup-recovery-test");
        assertThat(executor.executedPlans).hasSize(1);
        recoveringManager.close();
    }

    private static Map<TableBucket, RebalancePlanForBucket> plans(TableBucket... tableBuckets) {
        Map<TableBucket, RebalancePlanForBucket> plans = new LinkedHashMap<>();
        for (TableBucket tableBucket : tableBuckets) {
            plans.put(
                    tableBucket,
                    new RebalancePlanForBucket(
                            tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 3)));
        }
        return plans;
    }

    private static Map<TableBucket, RebalanceStatus> statuses(RebalanceManager manager) {
        Map<TableBucket, RebalanceStatus> statuses = new HashMap<>();
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                manager.listRebalanceProgress(null).progressForBucketMap().entrySet()) {
            statuses.put(entry.getKey(), entry.getValue().status());
        }
        return statuses;
    }

    private static final class TestingRebalanceExecutor implements RebalanceExecutor {
        private final CoordinatorContext coordinatorContext;
        private final List<RebalancePlanForBucket> executedPlans = new ArrayList<>();
        private final Set<TableBucket> completedBuckets = new HashSet<>();
        private final Set<TableBucket> originBuckets = new HashSet<>();

        private TestingRebalanceExecutor(CoordinatorContext coordinatorContext) {
            this.coordinatorContext = coordinatorContext;
        }

        @Override
        public CoordinatorContext getCoordinatorContext() {
            return coordinatorContext;
        }

        @Override
        public void tryToExecuteRebalanceTask(RebalancePlanForBucket planForBucket) {
            executedPlans.add(planForBucket);
        }

        @Override
        public boolean isRebalanceTaskComplete(RebalancePlanForBucket planForBucket) {
            return completedBuckets.contains(planForBucket.getTableBucket());
        }

        @Override
        public boolean isRebalanceTaskAtOrigin(RebalancePlanForBucket planForBucket) {
            return originBuckets.contains(planForBucket.getTableBucket());
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
