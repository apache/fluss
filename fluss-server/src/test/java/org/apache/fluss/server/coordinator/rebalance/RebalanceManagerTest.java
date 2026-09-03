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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
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
    private ManualClock clock;
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
        clock = new ManualClock();
        rebalanceManager =
                new RebalanceManager(
                        rebalanceExecutor,
                        zookeeperClient,
                        eventManager,
                        clock,
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
    void testRegisterRebalanceRespectsMaxInflightTasks() {
        rebalanceManager.updateMaxInflightRebalanceTasks(2);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        TableBucket tb4 = new TableBucket(1L, 3);

        rebalanceManager.registerRebalance(
                "concurrent-test", plans(tb1, tb2, tb3, tb4), NOT_STARTED);

        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2);
        assertThat(countStatus(rebalanceManager, REBALANCING)).isEqualTo(2);
        assertThat(countStatus(rebalanceManager, NOT_STARTED)).isEqualTo(2);

        rebalanceManager.finishRebalanceTask(tb1, COMPLETED);

        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3);
        assertThat(countStatus(rebalanceManager, REBALANCING)).isEqualTo(2);
    }

    @Test
    void testIncreaseMaxInflightTasksStartsPendingTasks() {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        TableBucket tb4 = new TableBucket(1L, 3);
        rebalanceManager.registerRebalance("scale-up-test", plans(tb1, tb2, tb3, tb4), NOT_STARTED);

        assertThat(rebalanceManager.getMaxInflightRebalanceTasks()).isEqualTo(1);
        assertThat(rebalanceExecutor.executedPlans).hasSize(1);

        rebalanceManager.updateMaxInflightRebalanceTasks(3);

        assertThat(rebalanceManager.getMaxInflightRebalanceTasks()).isEqualTo(3);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3);
    }

    @Test
    void testZeroMaxInflightTasksPausesAndResumesScheduling() {
        rebalanceManager.updateMaxInflightRebalanceTasks(0);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        rebalanceManager.registerRebalance("pause-test", plans(tb1, tb2, tb3), NOT_STARTED);

        assertThat(rebalanceManager.getMaxInflightRebalanceTasks()).isZero();
        assertThat(rebalanceExecutor.executedPlans).isEmpty();
        assertThat(countStatus(rebalanceManager, NOT_STARTED)).isEqualTo(3);
        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();

        rebalanceManager.updateMaxInflightRebalanceTasks(2);

        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2);
        assertThat(countStatus(rebalanceManager, REBALANCING)).isEqualTo(2);
    }

    @Test
    void testDecreaseMaxInflightTasksDoesNotCancelRunningTasks() {
        rebalanceManager.updateMaxInflightRebalanceTasks(3);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        TableBucket tb4 = new TableBucket(1L, 3);
        TableBucket tb5 = new TableBucket(1L, 4);
        rebalanceManager.registerRebalance(
                "scale-down-test", plans(tb1, tb2, tb3, tb4, tb5), NOT_STARTED);

        rebalanceManager.updateMaxInflightRebalanceTasks(1);

        assertThat(rebalanceExecutor.executedPlans).hasSize(3);
        rebalanceManager.finishRebalanceTask(tb1, COMPLETED);
        rebalanceManager.finishRebalanceTask(tb2, COMPLETED);
        assertThat(rebalanceExecutor.executedPlans).hasSize(3);

        rebalanceManager.finishRebalanceTask(tb3, COMPLETED);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3, tb4);
    }

    @Test
    void testTimeoutEnqueuesEventsForAllInflightTasks() {
        rebalanceManager.updateMaxInflightRebalanceTasks(2);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        rebalanceManager.registerRebalance("timeout-all-test", plans(tb1, tb2, tb3), NOT_STARTED);

        clock.advanceTime(Duration.ofMillis(130_000));
        rebalanceManager.checkTimeout();

        assertThat(eventManager.events)
                .filteredOn(RebalanceTaskTimeoutEvent.class::isInstance)
                .extracting(
                        event ->
                                ((RebalanceTaskTimeoutEvent) event)
                                        .getExecutionKey()
                                        .getTableBucket())
                .containsExactlyInAnyOrder(tb1, tb2);
        RebalanceTaskTimeoutEvent firstTimeout =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        RebalanceTaskTimeoutEvent secondTimeout =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(1);

        assertThat(rebalanceManager.timeoutRebalanceTask(firstTimeout.getExecutionKey())).isTrue();
        assertThat(rebalanceManager.timeoutRebalanceTask(secondTimeout.getExecutionKey())).isTrue();
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3);
    }

    @Test
    void testTimeoutTrackingCapIsPreservedWithConcurrentTasks() {
        rebalanceManager.updateMaxInflightRebalanceTasks(10);
        TableBucket[] tableBuckets = new TableBucket[10];
        for (int i = 0; i < tableBuckets.length; i++) {
            tableBuckets[i] = new TableBucket(1L, i);
        }
        rebalanceManager.registerRebalance(
                "concurrent-timeout-cap-test", plans(tableBuckets), NOT_STARTED);
        List<RebalanceExecutionKey> attempts = new ArrayList<>();
        for (TableBucket tableBucket : tableBuckets) {
            attempts.add(rebalanceManager.getExecutionKey(tableBucket));
        }

        for (int i = 0; i < 8; i++) {
            assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(i))).isTrue();
        }
        assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(8))).isFalse();
        assertThat(countStatus(rebalanceManager, TIMEOUT)).isEqualTo(8);
        assertThat(countStatus(rebalanceManager, REBALANCING)).isEqualTo(2);

        assertThat(rebalanceManager.finishRebalanceTask(attempts.get(0), COMPLETED)).isTrue();
        assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(8))).isTrue();
        assertThat(countStatus(rebalanceManager, TIMEOUT)).isEqualTo(8);
        assertThat(countStatus(rebalanceManager, REBALANCING)).isEqualTo(1);
    }

    @Test
    void testRejectNegativeMaxInflightTasks() {
        Configuration invalidConfig = new Configuration();
        invalidConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, -1);

        assertThatThrownBy(() -> rebalanceManager.validate(invalidConfig))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must be non-negative");
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

    @Test
    void testTrackedTimedOutTasksAreCapped() {
        ManualClock clock = new ManualClock(0L);
        TestingRebalanceExecutor executor =
                new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch));
        RebalanceManager manager = newManager(clock, new RecordingEventManager(), executor);

        TableBucket[] tableBuckets = new TableBucket[10];
        for (int i = 0; i < tableBuckets.length; i++) {
            tableBuckets[i] = new TableBucket(1L, i);
        }
        manager.registerRebalance("cap-test", plans(tableBuckets), NOT_STARTED);

        // every timed-out task keeps being tracked, so admitting new work has to stop at the cap.
        List<RebalanceExecutionKey> timedOut = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            TableBucket running =
                    executor.executedPlans.get(executor.executedPlans.size() - 1).getTableBucket();
            RebalanceExecutionKey attempt = manager.getExecutionKey(running);
            assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();
            timedOut.add(attempt);
        }
        assertThat(executor.executedPlans).hasSize(8);

        // once a tracked task reaches a final status the next pending task is admitted again.
        assertThat(manager.finishRebalanceTask(timedOut.get(0), COMPLETED)).isTrue();
        assertThat(executor.executedPlans).hasSize(9);

        manager.close();
    }

    @Test
    void testTimedOutTaskFailsWhenTargetReplicasStayUnavailable() throws Exception {
        ManualClock clock = new ManualClock(0L);
        // no tablet server is live, so the target replicas can never catch up.
        RebalanceManager manager =
                newManager(clock, new RecordingEventManager(), rebalanceExecutor);

        TableBucket tableBucket = new TableBucket(1L, 0);
        manager.registerRebalance("give-up-test", plans(tableBucket), NOT_STARTED);
        RebalanceExecutionKey attempt = manager.getExecutionKey(tableBucket);
        assertThat(manager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(manager.getPlanForReconciliation(attempt)).isNotNull();

        clock.advanceTime(Duration.ofMinutes(31));
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
        RebalanceManager manager =
                new RebalanceManager(
                        executor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        new Configuration(),
                        new NoOpScheduledExecutor());
        manager.startup();
        return manager;
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
