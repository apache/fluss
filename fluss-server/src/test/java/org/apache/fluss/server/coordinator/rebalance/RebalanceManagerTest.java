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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
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
