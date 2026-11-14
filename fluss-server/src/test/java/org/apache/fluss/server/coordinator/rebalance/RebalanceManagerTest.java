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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.CheckRebalanceTimeoutEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalancePersistenceCompleteEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.coordinator.rebalance.goal.ReplicaDistributionGoal;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.RebalanceTaskZNodeData;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.Scheduler;

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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.zk.ZooKeeperUtils.startZookeeperClient;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RebalanceManager}. */
public class RebalanceManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static ZkEpoch zkEpoch;

    private ManualClock clock;
    private TestingRebalanceExecutor rebalanceExecutor;
    private RecordingEventManager eventManager;
    private RebalanceManager rebalanceManager;
    private ManuallyTriggeredScheduledExecutorService timeoutChecker;

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
        clock = new ManualClock();
        rebalanceExecutor = new TestingRebalanceExecutor(new CoordinatorContext(zkEpoch));
        eventManager = new RecordingEventManager();
        rebalanceManager = createManager(new Configuration());
    }

    @AfterEach
    void afterEach() throws Exception {
        rebalanceManager.close();
        zookeeperClient.deleteRebalanceTask();
    }

    @Test
    void testRebalanceWithoutTask() throws Exception {
        assertThat(rebalanceManager.isRecoveryPending()).isFalse();
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(rebalanceManager.getRebalanceId()).isNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isNull();

        String rebalanceId = "test-rebalance-id";
        RebalanceTask rebalanceTask = new RebalanceTask(rebalanceId, NOT_STARTED, new HashMap<>());
        zookeeperClient.registerRebalanceTask(rebalanceTask);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(rebalanceTask);

        // register a rebalance task with empty plan.
        rebalanceManager.registerRebalance(rebalanceId, new HashMap<>());

        assertThat(rebalanceManager.getRebalanceId()).isEqualTo(rebalanceId);
        RebalanceStatus status = rebalanceManager.getRebalanceStatus();
        assertThat(status).isNotNull();
        assertThat(status).isEqualTo(COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask())
                .hasValue(new RebalanceTask(rebalanceId, COMPLETED, new HashMap<>()));
    }

    @Test
    void testScheduledTimeoutCheckRunsOnEventThread() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = plans(tb1, tb2);
        rebalanceManager.registerRebalance("timeout-test", plan);

        clock.advanceTime(Duration.ofMillis(130_000));
        timeoutChecker.triggerPeriodicScheduledTasks();

        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(CheckRebalanceTimeoutEvent.class);
        assertThat(statuses(rebalanceManager).get(tb1)).isEqualTo(REBALANCING);

        rebalanceManager.checkTimeout();

        assertThat(statuses(rebalanceManager).get(tb1)).isEqualTo(TIMEOUT);
        assertThat(eventManager.events).hasSize(2);
        assertThat(eventManager.events.get(1)).isInstanceOf(ReconcileRebalanceTaskEvent.class);
    }

    @Test
    void testStaleTimeoutCheckDoesNotAffectNextRebalance() {
        TableBucket firstBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("first-rebalance", plans(firstBucket));
        timeoutChecker.triggerPeriodicScheduledTasks();
        assertThat(eventManager.events.get(0)).isInstanceOf(CheckRebalanceTimeoutEvent.class);
        rebalanceManager.finishRebalanceTask(firstBucket, COMPLETED);

        TableBucket nextBucket = new TableBucket(2L, 0);
        rebalanceManager.registerRebalance("next-rebalance", plans(nextBucket));
        clock.advanceTime(Duration.ofMillis(100_000));
        rebalanceManager.checkTimeout();

        assertThat(statuses(rebalanceManager).get(nextBucket)).isEqualTo(REBALANCING);
    }

    @Test
    void testQueuedTimeoutCheckIsIgnoredAfterTimeoutCheckerStops() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("stopping-rebalance", plans(tableBucket));
        timeoutChecker.triggerPeriodicScheduledTasks();
        assertThat(eventManager.events.get(0)).isInstanceOf(CheckRebalanceTimeoutEvent.class);

        rebalanceManager.stopTimeoutChecker();
        clock.advanceTime(Duration.ofMillis(130_000));
        rebalanceManager.checkTimeout();

        assertThat(statuses(rebalanceManager).get(tableBucket)).isEqualTo(REBALANCING);
        assertThat(eventManager.events).hasSize(1);
    }

    @Test
    void testRegisterRebalanceRespectsMaxInflightTasks() {
        reconfigureMaxInflightTasks(2);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        TableBucket tb4 = new TableBucket(1L, 3);

        rebalanceManager.registerRebalance("concurrent-test", plans(tb1, tb2, tb3, tb4));

        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2);
        assertThat(statuses(rebalanceManager).values()).filteredOn(NOT_STARTED::equals).hasSize(2);
        assertThat(statuses(rebalanceManager).values()).filteredOn(REBALANCING::equals).hasSize(2);

        rebalanceManager.finishRebalanceTask(tb1, COMPLETED);

        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3);
        assertThat(statuses(rebalanceManager).values()).filteredOn(REBALANCING::equals).hasSize(2);
    }

    @Test
    void testIncreasingMaxInflightTasksAppliesOnNextAdmission() {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        rebalanceManager.registerRebalance("scale-up-test", plans(tb1, tb2, tb3));

        assertThat(rebalanceManager.getMaxInflightRebalanceTasks()).isEqualTo(1);
        assertThat(rebalanceExecutor.executedPlans).hasSize(1);

        reconfigureMaxInflightTasks(3);

        assertThat(rebalanceExecutor.executedPlans).hasSize(1);

        rebalanceManager.finishRebalanceTask(tb1, COMPLETED);

        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3);
    }

    @Test
    void testDecreasingMaxInflightTasksDoesNotCancelRunningTasks() {
        reconfigureMaxInflightTasks(3);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        TableBucket tb4 = new TableBucket(1L, 3);
        rebalanceManager.registerRebalance("scale-down-test", plans(tb1, tb2, tb3, tb4));

        reconfigureMaxInflightTasks(1);
        rebalanceManager.finishRebalanceTask(tb1, COMPLETED);
        rebalanceManager.finishRebalanceTask(tb2, COMPLETED);
        assertThat(rebalanceExecutor.executedPlans).hasSize(3);

        rebalanceManager.finishRebalanceTask(tb3, COMPLETED);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3, tb4);
    }

    @Test
    void testSynchronousCompletionsAreHandledIteratively() {
        int taskCount = 100;
        reconfigureMaxInflightTasks(3);
        TableBucket[] tableBuckets = new TableBucket[taskCount];
        for (int i = 0; i < taskCount; i++) {
            tableBuckets[i] = new TableBucket(1L, i);
            if (i < taskCount - 3) {
                rebalanceExecutor.synchronouslyCompletedBuckets.add(tableBuckets[i]);
            }
        }

        rebalanceManager.registerRebalance("synchronous-completion-test", plans(tableBuckets));

        assertThat(rebalanceExecutor.executedPlans).hasSize(taskCount);
        assertThat(statuses(rebalanceManager).values())
                .filteredOn(COMPLETED::equals)
                .hasSize(taskCount - 3);
        assertThat(statuses(rebalanceManager).values()).filteredOn(REBALANCING::equals).hasSize(3);
    }

    @Test
    void testTimeoutsAllInflightTasks() {
        reconfigureMaxInflightTasks(2);
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(1L, 2);
        rebalanceManager.registerRebalance("timeout-all-test", plans(tb1, tb2, tb3));

        clock.advanceTime(Duration.ofMillis(130_000));
        rebalanceManager.checkTimeout();

        assertThat(statuses(rebalanceManager).get(tb1)).isEqualTo(TIMEOUT);
        assertThat(statuses(rebalanceManager).get(tb2)).isEqualTo(TIMEOUT);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2, tb3);
    }

    @Test
    void testTimeoutTrackingCapIsPreservedWithConcurrentTasks() {
        configureManager(
                new Configuration()
                        .set(COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 4)
                        .set(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS, 2));
        TableBucket[] tableBuckets = new TableBucket[4];
        for (int i = 0; i < tableBuckets.length; i++) {
            tableBuckets[i] = new TableBucket(1L, i);
        }
        rebalanceManager.registerRebalance("concurrent-timeout-cap-test", plans(tableBuckets));
        List<RebalanceExecutionKey> attempts =
                Arrays.stream(tableBuckets)
                        .map(rebalanceManager::getExecutionKey)
                        .collect(Collectors.toList());

        assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(0))).isTrue();
        assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(1))).isTrue();
        assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(2))).isFalse();
        assertThat(statuses(rebalanceManager).values()).filteredOn(TIMEOUT::equals).hasSize(2);
        assertThat(statuses(rebalanceManager).values()).filteredOn(REBALANCING::equals).hasSize(2);

        assertThat(rebalanceManager.finishRebalanceTask(attempts.get(0), COMPLETED)).isTrue();
        assertThat(rebalanceManager.timeoutRebalanceTask(attempts.get(2))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectNonPositiveMaxInflightTasks(int maxInflightTasks) {
        assertThatThrownBy(
                        () ->
                                rebalanceManager.validate(
                                        new Configuration()
                                                .set(
                                                        COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS,
                                                        maxInflightTasks)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must be at least 1");
    }

    @Test
    void testSoftTimeoutAdmitsNextTaskAndTracksLateCompletion() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        rebalanceManager.registerRebalance("soft-timeout-test", plans(tb1, tb2));
        RebalanceExecutionKey firstAttempt = rebalanceManager.getExecutionKey(tb1);
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1);

        clock.advanceTime(Duration.ofMillis(130_000));
        rebalanceManager.checkTimeout();

        RebalanceExecutionKey secondAttempt = rebalanceManager.getExecutionKey(tb2);
        assertThat(secondAttempt).isNotNull();
        assertThat(rebalanceExecutor.executedPlans)
                .extracting(RebalancePlanForBucket::getTableBucket)
                .containsExactly(tb1, tb2);
        assertThat(rebalanceManager.listRebalanceProgress(null).status()).isEqualTo(REBALANCING);
        assertThat(
                        rebalanceManager
                                .listRebalanceProgress(null)
                                .progressForBucketMap()
                                .get(tb1)
                                .status())
                .isEqualTo(TIMEOUT);
        assertThat(eventManager.events.get(0)).isInstanceOf(ReconcileRebalanceTaskEvent.class);

        RebalancePlanForBucket retryPlan = rebalanceManager.prepareReconciliation(firstAttempt);
        assertThat(retryPlan).isNotNull();
        assertThat(retryPlan.getTableBucket()).isEqualTo(tb1);
        // the dispatched reconciliation backs off, so no event is enqueued right away.
        rebalanceManager.checkTimeout();
        assertThat(eventManager.events).hasSize(1);

        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(eventManager.events).hasSize(2);
        assertThat(eventManager.events.get(1)).isInstanceOf(ReconcileRebalanceTaskEvent.class);
        assertThat(((ReconcileRebalanceTaskEvent) eventManager.events.get(1)).getExecutionKey())
                .isEqualTo(firstAttempt);

        assertThat(rebalanceManager.timeoutRebalanceTask(firstAttempt)).isFalse();
        assertThat(rebalanceManager.finishRebalanceTask(firstAttempt, COMPLETED)).isTrue();
        assertThat(rebalanceManager.finishRebalanceTask(firstAttempt, COMPLETED)).isFalse();
        assertThat(rebalanceManager.finishRebalanceTask(secondAttempt, COMPLETED)).isTrue();

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(COMPLETED);
    }

    @Test
    void testFailureIsAggregatedIntoOverallStatus() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        rebalanceManager.registerRebalance("failed-test", plans(tb1, tb2));
        rebalanceManager.finishRebalanceTask(tb1, FAILED);
        rebalanceManager.finishRebalanceTask(tb2, COMPLETED);

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);
    }

    @Test
    void testCancelPersistsIntentAndDrainsOnlyAdmittedTasks() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        // A fresh pending task has never executed, even if failover changed its origin state.
        rebalanceManager.registerRebalance("cancel-test", plans(tb1, tb2));
        RebalanceExecutionKey runningAttempt = rebalanceManager.getExecutionKey(tb1);

        cancelRebalance("cancel-test");

        RebalanceTask storedTask = zookeeperClient.getRebalanceTask().get();
        assertThat(storedTask.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(storedTask.isCancelRequested()).isTrue();
        assertThat(storedTask.getCanceledBuckets()).containsExactly(tb2);
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
    void testCancelAcceptsCommittedWriteWhenRetryReportsBadVersion() throws Exception {
        rebalanceManager.close();
        try (TestingZooKeeperClient testingClient = new TestingZooKeeperClient()) {
            rebalanceManager = createManager(new Configuration(), testingClient);
            TableBucket running = new TableBucket(1L, 0);
            TableBucket pending = new TableBucket(1L, 1);
            Map<TableBucket, RebalancePlanForBucket> plan = plans(running, pending);
            String rebalanceId = "ambiguous-cancellation-write";
            testingClient.registerRebalanceTask(new RebalanceTask(rebalanceId, NOT_STARTED, plan));
            rebalanceManager.registerRebalance(rebalanceId, plan);
            testingClient.failNextCancellationAfterCommitWithBadVersion();

            cancelRebalance(rebalanceId);

            assertThat(testingClient.getRebalanceTask().get().isCancelRequested()).isTrue();
            assertThat(rebalanceManager.isCancelRequested()).isTrue();
            assertThat(statuses(rebalanceManager).get(pending)).isEqualTo(CANCELED);
            assertThat(rebalanceExecutor.executedPlans)
                    .extracting(RebalancePlanForBucket::getTableBucket)
                    .containsExactly(running);
        }
    }

    @Test
    void testCancelRechecksOriginAfterAsyncPersistence() throws Exception {
        rebalanceManager.close();
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        rebalanceManager = createManager(new Configuration(), zookeeperClient, ioExecutor);
        TableBucket running = new TableBucket(1L, 0);
        TableBucket pending = new TableBucket(1L, 1);
        String rebalanceId = "cancel-origin-recheck";
        rebalanceManager.registerRebalance(rebalanceId, plans(running, pending));
        RebalanceExecutionKey runningAttempt = rebalanceManager.getExecutionKey(running);
        AtomicBoolean cancellationCompleted = new AtomicBoolean();
        AtomicReference<Throwable> cancellationFailure = new AtomicReference<>();

        rebalanceManager.cancelRebalance(
                rebalanceId,
                failure -> {
                    cancellationFailure.set(failure);
                    cancellationCompleted.set(true);
                });
        rebalanceManager.finishRebalanceTask(runningAttempt, COMPLETED);
        RebalanceExecutionKey newlyAdmittedAttempt = rebalanceManager.getExecutionKey(pending);

        assertThat(newlyAdmittedAttempt).isNotNull();
        assertThat(statuses(rebalanceManager).get(pending)).isEqualTo(REBALANCING);

        ioExecutor.triggerAll();

        assertThat(cancellationCompleted).isTrue();
        assertThat(cancellationFailure).hasNullValue();
        assertThat(rebalanceManager.isCancelRequested()).isTrue();
        assertThat(statuses(rebalanceManager).get(pending)).isEqualTo(REBALANCING);
        assertThat(rebalanceManager.getExecutionKey(pending)).isEqualTo(newlyAdmittedAttempt);

        rebalanceManager.finishRebalanceTask(newlyAdmittedAttempt, COMPLETED);
        ioExecutor.triggerAll();
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(CANCELED);
    }

    @Test
    void testSecondCancelWhilePersistenceIsInProgressReturnsRebalanceFailure() throws Exception {
        rebalanceManager.close();
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        rebalanceManager = createManager(new Configuration(), zookeeperClient, ioExecutor);
        String rebalanceId = "concurrent-cancel";
        rebalanceManager.registerRebalance(rebalanceId, plans(new TableBucket(1L, 0)));
        AtomicBoolean firstCancellationCompleted = new AtomicBoolean();
        AtomicReference<Throwable> firstCancellationFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondCancellationFailure = new AtomicReference<>();

        rebalanceManager.cancelRebalance(
                rebalanceId,
                failure -> {
                    firstCancellationFailure.set(failure);
                    firstCancellationCompleted.set(true);
                });
        rebalanceManager.cancelRebalance(rebalanceId, secondCancellationFailure::set);

        assertThat(firstCancellationCompleted).isFalse();
        assertThat(secondCancellationFailure.get())
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessageContaining("still being persisted");

        ioExecutor.triggerAll();
        assertThat(firstCancellationCompleted).isTrue();
        assertThat(firstCancellationFailure).hasNullValue();
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
        assertThat(statuses.get(intermediateBucket)).isEqualTo(TIMEOUT);
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(intermediateBucket);
        rebalanceManager.finishRebalanceTask(attempt, COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(CANCELED);
    }

    @Test
    void testCanceledRebalanceReportsFailedWhenDrainFails() throws Exception {
        configureManager(
                new Configuration()
                        .set(COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT, Duration.ofMillis(1)));
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("cancel-drain-failure", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();

        cancelRebalance("cancel-drain-failure");
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(2));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNull();

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(zookeeperClient.getRebalanceTask().get().isCancelRequested()).isTrue();
    }

    @Test
    void testRecoverTracksExplicitlyCanceledReplicaMigrationThatAlreadyStarted() throws Exception {
        TableBucket canceledAtOrigin = new TableBucket(1L, 0);
        TableBucket canceledIntermediate = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plans =
                plans(canceledAtOrigin, canceledIntermediate);
        rebalanceExecutor.originBuckets.add(canceledAtOrigin);

        rebalanceManager.recoverRebalance(
                new RebalanceTask(
                        "recover-explicit-cancel-test",
                        REBALANCING,
                        plans,
                        true,
                        new HashSet<>(Arrays.asList(canceledAtOrigin, canceledIntermediate))));

        Map<TableBucket, RebalanceStatus> statuses = statuses(rebalanceManager);
        assertThat(statuses.get(canceledAtOrigin)).isEqualTo(CANCELED);
        assertThat(statuses.get(canceledIntermediate)).isEqualTo(TIMEOUT);
    }

    @Test
    void testRecoveryTracksIntermediateTasksBeforeAdmittingNewWork() throws Exception {
        Configuration conf =
                new Configuration().set(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS, 1);
        configureManager(conf);

        TableBucket[] buckets = new TableBucket[12];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new TableBucket(i + 1L, 31 * (11 - i));
        }
        RebalanceTask storedTask =
                new RebalanceTask("recovery-admission-test", REBALANCING, plans(buckets));
        zookeeperClient.registerRebalanceTask(storedTask);
        RebalanceTask recoveredTask = zookeeperClient.getRebalanceTask().get();
        List<TableBucket> recoveredOrder = new ArrayList<>(recoveredTask.getExecutePlan().keySet());
        Set<TableBucket> intermediateBuckets =
                new HashSet<>(
                        recoveredOrder.subList(recoveredOrder.size() - 3, recoveredOrder.size()));
        rebalanceExecutor.originBuckets.addAll(recoveredOrder);
        rebalanceExecutor.originBuckets.removeAll(intermediateBuckets);

        rebalanceManager.recoverRebalance(recoveredTask);

        assertThat(rebalanceExecutor.executedPlans).isEmpty();
        assertThat(statuses(rebalanceManager))
                .extractingByKeys(intermediateBuckets.toArray(new TableBucket[0]))
                .containsOnly(TIMEOUT);

        Set<TableBucket> remainingIntermediateBuckets = new HashSet<>(intermediateBuckets);
        while (!remainingIntermediateBuckets.isEmpty()) {
            List<TableBucket> activeBuckets =
                    remainingIntermediateBuckets.stream()
                            .filter(bucket -> rebalanceManager.getExecutionKey(bucket) != null)
                            .collect(Collectors.toList());
            assertThat(activeBuckets).hasSize(1);
            TableBucket activeBucket = activeBuckets.get(0);
            RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(activeBucket);
            assertThat(rebalanceManager.prepareReconciliation(attempt))
                    .isEqualTo(recoveredTask.getExecutePlan().get(activeBucket));
            assertThat(rebalanceManager.finishRebalanceTask(attempt, COMPLETED)).isTrue();
            remainingIntermediateBuckets.remove(activeBucket);
        }
        assertThat(rebalanceExecutor.executedPlans).hasSize(1);
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
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("backoff-test", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(1);

        // first retry is dispatched at the base interval, the next one only after twice that.
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(2);

        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(2);

        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(3);
    }

    @ParameterizedTest
    @MethodSource("trackedTimedOutTaskLimits")
    void testTrackedTimedOutTasksAreCapped(Configuration conf, int limit) {
        configureManager(conf);
        TableBucket[] tableBuckets = new TableBucket[limit + 2];
        for (int i = 0; i < tableBuckets.length; i++) {
            tableBuckets[i] = new TableBucket(1L, i);
        }
        rebalanceManager.registerRebalance("cap-test", plans(tableBuckets));

        // every timed-out task keeps being tracked, so admitting new work has to stop at the cap.
        List<RebalanceExecutionKey> timedOut = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            TableBucket running =
                    rebalanceExecutor
                            .executedPlans
                            .get(rebalanceExecutor.executedPlans.size() - 1)
                            .getTableBucket();
            RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(running);
            assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();
            timedOut.add(attempt);
        }
        assertThat(rebalanceExecutor.executedPlans).hasSize(limit);

        // once a tracked task reaches a final status the next pending task is admitted again.
        assertThat(rebalanceManager.finishRebalanceTask(timedOut.get(0), COMPLETED)).isTrue();
        assertThat(rebalanceExecutor.executedPlans).hasSize(limit + 1);
    }

    @ParameterizedTest
    @MethodSource("targetUnavailableTimeouts")
    void testTimedOutTaskFailsWhenTargetReplicasStayUnavailable(
            Configuration conf, Duration timeout) throws Exception {
        configureManager(conf);
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("give-up-test", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();

        clock.advanceTime(timeout);
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNull();

        // the rebalance reaches a final status, so later rebalance requests are not blocked.
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);
    }

    @Test
    void testDeletedTaskCompletesBeforeReconciliationDeadlineFailure() throws Exception {
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("deleted-reconciliation-test", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();

        clock.advanceTime(Duration.ofMinutes(30).plusMillis(1));
        rebalanceExecutor.deletedBuckets.add(tableBucket);

        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNull();
        assertThat(statuses(rebalanceManager).get(tableBucket)).isEqualTo(COMPLETED);
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(COMPLETED);
    }

    @ParameterizedTest
    @MethodSource("noProgressTimeouts")
    void testTimedOutTaskFailsWithoutProgressWhileTargetsAreLive(
            Configuration conf, Duration timeout) throws Exception {
        configureManager(conf);
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        for (int serverId : new int[] {1, 2, 3}) {
            context.addLiveTabletServer(tabletServer(serverId));
        }
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("no-progress-test", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();

        clock.advanceTime(timeout.minusMillis(1));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);

        TableBucket nextBucket = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> nextPlan = plans(nextBucket);
        rebalanceManager.registerRebalance("after-no-progress-timeout", nextPlan);
        assertThat(rebalanceManager.getRebalanceId()).isEqualTo("after-no-progress-timeout");
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        assertThat(rebalanceExecutor.executedPlans)
                .hasSize(2)
                .last()
                .isEqualTo(nextPlan.get(nextBucket));
    }

    @Test
    void testNonTargetIsrChangesDoNotResetReconciliationTimeouts() throws Exception {
        Duration timeout = Duration.ofMillis(10);
        configureManager(
                new Configuration()
                        .set(COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT, timeout)
                        .set(COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT, timeout));
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        for (int serverId : new int[] {0, 1, 2}) {
            context.addLiveTabletServer(tabletServer(serverId));
        }
        TableBucket tableBucket = new TableBucket(1L, 0);
        context.updateBucketReplicaAssignment(tableBucket, Arrays.asList(0, 1, 2));
        context.putBucketLeaderAndIsr(
                tableBucket,
                new LeaderAndIsr(
                        0,
                        1,
                        Arrays.asList(0, 1, 2),
                        Collections.emptyList(),
                        context.getCoordinatorEpoch(),
                        1));

        rebalanceManager.registerRebalance("non-target-isr-change", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();

        clock.advanceTime(Duration.ofMillis(5));
        context.putBucketLeaderAndIsr(
                tableBucket,
                new LeaderAndIsr(
                        0,
                        1,
                        Arrays.asList(1, 2),
                        Collections.emptyList(),
                        context.getCoordinatorEpoch(),
                        1));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNotNull();

        clock.advanceTime(Duration.ofMillis(6));
        context.putBucketLeaderAndIsr(
                tableBucket,
                new LeaderAndIsr(
                        0,
                        1,
                        Arrays.asList(0, 1, 2),
                        Collections.emptyList(),
                        context.getCoordinatorEpoch(),
                        1));
        assertThat(rebalanceManager.prepareReconciliation(attempt)).isNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGenerateRebalanceFromIntermediateAssignment(boolean targetUnavailable) {
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        for (int serverId : new int[] {0, 1, 2, 3, 4}) {
            if (!targetUnavailable || serverId != 3) {
                context.addLiveTabletServer(tabletServer(serverId));
            }
        }
        context.putServerTag(0, ServerTag.PERMANENT_OFFLINE);
        TableBucket bucket = new TableBucket(1L, 0);
        putBucketForPlanning(context, bucket, Arrays.asList(0, 1, 2, 3));

        RebalanceTask next =
                rebalanceManager.generateRebalanceTask(
                        Collections.singletonList(new ReplicaDistributionGoal()));
        RebalancePlanForBucket plan = next.getExecutePlan().get(bucket);
        assertThat(plan).isNotNull();
        assertThat(plan.getOriginReplicas()).containsExactly(0, 1, 2, 3);
        assertThat(plan.getNewReplicas()).hasSize(3).contains(1, 2).doesNotContain(0);
        assertThat(context.liveTabletServerSet()).containsAll(plan.getNewReplicas());
        // Planning must leave the real assignment intact until the replacement task executes.
        assertThat(context.getAssignment(bucket)).containsExactly(0, 1, 2, 3);
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
        assertThatThrownBy(() -> createManager(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(option.key())
                .hasMessageContaining("must be between 1 ms");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectsInvalidTrackedTimedOutTaskLimit(int limit) {
        Configuration conf =
                new Configuration().set(COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS, limit);
        assertThatThrownBy(() -> createManager(conf))
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
        configureManager(conf);
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
        rebalanceManager.registerRebalance("cancel-at-origin-test", plans(tb1, tb2));

        cancelRebalance("cancel-at-origin-test");

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(CANCELED);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(CANCELED);
    }

    @Test
    void testScheduledRetryPersistsFinalStatusAfterTransientFailure() throws Exception {
        rebalanceManager.close();
        try (TestingZooKeeperClient testingClient = new TestingZooKeeperClient()) {
            rebalanceManager = createManager(new Configuration(), testingClient);
            TableBucket tableBucket = new TableBucket(1L, 0);
            Map<TableBucket, RebalancePlanForBucket> plan = plans(tableBucket);
            String rebalanceId = "finalization-retry-test";
            testingClient.registerRebalanceTask(new RebalanceTask(rebalanceId, NOT_STARTED, plan));
            rebalanceManager.registerRebalance(rebalanceId, plan);
            RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
            testingClient.failNextFinalWrite();

            assertThat(rebalanceManager.finishRebalanceTask(attempt, COMPLETED)).isTrue();
            assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(REBALANCING);
            assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
            assertThat(testingClient.getRebalanceTask().get().getRebalanceStatus())
                    .isEqualTo(NOT_STARTED);

            timeoutChecker.triggerPeriodicScheduledTasks();
            assertThat(eventManager.events).anyMatch(CheckRebalanceTimeoutEvent.class::isInstance);
            rebalanceManager.checkTimeout();

            assertThat(testingClient.getRebalanceTask().get().getRebalanceStatus())
                    .isEqualTo(COMPLETED);
            assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(COMPLETED);
            assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();

            TableBucket nextBucket = new TableBucket(2L, 0);
            rebalanceManager.registerRebalance("next-rebalance", plans(nextBucket));
            assertThat(rebalanceManager.getExecutionKey(nextBucket)).isNotNull();
        }
    }

    @Test
    void testPersistenceRunsAsynchronouslyAndFencesRequests() throws Exception {
        rebalanceManager.close();
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        rebalanceManager = createManager(new Configuration(), zookeeperClient, ioExecutor);
        RebalanceTask task =
                new RebalanceTask("async-persistence", NOT_STARTED, plans(new TableBucket(1L, 0)));
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        rebalanceManager.persistRebalanceTask(
                task,
                persistenceFailure -> {
                    failure.set(persistenceFailure);
                    completed.set(true);
                });

        assertThat(completed).isFalse();
        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        assertThat(rebalanceManager.getRebalanceId()).isEqualTo("async-persistence");
        assertThat(zookeeperClient.getRebalanceTask()).isEmpty();
        assertThatThrownBy(() -> rebalanceManager.listRebalanceProgress(null))
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessageContaining("persistence is still in progress");
        AtomicReference<Throwable> cancellationFailure = new AtomicReference<>();
        rebalanceManager.cancelRebalance(null, cancellationFailure::set);
        assertThat(cancellationFailure.get()).isInstanceOf(RebalanceFailureException.class);

        ioExecutor.triggerAll();

        assertThat(completed).isTrue();
        assertThat(failure).hasNullValue();
        assertThat(zookeeperClient.getRebalanceTask()).contains(task);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
    }

    @Test
    void testNewPersistenceFencesRequestsAfterFinalRecovery() throws Exception {
        rebalanceManager.close();
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        rebalanceManager = createManager(new Configuration(), zookeeperClient, ioExecutor);
        TableBucket completedBucket = new TableBucket(1L, 0);
        assertThat(
                        rebalanceManager.recoverRebalance(
                                new RebalanceTask(
                                        "completed-rebalance", COMPLETED, plans(completedBucket))))
                .isTrue();

        String newRebalanceId = "pending-rebalance";
        Map<TableBucket, RebalancePlanForBucket> newPlan = plans(new TableBucket(2L, 0));
        RebalanceTask newTask = new RebalanceTask(newRebalanceId, NOT_STARTED, newPlan);
        AtomicReference<Throwable> persistenceFailure = new AtomicReference<>();
        rebalanceManager.persistRebalanceTask(
                newTask,
                failure -> {
                    persistenceFailure.set(failure);
                    if (failure == null) {
                        rebalanceManager.registerRebalance(newRebalanceId, newPlan);
                    }
                });

        assertThat(rebalanceManager.getRebalanceId()).isEqualTo(newRebalanceId);
        assertThatThrownBy(() -> rebalanceManager.listRebalanceProgress(null))
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessageContaining("persistence is still in progress");
        AtomicReference<Throwable> cancellationFailure = new AtomicReference<>();
        rebalanceManager.cancelRebalance(null, cancellationFailure::set);
        assertThat(cancellationFailure.get()).isInstanceOf(RebalanceFailureException.class);

        ioExecutor.triggerAll();

        assertThat(persistenceFailure).hasNullValue();
        assertThat(rebalanceManager.getRebalanceId()).isEqualTo(newRebalanceId);
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(REBALANCING);
    }

    private RebalanceManager createManager(Configuration conf) {
        return createManager(conf, zookeeperClient);
    }

    private RebalanceManager createManager(Configuration conf, ZooKeeperClient client) {
        return createManager(conf, client, Runnable::run);
    }

    private RebalanceManager createManager(
            Configuration conf, ZooKeeperClient client, Executor ioExecutor) {
        timeoutChecker = new ManuallyTriggeredScheduledExecutorService();
        RebalanceManager manager =
                new RebalanceManager(
                        rebalanceExecutor,
                        client,
                        eventManager,
                        clock,
                        conf,
                        new TestingScheduler(timeoutChecker),
                        ioExecutor);
        manager.startup();
        try {
            if (!manager.loadPersistedRebalanceTask().isPresent()) {
                manager.completeEmptyRecovery();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize rebalance recovery for test.", e);
        }
        return manager;
    }

    private void configureManager(Configuration conf) {
        rebalanceManager.close();
        rebalanceManager = createManager(conf);
    }

    private void cancelRebalance(String rebalanceId) {
        rebalanceManager.cancelRebalance(
                rebalanceId,
                failure -> {
                    if (failure != null) {
                        throw new RuntimeException(failure);
                    }
                });
    }

    private void reconfigureMaxInflightTasks(int maxInflightTasks) {
        rebalanceManager.reconfigure(
                new Configuration()
                        .set(COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, maxInflightTasks));
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
    void testStartupFencesNewRebalanceUntilExplicitRecovery() throws Exception {
        TableBucket tableBucket = new TableBucket(1L, 0);
        RebalanceTask storedTask =
                new RebalanceTask("startup-recovery-test", REBALANCING, plans(tableBucket));
        zookeeperClient.registerRebalanceTask(storedTask);
        configureManager(new Configuration());

        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        assertThat(rebalanceManager.isRecoveryPending()).isTrue();
        assertThat(rebalanceManager.getRebalanceId()).isNull();
        assertThatThrownBy(
                        () ->
                                rebalanceManager.registerRebalance(
                                        "too-early", plans(new TableBucket(2L, 0))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("startup recovery");
        assertThat(timeoutChecker.getActivePeriodicScheduledTask()).hasSize(1);
        assertThat(eventManager.events).isEmpty();

        rebalanceExecutor.originBuckets.add(tableBucket);
        RebalanceTask loadedTask = rebalanceManager.loadPersistedRebalanceTask().get();
        rebalanceManager.recoverRebalance(loadedTask);
        assertThat(rebalanceManager.isRecoveryPending()).isFalse();
        assertThat(rebalanceManager.getRebalanceId()).isEqualTo("startup-recovery-test");
        assertThat(rebalanceExecutor.executedPlans).hasSize(1);
    }

    @Test
    void testNewRebalanceIsAdmittedAfterRecoveryCompletes() throws Exception {
        TableBucket recoveredBucket = new TableBucket(1L, 0);
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("completed-recovery", COMPLETED, plans(recoveredBucket)));
        configureManager(new Configuration());

        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        assertThatThrownBy(
                        () ->
                                rebalanceManager.registerRebalance(
                                        "too-early", plans(new TableBucket(2L, 0))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("startup recovery");

        RebalanceTask loadedTask = rebalanceManager.loadPersistedRebalanceTask().get();
        rebalanceManager.recoverRebalance(loadedTask);
        assertThat(rebalanceManager.isRecoveryPending()).isFalse();
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();

        TableBucket nextBucket = new TableBucket(2L, 0);
        rebalanceManager.registerRebalance("after-recovery", plans(nextBucket));
        assertThat(rebalanceManager.getRebalanceId()).isEqualTo("after-recovery");
        assertThat(rebalanceManager.getExecutionKey(nextBucket)).isNotNull();
    }

    @Test
    void testRecoveryFailureIsRetriedWhileNewRebalanceRemainsFenced() throws Exception {
        TableBucket recoveredBucket = new TableBucket(1L, 0);
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask("failed-recovery", REBALANCING, plans(recoveredBucket)));
        configureManager(new Configuration());
        rebalanceExecutor.failOriginCheck = true;

        RebalanceTask loadedTask = rebalanceManager.loadPersistedRebalanceTask().get();
        assertThat(rebalanceManager.recoverRebalance(loadedTask)).isFalse();

        assertThat(rebalanceManager.isRecoveryPending()).isTrue();
        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        assertThatThrownBy(
                        () ->
                                rebalanceManager.registerRebalance(
                                        "too-early", plans(new TableBucket(2L, 0))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("startup recovery");

        rebalanceExecutor.failOriginCheck = false;
        timeoutChecker.triggerNonPeriodicScheduledTask();
        RecoverRebalanceEvent retryEvent =
                (RecoverRebalanceEvent)
                        eventManager.events.stream()
                                .filter(RecoverRebalanceEvent.class::isInstance)
                                .findFirst()
                                .get();
        assertThat(rebalanceManager.recoverRebalance(retryEvent.getRebalanceTask())).isTrue();
        assertThat(rebalanceManager.isRecoveryPending()).isFalse();
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

    private static final class TestingScheduler implements Scheduler {
        private final ManuallyTriggeredScheduledExecutorService executor;

        private TestingScheduler(ManuallyTriggeredScheduledExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void startup() {}

        @Override
        public void shutdown() {}

        @Override
        public ScheduledFuture<?> schedule(
                String name, Runnable task, long delayMs, long periodMs) {
            if (periodMs > 0) {
                return executor.scheduleAtFixedRate(task, delayMs, periodMs, TimeUnit.MILLISECONDS);
            }
            return executor.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private static final class TestingRebalanceExecutor implements RebalanceExecutor {
        private final CoordinatorContext coordinatorContext;
        private final List<RebalancePlanForBucket> executedPlans = new ArrayList<>();
        private final Set<TableBucket> originBuckets = new HashSet<>();
        private final Set<TableBucket> deletedBuckets = new HashSet<>();
        private final Set<TableBucket> synchronouslyCompletedBuckets = new HashSet<>();
        private boolean failOriginCheck;

        private TestingRebalanceExecutor(CoordinatorContext coordinatorContext) {
            this.coordinatorContext = coordinatorContext;
        }

        @Override
        public CoordinatorContext getCoordinatorContext() {
            return coordinatorContext;
        }

        @Override
        public ExecutionResult tryToExecuteRebalanceTask(RebalancePlanForBucket planForBucket) {
            executedPlans.add(planForBucket);
            return synchronouslyCompletedBuckets.contains(planForBucket.getTableBucket())
                    ? ExecutionResult.COMPLETED
                    : ExecutionResult.IN_PROGRESS;
        }

        @Override
        public boolean isRebalanceTaskAtOrigin(RebalancePlanForBucket planForBucket) {
            if (failOriginCheck) {
                throw new IllegalStateException("origin check failed");
            }
            return originBuckets.contains(planForBucket.getTableBucket());
        }

        @Override
        public boolean isRebalanceTaskDeleted(TableBucket tableBucket) {
            return deletedBuckets.contains(tableBucket);
        }
    }

    private static final class TestingZooKeeperClient extends ZooKeeperClient {

        private boolean failNextFinalWrite;
        private boolean failNextCancellationAfterCommitWithBadVersion;

        private TestingZooKeeperClient() {
            super(createCuratorFrameworkWrapper(), createConfiguration());
        }

        private void failNextFinalWrite() {
            failNextFinalWrite = true;
        }

        private void failNextCancellationAfterCommitWithBadVersion() {
            failNextCancellationAfterCommitWithBadVersion = true;
        }

        @Override
        public void upsertRebalanceTaskData(
                RebalanceTaskZNodeData data,
                int expectedRebalanceZkVersion,
                int expectedCoordinatorEpochZkVersion)
                throws Exception {
            RebalanceTask rebalanceTask = data.getInlineTask();
            if (failNextFinalWrite
                    && RebalanceStatus.FINAL_STATUSES.contains(
                            rebalanceTask.getRebalanceStatus())) {
                failNextFinalWrite = false;
                throw new RuntimeException("Expected finalization failure");
            }
            if (failNextCancellationAfterCommitWithBadVersion
                    && rebalanceTask.getRebalanceStatus() == REBALANCING
                    && rebalanceTask.isCancelRequested()) {
                failNextCancellationAfterCommitWithBadVersion = false;
                super.upsertRebalanceTaskData(
                        data, expectedRebalanceZkVersion, expectedCoordinatorEpochZkVersion);
                throw new KeeperException.BadVersionException();
            }
            super.upsertRebalanceTaskData(
                    data, expectedRebalanceZkVersion, expectedCoordinatorEpochZkVersion);
        }

        private static CuratorFrameworkWithUnhandledErrorListener createCuratorFrameworkWrapper() {
            Builder builder =
                    builder()
                            .connectString(
                                    ZOO_KEEPER_EXTENSION_WRAPPER
                                            .getCustomExtension()
                                            .getConnectString())
                            .namespace("fluss")
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3));
            return startZookeeperClient(builder, NOPErrorHandler.INSTANCE);
        }

        private static Configuration createConfiguration() {
            Configuration configuration = new Configuration();
            configuration.set(
                    ConfigOptions.REMOTE_DATA_DIR,
                    ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getRemoteDataDir());
            return configuration;
        }
    }

    /** Records events put into the coordinator event queue. */
    private static final class RecordingEventManager implements EventManager {
        final List<CoordinatorEvent> events = new ArrayList<>();

        @Override
        public void put(CoordinatorEvent event) {
            events.add(event);
            if (event instanceof RebalancePersistenceCompleteEvent) {
                ((RebalancePersistenceCompleteEvent) event).complete();
            }
        }
    }
}
