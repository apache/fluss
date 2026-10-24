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
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
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

    private ManualClock clock;
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
    void testTimeoutEnqueuesEvent() throws Exception {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        Map<TableBucket, RebalancePlanForBucket> plan = plans(tb1, tb2);
        rebalanceManager.registerRebalance("timeout-test", plan);
        RebalanceExecutionKey executionKey = rebalanceManager.getExecutionKey(tb1);

        clock.advanceTime(Duration.ofMillis(100_000));
        rebalanceManager.checkTimeout();
        assertThat(eventManager.events).isEmpty();

        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();

        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(RebalanceTaskTimeoutEvent.class);
        RebalanceTaskTimeoutEvent timeoutEvent =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        assertThat(timeoutEvent.getExecutionKey()).isEqualTo(executionKey);

        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(eventManager.events).hasSize(1);
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
        RebalanceTaskTimeoutEvent timeoutEvent =
                (RebalanceTaskTimeoutEvent) eventManager.events.get(0);
        assertThat(rebalanceManager.timeoutRebalanceTask(timeoutEvent.getExecutionKey())).isTrue();

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
        assertThat(eventManager.events.get(1)).isInstanceOf(ReconcileRebalanceTaskEvent.class);

        RebalancePlanForBucket retryPlan = rebalanceManager.getPlanForReconciliation(firstAttempt);
        assertThat(retryPlan).isNotNull();
        assertThat(retryPlan.getTableBucket()).isEqualTo(tb1);
        // the dispatched reconciliation backs off, so no event is enqueued right away.
        rebalanceManager.checkTimeout();
        assertThat(eventManager.events).hasSize(2);

        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(eventManager.events).hasSize(3);
        assertThat(eventManager.events.get(2)).isInstanceOf(ReconcileRebalanceTaskEvent.class);
        assertThat(((ReconcileRebalanceTaskEvent) eventManager.events.get(2)).getExecutionKey())
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
        TableBucket tableBucket = new TableBucket(1L, 0);
        rebalanceManager.registerRebalance("backoff-test", plans(tableBucket));
        RebalanceExecutionKey attempt = rebalanceManager.getExecutionKey(tableBucket);
        assertThat(rebalanceManager.timeoutRebalanceTask(attempt)).isTrue();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(1);

        // first retry is dispatched at the base interval, the next one only after twice that.
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(30_000));
        rebalanceManager.checkTimeout();
        assertThat(reconciliationsFor(eventManager, attempt)).isEqualTo(2);

        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNotNull();
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
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNotNull();

        clock.advanceTime(timeout);
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNull();

        // the rebalance reaches a final status, so later rebalance requests are not blocked.
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(FAILED);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus()).isEqualTo(FAILED);
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
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNotNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(REBALANCING);
        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNotNull();
        clock.advanceTime(Duration.ofMillis(1));
        assertThat(rebalanceManager.getPlanForReconciliation(attempt)).isNull();
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

        rebalanceManager.cancelRebalance("cancel-at-origin-test");

        assertThat(rebalanceManager.getRebalanceStatus()).isEqualTo(CANCELED);
        assertThat(rebalanceManager.hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(CANCELED);
    }

    private RebalanceManager createManager(Configuration conf) {
        RebalanceManager manager =
                new RebalanceManager(
                        rebalanceExecutor,
                        zookeeperClient,
                        eventManager,
                        clock,
                        conf,
                        new ManuallyTriggeredScheduledExecutorService());
        manager.startup();
        return manager;
    }

    private void configureManager(Configuration conf) {
        rebalanceManager.close();
        rebalanceManager = createManager(conf);
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
        configureManager(new Configuration());

        assertThat(rebalanceManager.hasInProgressRebalance()).isTrue();
        assertThat(rebalanceManager.getRebalanceId()).isNull();
        assertThat(eventManager.events).hasSize(1);
        RecoverRebalanceEvent recoveryEvent = (RecoverRebalanceEvent) eventManager.events.get(0);
        assertThat(recoveryEvent.getRebalanceTask()).isEqualTo(storedTask);

        rebalanceManager.recoverRebalance(recoveryEvent.getRebalanceTask());
        assertThat(rebalanceManager.getRebalanceId()).isEqualTo("startup-recovery-test");
        assertThat(rebalanceExecutor.executedPlans).hasSize(1);
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
}
