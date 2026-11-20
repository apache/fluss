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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.TestingControlledNotifyGateway.PendingNotify;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrRequestContext;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.RebalanceEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RetryOfflineLeaderEvent;
import org.apache.fluss.server.coordinator.rebalance.RebalanceExecutionKey;
import org.apache.fluss.server.coordinator.rebalance.goal.ReplicaDistributionGoal;
import org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElection.ReassignmentLeaderElection;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for rebalance execution and recovery through the coordinator event loop. */
class CoordinatorRebalanceTest extends CoordinatorEventProcessorTestBase {

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

    private final ConcurrentLinkedDeque<PendingNotify> pendingResponses =
            new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<PendingNotify> recoveredResponses =
            new ConcurrentLinkedDeque<>();
    private Clock testClock = SystemClock.getInstance();

    @AfterEach
    void cleanupRebalance() throws Exception {
        initCoordinatorChannel();
        drainPendingNotifyTriggers(pendingResponses);
        drainPendingNotifyTriggers(recoveredResponses);
        zookeeperClient.deleteRebalanceTask();
        if (Arrays.stream(zookeeperClient.getSortedTabletServerList()).noneMatch(id -> id == 2)) {
            registerTabletServer(2);
        }
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(ZkData.ServerIdZNode.path(3));
    }

    @Test
    void testApplyRebalanceConcurrencyChange() {
        Configuration newConfig = new Configuration();
        newConfig.set(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS, 3);

        eventProcessor.getRebalanceManager().reconfigure(newConfig);

        assertThat(eventProcessor.getRebalanceManager().getMaxInflightRebalanceTasks())
                .isEqualTo(3);
    }

    @Test
    void testLeaderOnlyRebalanceExecutesUpToConfiguredConcurrency() throws Exception {
        initCoordinatorChannel();
        List<Integer> replicas = Arrays.asList(0, 1, 2);
        long tableId = createTableWithReplicas(replicas, 3);
        TableBucket tb0 = new TableBucket(tableId, 0);
        TableBucket tb1 = new TableBucket(tableId, 1);
        TableBucket tb2 = new TableBucket(tableId, 2);
        installBlockingNotifyGateways(pendingResponses);

        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(tb0, new RebalancePlanForBucket(tb0, 0, 1, replicas, Arrays.asList(1, 0, 2)));
        plan.put(tb1, new RebalancePlanForBucket(tb1, 0, 2, replicas, Arrays.asList(2, 0, 1)));
        plan.put(tb2, new RebalancePlanForBucket(tb2, 0, 1, replicas, Arrays.asList(1, 2, 0)));

        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .reconfigure(
                                    new Configuration()
                                            .set(
                                                    ConfigOptions
                                                            .COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS,
                                                    2));
                    eventProcessor
                            .getRebalanceManager()
                            .registerRebalance("parallel-leader-only", plan);
                    return null;
                });

        assertThat(rebalanceExecutionKey(tb0)).isNotNull();
        assertThat(rebalanceExecutionKey(tb1)).isNotNull();
        assertThat(rebalanceExecutionKey(tb2)).isNull();
        verifyIsr(tb0, 1, replicas);
        verifyIsr(tb1, 2, replicas);
        verifyIsr(tb2, 0, replicas);

        drainPendingNotifyTriggers(pendingResponses);
        retry(Duration.ofMinutes(1), () -> assertThat(rebalanceExecutionKey(tb2)).isNotNull());
        drainPendingNotifyTriggers(pendingResponses);
        retry(Duration.ofMinutes(1), () -> assertThat(hasInProgressRebalance()).isFalse());
    }

    @Test
    void testReplicaMigrationsExecuteUpToConfiguredConcurrency() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 3);
        List<Integer> target = Arrays.asList(0, 1, 2);
        List<Integer> union = Arrays.asList(0, 1, 2, 3);
        long tableId = createTableWithReplicas(origin, 3);
        TableBucket tb0 = new TableBucket(tableId, 0);
        TableBucket tb1 = new TableBucket(tableId, 1);
        TableBucket tb2 = new TableBucket(tableId, 2);
        installBlockingNotifyGateways(pendingResponses);

        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(tb0, new RebalancePlanForBucket(tb0, 0, 0, origin, target));
        plan.put(tb1, new RebalancePlanForBucket(tb1, 0, 0, origin, target));
        plan.put(tb2, new RebalancePlanForBucket(tb2, 0, 0, origin, target));
        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .reconfigure(
                                    new Configuration()
                                            .set(
                                                    ConfigOptions
                                                            .COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS,
                                                    2));
                    eventProcessor
                            .getRebalanceManager()
                            .registerRebalance("parallel-replica-migration", plan);
                    return null;
                });

        TableAssignment assignment = zookeeperClient.getTableAssignment(tableId).get();
        assertThat(assignment.getBucketAssignment(0)).isEqualTo(new BucketAssignment(union));
        assertThat(assignment.getBucketAssignment(1)).isEqualTo(new BucketAssignment(union));
        assertThat(assignment.getBucketAssignment(2)).isEqualTo(new BucketAssignment(origin));

        adjustRebalanceIsr(tb0, union);
        adjustRebalanceIsr(tb1, union);
        drainPendingNotifyTriggers(pendingResponses);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        zookeeperClient
                                                .getTableAssignment(tableId)
                                                .get()
                                                .getBucketAssignment(2))
                                .isEqualTo(new BucketAssignment(union)));

        adjustRebalanceIsr(tb2, union);
        drainPendingNotifyTriggers(pendingResponses);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(pendingResponses);
                    assertThat(hasInProgressRebalance()).isFalse();
                });
        assignment = zookeeperClient.getTableAssignment(tableId).get();
        assertThat(assignment.getBucketAssignment(0)).isEqualTo(new BucketAssignment(target));
        assertThat(assignment.getBucketAssignment(1)).isEqualTo(new BucketAssignment(target));
        assertThat(assignment.getBucketAssignment(2)).isEqualTo(new BucketAssignment(target));
    }

    @Test
    void testTimedOutReassignmentRetriesPhaseAIdempotently() throws Exception {
        registerTabletServer(3);

        initCoordinatorChannel();

        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 3), 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);

        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 0, Arrays.asList(0, 1, 3), Arrays.asList(0, 1, 2));
        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .registerRebalance(
                                    "timed-out-retry-test",
                                    Collections.singletonMap(tableBucket, plan));
                    return null;
                });
        RebalanceExecutionKey executionKey = rebalanceExecutionKey(tableBucket);
        retry(Duration.ofMinutes(1), () -> assertThat(pendingResponses).isNotEmpty());
        int requestsAfterInitialPhaseA = pendingResponses.size();
        int epochAfterInitialPhaseA =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());

        timeoutRebalanceTask(executionKey);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.TIMEOUT));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(pendingResponses.size())
                                .isGreaterThan(requestsAfterInitialPhaseA));
        int requestsAfterFirstRetry = pendingResponses.size();
        int epochAfterFirstRetry =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());
        assertThat(epochAfterFirstRetry).isEqualTo(epochAfterInitialPhaseA);
        List<Integer> assignmentAfterFirstRetry = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(assignmentAfterFirstRetry).containsExactly(0, 1, 2, 3);

        eventProcessor
                .getCoordinatorEventManager()
                .put(new ReconcileRebalanceTaskEvent(executionKey));
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(pendingResponses.size()).isGreaterThan(requestsAfterFirstRetry));
        assertThat(rebalanceExecutionKey(tableBucket)).isEqualTo(executionKey);
        int epochAfterDuplicateRetry =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());
        assertThat(epochAfterDuplicateRetry).isEqualTo(epochAfterInitialPhaseA);
        List<Integer> assignmentAfterDuplicateRetry =
                fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(assignmentAfterDuplicateRetry).containsExactly(0, 1, 2, 3);

        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        adjustRebalanceIsr(tableBucket, Arrays.asList(0, 1, 2, 3));
        assertThat(hasInProgressRebalance()).isTrue();

        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(ZkData.ServerIdZNode.path(2));
        retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).doesNotContain(2));
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        assertThat(hasInProgressRebalance()).isTrue();
        assertThat(pendingResponses).isEmpty();
        List<Integer> assignmentWhileTargetOffline = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(assignmentWhileTargetOffline).containsExactly(0, 1, 2);

        registerTabletServer(2);
        retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).contains(2));
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        adjustRebalanceIsr(tableBucket, Arrays.asList(0, 1, 2));
        drainPendingNotifyTriggers(pendingResponses);
        retry(Duration.ofMinutes(1), () -> assertThat(hasInProgressRebalance()).isFalse());
        fromCtx(ctx -> null);
        List<Integer> finalAssignment = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(finalAssignment).containsExactly(0, 1, 2);
        verifyIsr(tableBucket, 0, Arrays.asList(0, 1, 2));
    }

    @Test
    void testPhaseAPersistenceIsNotReusedForTargetAssignment() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();

        List<Integer> origin = Arrays.asList(0, 1, 3);
        List<Integer> target = Arrays.asList(0, 1, 2);
        List<Integer> union = Arrays.asList(0, 1, 2, 3);
        long tableId = createTableWithReplicas(origin, 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        RebalancePlanForBucket plan = new RebalancePlanForBucket(tableBucket, 0, 0, origin, target);

        registerRebalance("phase-a-persistence", Collections.singletonMap(tableBucket, plan));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        zookeeperClient
                                                .getTableAssignment(tableId)
                                                .get()
                                                .getBucketAssignment(0))
                                .isEqualTo(new BucketAssignment(union)));

        // Reproduce the state left when phase B updated the in-memory assignment but failed before
        // persisting it: the target replicas have caught up, memory contains TRS, and ZooKeeper
        // still contains the Phase-A ORS+TRS assignment.
        LeaderAndIsr leaderAndIsr = zookeeperClient.getLeaderAndIsr(tableBucket).get();
        LeaderAndIsr caughtUp = leaderAndIsr.newLeaderAndIsr(union);
        zookeeperClient.updateLeaderAndIsr(
                tableBucket, caughtUp, fromCtx(CoordinatorContext::getCoordinatorZkVersion));
        fromCtx(
                ctx -> {
                    ctx.putBucketLeaderAndIsr(tableBucket, caughtUp);
                    ctx.updateBucketReplicaAssignment(tableBucket, target);
                    eventProcessor.tryToExecuteRebalanceTask(plan);
                    return null;
                });

        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(target));
    }

    @Test
    void testTimedOutRebalanceCompletesWhenTableIsBeingDeleted() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 2), 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 3));
        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .registerRebalance(
                                    "delete-during-rebalance",
                                    Collections.singletonMap(tableBucket, plan));
                    return null;
                });
        RebalanceExecutionKey executionKey = rebalanceExecutionKey(tableBucket);

        retry(Duration.ofMinutes(1), () -> assertThat(pendingResponses).isNotEmpty());
        fromCtx(
                ctx -> {
                    ctx.queueTableDeletion(Collections.singleton(tableId));
                    return null;
                });
        timeoutRebalanceTask(executionKey);

        retry(Duration.ofMinutes(1), () -> assertThat(hasInProgressRebalance()).isFalse());
        assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.COMPLETED);
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
        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .registerRebalance(
                                    "partition-delete-during-rebalance",
                                    Collections.singletonMap(tableBucket, plan));
                    return null;
                });

        retry(Duration.ofMinutes(1), () -> assertThat(hasInProgressRebalance()).isFalse());
        assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.COMPLETED);
    }

    @ParameterizedTest(name = "recover {0}")
    @ValueSource(strings = {"A", "B", "FINAL", "LEADER"})
    void testRestartResumesRebalanceAndWaitsForLeaderAck(String phase) throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> originReplicas = Arrays.asList(0, 1, 3);
        boolean leaderOnly = "LEADER".equals(phase);
        int targetLeader = leaderOnly ? 1 : 0;
        List<Integer> targetReplicas = leaderOnly ? Arrays.asList(1, 0, 3) : Arrays.asList(0, 1, 2);
        List<Integer> finalAssignment = leaderOnly ? originReplicas : targetReplicas;
        List<Integer> unionReplicas = Arrays.asList(0, 1, 2, 3);
        long tableId = createTableWithReplicas(originReplicas, 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);

        String rebalanceId = "restart-phase-" + phase;
        Map<TableBucket, RebalancePlanForBucket> plan =
                Collections.singletonMap(
                        tableBucket,
                        new RebalancePlanForBucket(
                                tableBucket, 0, targetLeader, originReplicas, targetReplicas));
        RebalanceTask persistedTask =
                new RebalanceTask(rebalanceId, RebalanceStatus.NOT_STARTED, plan);
        registerRebalance(rebalanceId, plan);
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(leaderOnly ? originReplicas : unionReplicas));

        if ("B".equals(phase) || "FINAL".equals(phase)) {
            // Reach Phase B through ISR expansion, but hold back the final leader ACK.
            adjustRebalanceIsr(tableBucket, unionReplicas);
            verifyIsr(tableBucket, targetLeader, targetReplicas);
            // A target falls out of ISR before completion. The persisted assignment is at
            // the target, but the next coordinator must still resume this unfinished phase.
            if ("B".equals(phase)) {
                adjustRebalanceIsr(tableBucket, Arrays.asList(0, 1));
            }
        }
        List<Integer> intermediateAssignment =
                leaderOnly ? originReplicas : "A".equals(phase) ? unionReplicas : targetReplicas;
        List<Integer> intermediateIsr =
                "B".equals(phase)
                        ? Arrays.asList(0, 1)
                        : "FINAL".equals(phase) ? targetReplicas : originReplicas;
        verifyIsr(tableBucket, targetLeader, intermediateIsr);
        LeaderAndIsr beforeRestart = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(intermediateAssignment));
        assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.REBALANCING);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(persistedTask);

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(hasPendingNotifyTrigger(pendingResponses, targetLeader)).isTrue());
        PendingNotify oldResponse =
                pendingResponses.stream()
                        .filter(response -> response.getResponseServerId() == targetLeader)
                        .findFirst()
                        .get();
        NotifyLeaderAndIsrRequest oldRequest = oldResponse.getRequest();
        assertThat(oldRequest.getNotifyBucketsLeaderReqsList()).hasSize(1);
        PbNotifyLeaderAndIsrReqForBucket oldBucketRequest =
                oldRequest.getNotifyBucketsLeaderReqsList().get(0);
        RebalanceExecutionKey executionKey = rebalanceExecutionKey(tableBucket);

        // Restart must resume the persisted plan without explicit registration.
        restartCoordinator(recoveredResponses);
        assertThat(eventProcessor.getCoordinatorEpoch())
                .isGreaterThan(oldRequest.getCoordinatorEpoch());
        fromCtx(ctx -> null);
        assertThat(rebalanceExecutionKey(tableBucket)).isEqualTo(executionKey);
        verifyIsr(tableBucket, targetLeader, intermediateIsr);
        LeaderAndIsr afterRecovery = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        assertThat(afterRecovery.leaderEpoch()).isEqualTo(beforeRestart.leaderEpoch());
        assertThat(afterRecovery.bucketEpoch()).isEqualTo(beforeRestart.bucketEpoch());
        List<Integer> recoveredAssignment = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(recoveredAssignment).containsExactlyElementsOf(intermediateAssignment);

        // The new leader reports catch-up. Keep its final ACK blocked so a stale success
        // would visibly and incorrectly complete the recovered attempt.
        if ("A".equals(phase) || "B".equals(phase)) {
            adjustRebalanceIsr(tableBucket, "B".equals(phase) ? targetReplicas : unionReplicas);
        }
        verifyIsr(tableBucket, targetLeader, targetReplicas);
        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        NotifyLeaderAndIsrRequestContext oldContext =
                new NotifyLeaderAndIsrRequestContext(
                        oldRequest.getCoordinatorEpoch(),
                        oldBucketRequest.getLeader(),
                        oldBucketRequest.getLeaderEpoch(),
                        oldBucketRequest.getBucketEpoch(),
                        executionKey);
        // Also isolate the coordinator-epoch fence: after restart the rebalance id and
        // bucket are unchanged, so rejecting a different execution key is insufficient.
        NotifyLeaderAndIsrRequestContext oldEpochContext =
                new NotifyLeaderAndIsrRequestContext(
                        oldRequest.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        executionKey);
        oldResponse.complete();
        for (NotifyLeaderAndIsrRequestContext staleContext :
                Arrays.asList(oldContext, oldEpochContext)) {
            notifyLeaderResponse(tableBucket, current.leader(), staleContext);
            assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.TIMEOUT);
            assertThat(zookeeperClient.getRebalanceTask()).hasValue(persistedTask);
        }

        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(hasInProgressRebalance()).isFalse();
                    assertThat(zookeeperClient.getRebalanceTask())
                            .hasValue(
                                    new RebalanceTask(
                                            rebalanceId, RebalanceStatus.COMPLETED, plan));
                });
        assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.COMPLETED);
        List<Integer> assignment = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(assignment).containsExactlyElementsOf(finalAssignment);
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(finalAssignment));
        verifyIsr(tableBucket, targetLeader, targetReplicas);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNewRebalanceConvergesAfterFailureAndRestart(boolean targetUnavailable)
            throws Exception {
        ManualClock clock = new ManualClock(0L);
        testClock = clock;
        eventProcessor.shutdown();
        zookeeperClient.deleteRebalanceTask();
        eventProcessor = buildCoordinatorEventProcessor(clock);
        eventProcessor.startup();
        eventProcessor.startRebalanceRecovery();
        waitUntilRebalanceRecoveryCompletes();
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 2);
        long tableId = createTableWithReplicas(origin, 1);
        TableBucket bucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);
        Map<TableBucket, RebalancePlanForBucket> plan =
                Collections.singletonMap(
                        bucket,
                        new RebalancePlanForBucket(bucket, 0, 0, origin, Arrays.asList(0, 1, 3)));
        String failedId = "failed-before-restart";
        registerRebalance(failedId, plan);
        if (targetUnavailable) {
            ZOO_KEEPER_EXTENSION_WRAPPER
                    .getCustomExtension()
                    .cleanupPath(ZkData.ServerIdZNode.path(3));
            retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).doesNotContain(3));
        }
        RebalanceExecutionKey key = rebalanceExecutionKey(bucket);
        timeoutRebalanceTask(key);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.TIMEOUT));
        // Wait for the immediate reconciliation to establish the unavailable-target timer.
        fromCtx(ctx -> null);
        clock.advanceTime(targetUnavailable ? Duration.ofMinutes(31) : Duration.ofHours(25));
        eventProcessor.getCoordinatorEventManager().put(new ReconcileRebalanceTaskEvent(key));
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.FAILED));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(hasInProgressRebalance()).isFalse();
                    assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                            .isEqualTo(RebalanceStatus.FAILED);
                });
        assertThat(
                        zookeeperClient
                                .getTableAssignment(tableId)
                                .get()
                                .getBucketAssignment(0)
                                .getReplicas())
                .containsExactlyInAnyOrder(0, 1, 2, 3);

        restartCoordinator(null);
        // Use the normal request path, including model construction, optimization and
        // execution.
        CompletableFuture<RebalanceResponse> response = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new RebalanceEvent(
                                Collections.singletonList(new ReplicaDistributionGoal()),
                                response));
        response.get();
        assertThat(currentRebalanceId()).isNotEqualTo(failedId);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
                    assertThat(hasInProgressRebalance()).isFalse();
                    assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                            .isEqualTo(RebalanceStatus.COMPLETED);
                });
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(origin));
        verifyIsr(bucket, 0, origin);
    }

    @ParameterizedTest(name = "leaderOnly={0}")
    @ValueSource(booleans = {false, true})
    void testRestartAfterCancellationDoesNotStartPendingBucketAfterLeaderChange(boolean leaderOnly)
            throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 2);
        List<Integer> target = Arrays.asList(0, 1, 3);
        long tableId = createTableWithReplicas(origin, 2);
        TableBucket running = new TableBucket(tableId, 0);
        TableBucket pending = new TableBucket(tableId, 1);
        // A leader election must not make the pending plan look started. For a leader-only plan,
        // the ordinary election deliberately chooses the plan's target before it is admitted.
        int pendingLeader = 1;
        List<Integer> pendingIsr = leaderOnly ? origin : Arrays.asList(0, 1);
        if (!leaderOnly) {
            adjustRebalanceIsr(pending, pendingIsr);
        }
        changeLeader(pending, pendingLeader);
        installBlockingNotifyGateways(pendingResponses);
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(running, new RebalancePlanForBucket(running, 0, 0, origin, target));
        plan.put(
                pending,
                leaderOnly
                        ? new RebalancePlanForBucket(pending, 0, 1, origin, Arrays.asList(1, 0, 2))
                        : new RebalancePlanForBucket(pending, 0, 0, origin, target));
        String rebalanceId = "cancel-before-restart";
        registerRebalance(rebalanceId, plan);
        cancelRebalance(rebalanceId);
        assertThat(rebalanceStatus(pending)).isEqualTo(RebalanceStatus.CANCELED);
        assertThat(zookeeperClient.getRebalanceTask().get().isCancelRequested()).isTrue();
        assertThat(hasInProgressRebalance()).isTrue();

        restartCoordinator(recoveredResponses);
        retry(Duration.ofMinutes(1), () -> assertThat(rebalanceExecutionKey(running)).isNotNull());
        assertThat(rebalanceStatus(pending)).isEqualTo(RebalanceStatus.CANCELED);
        assertThat(rebalanceExecutionKey(pending)).isNull();
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(1))
                .isEqualTo(new BucketAssignment(origin));
        verifyIsr(pending, pendingLeader, pendingIsr);

        // A later failover must not revive the canceled plan and move the leader back to its old
        // target.
        int expectedFinalPendingLeader = pendingLeader;
        if (leaderOnly) {
            expectedFinalPendingLeader = 2;
            changeLeader(pending, expectedFinalPendingLeader);
        }

        adjustRebalanceIsr(running, Arrays.asList(0, 1, 2, 3));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(hasInProgressRebalance()).isFalse();
                });
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(RebalanceStatus.CANCELED);
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(1))
                .isEqualTo(new BucketAssignment(origin));
        verifyIsr(pending, expectedFinalPendingLeader, pendingIsr);
        verifyIsr(running, 0, target);
    }

    @Test
    void testCancelAfterRestartDrainsEveryIntermediateBucket() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 3);
        List<Integer> target = Arrays.asList(0, 1, 2);
        List<Integer> union = Arrays.asList(0, 1, 2, 3);
        long tableId = createTableWithReplicas(origin, 2);
        TableBucket first = new TableBucket(tableId, 0);
        TableBucket second = new TableBucket(tableId, 1);
        installBlockingNotifyGateways(pendingResponses);
        Map<TableBucket, RebalancePlanForBucket> plans = new LinkedHashMap<>();
        for (TableBucket bucket : Arrays.asList(first, second)) {
            plans.put(bucket, new RebalancePlanForBucket(bucket, 0, 0, origin, target));
        }
        String rebalanceId = "review-cancel-after-restart";
        registerRebalance(rebalanceId, plans);
        RebalanceExecutionKey key = rebalanceExecutionKey(first);
        timeoutRebalanceTask(key);
        fromCtx(ctx -> null);
        for (TableBucket bucket : plans.keySet()) {
            assertThat(
                            zookeeperClient
                                    .getTableAssignment(tableId)
                                    .get()
                                    .getBucketAssignment(bucket.getBucket()))
                    .isEqualTo(new BucketAssignment(union));
        }

        restartCoordinator(recoveredResponses);
        retry(Duration.ofMinutes(1), () -> assertThat(currentRebalanceId()).isEqualTo(rebalanceId));
        Map<TableBucket, RebalanceResultForBucket> recoveredProgress =
                fromCtx(
                        ctx ->
                                eventProcessor
                                        .getRebalanceManager()
                                        .listRebalanceProgress(null)
                                        .progressForBucketMap());
        TableBucket recoveredIntermediate =
                plans.keySet().stream()
                        .filter(
                                bucket ->
                                        recoveredProgress.get(bucket).status()
                                                == RebalanceStatus.TIMEOUT)
                        .findFirst()
                        .get();
        assertThat(
                        zookeeperClient
                                .getTableAssignment(tableId)
                                .get()
                                .getBucketAssignment(recoveredIntermediate.getBucket()))
                .isEqualTo(new BucketAssignment(union));
        boolean intermediateAtOrigin =
                fromCtx(
                        ctx ->
                                eventProcessor.isRebalanceTaskAtOrigin(
                                        plans.get(recoveredIntermediate)));
        assertThat(intermediateAtOrigin).isFalse();
        cancelRebalance(rebalanceId);
        assertThat(rebalanceStatus(recoveredIntermediate))
                .as("A previously started migration still needs draining after recovery")
                .isNotEqualTo(RebalanceStatus.CANCELED);
        for (TableBucket bucket : plans.keySet()) {
            adjustRebalanceIsr(bucket, union);
        }
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(hasInProgressRebalance()).isFalse();
                });
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(RebalanceStatus.CANCELED);
        for (TableBucket bucket : plans.keySet()) {
            assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
            assertThat(
                            zookeeperClient
                                    .getTableAssignment(tableId)
                                    .get()
                                    .getBucketAssignment(bucket.getBucket()))
                    .isEqualTo(new BucketAssignment(target));
            verifyIsr(bucket, 0, target);
        }
    }

    private void adjustRebalanceIsr(TableBucket tableBucket, List<Integer> isr) throws Exception {
        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        assertThat(
                        submitAdjustIsr(
                                        tableBucket,
                                        new LeaderAndIsr(
                                                current.leader(),
                                                current.leaderEpoch(),
                                                isr,
                                                Collections.emptyList(),
                                                current.coordinatorEpoch(),
                                                current.bucketEpoch()))
                                .succeeded())
                .isTrue();
    }

    private void timeoutRebalanceTask(RebalanceExecutionKey executionKey) throws Exception {
        fromCtx(
                ctx -> {
                    assertThat(
                                    eventProcessor
                                            .getRebalanceManager()
                                            .timeoutRebalanceTask(executionKey))
                            .isTrue();
                    return null;
                });
    }

    @Test
    void testRebalanceAtOriginAllowsShrunkIsr() throws Exception {
        TableBucket tableBucket = new TableBucket(987L, 0);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 3));
        putBucketState(tableBucket, 0, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2));
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isTrue();
        putBucketState(tableBucket, 0, Arrays.asList(0, 1), Arrays.asList(0, 1, 2));
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isTrue();
        putBucketState(tableBucket, 0, Arrays.asList(0, 1, 2, 3), Arrays.asList(0, 1, 2));
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isFalse();
        putBucketState(tableBucket, 1, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2));
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isTrue();

        RebalancePlanForBucket leaderOnlyPlan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2));
        // The target leader is elected: wait for its acknowledgement instead of canceling.
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(leaderOnlyPlan)).isFalse();
        // An unrelated election does not apply a leader-only plan.
        putBucketState(tableBucket, 2, Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2));
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(leaderOnlyPlan)).isTrue();
    }

    private void changeLeader(TableBucket tableBucket, int newLeader) throws Exception {
        LeaderAndIsr current = zookeeperClient.getLeaderAndIsr(tableBucket).get();
        LeaderAndIsr changed =
                new LeaderAndIsr(
                        newLeader,
                        current.leaderEpoch() + 1,
                        current.isr(),
                        current.standbyReplicas(),
                        current.coordinatorEpoch(),
                        current.bucketEpoch() + 1);
        int coordinatorZkVersion = fromCtx(CoordinatorContext::getCoordinatorZkVersion);
        zookeeperClient.updateLeaderAndIsr(tableBucket, changed, coordinatorZkVersion);
        fromCtx(
                ctx -> {
                    ctx.putBucketLeaderAndIsr(tableBucket, changed);
                    return null;
                });
        verifyIsr(tableBucket, newLeader, current.isr());
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
    void testQueuedLeaderMoveMustWaitForPreviousAck() throws Exception {
        initCoordinatorChannel();
        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 2), 2);
        TableBucket first = new TableBucket(tableId, 0);
        TableBucket second = new TableBucket(tableId, 1);
        installBlockingNotifyGateways(pendingResponses);
        Map<TableBucket, RebalancePlanForBucket> plans = new LinkedHashMap<>();
        for (TableBucket bucket : Arrays.asList(first, second)) {
            plans.put(
                    bucket,
                    new RebalancePlanForBucket(
                            bucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2)));
        }
        registerRebalance("queued-leader-move", plans);
        assertThat(fromCtx(ctx -> ctx.getBucketLeaderAndIsr(first).get()).leader()).isEqualTo(1);
        adjustRebalanceIsr(second, Arrays.asList(0, 1, 2));
        assertThat(fromCtx(ctx -> ctx.getBucketLeaderAndIsr(second).get()).leader()).isZero();
        assertThat(rebalanceStatus(second)).isEqualTo(RebalanceStatus.NOT_STARTED);
        PendingNotify firstAck =
                pendingResponses.stream()
                        .filter(response -> response.getResponseServerId() == 1)
                        .findFirst()
                        .get();
        assertThat(pendingResponses.remove(firstAck)).isTrue();
        firstAck.complete();
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(first)).isEqualTo(RebalanceStatus.COMPLETED);
        assertThat(fromCtx(ctx -> ctx.getBucketLeaderAndIsr(second).get()).leader()).isEqualTo(1);
        assertThat(rebalanceStatus(second)).isEqualTo(RebalanceStatus.REBALANCING);
    }

    @Test
    void testOldAckAfterLeaderReelectionDoesNotComplete() throws Exception {
        initCoordinatorChannel();
        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 2), 1);
        TableBucket bucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);
        registerRebalance(
                "old-leader-ack",
                Collections.singletonMap(
                        bucket,
                        new RebalancePlanForBucket(
                                bucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2))));
        PendingNotify oldAck =
                pendingResponses.stream()
                        .filter(response -> response.getResponseServerId() == 1)
                        .findFirst()
                        .get();
        assertThat(pendingResponses.remove(oldAck)).isTrue();
        fromCtx(
                ctx -> {
                    CoordinatorRequestBatch requestBatch =
                            new CoordinatorRequestBatch(
                                    testCoordinatorChannelManager,
                                    eventProcessor.getCoordinatorEventManager(),
                                    ctx,
                                    eventProcessor.getRebalanceManager()::getExecutionKey);
                    TableBucketStateMachine stateMachine =
                            new TableBucketStateMachine(ctx, requestBatch, zookeeperClient);
                    stateMachine.handleStateChange(
                            Collections.singleton(bucket),
                            OnlineBucket,
                            new ReassignmentLeaderElection(Arrays.asList(0, 1, 2)));
                    stateMachine.handleStateChange(
                            Collections.singleton(bucket),
                            OnlineBucket,
                            new ReassignmentLeaderElection(Arrays.asList(1, 0, 2)));
                    return null;
                });
        LeaderAndIsr reelected = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        assertThat(reelected.leader()).isEqualTo(1);
        assertThat(reelected.leaderEpoch())
                .isGreaterThan(
                        oldAck.getRequest()
                                .getNotifyBucketsLeaderReqsList()
                                .get(0)
                                .getLeaderEpoch());
        oldAck.complete();
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.REBALANCING);
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCombinedMoveHonorsTargetLeader(boolean assignmentAlreadyAtTarget) throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        long tableId =
                createTableWithReplicas(
                        assignmentAlreadyAtTarget ? Arrays.asList(0, 1, 2) : Arrays.asList(0, 1, 3),
                        1);
        TableBucket bucket = new TableBucket(tableId, 0);
        List<Integer> target = Arrays.asList(1, 0, 2);
        if (assignmentAlreadyAtTarget) {
            zookeeperClient.updateTableAssignment(
                    tableId,
                    new TableAssignment(Collections.singletonMap(0, new BucketAssignment(target))),
                    ZkVersion.MATCH_ANY_VERSION.getVersion());
            fromCtx(
                    ctx -> {
                        ctx.updateBucketReplicaAssignment(bucket, target);
                        return null;
                    });
        }
        installBlockingNotifyGateways(pendingResponses);
        registerRebalance(
                "combined-move",
                Collections.singletonMap(
                        bucket,
                        new RebalancePlanForBucket(bucket, 0, 1, Arrays.asList(0, 1, 3), target)));
        if (!assignmentAlreadyAtTarget) {
            adjustRebalanceIsr(bucket, Arrays.asList(0, 1, 2, 3));
        }
        assertThat(fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get()).leader()).isEqualTo(1);
        completeResponsesExceptCurrentLeaderAck(bucket, pendingResponses);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.REBALANCING);

        LeaderAndIsr before = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        adjustRebalanceIsr(bucket, target);
        LeaderAndIsr after = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        assertThat(after.leaderEpoch()).isEqualTo(before.leaderEpoch());
        // Only AdjustIsr advances the bucket epoch; phase B must not run again.
        assertThat(after.bucketEpoch()).isEqualTo(before.bucketEpoch() + 1);
        completeResponsesExceptCurrentLeaderAck(bucket, pendingResponses);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.REBALANCING);
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(target));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "BEFORE_ELECTION",
                "AFTER_ELECTION",
                "AFTER_ISR_SHRINK",
                "AFTER_ASSIGNMENT_UPDATE",
                "WRONG_FINAL_LEADER"
            })
    void testRestartCombinedMoveAtPhaseBBoundary(String boundary) throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 3);
        List<Integer> target = Arrays.asList(1, 0, 2);
        List<Integer> union = Arrays.asList(1, 0, 2, 3);
        long tableId = createTableWithReplicas(origin, 1);
        TableBucket bucket = new TableBucket(tableId, 0);
        boolean finalAssignment =
                "AFTER_ASSIGNMENT_UPDATE".equals(boundary) || "WRONG_FINAL_LEADER".equals(boundary);
        int leader =
                "BEFORE_ELECTION".equals(boundary) || "WRONG_FINAL_LEADER".equals(boundary) ? 0 : 1;
        List<Integer> isr = finalAssignment || "AFTER_ISR_SHRINK".equals(boundary) ? target : union;
        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        eventProcessor.shutdown();
        zookeeperClient.updateTableAssignment(
                tableId,
                new TableAssignment(
                        Collections.singletonMap(
                                0, new BucketAssignment(finalAssignment ? target : union))),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        zookeeperClient.updateLeaderAndIsr(
                bucket,
                new LeaderAndIsr(
                        leader,
                        current.leaderEpoch() + 1,
                        isr,
                        Collections.emptyList(),
                        current.coordinatorEpoch(),
                        current.bucketEpoch() + 1),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask(
                        "restart-combined-" + boundary,
                        RebalanceStatus.REBALANCING,
                        Collections.singletonMap(
                                bucket, new RebalancePlanForBucket(bucket, 0, 1, origin, target))));

        testCoordinatorChannelManager.close();
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("2");
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        installBlockingNotifyGateways(recoveredResponses);
        eventProcessor = buildCoordinatorEventProcessor(testClock);
        eventProcessor.startup();
        eventProcessor.startRebalanceRecovery();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    RebalanceExecutionKey recoveredKey =
                            fromCtx(
                                    ctx ->
                                            eventProcessor
                                                    .getRebalanceManager()
                                                    .getExecutionKey(bucket));
                    assertThat(recoveredKey).isNotNull();
                    verifyIsr(bucket, 1, target);
                });
        completeResponsesExceptCurrentLeaderAck(bucket, recoveredResponses);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.TIMEOUT);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
                });
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(target));
    }

    @Test
    void testCombinedMoveRecoversAfterTargetLeaderAckFails() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 3);
        List<Integer> target = Arrays.asList(1, 0, 2);
        long tableId = createTableWithReplicas(origin, 1);
        TableBucket bucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);
        registerRebalance(
                "failed-target-ack",
                Collections.singletonMap(
                        bucket, new RebalancePlanForBucket(bucket, 0, 1, origin, target)));
        adjustRebalanceIsr(bucket, Arrays.asList(0, 1, 2, 3));
        completeResponsesExceptCurrentLeaderAck(bucket, pendingResponses);
        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        RebalanceExecutionKey key = rebalanceExecutionKey(bucket);
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new NotifyLeaderAndIsrResponseReceivedEvent(
                                Collections.singletonList(
                                        new NotifyLeaderAndIsrResultForBucket(
                                                bucket,
                                                new ApiError(
                                                        Errors.DISK_WRITE_LOCKED,
                                                        "target cannot activate"))),
                                1,
                                Collections.singletonMap(
                                        bucket,
                                        new NotifyLeaderAndIsrRequestContext(
                                                eventProcessor.getCoordinatorEpoch(),
                                                current.leader(),
                                                current.leaderEpoch(),
                                                current.bucketEpoch(),
                                                key))));
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.REBALANCING);
        timeoutRebalanceTask(key);
        fromCtx(ctx -> null);
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.TIMEOUT);

        eventProcessor.getCoordinatorEventManager().put(new RetryOfflineLeaderEvent());
        fromCtx(ctx -> null);
        adjustRebalanceIsr(bucket, target);
        verifyIsr(bucket, 1, target);
        completeResponsesExceptCurrentLeaderAck(bucket, pendingResponses);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.TIMEOUT);
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
    }

    private void completeResponsesExceptCurrentLeaderAck(
            TableBucket bucket, ConcurrentLinkedDeque<PendingNotify> responses) throws Exception {
        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        for (PendingNotify response : responses) {
            NotifyLeaderAndIsrRequest request = response.getRequest();
            assertThat(request.getNotifyBucketsLeaderReqsList()).hasSize(1);
            PbNotifyLeaderAndIsrReqForBucket bucketRequest =
                    request.getNotifyBucketsLeaderReqsList().get(0);
            if (response.getResponseServerId() != current.leader()
                    || request.getCoordinatorEpoch() != eventProcessor.getCoordinatorEpoch()
                    || bucketRequest.getLeaderEpoch() != current.leaderEpoch()
                    || bucketRequest.getBucketEpoch() != current.bucketEpoch()) {
                if (responses.remove(response)) {
                    response.complete();
                }
            }
        }
        fromCtx(ctx -> null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"LEADER", "PHASE_A", "FINAL"})
    void testUnavailableTargetLeaderWaitsForRecoveryWithoutReelecting(String phase)
            throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        boolean leaderOnly = "LEADER".equals(phase);
        boolean assignmentAtTarget = "FINAL".equals(phase);
        List<Integer> origin = leaderOnly ? Arrays.asList(0, 1, 2) : Arrays.asList(0, 1, 3);
        List<Integer> target = Arrays.asList(1, 0, 2);
        long tableId =
                createTableWithReplicas(assignmentAtTarget ? Arrays.asList(0, 1, 2) : origin, 1);
        TableBucket bucket = new TableBucket(tableId, 0);
        if (assignmentAtTarget) {
            zookeeperClient.updateTableAssignment(
                    tableId,
                    new TableAssignment(Collections.singletonMap(0, new BucketAssignment(target))),
                    ZkVersion.MATCH_ANY_VERSION.getVersion());
        }
        fromCtx(
                ctx -> {
                    if (assignmentAtTarget) {
                        ctx.updateBucketReplicaAssignment(bucket, target);
                    }
                    // The server is live, but this bucket cannot currently run on it.
                    ctx.addOfflineBucketInServer(bucket, 1);
                    return null;
                });
        LeaderAndIsr before = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        installBlockingNotifyGateways(pendingResponses);
        registerRebalance(
                "unavailable-target-" + phase,
                Collections.singletonMap(
                        bucket, new RebalancePlanForBucket(bucket, 0, 1, origin, target)));
        if (!leaderOnly && !assignmentAtTarget) {
            adjustRebalanceIsr(bucket, Arrays.asList(0, 1, 2, 3));
        }
        LeaderAndIsr waiting = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
        assertThat(waiting.leader()).isZero();
        assertThat(waiting.leaderEpoch()).isEqualTo(before.leaderEpoch());
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.REBALANCING);

        RebalanceExecutionKey key = rebalanceExecutionKey(bucket);
        timeoutRebalanceTask(key);
        for (int attempt = 0; attempt < 2; attempt++) {
            eventProcessor.getCoordinatorEventManager().put(new ReconcileRebalanceTaskEvent(key));
            fromCtx(ctx -> null);
            drainPendingNotifyTriggers(pendingResponses);
            fromCtx(ctx -> null);
            assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.TIMEOUT);
            LeaderAndIsr retried = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(bucket).get());
            assertThat(retried.leaderEpoch()).isEqualTo(waiting.leaderEpoch());
            assertThat(retried.bucketEpoch()).isEqualTo(waiting.bucketEpoch());
        }

        fromCtx(
                ctx -> {
                    ctx.removeOfflineBucketInServer(bucket, 1);
                    return null;
                });
        eventProcessor.getCoordinatorEventManager().put(new ReconcileRebalanceTaskEvent(key));
        verifyIsr(bucket, 1, target);
        assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.TIMEOUT);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(pendingResponses);
                    assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
                });
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(leaderOnly ? origin : target));
    }

    @Test
    void testStaleNotifyLeaderAndIsrResponseCannotCompleteRebalance() throws Exception {
        initCoordinatorChannel();

        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 2), 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        installBlockingNotifyGateways(pendingResponses);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2));
        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .registerRebalance(
                                    "stale-response-test",
                                    Collections.singletonMap(tableBucket, plan));
                    return null;
                });
        retry(Duration.ofMinutes(1), () -> assertThat(pendingResponses).isNotEmpty());

        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        NotifyLeaderAndIsrRequestContext staleContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch() - 1,
                        rebalanceExecutionKey(tableBucket));
        notifyLeaderResponse(tableBucket, current.leader(), staleContext);
        assertThat(hasInProgressRebalance()).isTrue();

        NotifyLeaderAndIsrRequestContext oldAttemptContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        new RebalanceExecutionKey("old-rebalance", tableBucket));
        notifyLeaderResponse(tableBucket, current.leader(), oldAttemptContext);
        assertThat(hasInProgressRebalance()).isTrue();

        NotifyLeaderAndIsrRequestContext currentContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        rebalanceExecutionKey(tableBucket));
        notifyLeaderResponse(tableBucket, current.leader(), currentContext);
        retry(Duration.ofMinutes(1), () -> assertThat(hasInProgressRebalance()).isFalse());
        drainPendingNotifyTriggers(pendingResponses);
    }

    private void notifyLeaderResponse(
            TableBucket bucket, int server, NotifyLeaderAndIsrRequestContext context)
            throws Exception {
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new NotifyLeaderAndIsrResponseReceivedEvent(
                                Collections.singletonList(
                                        new NotifyLeaderAndIsrResultForBucket(bucket)),
                                server,
                                Collections.singletonMap(bucket, context)));
        fromCtx(ctx -> null);
    }

    private boolean hasInProgressRebalance() throws Exception {
        return fromCtx(ctx -> eventProcessor.getRebalanceManager().hasInProgressRebalance());
    }

    private @Nullable String currentRebalanceId() throws Exception {
        return fromCtx(ctx -> eventProcessor.getRebalanceManager().getRebalanceId());
    }

    private @Nullable RebalanceExecutionKey rebalanceExecutionKey(TableBucket tableBucket)
            throws Exception {
        return fromCtx(ctx -> eventProcessor.getRebalanceManager().getExecutionKey(tableBucket));
    }

    private RebalanceStatus rebalanceStatus(TableBucket tableBucket) throws Exception {
        return fromCtx(
                ctx ->
                        eventProcessor
                                .getRebalanceManager()
                                .listRebalanceProgress(null)
                                .progressForBucketMap()
                                .get(tableBucket)
                                .status());
    }

    private long createTableWithReplicas(List<Integer> replicas, int bucketCount) throws Exception {
        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            assignments.put(bucket, new BucketAssignment(replicas));
        }
        long tableId =
                metadataManager.createTable(
                        TablePath.of(defaultDatabase, "rebalance"),
                        remoteDataDir,
                        TEST_TABLE,
                        new TableAssignment(assignments),
                        false);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            verifyIsr(new TableBucket(tableId, bucket), replicas.get(0), replicas);
        }
        return tableId;
    }

    private void registerRebalance(String id, Map<TableBucket, RebalancePlanForBucket> plans)
            throws Exception {
        zookeeperClient.registerRebalanceTask(
                new RebalanceTask(id, RebalanceStatus.NOT_STARTED, plans));
        fromCtx(
                ctx -> {
                    eventProcessor.getRebalanceManager().registerRebalance(id, plans);
                    return null;
                });
    }

    private void cancelRebalance(String rebalanceId) throws Exception {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        fromCtx(
                ctx -> {
                    eventProcessor
                            .getRebalanceManager()
                            .cancelRebalance(
                                    rebalanceId,
                                    failure -> {
                                        if (failure == null) {
                                            completion.complete(null);
                                        } else {
                                            completion.completeExceptionally(failure);
                                        }
                                    });
                    return null;
                });
        completion.get();
    }

    private void restartCoordinator(@Nullable ConcurrentLinkedDeque<PendingNotify> responses)
            throws Exception {
        eventProcessor.shutdown();
        testCoordinatorChannelManager.close();
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("2");
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        if (responses == null) {
            initCoordinatorChannel();
        } else {
            installBlockingNotifyGateways(responses);
        }
        eventProcessor = buildCoordinatorEventProcessor(testClock);
        eventProcessor.startup();
        eventProcessor.startRebalanceRecovery();
        waitUntilRebalanceRecoveryCompletes();
    }
}
