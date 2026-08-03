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
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.server.coordinator.TestingControlledNotifyGateway.PendingNotify;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrRequestContext;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.RebalanceEvent;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.rebalance.RebalanceExecutionKey;
import org.apache.fluss.server.coordinator.rebalance.goal.ReplicaDistributionGoal;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.ZkData;
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
    void testTimedOutReassignmentRetriesPhaseAIdempotently() throws Exception {
        registerTabletServer(3);

        initCoordinatorChannel();

        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 3), 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        installBlockingNotifyGateways(pendingResponses);

        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 0, Arrays.asList(0, 1, 3), Arrays.asList(0, 1, 2));
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "timed-out-retry-test", Collections.singletonMap(tableBucket, plan));
        RebalanceExecutionKey executionKey =
                eventProcessor.getRebalanceManager().getExecutionKey(tableBucket);
        retry(Duration.ofMinutes(1), () -> assertThat(pendingResponses).isNotEmpty());
        int requestsAfterInitialPhaseA = pendingResponses.size();
        int epochAfterInitialPhaseA =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());

        eventProcessor
                .getCoordinatorEventManager()
                .put(new RebalanceTaskTimeoutEvent(executionKey));
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
        assertThat(eventProcessor.getRebalanceManager().getExecutionKey(tableBucket))
                .isEqualTo(executionKey);
        int epochAfterDuplicateRetry =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get().bucketEpoch());
        assertThat(epochAfterDuplicateRetry).isEqualTo(epochAfterInitialPhaseA);
        List<Integer> assignmentAfterDuplicateRetry =
                fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(assignmentAfterDuplicateRetry).containsExactly(0, 1, 2, 3);

        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        adjustRebalanceIsr(tableBucket, Arrays.asList(0, 1, 2, 3));
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(ZkData.ServerIdZNode.path(2));
        retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).doesNotContain(2));
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();
        assertThat(pendingResponses).isEmpty();
        List<Integer> assignmentWhileTargetOffline = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(assignmentWhileTargetOffline).containsExactly(0, 1, 2);

        registerTabletServer(2);
        retryVerifyContext(ctx -> assertThat(ctx.liveTabletServerSet()).contains(2));
        drainPendingNotifyTriggers(pendingResponses);
        fromCtx(ctx -> null);
        adjustRebalanceIsr(tableBucket, Arrays.asList(0, 1, 2));
        drainPendingNotifyTriggers(pendingResponses);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
        fromCtx(ctx -> null);
        List<Integer> finalAssignment = fromCtx(ctx -> ctx.getAssignment(tableBucket));
        assertThat(finalAssignment).containsExactly(0, 1, 2);
        verifyIsr(tableBucket, 0, Arrays.asList(0, 1, 2));
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
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "delete-during-rebalance", Collections.singletonMap(tableBucket, plan));
        RebalanceExecutionKey executionKey =
                eventProcessor.getRebalanceManager().getExecutionKey(tableBucket);

        retry(Duration.ofMinutes(1), () -> assertThat(pendingResponses).isNotEmpty());
        fromCtx(
                ctx -> {
                    ctx.queueTableDeletion(Collections.singleton(tableId));
                    return null;
                });
        eventProcessor
                .getCoordinatorEventManager()
                .put(new RebalanceTaskTimeoutEvent(executionKey));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
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
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "partition-delete-during-rebalance",
                        Collections.singletonMap(tableBucket, plan));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
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
        RebalanceExecutionKey executionKey =
                eventProcessor.getRebalanceManager().getExecutionKey(tableBucket);

        // Restart must resume the persisted plan without explicit registration.
        restartCoordinator(recoveredResponses);
        assertThat(eventProcessor.getCoordinatorEpoch())
                .isGreaterThan(oldRequest.getCoordinatorEpoch());
        fromCtx(ctx -> null);
        assertThat(eventProcessor.getRebalanceManager().getExecutionKey(tableBucket))
                .isEqualTo(executionKey);
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
            assertThat(rebalanceStatus(tableBucket)).isEqualTo(RebalanceStatus.REBALANCING);
            assertThat(zookeeperClient.getRebalanceTask()).hasValue(persistedTask);
        }

        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                            .isFalse();
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
        RebalanceExecutionKey key = eventProcessor.getRebalanceManager().getExecutionKey(bucket);
        eventProcessor.getCoordinatorEventManager().put(new RebalanceTaskTimeoutEvent(key));
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
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isFalse();
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(RebalanceStatus.FAILED);
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
        assertThat(eventProcessor.getRebalanceManager().getRebalanceId()).isNotEqualTo(failedId);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(rebalanceStatus(bucket)).isEqualTo(RebalanceStatus.COMPLETED);
                    assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                            .isFalse();
                    assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                            .isEqualTo(RebalanceStatus.COMPLETED);
                });
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(0))
                .isEqualTo(new BucketAssignment(origin));
        verifyIsr(bucket, 0, origin);
    }

    @Test
    void testRestartAfterCancellationDoesNotStartPendingBucketWithShrunkIsr() throws Exception {
        registerTabletServer(3);
        initCoordinatorChannel();
        List<Integer> origin = Arrays.asList(0, 1, 2);
        List<Integer> target = Arrays.asList(0, 1, 3);
        long tableId = createTableWithReplicas(origin, 2);
        TableBucket running = new TableBucket(tableId, 0);
        TableBucket pending = new TableBucket(tableId, 1);
        adjustRebalanceIsr(pending, Arrays.asList(0, 1));
        installBlockingNotifyGateways(pendingResponses);
        Map<TableBucket, RebalancePlanForBucket> plan = new LinkedHashMap<>();
        plan.put(running, new RebalancePlanForBucket(running, 0, 0, origin, target));
        plan.put(pending, new RebalancePlanForBucket(pending, 0, 0, origin, target));
        String rebalanceId = "cancel-before-restart";
        registerRebalance(rebalanceId, plan);
        fromCtx(
                ctx -> {
                    eventProcessor.getRebalanceManager().cancelRebalance(rebalanceId);
                    return null;
                });
        assertThat(rebalanceStatus(pending)).isEqualTo(RebalanceStatus.CANCELED);
        assertThat(zookeeperClient.getRebalanceTask().get().isCancelRequested()).isTrue();
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        restartCoordinator(recoveredResponses);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().getExecutionKey(running))
                                .isNotNull());
        assertThat(rebalanceStatus(pending)).isEqualTo(RebalanceStatus.CANCELED);
        assertThat(eventProcessor.getRebalanceManager().getExecutionKey(pending)).isNull();
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(1))
                .isEqualTo(new BucketAssignment(origin));
        verifyIsr(pending, 0, Arrays.asList(0, 1));

        adjustRebalanceIsr(running, Arrays.asList(0, 1, 2, 3));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                            .isFalse();
                });
        assertThat(zookeeperClient.getRebalanceTask().get().getRebalanceStatus())
                .isEqualTo(RebalanceStatus.CANCELED);
        assertThat(zookeeperClient.getTableAssignment(tableId).get().getBucketAssignment(1))
                .isEqualTo(new BucketAssignment(origin));
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
        RebalanceExecutionKey key = eventProcessor.getRebalanceManager().getExecutionKey(first);
        eventProcessor.getCoordinatorEventManager().put(new RebalanceTaskTimeoutEvent(key));
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
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().getRebalanceId())
                                .isEqualTo(rebalanceId));
        TableBucket recoveredPending =
                plans.keySet().stream()
                        .filter(bucket -> rebalanceStatus(bucket) == RebalanceStatus.NOT_STARTED)
                        .findFirst()
                        .get();
        assertThat(
                        zookeeperClient
                                .getTableAssignment(tableId)
                                .get()
                                .getBucketAssignment(recoveredPending.getBucket()))
                .isEqualTo(new BucketAssignment(union));
        fromCtx(
                ctx -> {
                    assertThat(eventProcessor.isRebalanceTaskAtOrigin(plans.get(recoveredPending)))
                            .isFalse();
                    eventProcessor.getRebalanceManager().cancelRebalance(rebalanceId);
                    return null;
                });
        assertThat(rebalanceStatus(recoveredPending))
                .as("A previously started migration still needs draining after recovery")
                .isNotEqualTo(RebalanceStatus.CANCELED);
        for (TableBucket bucket : plans.keySet()) {
            adjustRebalanceIsr(bucket, union);
        }
        retry(
                Duration.ofMinutes(1),
                () -> {
                    drainPendingNotifyTriggers(recoveredResponses);
                    assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                            .isFalse();
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
        assertThat(eventProcessor.isRebalanceTaskAtOrigin(plan)).isFalse();
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
    void testStaleNotifyLeaderAndIsrResponseCannotCompleteRebalance() throws Exception {
        initCoordinatorChannel();

        long tableId = createTableWithReplicas(Arrays.asList(0, 1, 2), 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        installBlockingNotifyGateways(pendingResponses);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2));
        eventProcessor
                .getRebalanceManager()
                .registerRebalance(
                        "stale-response-test", Collections.singletonMap(tableBucket, plan));
        retry(Duration.ofMinutes(1), () -> assertThat(pendingResponses).isNotEmpty());

        LeaderAndIsr current = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        NotifyLeaderAndIsrRequestContext staleContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch() - 1,
                        eventProcessor.getRebalanceManager().getExecutionKey(tableBucket));
        notifyLeaderResponse(tableBucket, current.leader(), staleContext);
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        NotifyLeaderAndIsrRequestContext oldAttemptContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        new RebalanceExecutionKey("old-rebalance", tableBucket));
        notifyLeaderResponse(tableBucket, current.leader(), oldAttemptContext);
        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance()).isTrue();

        NotifyLeaderAndIsrRequestContext currentContext =
                new NotifyLeaderAndIsrRequestContext(
                        eventProcessor.getCoordinatorEpoch(),
                        current.leader(),
                        current.leaderEpoch(),
                        current.bucketEpoch(),
                        eventProcessor.getRebalanceManager().getExecutionKey(tableBucket));
        notifyLeaderResponse(tableBucket, current.leader(), currentContext);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventProcessor.getRebalanceManager().hasInProgressRebalance())
                                .isFalse());
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

    private RebalanceStatus rebalanceStatus(TableBucket tableBucket) {
        return eventProcessor
                .getRebalanceManager()
                .listRebalanceProgress(null)
                .progressForBucketMap()
                .get(tableBucket)
                .status();
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
        fromCtx(ctx -> null);
    }
}
