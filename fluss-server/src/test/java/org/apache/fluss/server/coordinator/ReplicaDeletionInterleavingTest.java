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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ControlledShutdownResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.ControlledShutdownEvent;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.event.NewTabletServerEvent;
import org.apache.fluss.server.coordinator.statemachine.ReplicaState;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData.ServerIdZNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OfflineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionIneligible;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionStarted;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests replica deletion across server lifecycle changes and delayed coordinator events. */
class ReplicaDeletionInterleavingTest extends CoordinatorEventProcessorTestBase {

    private final Map<Integer, TabletServerGateway> gateways = new ConcurrentHashMap<>();

    @BeforeEach
    void initializeGateways() {
        for (int serverId = 0; serverId < 3; serverId++) {
            gateways.put(serverId, new TestTabletServerGateway(false, Collections.emptySet()));
        }
        testCoordinatorChannelManager.setGateways(gateways);
    }

    @Test
    void testDeleteFormerLeaderDuringControlledShutdown() throws Exception {
        TablePath tablePath = TablePath.of(defaultDatabase, "drop_during_shutdown");
        long tableId = createTable(tablePath, 0, 1, 2);
        TableBucket bucket = new TableBucket(tableId, 0);
        TableBucketReplica replica = new TableBucketReplica(bucket, 0);
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));

        CompletableFuture<ControlledShutdownResponse> shutdownResponse = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(new ControlledShutdownEvent(0, 0, shutdownResponse));
        shutdownResponse.get(30, TimeUnit.SECONDS);
        verifyContext(
                ctx -> {
                    assertThat(ctx.getBucketLeaderAndIsr(bucket).get().leader()).isNotZero();
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(OnlineReplica);
                    assertThat(ctx.liveTabletServerSet()).doesNotContain(0);
                    assertThat(ctx.liveOrShuttingDownTabletServers()).contains(0);
                });

        // Keep the delete response pending until the server goes down.
        DeferredDeletionGateway shutdownGateway = new DeferredDeletionGateway();
        gateways.put(0, shutdownGateway);
        metadataManager.dropTable(tablePath, false);
        awaitReplicaState(replica, ReplicaDeletionStarted);
        shutdownGateway.deletionRequested.get(30, TimeUnit.SECONDS);

        gateways.remove(0);
        eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
        awaitReplicaState(replica, ReplicaDeletionIneligible);
        assertThat(zookeeperClient.getTableAssignment(tableId)).isPresent();

        // A fresh gateway cannot complete the old process's pending response. Cleanup must use
        // a new delete request generated by the server-up handler.
        gateways.put(0, new TestTabletServerGateway(false, Collections.emptySet()));
        eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
        awaitAssignmentRemoved(tableId);
    }

    @Test
    void testServerDownBeforeRecoveredDeletionIsAdmitted() throws Exception {
        TablePath blockingTablePath = TablePath.of(defaultDatabase, "blocking_drop");
        long blockingTableId = createTable(blockingTablePath, 1);
        TablePath tablePath = TablePath.of(defaultDatabase, "drop_after_recovery");
        long tableId = createTable(tablePath, 0);
        TableBucketReplica replica = new TableBucketReplica(new TableBucket(tableId, 0), 0);

        eventProcessor.shutdown();
        metadataManager.dropTable(blockingTablePath, false);
        metadataManager.dropTable(tablePath, false);

        DeferredDeletionGateway blockingGateway = new DeferredDeletionGateway();
        gateways.put(1, blockingGateway);
        eventProcessor = buildCoordinatorEventProcessor();
        // Fix the admission order so the recovered target table waits behind another deletion.
        eventProcessor.getLifecycleThrottler().submitTableDropForResume(blockingTableId);
        eventProcessor.startup();
        blockingGateway.deletionRequested.get(30, TimeUnit.SECONDS);
        verifyContext(
                ctx -> {
                    assertThat(ctx.isTableQueuedForDeletion(tableId)).isTrue();
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(OnlineReplica);
                    assertThat(eventProcessor.getLifecycleThrottler().getPendingDropCount())
                            .isEqualTo(1);
                });
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));

        gateways.remove(0);
        eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
        verifyContext(
                ctx -> {
                    assertThat(ctx.liveOrShuttingDownTabletServers()).doesNotContain(0);
                    // Normal server-down handling skips replicas pending deletion, and the
                    // deletion has not yet reached OfflineReplica or ReplicaDeletionStarted.
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(OnlineReplica);
                });

        blockingGateway.allowDeletion.complete(null);
        awaitAssignmentRemoved(blockingTableId);
        awaitReplicaState(replica, ReplicaDeletionIneligible);
        assertThat(zookeeperClient.getTableAssignment(tableId)).isPresent();

        gateways.put(0, new TestTabletServerGateway(false, Collections.emptySet()));
        eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
        awaitAssignmentRemoved(tableId);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLateLeaderFailureDoesNotResetPendingDeletion(boolean partitioned) throws Exception {
        DeferredLeaderFailureGateway oldGateway = new DeferredLeaderFailureGateway();
        gateways.put(0, oldGateway);
        TablePath tablePath = TablePath.of(defaultDatabase, "late_leader_failure");
        TableBucket bucket =
                partitioned
                        ? createPartition(tablePath, "p1")
                        : new TableBucket(createTable(tablePath, 0, 1, 2), 0);
        TableBucketReplica replica = new TableBucketReplica(bucket, 0);
        oldGateway.notificationReceived.get(30, TimeUnit.SECONDS);
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));

        gateways.remove(0);
        eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
        verifyContext(ctx -> assertThat(ctx.liveOrShuttingDownTabletServers()).doesNotContain(0));
        if (partitioned) {
            zookeeperClient.deletePartition(tablePath, "p1");
        } else {
            metadataManager.dropTable(tablePath, false);
        }
        awaitReplicaState(replica, ReplicaDeletionIneligible);

        // An activation request sent before deletion fails after the server-down event. Its
        // response must not move the replica back to OfflineReplica or recreate bucket state.
        oldGateway.releaseFailure.complete(null);
        verifyContext(
                ctx -> {
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(ReplicaDeletionIneligible);
                    assertThat(ctx.getBucketState(bucket)).isNull();
                });

        gateways.put(0, new TestTabletServerGateway(false, Collections.emptySet()));
        eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
        if (partitioned) {
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        assertThat(zookeeperClient.getPartitionAssignment(bucket.getPartitionId()))
                                .isEmpty();
                        verifyContext(
                                ctx -> {
                                    assertThat(ctx.getAssignment(bucket)).isEmpty();
                                    assertThat(ctx.getReplicaState(replica)).isNull();
                                    assertThat(ctx.isToBeDeleted(bucket)).isFalse();
                                });
                    });
            assertThat(zookeeperClient.tableExist(tablePath)).isTrue();
        } else {
            awaitAssignmentRemoved(bucket.getTableId());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLateLeaderFailureAfterDeletionDoesNotRecreateReplica(boolean partitioned)
            throws Exception {
        DeferredLeaderFailureGateway oldGateway = new DeferredLeaderFailureGateway();
        gateways.put(0, oldGateway);
        TablePath tablePath = TablePath.of(defaultDatabase, "leader_failure_after_drop");
        TableBucket bucket =
                partitioned
                        ? createPartition(tablePath, "p1")
                        : new TableBucket(createTable(tablePath, 0, 1, 2), 0);
        TableBucketReplica replica = new TableBucketReplica(bucket, 0);
        oldGateway.notificationReceived.get(30, TimeUnit.SECONDS);

        if (partitioned) {
            zookeeperClient.deletePartition(tablePath, "p1");
            retry(
                    Duration.ofSeconds(30),
                    () ->
                            assertThat(
                                            zookeeperClient.getPartitionAssignment(
                                                    bucket.getPartitionId()))
                                    .isEmpty());
        } else {
            metadataManager.dropTable(tablePath, false);
            awaitAssignmentRemoved(bucket.getTableId());
        }
        verifyContext(ctx -> assertThat(ctx.getReplicaState(replica)).isNull());

        oldGateway.releaseFailure.complete(null);
        verifyContext(
                ctx -> {
                    assertThat(ctx.getReplicaState(replica)).isNull();
                    assertThat(ctx.getAssignment(bucket)).isEmpty();
                    assertThat(ctx.getBucketState(bucket)).isNull();
                    assertThat(ctx.isToBeDeleted(bucket)).isFalse();
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLateDeleteResponseAfterRetryDoesNotRecreateReplica(boolean failed) throws Exception {
        TablePath tablePath = TablePath.of(defaultDatabase, "delete_response_after_retry");
        long tableId = createTable(tablePath, 0);
        TableBucketReplica replica = new TableBucketReplica(new TableBucket(tableId, 0), 0);
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));
        DeferredDeletionGateway oldGateway = new DeferredDeletionGateway(failed);
        gateways.put(0, oldGateway);
        metadataManager.dropTable(tablePath, false);
        oldGateway.deletionRequested.get(30, TimeUnit.SECONDS);
        awaitReplicaState(replica, ReplicaDeletionStarted);

        gateways.remove(0);
        eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
        awaitReplicaState(replica, ReplicaDeletionIneligible);
        gateways.put(0, new TestTabletServerGateway(false, Collections.emptySet()));
        eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
        awaitAssignmentRemoved(tableId);

        // The first process's response arrives after the retry has completed deletion.
        oldGateway.allowDeletion.complete(null);
        verifyContext(ctx -> assertThat(ctx.getReplicaState(replica)).isNull());
    }

    @Test
    void testOfflineRecoveredDeletionWaitsForAdmissionAcrossServerBounce() throws Exception {
        TablePath blockingTablePath = TablePath.of(defaultDatabase, "blocking_offline_drop");
        long blockingTableId = createTable(blockingTablePath, 1);
        TablePath tablePath = TablePath.of(defaultDatabase, "offline_queued_drop");
        long tableId = createTable(tablePath, 0);
        TableBucketReplica replica = new TableBucketReplica(new TableBucket(tableId, 0), 0);
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));
        TabletServerRegistration registration = zookeeperClient.getTabletServer(0).get();

        eventProcessor.shutdown();
        metadataManager.dropTable(blockingTablePath, false);
        metadataManager.dropTable(tablePath, false);
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(ServerIdZNode.path(0));
        DeferredDeletionGateway blockingGateway = new DeferredDeletionGateway();
        gateways.put(1, blockingGateway);
        eventProcessor = buildCoordinatorEventProcessor();
        eventProcessor.getLifecycleThrottler().submitTableDropForResume(blockingTableId);
        try {
            eventProcessor.startup();
            blockingGateway.deletionRequested.get(30, TimeUnit.SECONDS);
            awaitReplicaState(replica, OfflineReplica);

            // The server returns and fails again while another table still owns admission.
            eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
            verifyContext(
                    ctx -> assertThat(ctx.getReplicaState(replica)).isEqualTo(OfflineReplica));
            gateways.remove(0);
            eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
            verifyContext(
                    ctx -> assertThat(ctx.getReplicaState(replica)).isEqualTo(OfflineReplica));

            DeferredDeletionGateway targetGateway = new DeferredDeletionGateway();
            gateways.put(0, targetGateway);
            eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
            verifyContext(
                    ctx -> {
                        assertThat(ctx.getReplicaState(replica)).isEqualTo(OfflineReplica);
                        assertThat(targetGateway.deletionRequested).isNotDone();
                        assertThat(eventProcessor.getLifecycleThrottler().getPendingDropCount())
                                .isEqualTo(1);
                    });

            blockingGateway.allowDeletion.complete(null);
            awaitAssignmentRemoved(blockingTableId);
            targetGateway.deletionRequested.get(30, TimeUnit.SECONDS);
            awaitReplicaState(replica, ReplicaDeletionStarted);
            targetGateway.allowDeletion.complete(null);
            awaitAssignmentRemoved(tableId);
        } finally {
            zookeeperClient.registerTabletServer(0, registration);
        }
    }

    private TableBucket createPartition(TablePath tablePath, String partitionName)
            throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_TABLE_DESCRIPTOR.getSchema())
                        .distributedBy(1)
                        .partitionedBy("b")
                        .build()
                        .withReplicationFactor(3);
        long tableId =
                metadataManager.createTable(tablePath, remoteDataDir, descriptor, null, false);
        retry(
                Duration.ofSeconds(30),
                () -> verifyContext(ctx -> assertThat(ctx.containsTableId(tableId)).isTrue()));
        long partitionId = zookeeperClient.getPartitionIdAndIncrement();
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId,
                partitionName,
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(0, 1, 2))),
                remoteDataDir,
                tablePath,
                tableId,
                1);
        TableBucket bucket = new TableBucket(tableId, partitionId, 0);
        awaitReplicaState(new TableBucketReplica(bucket, 0), OnlineReplica);
        return bucket;
    }

    private long createTable(TablePath tablePath, Integer... replicas) throws Exception {
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(replicas)).build();
        long tableId =
                metadataManager.createTable(
                        tablePath,
                        remoteDataDir,
                        DATA1_TABLE_DESCRIPTOR
                                .withBucketCount(1)
                                .withReplicationFactor(replicas.length),
                        assignment,
                        false);
        retry(
                Duration.ofSeconds(30),
                () ->
                        verifyContext(
                                ctx -> {
                                    assertThat(ctx.replicaCounts(tableId))
                                            .isEqualTo(replicas.length);
                                    assertThat(ctx.areAllReplicasInState(tableId, OnlineReplica))
                                            .isTrue();
                                }));
        return tableId;
    }

    private void awaitReplicaState(TableBucketReplica replica, ReplicaState state) {
        retry(
                Duration.ofSeconds(30),
                () ->
                        verifyContext(
                                ctx -> assertThat(ctx.getReplicaState(replica)).isEqualTo(state)));
    }

    private void awaitAssignmentRemoved(long tableId) {
        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty();
                    verifyContext(
                            ctx -> {
                                assertThat(ctx.getAllReplicasForTable(tableId)).isEmpty();
                                assertThat(ctx.isTableQueuedForDeletion(tableId)).isFalse();
                            });
                });
    }

    private void verifyContext(Consumer<CoordinatorContext> assertion) throws Exception {
        fromContext(
                ctx -> {
                    assertion.accept(ctx);
                    return null;
                });
    }

    private <T> T fromContext(Function<CoordinatorContext, T> action) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(action);
        eventProcessor.getCoordinatorEventManager().put(event);
        try {
            return event.getResultFuture().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AssertionError) {
                throw (AssertionError) e.getCause();
            }
            throw e;
        }
    }

    /** Delays activation failures until deletion has started and the server is offline. */
    private static class DeferredLeaderFailureGateway extends TestTabletServerGateway {
        private final CompletableFuture<Void> notificationReceived = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseFailure = new CompletableFuture<>();

        private DeferredLeaderFailureGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
                NotifyLeaderAndIsrRequest request) {
            NotifyLeaderAndIsrResponse response = super.notifyLeaderAndIsr(request).join();
            response.getNotifyBucketsLeaderRespsList()
                    .forEach(bucket -> bucket.setErrorCode(Errors.DISK_WRITE_LOCKED.code()));
            notificationReceived.complete(null);
            return releaseFailure.thenApply(ignored -> response);
        }
    }

    /** Holds deletion responses so tests can order server-down and deletion admission events. */
    private static class DeferredDeletionGateway extends TestTabletServerGateway {
        private final CompletableFuture<Void> deletionRequested = new CompletableFuture<>();
        private final CompletableFuture<Void> allowDeletion = new CompletableFuture<>();
        private final boolean failed;

        private DeferredDeletionGateway() {
            this(false);
        }

        private DeferredDeletionGateway(boolean failed) {
            super(false, Collections.emptySet());
            this.failed = failed;
        }

        @Override
        public CompletableFuture<StopReplicaResponse> stopReplica(StopReplicaRequest request) {
            CompletableFuture<StopReplicaResponse> response = super.stopReplica(request);
            if (request.getStopReplicasReqsList().stream()
                    .anyMatch(PbStopReplicaReqForBucket::isDelete)) {
                deletionRequested.complete(null);
                return allowDeletion.thenCompose(
                        ignored ->
                                response.thenApply(
                                        result -> {
                                            if (failed) {
                                                result.getStopReplicasRespsList()
                                                        .forEach(
                                                                bucket ->
                                                                        bucket.setErrorCode(
                                                                                Errors
                                                                                        .STORAGE_EXCEPTION
                                                                                        .code()));
                                            }
                                            return result;
                                        }));
            }
            return response;
        }
    }
}
