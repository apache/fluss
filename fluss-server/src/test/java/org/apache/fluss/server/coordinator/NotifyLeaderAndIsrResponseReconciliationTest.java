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

import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import org.apache.fluss.server.coordinator.statemachine.ReplicaState;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OfflineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests reconciliation of explicit per-bucket NotifyLeaderAndIsr failures. */
class NotifyLeaderAndIsrResponseReconciliationTest extends CoordinatorEventProcessorTestBase {

    private static final TableDescriptor TEST_TABLE =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .primaryKey("a")
                                    .build())
                    .distributedBy(1, "a")
                    .property(ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED.key(), "true")
                    .build()
                    .withReplicationFactor(1);

    @Test
    void testRetriableErrorsAreExplicitlyClassified() {
        assertThat(
                        Arrays.asList(
                                Errors.FENCED_LEADER_EPOCH_EXCEPTION,
                                Errors.INVALID_UPDATE_VERSION_EXCEPTION,
                                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                                Errors.NOT_LEADER_OR_FOLLOWER))
                .allMatch(CoordinatorEventProcessor::isRetriableNotifyLeaderAndIsrError);
        assertThat(
                        Arrays.asList(
                                Errors.DISK_WRITE_LOCKED,
                                Errors.STORAGE_EXCEPTION,
                                Errors.LOG_STORAGE_EXCEPTION,
                                Errors.KV_STORAGE_EXCEPTION,
                                Errors.UNKNOWN_SERVER_ERROR))
                .noneMatch(CoordinatorEventProcessor::isRetriableNotifyLeaderAndIsrError);
    }

    @Test
    void testRetriableFailureResendsNewerCoordinatorState() throws Exception {
        TableSetup table = createTable("retry_newer_coordinator_state");
        LeaderAndIsr newerLeaderAndIsr =
                new LeaderAndIsr(
                        table.leaderAndIsr.leader(),
                        table.leaderAndIsr.leaderEpoch() + 1,
                        table.leaderAndIsr.isr(),
                        table.leaderAndIsr.standbyReplicas(),
                        table.leaderAndIsr.coordinatorEpoch(),
                        table.leaderAndIsr.bucketEpoch() + 1);
        fromCtx(
                context -> {
                    context.putBucketLeaderAndIsr(table.tableBucket, newerLeaderAndIsr);
                    return null;
                });

        AtomicReference<NotifyLeaderAndIsrRequest> retriedRequest = new AtomicReference<>();
        installRecordingGateway(table.serverId, retriedRequest);

        sendFailureEvent(table, Errors.FENCED_LEADER_EPOCH_EXCEPTION);

        retry(Duration.ofMinutes(1), () -> assertThat(retriedRequest.get()).isNotNull());
        PbNotifyLeaderAndIsrReqForBucket retriedBucket =
                retriedRequest.get().getNotifyBucketsLeaderReqsList().get(0);
        assertThat(retriedBucket.getLeaderEpoch()).isEqualTo(newerLeaderAndIsr.leaderEpoch());
        assertThat(retriedBucket.getBucketEpoch()).isEqualTo(newerLeaderAndIsr.bucketEpoch());
        ReplicaState replicaState = fromCtx(ctx -> ctx.getReplicaState(table.replica));
        assertThat(replicaState).isEqualTo(OnlineReplica);
    }

    @Test
    void testRetriableFailureWithUnchangedStateMarksReplicaOffline() throws Exception {
        TableSetup table = createTable("unchanged_coordinator_state");

        sendFailureEvent(table, Errors.INVALID_UPDATE_VERSION_EXCEPTION);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    ReplicaState replicaState = fromCtx(ctx -> ctx.getReplicaState(table.replica));
                    assertThat(replicaState).isEqualTo(OfflineReplica);
                });
    }

    @Test
    void testRetriableFailureIgnoresReplicasNoLongerRelevantToCurrentState() throws Exception {
        TableSetup removedReplica = createTable("removed_replica");
        TableSetup deletingBucket = createTable("deleting_bucket");
        AtomicReference<NotifyLeaderAndIsrRequest> retriedRequest = new AtomicReference<>();
        installRecordingGateway(removedReplica.serverId, retriedRequest);
        fromCtx(
                context -> {
                    context.updateBucketReplicaAssignment(
                            removedReplica.tableBucket, Collections.emptyList());
                    context.queueTableDeletion(
                            Collections.singleton(deletingBucket.tableBucket.getTableId()));
                    return null;
                });

        sendFailureEvent(removedReplica, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION);
        sendFailureEvent(deletingBucket, Errors.NOT_LEADER_OR_FOLLOWER);
        fromCtx(context -> null);

        assertThat(retriedRequest.get()).isNull();
        ReplicaState removedReplicaState =
                fromCtx(ctx -> ctx.getReplicaState(removedReplica.replica));
        ReplicaState deletingReplicaState =
                fromCtx(ctx -> ctx.getReplicaState(deletingBucket.replica));
        assertThat(removedReplicaState).isEqualTo(OnlineReplica);
        assertThat(deletingReplicaState).isEqualTo(OnlineReplica);
    }

    private TableSetup createTable(String tableName) throws Exception {
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                Arrays.stream(zookeeperClient.getSortedTabletServerList())
                        .boxed()
                        .collect(Collectors.toSet()),
                Collections.<ApiKeys>emptySet());
        TablePath tablePath = TablePath.of(defaultDatabase, tableName);
        TableAssignment assignment =
                generateAssignment(1, 1, new TabletServerInfo[] {new TabletServerInfo(0, "rack0")});
        long tableId =
                metadataManager.createTable(
                        tablePath, remoteDataDir, TEST_TABLE, assignment, false);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        TableBucketReplica replica = new TableBucketReplica(tableBucket, 0);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    ReplicaState replicaState = fromCtx(ctx -> ctx.getReplicaState(replica));
                    assertThat(replicaState).isEqualTo(OnlineReplica);
                });
        LeaderAndIsr leaderAndIsr = fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
        return new TableSetup(tablePath, tableBucket, replica, 0, leaderAndIsr);
    }

    private void sendFailureEvent(TableSetup table, Errors error) {
        NotifyLeaderAndIsrData requestData =
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(table.tablePath),
                        table.tableBucket,
                        Collections.singletonList(table.serverId),
                        table.leaderAndIsr);
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new NotifyLeaderAndIsrResponseReceivedEvent(
                                Collections.singletonList(
                                        new NotifyLeaderAndIsrResultForBucket(
                                                table.tableBucket,
                                                new ApiError(error, "test failure"))),
                                table.serverId,
                                Collections.singletonList(requestData)));
    }

    private Map<Integer, TabletServerGateway> successfulGateways() throws Exception {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int serverId : zookeeperClient.getSortedTabletServerList()) {
            gateways.put(serverId, new TestTabletServerGateway(false, Collections.emptySet()));
        }
        return gateways;
    }

    private void installRecordingGateway(
            int serverId, AtomicReference<NotifyLeaderAndIsrRequest> request) throws Exception {
        Map<Integer, TabletServerGateway> gateways = successfulGateways();
        gateways.put(
                serverId,
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
                            NotifyLeaderAndIsrRequest notifyRequest) {
                        request.set(notifyRequest);
                        return super.notifyLeaderAndIsr(notifyRequest);
                    }
                });
        testCoordinatorChannelManager.setGateways(gateways);
    }

    private <T> T fromCtx(Function<CoordinatorContext, T> function) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(function);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private static final class TableSetup {
        private final TablePath tablePath;
        private final TableBucket tableBucket;
        private final TableBucketReplica replica;
        private final int serverId;
        private final LeaderAndIsr leaderAndIsr;

        private TableSetup(
                TablePath tablePath,
                TableBucket tableBucket,
                TableBucketReplica replica,
                int serverId,
                LeaderAndIsr leaderAndIsr) {
            this.tablePath = tablePath;
            this.tableBucket = tableBucket;
            this.replica = replica;
            this.serverId = serverId;
            this.leaderAndIsr = leaderAndIsr;
        }
    }
}
