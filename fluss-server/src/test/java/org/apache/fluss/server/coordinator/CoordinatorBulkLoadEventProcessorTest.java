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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitKvSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import org.apache.fluss.server.entity.CommitKvSnapshotData;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.testutils.KvTestUtils.mockCompletedSnapshot;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Bulk-load related tests for {@link CoordinatorEventProcessor}. */
class CoordinatorBulkLoadEventProcessorTest extends CoordinatorEventProcessorTestBase {

    private static final String MANIFEST_SHA256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @Test
    void testOrdinaryCommitsRequireCurrentActiveMetadataIdentity(@TempDir Path tempDir)
            throws Exception {
        TablePath tablePath = TablePath.of(defaultDatabase, "recreated_commit_identity");
        long oldTableId =
                createTable(
                        tablePath,
                        new TabletServerInfo[] {
                            new TabletServerInfo(0, "rack0"),
                            new TabletServerInfo(1, "rack1"),
                            new TabletServerInfo(2, "rack2")
                        });
        TableBucket tableBucket = new TableBucket(oldTableId, 0);
        waitValue(
                () -> fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket)),
                Duration.ofMinutes(1),
                "leader not elected");
        String metadataPath = ZkData.TableZNode.path(tablePath);
        TableRegistration oldRegistration = zookeeperClient.getTable(tablePath).get();
        int oldMetadataVersion =
                zookeeperClient.getDataWithStat(metadataPath).getStat().getVersion();

        zookeeperClient.updateTable(
                tablePath,
                oldRegistration.withDataState(
                        BulkLoadDataState.LOADING, "550e8400-e29b-41d4-a716-446655440000"));
        assertSnapshotAndManifestCommitRejected(tempDir, tableBucket, 98);

        zookeeperClient
                .getCuratorClient()
                .delete()
                .deletingChildrenIfNeeded()
                .forPath(metadataPath);
        TableRegistration recreated =
                TableRegistration.newTable(oldTableId + 1, remoteDataDir, TEST_TABLE);
        zookeeperClient.registerTable(tablePath, recreated);
        for (int i = 0; i < oldMetadataVersion; i++) {
            zookeeperClient.updateTable(tablePath, recreated);
        }
        assertThat(zookeeperClient.getDataWithStat(metadataPath).getStat().getVersion())
                .isEqualTo(oldMetadataVersion);

        assertSnapshotAndManifestCommitRejected(tempDir, tableBucket, 99);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNotifyResponseReleasesOnlyItsRequestPendingOwnership(boolean oldRequestFails)
            throws Exception {
        long tableId = oldRequestFails ? 92_001L : 92_000L;
        TableBucket tableBucket = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db", "pending_leader_" + tableId);
        List<Integer> assignment = Arrays.asList(0, 1);
        ControlledResponseGateway oldLeaderGateway = new ControlledResponseGateway(oldRequestFails);
        ControlledResponseGateway newLeaderGateway = new ControlledResponseGateway(false);
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        gateways.put(0, oldLeaderGateway);
        gateways.put(1, newLeaderGateway);
        testCoordinatorChannelManager.setGateways(gateways);

        LeaderAndIsr oldLeader =
                new LeaderAndIsr(
                        0,
                        0,
                        assignment,
                        Collections.emptyList(),
                        eventProcessor.getCoordinatorContext().getCoordinatorEpoch(),
                        0);
        LeaderAndIsr newLeader =
                new LeaderAndIsr(
                        1,
                        1,
                        oldRequestFails ? Collections.singletonList(1) : assignment,
                        Collections.emptyList(),
                        eventProcessor.getCoordinatorContext().getCoordinatorEpoch(),
                        1);
        CoordinatorRequestBatch requestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager,
                        eventProcessor.getCoordinatorEventManager(),
                        eventProcessor.getCoordinatorContext());

        fromCtx(
                context -> {
                    assertThat(context.liveTabletServerSet()).contains(0, 1);
                    context.putTablePath(tableId, tablePath);
                    context.updateBucketReplicaAssignment(tableBucket, assignment);
                    context.putReplicaState(new TableBucketReplica(tableBucket, 0), OnlineReplica);
                    context.putReplicaState(new TableBucketReplica(tableBucket, 1), OnlineReplica);
                    context.putBucketState(tableBucket, OnlineBucket);
                    context.putBucketLeaderAndIsr(tableBucket, oldLeader);
                    dispatch(requestBatch, context, tablePath, tableBucket, assignment, oldLeader);
                    return null;
                });

        fromCtx(
                context -> {
                    context.putBucketLeaderAndIsr(tableBucket, newLeader);
                    dispatch(requestBatch, context, tablePath, tableBucket, assignment, newLeader);
                    return null;
                });
        assertThat(oldLeaderGateway.getRequestCount()).isOne();
        assertThat(newLeaderGateway.getRequestCount()).isOne();
        assertInactive(tableBucket);

        oldLeaderGateway.releaseResponse();
        assertInactive(tableBucket);

        newLeaderGateway.releaseResponse();
        fromCtx(
                context -> {
                    assertThat(context.getPendingLeaderActivationBuckets()).isEmpty();
                    assertThat(context.isLeaderActive(tableBucket)).isTrue();
                    assertThat(
                                    CoordinatorService.computeClusterHealth(context)
                                            .getActiveLeaderReplicas())
                            .isOne();
                    assertThat(CoordinatorService.computeClusterHealth(context).getStatus())
                            .isEqualTo(oldRequestFails ? 1 : 0);
                    return null;
                });
    }

    @Test
    void testFeatureAdvertisementDoesNotSequenceOrdinaryTabletServerRestart() throws Exception {
        int serverId = 31;
        String serverPath = ZkData.ServerIdZNode.path(serverId);
        ZooKeeperClient processClient = newProcessClient();
        processClient.registerTabletServer(serverId, registration(serverId, 0));
        try {
            waitForServer(serverId, true);
            initCoordinatorChannel();

            TablePath tablePath = TablePath.of(defaultDatabase, "ordinary_restart_dispatch");
            TableAssignment assignment =
                    TableAssignment.builder().add(0, BucketAssignment.of(serverId)).build();
            metadataManager.createTable(tablePath, remoteDataDir, TEST_TABLE, assignment, false);
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
            waitValue(
                    () -> fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket)),
                    Duration.ofMinutes(1),
                    "leader not elected");

            processClient.close();
            waitForServer(serverId, false);

            processClient = newProcessClient();
            ControlledGateway gateway = new ControlledGateway();
            testCoordinatorChannelManager.setGateways(gateways(gateway, serverId));
            processClient.registerTabletServer(
                    serverId, registration(serverId, ServerProtocolVersion.CURRENT));
            waitForServer(serverId, true);
            fromCtx(Function.identity());

            assertThat(gateway.sequence())
                    .startsWith("metadata:alive", "metadata:full", "notify-role");
        } finally {
            processClient.close();
            ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(serverPath);
        }
    }

    @ParameterizedTest
    @CsvSource({"false, BEGUN", "false, COMMITTING", "true, BEGUN", "true, COMMITTING"})
    void testReassignmentRejectsActiveBulkLoadWithoutMetadataMutation(
            boolean partitioned, BulkLoadState state) throws Exception {
        int replacementServer = 19_003 + state.ordinal() + (partitioned ? 10 : 0);
        zookeeperClient.registerTabletServer(
                replacementServer,
                new TabletServerRegistration(
                        "rack" + replacementServer,
                        Collections.singletonList(
                                new Endpoint(
                                        "host" + replacementServer, 1001, DEFAULT_LISTENER_NAME)),
                        System.currentTimeMillis()));
        try {
            initCoordinatorChannel();
            TablePath tablePath =
                    TablePath.of(
                            defaultDatabase,
                            "bulk_load_reassignment_"
                                    + partitioned
                                    + "_"
                                    + state.name().toLowerCase());
            TableDescriptor descriptor =
                    partitioned
                            ? TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .column("part", DataTypes.STRING())
                                                    .primaryKey("id", "part")
                                                    .build())
                                    .distributedBy(1, "id")
                                    .partitionedBy("part")
                                    .build()
                                    .withReplicationFactor(3)
                            : TEST_TABLE;
            TableAssignment tableAssignment =
                    TableAssignment.builder().add(0, BucketAssignment.of(0, 1, 2)).build();
            long tableId =
                    metadataManager.createTable(
                            tablePath,
                            remoteDataDir,
                            descriptor,
                            partitioned ? null : tableAssignment,
                            false);
            Long partitionId = null;
            String registrationPath = ZkData.TableZNode.path(tablePath);
            String assignmentPath = ZkData.TableIdZNode.path(tableId);
            if (partitioned) {
                String partitionName = "p0";
                metadataManager.createPartition(
                        tablePath,
                        tableId,
                        remoteDataDir,
                        new PartitionAssignment(tableId, tableAssignment.getBucketAssignments()),
                        ResolvedPartitionSpec.fromPartitionValue("part", partitionName),
                        false,
                        tableAssignment.getBuckets().size());
                PartitionRegistration partitionRegistration =
                        zookeeperClient
                                .getPartition(tablePath, partitionName)
                                .orElseThrow(AssertionError::new);
                partitionId = partitionRegistration.getPartitionId();
                registrationPath = ZkData.PartitionZNode.path(tablePath, partitionName);
                assignmentPath = ZkData.PartitionIdZNode.path(partitionId);
            }
            TableBucket bucket = new TableBucket(tableId, partitionId, 0);
            String bulkLoadId =
                    String.format(
                            "550e8400-e29b-41d4-a716-%012d",
                            (partitioned ? 10 : 0) + state.ordinal());
            BulkLoadHandle handle =
                    new BulkLoadHandle(
                            partitioned
                                    ? PhysicalTablePath.of(tablePath, "p0")
                                    : PhysicalTablePath.of(tablePath),
                            tableId,
                            partitionId,
                            bulkLoadId);
            setDataState(
                    registrationPath,
                    tablePath,
                    partitioned,
                    BulkLoadDataState.LOADING,
                    bulkLoadId);
            String transactionPath =
                    partitioned
                            ? ZkData.BulkLoadPartitionTransactionZNode.path(partitionId, bulkLoadId)
                            : ZkData.BulkLoadTableTransactionZNode.path(tableId, bulkLoadId);
            zookeeperClient
                    .getCuratorClient()
                    .create()
                    .creatingParentsIfNeeded()
                    .forPath(
                            transactionPath,
                            ZkData.BulkLoadTransactionsZNode.encode(
                                    reassignmentTransaction(handle, state)));
            Map<String, ZooKeeperClient.DataWithStat> before = new LinkedHashMap<>();
            for (String path :
                    Arrays.asList(
                            assignmentPath,
                            ZkData.LeaderAndIsrZNode.path(bucket),
                            registrationPath,
                            transactionPath)) {
                before.put(path, zookeeperClient.getDataWithStat(path));
            }
            RebalancePlanForBucket plan =
                    new RebalancePlanForBucket(
                            bucket,
                            0,
                            0,
                            Arrays.asList(0, 1, 2),
                            Arrays.asList(0, 1, replacementServer));

            eventProcessor
                    .getRebalanceManager()
                    .registerRebalance(
                            "bulk-load-reassignment-" + bulkLoadId,
                            Collections.singletonMap(bucket, plan),
                            RebalanceStatus.NOT_STARTED);

            for (Map.Entry<String, ZooKeeperClient.DataWithStat> entry : before.entrySet()) {
                String path = entry.getKey();
                ZooKeeperClient.DataWithStat current = zookeeperClient.getDataWithStat(path);
                assertThat(current.getData())
                        .as(path + " bytes")
                        .isEqualTo(entry.getValue().getData());
                assertThat(current.getStat().getVersion())
                        .as(path + " version")
                        .isEqualTo(entry.getValue().getStat().getVersion());
            }
            setDataState(registrationPath, tablePath, partitioned, BulkLoadDataState.ACTIVE, null);
            zookeeperClient.getCuratorClient().delete().forPath(transactionPath);

        } finally {
            ZOO_KEEPER_EXTENSION_WRAPPER
                    .getCustomExtension()
                    .cleanupPath(ZkData.ServerIdZNode.path(replacementServer));
        }
    }

    private void assertSnapshotAndManifestCommitRejected(
            Path tempDir, TableBucket tableBucket, long snapshotId) throws Exception {
        CompletedSnapshot completedSnapshot =
                mockCompletedSnapshot(tempDir, tableBucket, snapshotId);
        LeaderAndIsr leaderAndIsr = currentLeaderAndIsr(tableBucket);
        CompletableFuture<CommitKvSnapshotResponse> snapshotFuture = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new CommitKvSnapshotEvent(
                                new CommitKvSnapshotData(
                                        completedSnapshot,
                                        coordinatorContextEpoch(),
                                        leaderAndIsr.leaderEpoch()),
                                snapshotFuture));
        assertThatThrownBy(() -> snapshotFuture.get(20, TimeUnit.SECONDS))
                .cause()
                .isInstanceOf(IllegalStateException.class);

        CompletableFuture<CommitRemoteLogManifestResponse> manifestFuture =
                new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new CommitRemoteLogManifestEvent(
                                new CommitRemoteLogManifestData(
                                        tableBucket,
                                        new FsPath("file:///tmp/rejected-manifest-" + snapshotId),
                                        0,
                                        1,
                                        coordinatorContextEpoch(),
                                        leaderAndIsr.leaderEpoch()),
                                manifestFuture));
        assertThat(manifestFuture.get(20, TimeUnit.SECONDS).isCommitSuccess()).isFalse();
        assertThat(
                        new ZooKeeperCompletedSnapshotHandleStore(zookeeperClient)
                                .get(tableBucket, snapshotId))
                .isEmpty();
        assertThat(zookeeperClient.getRemoteLogManifestHandle(tableBucket)).isEmpty();
    }

    private int coordinatorContextEpoch() throws Exception {
        return fromCtx(CoordinatorContext::getCoordinatorEpoch);
    }

    private LeaderAndIsr currentLeaderAndIsr(TableBucket tableBucket) throws Exception {
        return fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket).get());
    }

    private static void dispatch(
            CoordinatorRequestBatch requestBatch,
            CoordinatorContext context,
            TablePath tablePath,
            TableBucket tableBucket,
            List<Integer> assignment,
            LeaderAndIsr leaderAndIsr) {
        requestBatch.newBatch();
        requestBatch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(leaderAndIsr.leader()),
                PhysicalTablePath.of(tablePath),
                tableBucket,
                assignment,
                leaderAndIsr);
        requestBatch.sendRequestToTabletServers(context.getCoordinatorEpoch());
    }

    private void assertInactive(TableBucket tableBucket) throws Exception {
        fromCtx(
                context -> {
                    assertThat(context.getPendingLeaderActivationBuckets())
                            .containsExactly(tableBucket);
                    assertThat(context.isLeaderActive(tableBucket)).isFalse();
                    assertThat(CoordinatorService.computeClusterHealth(context).getStatus())
                            .isEqualTo(2);
                    return null;
                });
    }

    private static final class ControlledResponseGateway extends TestTabletServerGateway {

        private final CompletableFuture<Void> responseRelease = new CompletableFuture<>();
        private final AtomicInteger requestCount = new AtomicInteger();

        private ControlledResponseGateway(boolean failResponse) {
            super(failResponse, Collections.emptySet());
        }

        @Override
        public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
                NotifyLeaderAndIsrRequest request) {
            requestCount.incrementAndGet();
            NotifyLeaderAndIsrResponse response = super.notifyLeaderAndIsr(request).join();
            return responseRelease.thenApply(ignored -> response);
        }

        @Override
        public CompletableFuture<UpdateMetadataResponse> updateMetadata(
                UpdateMetadataRequest request) {
            return CompletableFuture.completedFuture(new UpdateMetadataResponse());
        }

        private int getRequestCount() {
            return requestCount.get();
        }

        private void releaseResponse() {
            assertThat(responseRelease.complete(null)).isTrue();
        }
    }

    private static ZooKeeperClient newProcessClient() throws Exception {
        ZooKeeperClient client =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .createZooKeeperClient(NOPErrorHandler.INSTANCE);
        if (!client.getCuratorClient().blockUntilConnected(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Test TabletServer ZooKeeper session did not connect.");
        }
        return client;
    }

    private static Map<Integer, TabletServerGateway> gateways(
            TabletServerGateway controlledGateway, int controlledServerId) throws Exception {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int serverId : zookeeperClient.getSortedTabletServerList()) {
            gateways.put(serverId, new TestTabletServerGateway(false, Collections.emptySet()));
        }
        gateways.put(controlledServerId, controlledGateway);
        return gateways;
    }

    private static TabletServerRegistration registration(int serverId, int serverProtocolVersion) {
        return new TabletServerRegistration(
                "rack" + serverId,
                Collections.singletonList(
                        new Endpoint("host" + serverId, 1234, DEFAULT_LISTENER_NAME)),
                System.currentTimeMillis(),
                TabletServerResource.unknown(),
                serverProtocolVersion);
    }

    private void waitForServer(int serverId, boolean expectedLive) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    boolean actual =
                            fromCtx(context -> context.liveTabletServerSet().contains(serverId));
                    assertThat(actual).isEqualTo(expectedLive);
                });
    }

    private static final class ControlledGateway extends TestTabletServerGateway {

        private final CompletableFuture<Void> membershipResponseRelease = new CompletableFuture<>();
        private final CompletableFuture<Void> roleResponseRelease = new CompletableFuture<>();
        private final ConcurrentLinkedDeque<String> sequence = new ConcurrentLinkedDeque<>();

        private ControlledGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<UpdateMetadataResponse> updateMetadata(
                UpdateMetadataRequest request) {
            boolean empty =
                    request.getTableMetadatasCount() == 0
                            && request.getPartitionMetadatasCount() == 0;
            sequence.add(empty ? "metadata:alive" : "metadata:full");
            return empty
                    ? membershipResponseRelease.thenApply(ignored -> new UpdateMetadataResponse())
                    : CompletableFuture.completedFuture(new UpdateMetadataResponse());
        }

        @Override
        public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
                NotifyLeaderAndIsrRequest request) {
            sequence.add("notify-role");
            NotifyLeaderAndIsrResponse response = super.notifyLeaderAndIsr(request).join();
            return roleResponseRelease.thenApply(ignored -> response);
        }

        private List<String> sequence() {
            return sequence.stream().collect(Collectors.toList());
        }
    }

    private static void setDataState(
            String registrationPath,
            TablePath tablePath,
            boolean partitioned,
            BulkLoadDataState dataState,
            @Nullable String bulkLoadId)
            throws Exception {
        byte[] encoded;
        if (partitioned) {
            PartitionRegistration registration =
                    zookeeperClient.getPartition(tablePath, "p0").orElseThrow(AssertionError::new);
            encoded =
                    ZkData.PartitionZNode.encode(registration.withDataState(dataState, bulkLoadId));
        } else {
            TableRegistration registration =
                    zookeeperClient.getTable(tablePath).orElseThrow(AssertionError::new);
            encoded = ZkData.TableZNode.encode(registration.withDataState(dataState, bulkLoadId));
        }
        zookeeperClient.getCuratorClient().setData().forPath(registrationPath, encoded);
    }

    private static BulkLoadTransaction reassignmentTransaction(
            BulkLoadHandle handle, BulkLoadState state) {
        boolean commitDecided = state == BulkLoadState.COMMITTING;
        return new BulkLoadTransaction(
                handle,
                commitDecided ? BulkLoadState.COMMITTING : BulkLoadState.BEGUN,
                "alice",
                "USER",
                "file:///tmp",
                1,
                0,
                new long[] {1L},
                Long.MAX_VALUE,
                commitDecided ? MANIFEST_SHA256 : null,
                null,
                null);
    }
}
