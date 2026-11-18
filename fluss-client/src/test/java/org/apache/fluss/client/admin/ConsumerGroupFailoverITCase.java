/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerGroupClient;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotConsumerGroupOwnerException;
import org.apache.fluss.metadata.GroupId;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that an existing client recovers when the offsets-table leader moves, whether the old
 * owner is restarted or remains online.
 *
 * <p>Existing scanners and offset readers retain their cached owner through each transition. After
 * a server restart, a fresh {@link LogScanner} also verifies the position written by the recovered
 * scanner.
 */
public class ConsumerGroupFailoverITCase extends ClientToServerITCaseBase {

    @Test
    void testCommittedOffsetSurvivesOwnerFailover() throws Exception {
        TablePath tablePath =
                TablePath.of("consumer_offset_failover", "table_" + System.nanoTime());
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("value", DataTypes.STRING())
                                                .build())
                                .distributedBy(1, "id")
                                .build(),
                        false);
        appendRows(tablePath, 20);
        String groupId = "failover-group-" + System.nanoTime();
        TableBucket dataBucket = new TableBucket(tableId, null, 0);

        Configuration groupConfiguration = new Configuration(clientConf);
        groupConfiguration.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        groupConfiguration.setBoolean(
                ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), false);
        try (Connection groupConnection = ConnectionFactory.createConnection(groupConfiguration);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner();
                ConsumerGroupClient offsetReader =
                        new ConsumerGroupClient(
                                groupId,
                                ((FlussConnection) groupConnection).getMetadataUpdater(),
                                false,
                                0L,
                                ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis())) {
            scanner.subscribeFromBeginning(0);
            scanner.commitSync(
                    Collections.singletonMap(dataBucket, new OffsetMetadata(17L, "before")));
            assertThat(
                            offsetReader
                                    .fetchCommittedOffsets(
                                            Collections.singleton(dataBucket), 30_000L)
                                    .get(dataBucket)
                                    .getOffset())
                    .isEqualTo(17L);

            long offsetsTableId =
                    admin.getTableInfo(TablePath.of("sys", "consumer_offsets")).get().getTableId();
            int bucketCount =
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getTableAssignment(offsetsTableId)
                            .orElseThrow(
                                    () -> new AssertionError("consumer offsets table is missing"))
                            .getBucketAssignments()
                            .size();
            waitAllReplicasReady(offsetsTableId, bucketCount);

            int bucketId =
                    FlussBucketingFunction.bucketForRowKey(
                            new ConsumerOffsetSchema().encodeGroupBucketKey(GroupId.of(groupId)),
                            bucketCount);
            TableBucket offsetsBucket = new TableBucket(offsetsTableId, null, bucketId);
            int oldLeader =
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getLeaderAndIsr(offsetsBucket)
                            .orElseThrow(() -> new AssertionError("offsets bucket has no leader"))
                            .leader();
            TabletServerGateway cachedOwnerGateway = offsetReader.getOrDiscoverOwner().get();

            FLUSS_CLUSTER_EXTENSION.restartTabletServer(oldLeader, new Configuration());
            int newLeader = waitUntilLeaderChanges(offsetsBucket, oldLeader);
            assertThat(newLeader).isNotEqualTo(oldLeader);

            // Both clients cached the old owner before failover. They must invalidate it when
            // the old endpoint fails and discover the new leader without being recreated.
            scanner.commitSync(
                    Collections.singletonMap(dataBucket, new OffsetMetadata(18L, "after")),
                    Duration.ofMinutes(1));
            assertThat(
                            offsetReader
                                    .fetchCommittedOffsets(
                                            Collections.singleton(dataBucket), 60_000L)
                                    .get(dataBucket)
                                    .getOffset())
                    .isEqualTo(18L);
            assertThat(offsetReader.getOrDiscoverOwner().get()).isNotSameAs(cachedOwnerGateway);
        }

        // A new scanner must resume from the offset committed by the original scanner.
        try (Connection freshConnection = ConnectionFactory.createConnection(groupConfiguration);
                Table table = freshConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            int records = 0;
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = scanner.poll(Duration.ofMillis(200));
                for (ScanRecord ignored : scanRecords) {
                    records++;
                }
                if (records >= 2) {
                    break;
                }
            }
            assertThat(records).isEqualTo(2);
        }
    }

    @Test
    void testCachedOwnerRecoversAfterOnlineLeaderTransfer() throws Exception {
        TablePath tablePath =
                TablePath.of("consumer_offset_failover", "online_" + System.nanoTime());
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("value", DataTypes.STRING())
                                                .build())
                                .distributedBy(1, "id")
                                .build(),
                        false);
        appendRows(tablePath, 5);
        String groupId = "online-owner-move-" + System.nanoTime();
        TableBucket dataBucket = new TableBucket(tableId, null, 0);
        Configuration groupConfiguration = new Configuration(clientConf);
        groupConfiguration.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        groupConfiguration.setBoolean(
                ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), false);

        try (Connection groupConnection = ConnectionFactory.createConnection(groupConfiguration);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner();
                LogScanner resumeScanner = table.newScan().createLogScanner();
                ConsumerGroupClient offsetReader =
                        new ConsumerGroupClient(
                                groupId,
                                ((FlussConnection) groupConnection).getMetadataUpdater(),
                                false,
                                0L,
                                ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis())) {
            scanner.subscribeFromBeginning(0);
            scanner.commitSync(
                    Collections.singletonMap(dataBucket, new OffsetMetadata(3L, "before")));
            resumeScanner.commitSync(
                    Collections.singletonMap(dataBucket, new OffsetMetadata(3L, "before")));
            assertThat(
                            offsetReader
                                    .fetchCommittedOffsets(
                                            Collections.singleton(dataBucket), 30_000L)
                                    .get(dataBucket)
                                    .getOffset())
                    .isEqualTo(3L);

            long offsetsTableId =
                    admin.getTableInfo(TablePath.of("sys", "consumer_offsets")).get().getTableId();
            TableAssignment assignment =
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getTableAssignment(offsetsTableId)
                            .orElseThrow(
                                    () -> new AssertionError("consumer offsets table is missing"));
            int bucketId =
                    FlussBucketingFunction.bucketForRowKey(
                            new ConsumerOffsetSchema().encodeGroupBucketKey(GroupId.of(groupId)),
                            assignment.getBucketAssignments().size());
            TableBucket offsetsBucket = new TableBucket(offsetsTableId, null, bucketId);
            List<Integer> replicas = assignment.getBucketAssignment(bucketId).getReplicas();
            assertThat(replicas).hasSize(3);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(offsetsBucket);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            FLUSS_CLUSTER_EXTENSION
                                                    .getZooKeeperClient()
                                                    .getLeaderAndIsr(offsetsBucket)
                                                    .get()
                                                    .isr())
                                    .containsExactlyInAnyOrderElementsOf(replicas));
            int oldOwner =
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getLeaderAndIsr(offsetsBucket)
                            .get()
                            .leader();
            TabletServerGateway cachedOwnerGateway = offsetReader.getOrDiscoverOwner().get();
            int newOwner = replicas.stream().filter(id -> id != oldOwner).findFirst().get();
            TabletServerGateway oldGateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(oldOwner);

            transferLeader(offsetsBucket, oldOwner, newOwner, replicas);
            assertThat(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(offsetsBucket)).isEqualTo(newOwner);
            FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(offsetsBucket, oldOwner);
            waitUntilOldOwnerRejectsCommit(oldGateway, groupId, dataBucket);

            // The second scanner fetches through its own stale owner without refreshing the
            // committing scanner's cached owner.
            resumeScanner.subscribeFromCommittedOffset(0);
            List<Integer> resumedIds = new ArrayList<>();
            long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
            while (resumedIds.size() < 2 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = resumeScanner.poll(Duration.ofMillis(200));
                for (ScanRecord record : scanRecords) {
                    resumedIds.add(record.getRow().getInt(0));
                }
            }
            assertThat(resumedIds).containsExactly(3, 4);

            // The scanner still has the old owner cached and must rediscover before committing.
            scanner.commitSync(
                    Collections.singletonMap(dataBucket, new OffsetMetadata(4L, "after")),
                    Duration.ofMinutes(1));
            assertThat(
                            offsetReader
                                    .fetchCommittedOffsets(
                                            Collections.singleton(dataBucket), 60_000L)
                                    .get(dataBucket)
                                    .getOffset())
                    .isEqualTo(4L);
            assertThat(offsetReader.getOrDiscoverOwner().get()).isNotSameAs(cachedOwnerGateway);
        }
    }

    @Test
    void testAsyncCommitInFlightDuringOnlineLeaderTransfer() throws Exception {
        TablePath tablePath =
                TablePath.of("consumer_offset_failover", "async_" + System.nanoTime());
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("value", DataTypes.STRING())
                                                .build())
                                .distributedBy(1, "id")
                                .build(),
                        false);
        TableBucket dataBucket = new TableBucket(tableId, null, 0);
        String groupId = "async-owner-move-" + System.nanoTime();
        Configuration groupConfiguration = new Configuration(clientConf);
        groupConfiguration.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        groupConfiguration.setBoolean(
                ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), false);
        groupConfiguration.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofMinutes(2));

        try (Connection groupConnection = ConnectionFactory.createConnection(groupConfiguration);
                Table table = groupConnection.getTable(tablePath)) {
            FlussConnection connection = (FlussConnection) groupConnection;
            MetadataUpdater realUpdater = connection.getMetadataUpdater();
            try (ConsumerGroupClient offsetReader =
                    new ConsumerGroupClient(
                            groupId,
                            realUpdater,
                            false,
                            0L,
                            ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis())) {
                Map<TableBucket, OffsetMetadata> initialOffset =
                        Collections.singletonMap(dataBucket, new OffsetMetadata(3L, "before"));
                assertThat(offsetReader.commitOffsetsSync(initialOffset, 60_000L)).isTrue();

                long offsetsTableId =
                        admin.getTableInfo(TablePath.of("sys", "consumer_offsets"))
                                .get()
                                .getTableId();
                TableAssignment assignment =
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getTableAssignment(offsetsTableId)
                                .orElseThrow(
                                        () ->
                                                new AssertionError(
                                                        "consumer offsets table is missing"));
                int bucketId =
                        FlussBucketingFunction.bucketForRowKey(
                                new ConsumerOffsetSchema()
                                        .encodeGroupBucketKey(GroupId.of(groupId)),
                                assignment.getBucketAssignments().size());
                TableBucket offsetsBucket = new TableBucket(offsetsTableId, null, bucketId);
                List<Integer> replicas = assignment.getBucketAssignment(bucketId).getReplicas();
                assertThat(replicas).hasSize(3);
                FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(offsetsBucket);
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                assertThat(
                                                FLUSS_CLUSTER_EXTENSION
                                                        .getZooKeeperClient()
                                                        .getLeaderAndIsr(offsetsBucket)
                                                        .get()
                                                        .isr())
                                        .containsExactlyInAnyOrderElementsOf(replicas));
                int oldOwner =
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getLeaderAndIsr(offsetsBucket)
                                .get()
                                .leader();
                int newOwner = replicas.stream().filter(id -> id != oldOwner).findFirst().get();
                TabletServerGateway oldGateway = realUpdater.newTabletServerClientForNode(oldOwner);

                CompletableFuture<Void> requestHeld = new CompletableFuture<>();
                CompletableFuture<Void> sendHeldRequest = new CompletableFuture<>();
                AtomicBoolean pauseNextCommit = new AtomicBoolean();
                TabletServerGateway gatedGateway =
                        new TestTabletServerGateway(false, Collections.emptySet()) {
                            @Override
                            public CompletableFuture<FindConsumerGroupOwnerResponse>
                                    findConsumerGroupOwner(FindConsumerGroupOwnerRequest request) {
                                return oldGateway.findConsumerGroupOwner(request);
                            }

                            @Override
                            public CompletableFuture<CommitConsumerOffsetsResponse>
                                    commitConsumerOffsets(CommitConsumerOffsetsRequest request) {
                                if (pauseNextCommit.compareAndSet(true, false)) {
                                    requestHeld.complete(null);
                                    return sendHeldRequest.thenCompose(
                                            ignored -> oldGateway.commitConsumerOffsets(request));
                                }
                                return oldGateway.commitConsumerOffsets(request);
                            }
                        };
                try (RpcClient rpcClient =
                        RpcClient.create(groupConfiguration, connection.getClientMetricGroup())) {
                    MetadataUpdater gatedUpdater =
                            new MetadataUpdater(
                                    rpcClient, groupConfiguration, realUpdater.getCluster()) {
                                @Override
                                public TabletServerGateway newTabletServerClientForNode(
                                        ServerNode serverNode) {
                                    return serverNode.id() == oldOwner
                                            ? gatedGateway
                                            : realUpdater.newTabletServerClientForNode(serverNode);
                                }
                            };
                    try (LogScanner scanner =
                            new LogScannerImpl(
                                    groupConfiguration,
                                    table.getTableInfo(),
                                    gatedUpdater,
                                    connection.getClientMetricGroup(),
                                    connection.getOrCreateRemoteFileDownloader(),
                                    null,
                                    new ClientSchemaGetter(
                                            tablePath,
                                            table.getTableInfo().getSchemaInfo(),
                                            connection.getAdmin()),
                                    null)) {
                        scanner.subscribeFromBeginning(0);
                        scanner.commitSync(initialOffset);
                        CompletableFuture<Exception> callbackError = new CompletableFuture<>();
                        try {
                            pauseNextCommit.set(true);
                            scanner.commitAsync(
                                    Collections.singletonMap(
                                            dataBucket, new OffsetMetadata(4L, "in-flight")),
                                    (ignored, error) -> callbackError.complete(error));
                            requestHeld.get(10, TimeUnit.SECONDS);
                            assertThat(callbackError).isNotDone();

                            transferLeader(offsetsBucket, oldOwner, newOwner, replicas);
                            assertThat(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(offsetsBucket))
                                    .isEqualTo(newOwner);
                            FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(
                                    offsetsBucket, oldOwner);
                            waitUntilOldOwnerRejectsCommit(oldGateway, groupId, dataBucket);

                            // Release the pending request against the old server after it steps
                            // down. Its failure must reach the callback and invalidate the cache.
                            sendHeldRequest.complete(null);
                            waitUntil(
                                    () -> {
                                        scanner.commitSync(Collections.emptyMap());
                                        return callbackError.isDone();
                                    },
                                    Duration.ofSeconds(10),
                                    Duration.ofMillis(50),
                                    "Failed commit callback was not delivered");
                            assertThat(callbackError.get(10, TimeUnit.SECONDS))
                                    .isInstanceOf(NotConsumerGroupOwnerException.class);
                            assertThat(
                                            offsetReader
                                                    .fetchCommittedOffsets(
                                                            Collections.singleton(dataBucket),
                                                            60_000L)
                                                    .get(dataBucket)
                                                    .getOffset())
                                    .isEqualTo(3L);

                            scanner.commitSync(
                                    Collections.singletonMap(
                                            dataBucket, new OffsetMetadata(5L, "after")),
                                    Duration.ofMinutes(1));
                            assertThat(
                                            offsetReader
                                                    .fetchCommittedOffsets(
                                                            Collections.singleton(dataBucket),
                                                            60_000L)
                                                    .get(dataBucket)
                                                    .getOffset())
                                    .isEqualTo(5L);
                        } finally {
                            sendHeldRequest.complete(null);
                        }
                    }
                }
            }
        }
    }

    private void waitUntilOldOwnerRejectsCommit(
            TabletServerGateway oldGateway, String groupId, TableBucket dataBucket) {
        CommitConsumerOffsetsRequest staleOwnerRequest =
                ClientRpcMessageUtils.makeCommitConsumerOffsetsRequest(
                        groupId,
                        Collections.singletonMap(dataBucket, new OffsetMetadata(999L, "rejected")));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThatThrownBy(
                                        () ->
                                                oldGateway
                                                        .commitConsumerOffsets(staleOwnerRequest)
                                                        .get())
                                .rootCause()
                                .isInstanceOf(NotConsumerGroupOwnerException.class));
    }

    private void transferLeader(
            TableBucket offsetsBucket, int oldOwner, int newOwner, List<Integer> replicas)
            throws Exception {
        List<Integer> newReplicas = new ArrayList<>(replicas);
        newReplicas.remove(Integer.valueOf(newOwner));
        newReplicas.add(0, newOwner);
        Map<TableBucket, RebalancePlanForBucket> plan =
                Collections.singletonMap(
                        offsetsBucket,
                        new RebalancePlanForBucket(
                                offsetsBucket, oldOwner, newOwner, replicas, newReplicas));
        String rebalanceId = "consumer-offset-client-rebalance-" + System.nanoTime();
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .registerRebalanceTask(
                        new RebalanceTask(rebalanceId, RebalanceStatus.NOT_STARTED, plan));
        AccessContextEvent<Void> event =
                new AccessContextEvent<>(
                        context -> {
                            FLUSS_CLUSTER_EXTENSION
                                    .getRebalanceManager()
                                    .registerRebalance(rebalanceId, plan);
                            return null;
                        });
        FLUSS_CLUSTER_EXTENSION
                .getCoordinatorServer()
                .getCoordinatorEventProcessor()
                .getCoordinatorEventManager()
                .put(event);
        event.getResultFuture().get();
    }

    private void appendRows(TablePath tablePath, int count) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < count; i++) {
                writer.append(GenericRow.of(i, BinaryString.fromString("v" + i)));
            }
            writer.flush();
        }
    }

    private int waitUntilLeaderChanges(TableBucket bucket, int oldLeader) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<LeaderAndIsr> leaderAndIsr =
                    FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getLeaderAndIsr(bucket);
            if (leaderAndIsr.isPresent() && leaderAndIsr.get().leader() != oldLeader) {
                return leaderAndIsr.get().leader();
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Leader did not change for " + bucket);
    }
}
