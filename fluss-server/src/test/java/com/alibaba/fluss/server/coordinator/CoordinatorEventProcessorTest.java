/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FencedLeaderEpochException;
import com.alibaba.fluss.exception.InvalidCoordinatorException;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.server.coordinator.event.AccessContextEvent;
import com.alibaba.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEventManager;
import com.alibaba.fluss.server.coordinator.statemachine.BucketState;
import com.alibaba.fluss.server.coordinator.statemachine.ReplicaState;
import com.alibaba.fluss.server.entity.CommitKvSnapshotData;
import com.alibaba.fluss.server.entity.CommitRemoteLogManifestData;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.ServerMetadataCacheImpl;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;
import com.alibaba.fluss.server.tablet.TestTabletServerGateway;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdsZNode;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static com.alibaba.fluss.server.coordinator.CoordinatorTestUtils.checkLeaderAndIsr;
import static com.alibaba.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static com.alibaba.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestFailContext;
import static com.alibaba.fluss.server.coordinator.statemachine.BucketState.OfflineBucket;
import static com.alibaba.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.OfflineReplica;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static com.alibaba.fluss.server.testutils.KvTestUtils.mockCompletedSnapshot;
import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CoordinatorEventProcessor}. */
class CoordinatorEventProcessorTest {

    private static final int N_BUCKETS = 3;
    private static final int REPLICATION_FACTOR = 3;

    private static final TableDescriptor TEST_TABLE =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .primaryKey("a")
                                    .build())
                    .distributedBy(3, "a")
                    .build()
                    .withReplicationFactor(REPLICATION_FACTOR);

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;

    private CoordinatorEventProcessor eventProcessor;
    private final String defaultDatabase = "db";
    private ServerMetadataCache serverMetadataCache;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private AutoPartitionManager autoPartitionManager;
    private LakeTableTieringManager lakeTableTieringManager;
    private CompletedSnapshotStoreManager completedSnapshotStoreManager;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager = new MetadataManager(zookeeperClient, new Configuration());
        // register 3 tablet servers
        for (int i = 0; i < 3; i++) {
            zookeeperClient.registerTabletServer(
                    i,
                    new TabletServerRegistration(
                            "rack" + i,
                            Collections.singletonList(
                                    new Endpoint("host" + i, 1000, DEFAULT_LISTENER_NAME)),
                            System.currentTimeMillis()));
        }
    }

    @BeforeEach
    void beforeEach() throws IOException {
        serverMetadataCache = new ServerMetadataCacheImpl();
        // set a test channel manager for the context
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        autoPartitionManager =
                new AutoPartitionManager(serverMetadataCache, metadataManager, new Configuration());
        lakeTableTieringManager = new LakeTableTieringManager();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        eventProcessor = buildCoordinatorEventProcessor();
        eventProcessor.startup();
        metadataManager.createDatabase(
                defaultDatabase, DatabaseDescriptor.builder().build(), false);
        completedSnapshotStoreManager = eventProcessor.completedSnapshotStoreManager();
    }

    @AfterEach
    void afterEach() {
        eventProcessor.shutdown();
        metadataManager.dropDatabase(defaultDatabase, false, true);
        // clear the assignment info for all tables;
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(TableIdsZNode.path());
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(PartitionIdsZNode.path());
    }

    @Test
    void testCreateAndDropTable() throws Exception {
        // make sure all request to gateway should be successful
        initCoordinatorChannel();
        // create a table,
        TablePath t1 = TablePath.of(defaultDatabase, "create_drop_t1");
        TableDescriptor tableDescriptor = TEST_TABLE;
        int nBuckets = 3;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        new TabletServerInfo[] {
                            new TabletServerInfo(0, "rack0"),
                            new TabletServerInfo(1, "rack1"),
                            new TabletServerInfo(2, "rack2")
                        });
        long t1Id = metadataManager.createTable(t1, tableDescriptor, tableAssignment, false);

        TablePath t2 = TablePath.of(defaultDatabase, "create_drop_t2");
        long t2Id = metadataManager.createTable(t2, tableDescriptor, tableAssignment, false);

        verifyTableCreated(t2Id, tableAssignment, nBuckets, replicationFactor);

        // mock CompletedSnapshotStore
        for (TableBucket tableBucket : allTableBuckets(t1Id, nBuckets)) {
            completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(
                    new TableBucket(tableBucket.getTableId(), tableBucket.getBucket()));
        }
        assertThat(completedSnapshotStoreManager.getBucketCompletedSnapshotStores()).isNotEmpty();

        // drop the table;
        metadataManager.dropTable(t1, false);

        verifyTableDropped(t1Id);

        // verify CompleteSnapshotStore has been removed when the table is dropped
        assertThat(completedSnapshotStoreManager.getBucketCompletedSnapshotStores()).isEmpty();

        // replicas and buckets for t2 should still be online
        verifyBucketForTableInState(t2Id, nBuckets, BucketState.OnlineBucket);
        verifyReplicaForTableInState(
                t2Id, nBuckets * replicationFactor, ReplicaState.OnlineReplica);

        // shutdown event processor and delete the table node for t2 from zk
        // to mock the case that the table hasn't been deleted completely
        // , but the coordinator shut down
        eventProcessor.shutdown();
        metadataManager.dropTable(t2, false);

        // start the coordinator
        eventProcessor = buildCoordinatorEventProcessor();
        initCoordinatorChannel();
        eventProcessor.startup();
        // make sure the table can still be deleted successfully
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getTableAssignment(t2Id)).isEmpty());

        // no replica and bucket for t2 should exist in the context
        Set<TableBucket> tableBuckets = fromCtx(ctx -> ctx.getAllBucketsForTable(t2Id));
        assertThat(tableBuckets).isEmpty();

        Set<TableBucketReplica> tableBucketReplicas =
                fromCtx(ctx -> ctx.getAllReplicasForTable(t2Id));
        assertThat(tableBucketReplicas).isEmpty();
    }

    @Test
    void testDropTableWithRetry() throws Exception {
        // make request to some server should fail, but delete will still be successful
        // finally with retry logic
        int failedServer = 0;
        initCoordinatorChannel(failedServer);
        // create a table,
        TablePath t1 = TablePath.of(defaultDatabase, "tdrop");
        final long t1Id =
                createTable(
                        t1,
                        new TabletServerInfo[] {
                            new TabletServerInfo(0, "rack0"),
                            new TabletServerInfo(1, "rack1"),
                            new TabletServerInfo(2, "rack2")
                        });

        // retry until the create table t1 has been handled by coordinator
        // otherwise, when receive create table event, it can't find the schema of the table
        // since it has been deleted by the following code) which cause delete
        // won't don anything
        // todo: may need to fix this case;
        retryVerifyContext(ctx -> assertThat(ctx.getTablePathById(t1Id)).isNotNull());

        // drop the table;
        metadataManager.dropTable(t1, false);

        // retry until the assignment has been deleted from zk, then it means
        // the table has been deleted successfully
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getTableAssignment(t1Id)).isEmpty());
    }

    @Test
    void testServerBecomeOnlineAndOfflineLine() throws Exception {
        // make sure all request to gateway should be successful
        initCoordinatorChannel();
        // assume a new server become online;
        // check the server has been added into coordinator context
        ZooKeeperClient client =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .createZooKeeperClient(NOPErrorHandler.INSTANCE);
        int newlyServerId = 3;
        TabletServerRegistration tabletServerRegistration =
                new TabletServerRegistration(
                        "rack3",
                        Endpoint.fromListenersString(DEFAULT_LISTENER_NAME + "://host3:1234"),
                        System.currentTimeMillis());
        client.registerTabletServer(newlyServerId, tabletServerRegistration);

        // retry until the tablet server register event is been handled
        retryVerifyContext(
                ctx -> assertThat(ctx.getLiveTabletServers()).containsKey(newlyServerId));

        initCoordinatorChannel();
        // verify the context has the exact tablet server
        retryVerifyContext(
                ctx -> {
                    ServerInfo tabletServer = ctx.getLiveTabletServers().get(newlyServerId);
                    assertThat(tabletServer.id()).isEqualTo(newlyServerId);

                    assertThat(tabletServer.endpoints())
                            .isEqualTo(tabletServerRegistration.getEndpoints());
                });

        // we try to assign a replica to this newly server, every thing will
        // be fine
        // t1: {bucket0: [0, 3, 2], bucket1: [3, 2, 0]}, t2: {bucket0: [3]}
        MetadataManager metadataManager = new MetadataManager(zookeeperClient, new Configuration());
        TableAssignment table1Assignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0, 3, 2))
                        .add(1, BucketAssignment.of(3, 2, 0))
                        .build();

        TablePath table1Path = TablePath.of(defaultDatabase, "t1");
        long table1Id =
                metadataManager.createTable(table1Path, TEST_TABLE, table1Assignment, false);

        TableAssignment table2Assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(3)).build();
        TablePath table2Path = TablePath.of(defaultDatabase, "t2");
        long table2Id =
                metadataManager.createTable(table2Path, TEST_TABLE, table2Assignment, false);

        // retry until the table2 been created
        retryVerifyContext(
                ctx ->
                        assertThat(ctx.getBucketLeaderAndIsr(new TableBucket(table2Id, 0)))
                                .isNotEmpty());

        // now, assume the server 3 is down;
        client.close();

        // retry until the server has been removed from coordinator context
        retryVerifyContext(
                ctx -> assertThat(ctx.getLiveTabletServers()).doesNotContainKey(newlyServerId));

        // check replica state
        // all replicas should be online but the replica in the down server
        // should be offline
        verifyReplicaOnlineOrOffline(
                table1Id, table1Assignment, Collections.singleton(newlyServerId));
        verifyBucketIsr(table1Id, 0, new int[] {0, 2});
        verifyBucketIsr(table1Id, 1, new int[] {2, 0});
        verifyReplicaOnlineOrOffline(
                table2Id, table2Assignment, Collections.singleton(newlyServerId));
        verifyBucketIsr(table2Id, 0, new int[] {3});

        // now, check bucket state
        TableBucket t1Bucket0 = new TableBucket(table1Id, 0);
        TableBucket t1Bucket1 = new TableBucket(table1Id, 1);
        TableBucket t2Bucket0 = new TableBucket(table2Id, 0);
        // t1 bucket 0 should still be online since the leader is alive

        BucketState t1Bucket0State = fromCtx(ctx -> ctx.getBucketState(t1Bucket0));
        assertThat(t1Bucket0State).isEqualTo(OnlineBucket);
        // t1 bucket 1 should reelect a leader since the leader is not alive
        // the bucket whose leader is in the server should be online a again, but the leadership
        // should change the leader for bucket2 of t1 should change since the leader fail
        BucketState t1Bucket1State = fromCtx(ctx -> ctx.getBucketState(t1Bucket1));
        assertThat(t1Bucket1State).isEqualTo(OnlineBucket);
        // leader should change to replica2, leader epoch should be 1
        checkLeaderAndIsr(zookeeperClient, t1Bucket1, 1, 2);

        // the bucket with no any other available servers should be still offline,
        // t2 bucket0 should still be offline
        BucketState t2Bucket0State = fromCtx(ctx -> ctx.getBucketState(t2Bucket0));
        assertThat(t2Bucket0State).isEqualTo(OfflineBucket);

        // assume the server that comes again
        zookeeperClient.registerTabletServer(newlyServerId, tabletServerRegistration);
        // retry until the server has been added to coordinator context
        retryVerifyContext(
                ctx -> assertThat(ctx.getLiveTabletServers()).containsKey(newlyServerId));

        // make sure the bucket that remains in offline should be online again
        // since the server become online
        // bucket0 for t2 should then be online
        // retry until the state changes
        retryVerifyContext(
                ctx -> assertThat(ctx.getBucketState(t2Bucket0)).isEqualTo(OnlineBucket));

        // make sure all the replica will be online again
        verifyReplicaOnlineOrOffline(table1Id, table1Assignment, Collections.emptySet());
        verifyReplicaOnlineOrOffline(table2Id, table2Assignment, Collections.emptySet());

        // let's restart to check everything is ok
        eventProcessor.shutdown();
        eventProcessor = buildCoordinatorEventProcessor();

        // in this test case, so make requests to gateway should always be
        // successful for when start up, it will send request to tablet servers
        initCoordinatorChannel();
        eventProcessor.startup();

        // check every thing is ok
        // all replicas should be online again
        verifyReplicaOnlineOrOffline(table1Id, table1Assignment, Collections.emptySet());
        verifyReplicaOnlineOrOffline(table2Id, table2Assignment, Collections.emptySet());
        // all bucket should be online
        t1Bucket0State = fromCtx(ctx -> ctx.getBucketState(t1Bucket0));
        assertThat(t1Bucket0State).isEqualTo(OnlineBucket);

        t1Bucket1State = fromCtx(ctx -> ctx.getBucketState(t1Bucket1));
        assertThat(t1Bucket1State).isEqualTo(OnlineBucket);

        t2Bucket0State = fromCtx(ctx -> ctx.getBucketState(t2Bucket0));
        assertThat(t2Bucket0State).isEqualTo(OnlineBucket);
    }

    @Test
    void testRestartTriggerReplicaToOffline() throws Exception {
        // case1: coordinator server restart, and first set the replica to online
        // but the request to the replica server fail which will then cause it offline
        MetadataManager metadataManager = new MetadataManager(zookeeperClient, new Configuration());
        TableAssignment tableAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0, 1, 2))
                        .add(1, BucketAssignment.of(1, 2, 0))
                        .build();
        TablePath tablePath = TablePath.of(defaultDatabase, "t_restart");
        long table1Id = metadataManager.createTable(tablePath, TEST_TABLE, tableAssignment, false);

        // let's restart
        initCoordinatorChannel();
        eventProcessor.shutdown();
        eventProcessor = buildCoordinatorEventProcessor();
        int failedServer = 0;
        initCoordinatorChannel(failedServer);
        eventProcessor.startup();

        // all buckets should be online
        TableBucket t1Bucket0 = new TableBucket(table1Id, 0);
        TableBucket t1Bucket1 = new TableBucket(table1Id, 1);
        // retry until the bucket0 change leader to 1
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsr =
                            zookeeperClient.getLeaderAndIsr(t1Bucket0);
                    assertThat(leaderAndIsr).isPresent();
                    assertThat(leaderAndIsr.get().leader()).isEqualTo(1);
                });

        // check the changed leader and isr info
        checkLeaderAndIsr(zookeeperClient, t1Bucket0, 1, 1);
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.getBucketState(t1Bucket0)).isEqualTo(OnlineBucket);
                    assertThat(ctx.getBucketState(t1Bucket1)).isEqualTo(OnlineBucket);
                });
        // only replica0 will be offline
        verifyReplicaOnlineOrOffline(
                table1Id, tableAssignment, Collections.singleton(failedServer));
    }

    @Test
    void testAddBucketCompletedSnapshot(@TempDir Path tempDir) throws Exception {
        ZooKeeperCompletedSnapshotHandleStore completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(zookeeperClient);
        TablePath t1 = TablePath.of(defaultDatabase, "t_completed_snapshot");
        final long t1Id =
                createTable(
                        t1,
                        new TabletServerInfo[] {
                            new TabletServerInfo(0, "rack0"),
                            new TabletServerInfo(1, "rack1"),
                            new TabletServerInfo(2, "rack2")
                        });
        CoordinatorEventManager coordinatorEventManager =
                eventProcessor.getCoordinatorEventManager();
        int snapshotNum = 2;
        int bucketLeaderEpoch = 0;
        int coordinatorEpoch = 0;
        for (int i = 0; i < N_BUCKETS; i++) {
            TableBucket tableBucket = new TableBucket(t1Id, i);
            // wait until the leader is elected
            waitValue(
                    () -> zookeeperClient.getLeaderAndIsr(tableBucket),
                    Duration.ofMinutes(1),
                    "leader not elected");
            for (int snapshot = 0; snapshot < snapshotNum; snapshot++) {
                CompletedSnapshot completedSnapshot =
                        mockCompletedSnapshot(tempDir, tableBucket, snapshot);
                CompletableFuture<CommitKvSnapshotResponse> responseCompletableFuture =
                        new CompletableFuture<>();
                coordinatorEventManager.put(
                        new CommitKvSnapshotEvent(
                                new CommitKvSnapshotData(
                                        completedSnapshot, coordinatorEpoch, bucketLeaderEpoch),
                                responseCompletableFuture));

                // get the response
                responseCompletableFuture.get();

                // get completed snapshot
                CompletedSnapshot gotCompletedSnapshot =
                        completedSnapshotHandleStore
                                .get(tableBucket, snapshot)
                                .get()
                                .retrieveCompleteSnapshot();
                // check the gotten snapshot
                assertThat(gotCompletedSnapshot).isEqualTo(completedSnapshot);
            }
        }

        // we check invalid case
        TableBucket tableBucket = new TableBucket(t1Id, 0);

        // in valid bucket leader epoch
        int invalidBucketLeaderEpoch = -1;
        CompletedSnapshot completedSnapshot = mockCompletedSnapshot(tempDir, tableBucket, 2);
        CompletableFuture<CommitKvSnapshotResponse> responseCompletableFuture =
                new CompletableFuture<>();
        coordinatorEventManager.put(
                new CommitKvSnapshotEvent(
                        new CommitKvSnapshotData(
                                completedSnapshot, coordinatorEpoch, invalidBucketLeaderEpoch),
                        responseCompletableFuture));
        assertThatThrownBy(responseCompletableFuture::get)
                .cause()
                .isInstanceOf(FencedLeaderEpochException.class);

        // invalid coordinator epoch
        int invalidCoordinatorEpoch = 1;
        completedSnapshot = mockCompletedSnapshot(tempDir, tableBucket, 2);
        responseCompletableFuture = new CompletableFuture<>();
        coordinatorEventManager.put(
                new CommitKvSnapshotEvent(
                        new CommitKvSnapshotData(
                                completedSnapshot, bucketLeaderEpoch, invalidCoordinatorEpoch),
                        responseCompletableFuture));
        assertThatThrownBy(responseCompletableFuture::get)
                .cause()
                .isInstanceOf(InvalidCoordinatorException.class);
    }

    @Test
    void testCreateAndDropPartition() throws Exception {
        TablePath tablePath = TablePath.of(defaultDatabase, "test_create_drop_partition");
        // make sure all request to gateway should be successful
        initCoordinatorChannel();
        // create a partitioned table
        TableDescriptor tablePartitionTableDescriptor = getPartitionedTable();
        long tableId =
                metadataManager.createTable(tablePath, tablePartitionTableDescriptor, null, false);

        int nBuckets = 3;
        int replicationFactor = 3;
        Map<Integer, BucketAssignment> assignments =
                generateAssignment(
                                nBuckets,
                                replicationFactor,
                                new TabletServerInfo[] {
                                    new TabletServerInfo(0, "rack0"),
                                    new TabletServerInfo(1, "rack1"),
                                    new TabletServerInfo(2, "rack2")
                                })
                        .getBucketAssignments();
        PartitionAssignment partitionAssignment = new PartitionAssignment(tableId, assignments);
        Tuple2<PartitionIdName, PartitionIdName> partitionIdAndNameTuple2 =
                preparePartitionAssignment(tablePath, tableId, partitionAssignment);

        long partition1Id = partitionIdAndNameTuple2.f0.partitionId;
        String partition1Name = partitionIdAndNameTuple2.f0.partitionName;
        long partition2Id = partitionIdAndNameTuple2.f1.partitionId;

        verifyPartitionCreated(
                new TablePartition(tableId, partition1Id),
                partitionAssignment,
                nBuckets,
                replicationFactor);
        verifyPartitionCreated(
                new TablePartition(tableId, partition2Id),
                partitionAssignment,
                nBuckets,
                replicationFactor);

        // mock CompletedSnapshotStore for partition1
        for (TableBucket tableBucket : allTableBuckets(tableId, partition1Id, nBuckets)) {
            completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(
                    new TableBucket(
                            tableBucket.getTableId(),
                            tableBucket.getPartitionId(),
                            tableBucket.getBucket()));
        }

        assertThat(completedSnapshotStoreManager.getBucketCompletedSnapshotStores()).isNotEmpty();

        // drop the partition
        zookeeperClient.deletePartition(tablePath, partition1Name);
        verifyPartitionDropped(tableId, partition1Id);

        // verify CompleteSnapshotStore has been removed when the table partition1 is dropped
        assertThat(completedSnapshotStoreManager.getBucketCompletedSnapshotStores()).isEmpty();

        // now, drop the table and restart the coordinator event processor,
        // the partition2 should be dropped
        eventProcessor.shutdown();
        metadataManager.dropTable(tablePath, false);

        // start the coordinator
        eventProcessor = buildCoordinatorEventProcessor();
        initCoordinatorChannel();
        eventProcessor.startup();
        verifyPartitionDropped(tableId, partition2Id);
    }

    @Test
    void testRestartResumeDropPartition() throws Exception {
        TablePath tablePath = TablePath.of(defaultDatabase, "test_resume_drop_partition");
        // make sure all request to gateway should be successful
        initCoordinatorChannel();
        // create a partitioned table
        TableDescriptor tablePartitionTableDescriptor = getPartitionedTable();
        long tableId =
                metadataManager.createTable(tablePath, tablePartitionTableDescriptor, null, false);

        int nBuckets = 3;
        int replicationFactor = 3;
        Map<Integer, BucketAssignment> assignments =
                generateAssignment(
                                nBuckets,
                                replicationFactor,
                                new TabletServerInfo[] {
                                    new TabletServerInfo(0, "rack0"),
                                    new TabletServerInfo(1, "rack1"),
                                    new TabletServerInfo(2, "rack2")
                                })
                        .getBucketAssignments();
        PartitionAssignment partitionAssignment = new PartitionAssignment(tableId, assignments);
        Tuple2<PartitionIdName, PartitionIdName> partitionIdAndNameTuple2 =
                preparePartitionAssignment(tablePath, tableId, partitionAssignment);

        long partition1Id = partitionIdAndNameTuple2.f0.partitionId;
        String partition2Name = partitionIdAndNameTuple2.f1.partitionName;
        long partition2Id = partitionIdAndNameTuple2.f1.partitionId;

        verifyPartitionCreated(
                new TablePartition(tableId, partition1Id),
                partitionAssignment,
                nBuckets,
                replicationFactor);
        verifyPartitionCreated(
                new TablePartition(tableId, partition2Id),
                partitionAssignment,
                nBuckets,
                replicationFactor);

        // now, drop partition2 and restart the coordinator event processor,
        // the partition2 should be dropped
        eventProcessor.shutdown();
        zookeeperClient.deletePartition(tablePath, partition2Name);

        // start the coordinator
        eventProcessor = buildCoordinatorEventProcessor();
        initCoordinatorChannel();
        eventProcessor.startup();

        // verify partition2 is dropped
        verifyPartitionDropped(tableId, partition2Id);
        // verify the status of partition1
        verifyPartitionCreated(
                new TablePartition(tableId, partition1Id),
                partitionAssignment,
                nBuckets,
                replicationFactor);
    }

    @Test
    void testNotifyOffsetsWithShrinkISR(@TempDir Path tempDir) throws Exception {
        initCoordinatorChannel();
        TablePath t1 = TablePath.of(defaultDatabase, "test_notify_with_shrink_isr");
        final long t1Id =
                createTable(
                        t1,
                        new TabletServerInfo[] {
                            new TabletServerInfo(0, "rack0"),
                            new TabletServerInfo(1, "rack1"),
                            new TabletServerInfo(2, "rack2")
                        });
        TableBucket tableBucket = new TableBucket(t1Id, 0);
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> fromCtx((ctx) -> ctx.getBucketLeaderAndIsr(tableBucket)),
                        Duration.ofMinutes(1),
                        "leader not elected");
        // remove one follower from isr
        int leader = leaderAndIsr.leader();
        int bucketLeaderEpoch = leaderAndIsr.leaderEpoch();
        int coordinatorEpoch = leaderAndIsr.coordinatorEpoch();
        List<Integer> newIsr = leaderAndIsr.isr();
        Integer follower = newIsr.stream().filter(i -> i != leader).findFirst().get();
        newIsr.remove(follower);
        // change isr in coordinator context
        fromCtx(
                ctx -> {
                    ctx.putBucketLeaderAndIsr(
                            tableBucket,
                            new LeaderAndIsr(
                                    leader,
                                    leaderAndIsr.leaderEpoch(),
                                    newIsr,
                                    coordinatorEpoch,
                                    bucketLeaderEpoch));
                    return null;
                });

        CoordinatorEventManager coordinatorEventManager =
                eventProcessor.getCoordinatorEventManager();

        // verify CommitRemoteLogManifest trigger notify offsets request
        CompletableFuture<CommitRemoteLogManifestResponse> responseCompletableFuture1 =
                new CompletableFuture<>();
        coordinatorEventManager.put(
                new CommitRemoteLogManifestEvent(
                        new CommitRemoteLogManifestData(
                                tableBucket,
                                new FsPath(tempDir.toString()),
                                0,
                                0,
                                coordinatorEpoch,
                                bucketLeaderEpoch),
                        responseCompletableFuture1));
        responseCompletableFuture1.get();
        verifyReceiveRequestExceptFor(3, leader, NotifyRemoteLogOffsetsRequest.class);

        // verify CommitKvSnapshot trigger notify offsets request
        initCoordinatorChannel();
        CompletedSnapshot completedSnapshot = mockCompletedSnapshot(tempDir, tableBucket, 0);
        CompletableFuture<CommitKvSnapshotResponse> responseCompletableFuture2 =
                new CompletableFuture<>();
        coordinatorEventManager.put(
                new CommitKvSnapshotEvent(
                        new CommitKvSnapshotData(
                                completedSnapshot, coordinatorEpoch, bucketLeaderEpoch),
                        responseCompletableFuture2));
        responseCompletableFuture2.get();
        verifyReceiveRequestExceptFor(3, leader, NotifyKvSnapshotOffsetRequest.class);
    }

    private CoordinatorEventProcessor buildCoordinatorEventProcessor() {
        return new CoordinatorEventProcessor(
                zookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                new Configuration(),
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")));
    }

    private void initCoordinatorChannel() throws Exception {
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                Arrays.stream(zookeeperClient.getSortedTabletServerList())
                        .boxed()
                        .collect(Collectors.toSet()));
    }

    private void initCoordinatorChannel(int failedServer) throws Exception {
        makeSendLeaderAndStopRequestFailContext(
                testCoordinatorChannelManager,
                Arrays.stream(zookeeperClient.getSortedTabletServerList())
                        .boxed()
                        .collect(Collectors.toSet()),
                Collections.singleton(failedServer));
    }

    private Tuple2<PartitionIdName, PartitionIdName> preparePartitionAssignment(
            TablePath tablePath, long tableId, PartitionAssignment partitionAssignment)
            throws Exception {
        // retry util the table has been put into context
        retryVerifyContext(ctx -> assertThat(ctx.getTablePathById(tableId)).isNotNull());

        // create partition
        long partition1Id = zookeeperClient.getPartitionIdAndIncrement();
        long partition2Id = zookeeperClient.getPartitionIdAndIncrement();
        String partition1Name = "2024";
        String partition2Name = "2025";
        zookeeperClient.registerPartitionAssignment(partition1Id, partitionAssignment);
        zookeeperClient.registerPartition(tablePath, tableId, partition1Name, partition1Id);
        zookeeperClient.registerPartitionAssignment(partition2Id, partitionAssignment);
        zookeeperClient.registerPartition(tablePath, tableId, partition2Name, partition2Id);

        return Tuple2.of(
                new PartitionIdName(partition1Id, partition1Name),
                new PartitionIdName(partition2Id, partition2Name));
    }

    private void verifyTableCreated(
            long tableId, TableAssignment tableAssignment, int nBuckets, int replicationFactor)
            throws Exception {
        int replicasCount = nBuckets * replicationFactor;
        // retry until the all replicas in t2 is online
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.replicaCounts(tableId)).isEqualTo(replicasCount);
                    assertThat(ctx.areAllReplicasInState(tableId, ReplicaState.OnlineReplica))
                            .isTrue();
                });
        // make sure all should be online
        verifyBucketForTableInState(tableId, nBuckets, OnlineBucket);
        verifyReplicaForTableInState(tableId, replicasCount, OnlineReplica);

        for (TableBucket tableBucket : allTableBuckets(tableId, nBuckets)) {
            checkLeaderAndIsr(
                    zookeeperClient,
                    tableBucket,
                    0,
                    tableAssignment
                            .getBucketAssignment(tableBucket.getBucket())
                            .getReplicas()
                            .get(0));
        }
    }

    private void verifyPartitionCreated(
            TablePartition tablePartition,
            TableAssignment tableAssignment,
            int nBuckets,
            int replicationFactor)
            throws Exception {
        int replicasCount = nBuckets * replicationFactor;
        // retry until the all replicas in t2 is online
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.replicaCounts(tablePartition)).isEqualTo(replicasCount);
                    assertThat(
                                    ctx.areAllReplicasInState(
                                            tablePartition, ReplicaState.OnlineReplica))
                            .isTrue();
                });

        // make sure all should be online
        verifyBucketForPartitionInState(tablePartition, nBuckets, BucketState.OnlineBucket);
        verifyReplicaForPartitionInState(
                tablePartition, nBuckets * replicationFactor, ReplicaState.OnlineReplica);

        for (TableBucket tableBucket :
                allTableBuckets(
                        tablePartition.getTableId(), tablePartition.getPartitionId(), nBuckets)) {
            checkLeaderAndIsr(
                    zookeeperClient,
                    tableBucket,
                    0,
                    tableAssignment
                            .getBucketAssignment(tableBucket.getBucket())
                            .getReplicas()
                            .get(0));
        }
    }

    private void verifyTableDropped(long tableId) {
        // retry until the assignment has been deleted from zk, then it means
        // the table/partition has been deleted successfully
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty());
        // no replica and bucket for the table/partition should exist in the context
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.getAllBucketsForTable(tableId)).isEmpty();
                    assertThat(ctx.getAllReplicasForTable(tableId)).isEmpty();
                });
    }

    private void verifyPartitionDropped(long tableId, long partitionId) throws Exception {
        // retry until the assignment has been deleted from zk, then it means
        // the table/partition has been deleted successfully
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getPartitionAssignment(partitionId)).isEmpty());
        // no replica and bucket for the partition should exist in the context
        Set<TableBucket> tableBuckets =
                fromCtx(ctx -> ctx.getAllBucketsForPartition(tableId, partitionId));
        assertThat(tableBuckets).isEmpty();

        Set<TableBucketReplica> tableBucketReplicas =
                fromCtx(ctx -> ctx.getAllReplicasForPartition(tableId, partitionId));
        assertThat(tableBucketReplicas).isEmpty();

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getPartitionAssignment(partitionId)).isEmpty());
    }

    private void verifyBucketForTableInState(
            long tableId, int expectedBucketCount, BucketState expectedState) {
        retryVerifyContext(
                ctx -> {
                    Set<TableBucket> buckets = ctx.getAllBucketsForTable(tableId);
                    assertThat(buckets.size()).isEqualTo(expectedBucketCount);
                    for (TableBucket tableBucket : buckets) {
                        assertThat(ctx.getBucketState(tableBucket)).isEqualTo(expectedState);
                    }
                });
    }

    private void verifyReplicaForTableInState(
            long tableId, int expectedReplicaCount, ReplicaState expectedState) {
        retryVerifyContext(
                ctx -> {
                    Set<TableBucketReplica> replicas = ctx.getAllReplicasForTable(tableId);
                    assertThat(replicas.size()).isEqualTo(expectedReplicaCount);
                    for (TableBucketReplica tableBucketReplica : replicas) {
                        assertThat(ctx.getReplicaState(tableBucketReplica))
                                .isEqualTo(expectedState);
                    }
                });
    }

    private void verifyReplicaOnlineOrOffline(
            long tableId, TableAssignment assignment, Set<Integer> expectedOfflineReplicas) {
        // iterate each bucket and the replicas
        assignment
                .getBucketAssignments()
                .forEach(
                        (bucketId, replicas) -> {
                            TableBucket bucket = new TableBucket(tableId, bucketId);
                            // iterate each replicas
                            for (Integer replica : replicas.getReplicas()) {
                                TableBucketReplica bucketReplica =
                                        new TableBucketReplica(bucket, replica);
                                // if expected to be offline
                                if (expectedOfflineReplicas.contains(replica)) {
                                    retryVerifyContext(
                                            ctx ->
                                                    assertThat(ctx.getReplicaState(bucketReplica))
                                                            .isEqualTo(OfflineReplica));

                                } else {
                                    // otherwise, should be online
                                    retryVerifyContext(
                                            ctx ->
                                                    assertThat(ctx.getReplicaState(bucketReplica))
                                                            .isEqualTo(OnlineReplica));
                                }
                            }
                        });
    }

    private void verifyBucketIsr(long tableId, int bucket, int[] expectedIsr) {
        retryVerifyContext(
                ctx -> {
                    TableBucket tableBucket = new TableBucket(tableId, bucket);
                    // verify leaderAndIsr from coordinator context
                    LeaderAndIsr leaderAndIsr = ctx.getBucketLeaderAndIsr(tableBucket).get();
                    assertThat(leaderAndIsr.isrArray()).isEqualTo(expectedIsr);
                    // verify leaderAndIsr from tablet server
                    try {
                        leaderAndIsr = zookeeperClient.getLeaderAndIsr(tableBucket).get();
                    } catch (Exception e) {
                        throw new RuntimeException("Fail to get leaderAndIsr of " + tableBucket);
                    }
                    assertThat(leaderAndIsr.isrArray()).isEqualTo(expectedIsr);
                });
    }

    private void verifyReplicaForPartitionInState(
            TablePartition tablePartition, int expectedReplicaCount, ReplicaState expectedState) {
        retryVerifyContext(
                ctx -> {
                    Set<TableBucketReplica> replicas =
                            ctx.getAllReplicasForPartition(
                                    tablePartition.getTableId(), tablePartition.getPartitionId());
                    assertThat(replicas.size()).isEqualTo(expectedReplicaCount);
                    for (TableBucketReplica tableBucketReplica : replicas) {
                        assertThat(ctx.getReplicaState(tableBucketReplica))
                                .isEqualTo(expectedState);
                    }
                });
    }

    private void verifyBucketForPartitionInState(
            TablePartition tablePartition, int expectedBucketCount, BucketState expectedState) {
        retryVerifyContext(
                ctx -> {
                    Set<TableBucket> buckets =
                            ctx.getAllBucketsForPartition(
                                    tablePartition.getTableId(), tablePartition.getPartitionId());
                    assertThat(buckets.size()).isEqualTo(expectedBucketCount);
                    for (TableBucket tableBucket : buckets) {
                        assertThat(ctx.getBucketState(tableBucket)).isEqualTo(expectedState);
                    }
                });
    }

    private void verifyReceiveRequestExceptFor(
            int serverCount, int notReceiveServer, Class<?> requestClass) {
        // make sure all follower should receive notify offsets request
        for (int i = 0; i < serverCount; i++) {
            TestTabletServerGateway testTabletServerGateway =
                    (TestTabletServerGateway)
                            testCoordinatorChannelManager.getTabletServerGateway(i).get();
            if (i == notReceiveServer) {
                // should not contain NotifyKvSnapshotOffsetRequest
                assertThatThrownBy(() -> testTabletServerGateway.getRequest(0))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("No requests pending for inbound response.");
            } else {
                // should contain NotifyKvSnapshotOffsetRequest
                assertThat(testTabletServerGateway.getRequest(0)).isInstanceOf(requestClass);
            }
        }
    }

    private void retryVerifyContext(Consumer<CoordinatorContext> verifyFunction) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    AccessContextEvent<Void> event =
                            new AccessContextEvent<>(
                                    ctx -> {
                                        verifyFunction.accept(ctx);
                                        return null;
                                    });
                    eventProcessor.getCoordinatorEventManager().put(event);
                    try {
                        event.getResultFuture().get(30, TimeUnit.SECONDS);
                    } catch (Throwable t) {
                        throw ExceptionUtils.stripExecutionException(t);
                    }
                });
    }

    private <T> T fromCtx(Function<CoordinatorContext, T> retriveFunction) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(retriveFunction);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private long createTable(TablePath tablePath, TabletServerInfo[] servers) {
        TableAssignment tableAssignment =
                generateAssignment(N_BUCKETS, REPLICATION_FACTOR, servers);
        return metadataManager.createTable(
                tablePath, CoordinatorEventProcessorTest.TEST_TABLE, tableAssignment, false);
    }

    private TableDescriptor getPartitionedTable() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .primaryKey("a", "b")
                                .build())
                .distributedBy(3)
                .partitionedBy("b")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "YEAR")
                // set to 0 to disable pre-create partition
                .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                .build()
                .withReplicationFactor(REPLICATION_FACTOR);
    }

    private static List<TableBucket> allTableBuckets(long tableId, int numBuckets) {
        return IntStream.range(0, numBuckets)
                .mapToObj(i -> new TableBucket(tableId, i))
                .collect(Collectors.toList());
    }

    private static List<TableBucket> allTableBuckets(
            long tableId, long partitionId, int numBuckets) {
        return IntStream.range(0, numBuckets)
                .mapToObj(i -> new TableBucket(tableId, partitionId, i))
                .collect(Collectors.toList());
    }

    private static class PartitionIdName {
        private final long partitionId;
        private final String partitionName;

        private PartitionIdName(long partitionId, String partitionName) {
            this.partitionId = partitionId;
            this.partitionName = partitionName;
        }
    }
}
