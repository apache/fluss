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

package org.apache.fluss.server.coordinator.statemachine;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.AutoPartitionManager;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.CoordinatorRequestBatch;
import org.apache.fluss.server.coordinator.CoordinatorTestUtils;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.LakeTableTieringManager;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.ReplicaCapacityController;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElection.ControlledShutdownLeaderElection;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine.ElectionResult;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Sets;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.createServers;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.NewBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.NonExistentBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OfflineBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine.initReplicaLeaderElection;
import static org.apache.fluss.server.zk.ZooKeeperUtils.startZookeeperClient;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableBucketStateMachine}. */
class TableBucketStateMachineTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static CoordinatorContext coordinatorContext;
    private static ZkEpoch zkEpoch;

    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private CoordinatorRequestBatch coordinatorRequestBatch;
    private AutoPartitionManager autoPartitionManager;
    private ReplicaCapacityController replicaCapacityController;
    private LakeTableTieringManager lakeTableTieringManager;
    private CoordinatorMetadataCache serverMetadataCache;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;
    private Scheduler scheduler;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("1");
    }

    @BeforeEach
    void beforeEach() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_HOST, "localhost");
        String remoteDir = "/tmp/fluss/remote-data";
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, remoteDir);
        coordinatorContext = new CoordinatorContext(zkEpoch);
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        coordinatorRequestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager,
                        event -> {
                            // do nothing
                        },
                        coordinatorContext);
        serverMetadataCache = new CoordinatorMetadataCache();
        replicaCapacityController =
                new ReplicaCapacityController(
                        conf, serverMetadataCache, TestingMetricGroups.COORDINATOR_METRICS);
        autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        new MetadataManager(
                                zookeeperClient,
                                new Configuration(),
                                new LakeCatalogDynamicLoader(new Configuration(), null, true)),
                        new RemoteDirDynamicLoader(conf),
                        conf,
                        replicaCapacityController);
        lakeTableTieringManager =
                new LakeTableTieringManager(TestingMetricGroups.LAKE_TIERING_METRICS);

        kvSnapshotLeaseManager =
                new KvSnapshotLeaseManager(
                        Duration.ofMinutes(10).toMillis(),
                        zookeeperClient,
                        remoteDir,
                        SystemClock.getInstance(),
                        TestingMetricGroups.COORDINATOR_METRICS);
        kvSnapshotLeaseManager.start();

        scheduler = new FlussScheduler(1);
        scheduler.startup();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void testStartup() throws Exception {
        // create two tables
        long t1Id = 1;
        TableBucket t1b0 = new TableBucket(t1Id, 0);
        TableBucket t1b1 = new TableBucket(t1Id, 1);
        long t2Id = 2;
        TableBucket t2b0 = new TableBucket(t2Id, 0);
        coordinatorContext.putTablePath(t1Id, TablePath.of("db1", "t1"));
        coordinatorContext.putTablePath(t2Id, TablePath.of("db1", "t2"));
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        TablePath.of("db1", "t1"),
                        t1Id,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        TablePath.of("db1", "t2"),
                        t2Id,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));

        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(0, 1, 3)));
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);
        // set assignments
        coordinatorContext.updateBucketReplicaAssignment(t1b0, Arrays.asList(0, 1));
        coordinatorContext.updateBucketReplicaAssignment(t1b1, Arrays.asList(2, 3));
        coordinatorContext.updateBucketReplicaAssignment(t2b0, Arrays.asList(1, 2));

        // create LeaderAndIsr for t10/t11 info in zk,
        zookeeperClient.registerLeaderAndIsr(
                new TableBucket(t1Id, 0),
                new LeaderAndIsr(0, 0, Arrays.asList(0, 1), Collections.emptyList(), 0, 0),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        zookeeperClient.registerLeaderAndIsr(
                new TableBucket(t1Id, 1),
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3), Collections.emptyList(), 0, 0),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        // update the LeaderAndIsr to context
        coordinatorContext.putBucketLeaderAndIsr(
                t1b0, zookeeperClient.getLeaderAndIsr(new TableBucket(t1Id, 0)).get());
        coordinatorContext.putBucketLeaderAndIsr(
                t1b1, zookeeperClient.getLeaderAndIsr(new TableBucket(t1Id, 1)).get());

        TableBucketStateMachine tableBucketStateMachine = createTableBucketStateMachine();
        // on state machine startup, t1b0 will be online, t1b1 will be offline as the leader server
        // is offline, t2b0 will be new as no LeaderAndIsr in zk
        tableBucketStateMachine.startup();

        // t1b1 will then be online with leader change to 3
        assertThat(coordinatorContext.getBucketState(t1b1)).isEqualTo(OnlineBucket);
        CoordinatorTestUtils.checkLeaderAndIsr(zookeeperClient, t1b1, 1, 3);

        // t1b0 will remain same
        assertThat(coordinatorContext.getBucketState(t1b0)).isEqualTo(OnlineBucket);
        CoordinatorTestUtils.checkLeaderAndIsr(zookeeperClient, t1b0, 0, 0);

        // t2b0 will be online wth 1 as the leader
        assertThat(coordinatorContext.getBucketState(t2b0)).isEqualTo(OnlineBucket);
        CoordinatorTestUtils.checkLeaderAndIsr(zookeeperClient, t2b0, 0, 1);

        tableBucketStateMachine.shutdown();
    }

    @Test
    void testInvalidBucketStateChange() {
        TableBucketStateMachine tableBucketStateMachine = createTableBucketStateMachine();
        long tableId = 3;
        TableBucket tableBucket0 = new TableBucket(tableId, 0);
        TableBucket tableBucket1 = new TableBucket(tableId, 1);

        // NonExistent to Online/Offline is invalid, shouldn't do state transmit
        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket0), OnlineBucket);
        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket1), OfflineBucket);

        // check it
        assertThat(coordinatorContext.getBucketState(tableBucket0)).isEqualTo(NonExistentBucket);
        assertThat(coordinatorContext.getBucketState(tableBucket1)).isEqualTo(NonExistentBucket);
    }

    @Test
    void testStateChangeToOnline() throws Exception {
        TableBucketStateMachine tableBucketStateMachine = createTableBucketStateMachine();
        TablePath fakeTablePath = TablePath.of("db1", "t1");
        // init a table bucket assignment to coordinator context
        long tableId = 4;
        TableBucket tableBucket = new TableBucket(tableId, 0);
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        fakeTablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, fakeTablePath);
        coordinatorContext.updateBucketReplicaAssignment(tableBucket, Arrays.asList(0, 1, 2));
        coordinatorContext.putBucketState(tableBucket, NewBucket);
        // case1: init a new leader for NewBucket to OnlineBucket
        tableBucketStateMachine.handleStateChange(Collections.singleton(tableBucket), OnlineBucket);
        // non any alive servers, the state change fail
        assertThat(coordinatorContext.getBucketState(tableBucket)).isEqualTo(NewBucket);

        // now, we set 3 live servers
        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(0, 1, 2)));
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        // change to online again
        tableBucketStateMachine.handleStateChange(Collections.singleton(tableBucket), OnlineBucket);
        assertThat(coordinatorContext.getBucketState(tableBucket)).isEqualTo(OnlineBucket);

        // check bucket LeaderAndIsr
        CoordinatorTestUtils.checkLeaderAndIsr(zookeeperClient, tableBucket, 0, 0);

        // case2: assuming the leader replica fail(we remove it to server list),
        // we need elect another replica,
        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(1, 2)));

        tableBucketStateMachine.handleStateChange(Collections.singleton(tableBucket), OnlineBucket);
        // check state is online
        assertThat(coordinatorContext.getBucketState(tableBucket)).isEqualTo(OnlineBucket);

        // check the zk node that the leader has changed
        // new leader node, new leader epoch
        CoordinatorTestUtils.checkLeaderAndIsr(zookeeperClient, tableBucket, 1, 1);

        // case4: the leader replica fail, but non replicas is available
        coordinatorContext.putBucketState(tableBucket, OfflineBucket);
        coordinatorContext.setLiveTabletServers(createServers(Collections.emptyList()));
        tableBucketStateMachine.handleStateChange(Collections.singleton(tableBucket), OnlineBucket);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.emptyList()));
        tableBucketStateMachine.handleStateChange(Collections.singleton(tableBucket), OnlineBucket);
        // the state will still be offline
        assertThat(coordinatorContext.getBucketState(tableBucket)).isEqualTo(OfflineBucket);

        // case5: new to online, but the leader and the follower fail, should elect a new leader
        // we need to create the state machine with an event manager so that the fail request
        // will be handled by which will then cause electing a new leader
        CoordinatorEventProcessor coordinatorEventProcessor =
                new CoordinatorEventProcessor(
                        zookeeperClient,
                        serverMetadataCache,
                        new CoordinatorChannelManager(
                                RpcClient.create(
                                        new Configuration(),
                                        TestingClientMetricGroup.newInstance())),
                        coordinatorContext,
                        replicaCapacityController,
                        autoPartitionManager,
                        lakeTableTieringManager,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        new Configuration(),
                        Executors.newFixedThreadPool(
                                1, new ExecutorThreadFactory("test-coordinator-io")),
                        new MetadataManager(
                                zookeeperClient,
                                new Configuration(),
                                new LakeCatalogDynamicLoader(new Configuration(), null, true)),
                        kvSnapshotLeaseManager,
                        scheduler,
                        SystemClock.getInstance());
        CoordinatorEventManager eventManager =
                new CoordinatorEventManager(
                        coordinatorEventProcessor, TestingMetricGroups.COORDINATOR_METRICS);
        coordinatorRequestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager, eventManager, coordinatorContext);
        tableBucketStateMachine =
                new TableBucketStateMachine(
                        coordinatorContext, coordinatorRequestBatch, zookeeperClient);
        eventManager.start();

        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(0, 1, 2)));
        CoordinatorTestUtils.makeSendLeaderAndStopRequestFailContext(
                coordinatorContext, testCoordinatorChannelManager, Sets.newHashSet(0, 2));
        // init a table bucket assignment to coordinator context
        tableId = 5;
        final TableBucket tableBucket1 = new TableBucket(tableId, 0);
        coordinatorContext.putTablePath(tableId, fakeTablePath);
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        fakeTablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, fakeTablePath);
        coordinatorContext.updateBucketReplicaAssignment(tableBucket1, Arrays.asList(0, 1, 2));
        coordinatorContext.putBucketState(tableBucket1, NewBucket);
        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket1), OnlineBucket);
        // retry util the leader has changed to 1
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsrOpt =
                            coordinatorContext.getBucketLeaderAndIsr(tableBucket1);
                    assertThat(leaderAndIsrOpt).isPresent();
                    assertThat(leaderAndIsrOpt.get().leader()).isEqualTo(1);
                });

        // check state is online
        assertThat(coordinatorContext.getBucketState(tableBucket1)).isEqualTo(OnlineBucket);
        // check the zk node that the leader has changed
        // the leader should be 1 as 1 is live, the epoch should be 1 as we elect a new leader
        CoordinatorTestUtils.checkLeaderAndIsr(zookeeperClient, tableBucket1, 1, 1);
    }

    @Test
    void testStateChangeForDropTable() {
        TableBucketStateMachine tableBucketStateMachine = createTableBucketStateMachine();
        TableBucket tableBucket0 = new TableBucket(6, 0);
        TableBucket tableBucket1 = new TableBucket(6, 1);
        coordinatorContext.putBucketState(tableBucket0, OnlineBucket);
        coordinatorContext.putBucketState(tableBucket1, OnlineBucket);

        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket0), OfflineBucket);
        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket0), NonExistentBucket);
        // bucket 0 should be removed
        assertThat(coordinatorContext.getBucketState(tableBucket0)).isNull();
        // bucket 1 should still exist
        assertThat(coordinatorContext.getBucketState(tableBucket1)).isNotNull();

        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket1), OfflineBucket);
        tableBucketStateMachine.handleStateChange(
                Collections.singleton(tableBucket1), NonExistentBucket);
        assertThat(coordinatorContext.getBucketState(tableBucket0)).isNull();
    }

    @Test
    void testStateChangeForTabletServerControlledShutdown() throws Exception {
        TableBucketStateMachine tableBucketStateMachine = createTableBucketStateMachine();
        long tableId = 7;
        TablePath fakeTablePath = TablePath.of("db1", "t2");
        Set<TableBucket> tableBuckets =
                Sets.newHashSet(
                        new TableBucket(tableId, 0),
                        new TableBucket(tableId, 1),
                        new TableBucket(tableId, 2));

        // init coordinator context.
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        fakeTablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, fakeTablePath);
        tableBuckets.forEach(
                tableBucket -> {
                    coordinatorContext.updateBucketReplicaAssignment(
                            tableBucket, Arrays.asList(0, 1, 2));
                    coordinatorContext.putBucketState(tableBucket, NewBucket);
                });

        List<Integer> aliveServers = Arrays.asList(0, 1, 2);
        coordinatorContext.setLiveTabletServers(createServers(aliveServers));
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        // check state is online.
        tableBucketStateMachine.handleStateChange(tableBuckets, OnlineBucket);
        assertThat(tableBuckets)
                .allSatisfy(
                        tableBucket ->
                                assertThat(coordinatorContext.getBucketState(tableBucket))
                                        .isEqualTo(OnlineBucket));
        assertThat(coordinatorContext.liveTabletServerSet())
                .containsExactlyInAnyOrderElementsOf(aliveServers);
        assertThat(coordinatorContext.shuttingDownTabletServers()).isEmpty();
        assertThat(coordinatorContext.liveOrShuttingDownTabletServers())
                .containsExactlyInAnyOrderElementsOf(aliveServers);

        int oldLeader =
                coordinatorContext
                        .getBucketLeaderAndIsr(tableBuckets.iterator().next())
                        .get()
                        .leader();
        assertThat(tableBuckets)
                .allSatisfy(
                        tableBucket ->
                                assertThat(
                                                coordinatorContext
                                                        .getBucketLeaderAndIsr(tableBucket)
                                                        .get()
                                                        .leader())
                                        .isEqualTo(oldLeader));
        aliveServers =
                aliveServers.stream().filter(s -> s != oldLeader).collect(Collectors.toList());

        // trigger controlled shutdown for oldLeader.
        coordinatorContext.shuttingDownTabletServers().add(oldLeader);
        assertThat(coordinatorContext.liveTabletServerSet())
                .containsExactlyInAnyOrderElementsOf(aliveServers);
        assertThat(coordinatorContext.shuttingDownTabletServers())
                .containsExactlyInAnyOrder(oldLeader);
        assertThat(coordinatorContext.liveOrShuttingDownTabletServers())
                .containsExactlyInAnyOrder(0, 1, 2);

        // handle state change for controlled shutdown.
        tableBucketStateMachine.handleStateChange(
                tableBuckets, OnlineBucket, new ControlledShutdownLeaderElection());
        assertThat(tableBuckets)
                .allSatisfy(
                        tableBucket -> {
                            assertThat(coordinatorContext.getBucketState(tableBucket))
                                    .isEqualTo(OnlineBucket);
                            assertThat(
                                            coordinatorContext
                                                    .getBucketLeaderAndIsr(tableBucket)
                                                    .get()
                                                    .leader())
                                    .isNotEqualTo(oldLeader);
                        });
        for (TableBucket tableBucket : tableBuckets) {
            assertThat(zookeeperClient.getLeaderAndIsr(tableBucket))
                    .hasValue(coordinatorContext.getBucketLeaderAndIsr(tableBucket).get());
        }
    }

    @Test
    void testControlledShutdownWithMixedBucketStates() {
        long tableId = 10;
        TablePath tablePath = TablePath.of("db1", "mixed-bucket-states");
        TableBucket onlineBucket = new TableBucket(tableId, 0);
        TableBucket newBucket = new TableBucket(tableId, 1);
        Set<TableBucket> tableBuckets = Sets.newHashSet(onlineBucket, newBucket);
        List<Integer> assignedServers = Arrays.asList(0, 1, 2);
        LeaderAndIsr originalLeaderAndIsr =
                new LeaderAndIsr(
                        0,
                        0,
                        assignedServers,
                        Collections.emptyList(),
                        coordinatorContext.getCoordinatorEpoch(),
                        0);
        LeaderAndIsr expectedOnlineLeaderAndIsr =
                originalLeaderAndIsr.newLeaderAndIsr(
                        1, Arrays.asList(1, 2), Collections.emptyList());
        LeaderAndIsr expectedNewLeaderAndIsr =
                new LeaderAndIsr(
                        1,
                        0,
                        Arrays.asList(1, 2),
                        Collections.emptyList(),
                        coordinatorContext.getCoordinatorEpoch(),
                        0);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        tablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, tablePath);
        tableBuckets.forEach(
                tableBucket ->
                        coordinatorContext.updateBucketReplicaAssignment(
                                tableBucket, assignedServers));
        coordinatorContext.putBucketLeaderAndIsr(onlineBucket, originalLeaderAndIsr);
        coordinatorContext.putBucketState(onlineBucket, OnlineBucket);
        coordinatorContext.putBucketState(newBucket, NewBucket);
        coordinatorContext.setLiveTabletServers(createServers(assignedServers));
        coordinatorContext.shuttingDownTabletServers().add(0);
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        try (TestingZooKeeperClient testingZooKeeperClient =
                new TestingZooKeeperClient(
                        Collections.singletonMap(onlineBucket, originalLeaderAndIsr),
                        Collections.emptyMap(),
                        false)) {
            TableBucketStateMachine tableBucketStateMachine =
                    new TableBucketStateMachine(
                            coordinatorContext, coordinatorRequestBatch, testingZooKeeperClient);
            tableBucketStateMachine.handleStateChange(
                    tableBuckets, OnlineBucket, new ControlledShutdownLeaderElection());

            assertThat(coordinatorContext.getBucketState(onlineBucket)).isEqualTo(OnlineBucket);
            assertThat(coordinatorContext.getBucketState(newBucket)).isEqualTo(OnlineBucket);
            assertThat(coordinatorContext.getBucketLeaderAndIsr(onlineBucket))
                    .hasValue(expectedOnlineLeaderAndIsr);
            assertThat(coordinatorContext.getBucketLeaderAndIsr(newBucket))
                    .hasValue(expectedNewLeaderAndIsr);
            assertThat(testingZooKeeperClient.batchUpdates)
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(onlineBucket, expectedOnlineLeaderAndIsr));
            assertThat(testingZooKeeperClient.registeredLeaderAndIsrs)
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(newBucket, expectedNewLeaderAndIsr));
        }
    }

    @Test
    void testControlledShutdownFallsBackToIndividualUpdates() {
        long tableId = 8;
        TablePath tablePath = TablePath.of("db1", "fallback");
        TableBucket tableBucket = new TableBucket(tableId, 0);
        List<Integer> aliveServers = Arrays.asList(0, 1, 2);
        LeaderAndIsr originalLeaderAndIsr =
                new LeaderAndIsr(
                        0,
                        0,
                        aliveServers,
                        Collections.emptyList(),
                        coordinatorContext.getCoordinatorEpoch(),
                        0);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        tablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, tablePath);
        coordinatorContext.updateBucketReplicaAssignment(tableBucket, aliveServers);
        coordinatorContext.putBucketLeaderAndIsr(tableBucket, originalLeaderAndIsr);
        coordinatorContext.putBucketState(tableBucket, OnlineBucket);
        coordinatorContext.setLiveTabletServers(createServers(aliveServers));
        coordinatorContext.shuttingDownTabletServers().add(0);
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        try (TestingZooKeeperClient testingZooKeeperClient =
                new TestingZooKeeperClient(
                        Collections.singletonMap(tableBucket, originalLeaderAndIsr),
                        Collections.emptyMap(),
                        true)) {
            TableBucketStateMachine tableBucketStateMachine =
                    new TableBucketStateMachine(
                            coordinatorContext, coordinatorRequestBatch, testingZooKeeperClient);
            tableBucketStateMachine.handleStateChange(
                    Collections.singleton(tableBucket),
                    OnlineBucket,
                    new ControlledShutdownLeaderElection());

            LeaderAndIsr updatedLeaderAndIsr =
                    coordinatorContext.getBucketLeaderAndIsr(tableBucket).get();
            assertThat(updatedLeaderAndIsr.leader()).isNotEqualTo(0);
            assertThat(testingZooKeeperClient.batchReadBuckets).containsExactly(tableBucket);
            assertThat(testingZooKeeperClient.batchUpdates)
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(tableBucket, updatedLeaderAndIsr));
            assertThat(testingZooKeeperClient.batchUpdateZkVersion)
                    .isEqualTo(zkEpoch.getCoordinatorEpochZkVersion());
            assertThat(testingZooKeeperClient.individualReadBuckets).isEmpty();
            assertThat(testingZooKeeperClient.individualUpdates)
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(tableBucket, updatedLeaderAndIsr));
            assertThat(testingZooKeeperClient.individualUpdateZkVersions)
                    .containsEntry(tableBucket, zkEpoch.getCoordinatorEpochZkVersion());
        }
    }

    @Test
    void testControlledShutdownRetriesMissingBatchReadResultIndividually() {
        long tableId = 9;
        TablePath tablePath = TablePath.of("db1", "partial-batch-read");
        TableBucket batchUpdatedBucket = new TableBucket(tableId, 0);
        TableBucket individuallyRetriedBucket = new TableBucket(tableId, 1);
        Set<TableBucket> tableBuckets =
                Sets.newHashSet(batchUpdatedBucket, individuallyRetriedBucket);
        List<Integer> aliveServers = Arrays.asList(0, 1, 2);
        LeaderAndIsr originalLeaderAndIsr =
                new LeaderAndIsr(
                        0,
                        0,
                        aliveServers,
                        Collections.emptyList(),
                        coordinatorContext.getCoordinatorEpoch(),
                        0);
        LeaderAndIsr expectedLeaderAndIsr =
                originalLeaderAndIsr.newLeaderAndIsr(
                        1, Arrays.asList(1, 2), Collections.emptyList());

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        tablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, tablePath);
        tableBuckets.forEach(
                tableBucket -> {
                    coordinatorContext.updateBucketReplicaAssignment(tableBucket, aliveServers);
                    coordinatorContext.putBucketLeaderAndIsr(tableBucket, originalLeaderAndIsr);
                    coordinatorContext.putBucketState(tableBucket, OnlineBucket);
                });
        coordinatorContext.setLiveTabletServers(createServers(aliveServers));
        coordinatorContext.shuttingDownTabletServers().add(0);
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        try (TestingZooKeeperClient testingZooKeeperClient =
                new TestingZooKeeperClient(
                        Collections.singletonMap(batchUpdatedBucket, originalLeaderAndIsr),
                        Collections.singletonMap(individuallyRetriedBucket, originalLeaderAndIsr),
                        false)) {
            TableBucketStateMachine tableBucketStateMachine =
                    new TableBucketStateMachine(
                            coordinatorContext, coordinatorRequestBatch, testingZooKeeperClient);
            tableBucketStateMachine.handleStateChange(
                    tableBuckets, OnlineBucket, new ControlledShutdownLeaderElection());

            assertThat(coordinatorContext.getBucketLeaderAndIsr(batchUpdatedBucket))
                    .hasValue(expectedLeaderAndIsr);
            assertThat(coordinatorContext.getBucketLeaderAndIsr(individuallyRetriedBucket))
                    .hasValue(expectedLeaderAndIsr);
            assertThat(testingZooKeeperClient.batchReadBuckets)
                    .containsExactlyInAnyOrderElementsOf(tableBuckets);
            assertThat(testingZooKeeperClient.batchUpdates)
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(batchUpdatedBucket, expectedLeaderAndIsr));
            assertThat(testingZooKeeperClient.batchUpdateZkVersion)
                    .isEqualTo(zkEpoch.getCoordinatorEpochZkVersion());
            assertThat(testingZooKeeperClient.individualReadBuckets)
                    .containsExactly(individuallyRetriedBucket);
            assertThat(testingZooKeeperClient.individualUpdates)
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(
                                    individuallyRetriedBucket, expectedLeaderAndIsr));
            assertThat(testingZooKeeperClient.individualUpdateZkVersions)
                    .containsEntry(
                            individuallyRetriedBucket, zkEpoch.getCoordinatorEpochZkVersion());
        }
    }

    @Test
    void testInitReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Collections.singletonList(4);

        Optional<ElectionResult> leaderElectionResultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0, false);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testInitReplicaLeaderElectionForPkTable() {
        // PK table with multiple available replicas: first as leader, second as standby
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Arrays.asList(1, 2, 3);

        Optional<ElectionResult> resultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0, true);
        assertThat(resultOpt).isPresent();
        ElectionResult result = resultOpt.get();
        assertThat(result.getLeaderAndIsr().leader()).isEqualTo(1);
        assertThat(result.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(1, 2, 3);
        assertThat(result.getLeaderAndIsr().standbyReplicas()).containsExactly(2);
    }

    @Test
    void testInitReplicaLeaderElectionForPkTableWithSingleAvailableReplica() {
        // PK table with only one available replica: leader only, no standby
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Collections.singletonList(2);

        Optional<ElectionResult> resultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0, true);
        assertThat(resultOpt).isPresent();
        ElectionResult result = resultOpt.get();
        assertThat(result.getLeaderAndIsr().leader()).isEqualTo(2);
        assertThat(result.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2);
        assertThat(result.getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testInitReplicaLeaderElectionForPkTableWithNoAliveReplica() {
        // PK table with no alive replicas: should return empty
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Collections.emptyList();

        Optional<ElectionResult> resultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0, true);
        assertThat(resultOpt).isEmpty();
    }

    @Test
    void testInitReplicaLeaderElectionForPkTableWithPartiallyAliveReplicas() {
        // PK table where first assigned replica is not alive,
        // second and third are alive
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Arrays.asList(2, 3);

        Optional<ElectionResult> resultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0, true);
        assertThat(resultOpt).isPresent();
        ElectionResult result = resultOpt.get();
        // First available (2) should be leader, second available (3) should be standby
        assertThat(result.getLeaderAndIsr().leader()).isEqualTo(2);
        assertThat(result.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 3);
        assertThat(result.getLeaderAndIsr().standbyReplicas()).containsExactly(3);
    }

    private TableBucketStateMachine createTableBucketStateMachine() {
        return new TableBucketStateMachine(
                coordinatorContext, coordinatorRequestBatch, zookeeperClient);
    }

    private static final class TestingZooKeeperClient extends ZooKeeperClient {

        private final Map<TableBucket, LeaderAndIsr> batchReadResults;
        private final Map<TableBucket, LeaderAndIsr> individualReadResults;
        private final boolean failBatchUpdate;
        private final Set<TableBucket> batchReadBuckets = new HashSet<>();
        private final Set<TableBucket> individualReadBuckets = new HashSet<>();
        private final Map<TableBucket, LeaderAndIsr> batchUpdates = new HashMap<>();
        private final Map<TableBucket, LeaderAndIsr> individualUpdates = new HashMap<>();
        private final Map<TableBucket, Integer> individualUpdateZkVersions = new HashMap<>();
        private final Map<TableBucket, LeaderAndIsr> registeredLeaderAndIsrs = new HashMap<>();
        private int batchUpdateZkVersion;

        private TestingZooKeeperClient(
                Map<TableBucket, LeaderAndIsr> batchReadResults,
                Map<TableBucket, LeaderAndIsr> individualReadResults,
                boolean failBatchUpdate) {
            super(createCuratorFrameworkWrapper(), createConfiguration());
            this.batchReadResults = new HashMap<>(batchReadResults);
            this.individualReadResults = new HashMap<>(individualReadResults);
            this.failBatchUpdate = failBatchUpdate;
        }

        @Override
        public Map<TableBucket, LeaderAndIsr> getLeaderAndIsrs(
                Collection<TableBucket> tableBuckets) {
            batchReadBuckets.addAll(tableBuckets);
            return new HashMap<>(batchReadResults);
        }

        @Override
        public Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket) {
            individualReadBuckets.add(tableBucket);
            return Optional.ofNullable(individualReadResults.get(tableBucket));
        }

        @Override
        public void batchUpdateLeaderAndIsr(
                Map<TableBucket, LeaderAndIsr> leaderAndIsrs, int expectedZkVersion) {
            batchUpdates.putAll(leaderAndIsrs);
            batchUpdateZkVersion = expectedZkVersion;
            if (failBatchUpdate) {
                throw new RuntimeException("Expected batch update failure");
            }
        }

        @Override
        public void updateLeaderAndIsr(
                TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion) {
            individualUpdates.put(tableBucket, leaderAndIsr);
            individualUpdateZkVersions.put(tableBucket, expectedZkVersion);
        }

        @Override
        public void registerLeaderAndIsr(
                TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion) {
            registeredLeaderAndIsrs.put(tableBucket, leaderAndIsr);
        }

        private static CuratorFrameworkWithUnhandledErrorListener createCuratorFrameworkWrapper() {
            Builder builder =
                    builder()
                            .connectString(
                                    ZOO_KEEPER_EXTENSION_WRAPPER
                                            .getCustomExtension()
                                            .getConnectString())
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3));
            return startZookeeperClient(builder, NOPErrorHandler.INSTANCE);
        }

        private static Configuration createConfiguration() {
            Configuration configuration = new Configuration();
            configuration.set(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DATA_DIR);
            return configuration;
        }
    }
}
