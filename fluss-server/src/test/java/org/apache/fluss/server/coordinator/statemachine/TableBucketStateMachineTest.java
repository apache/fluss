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
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.TestCoordinatorContext;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Sets;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.createServers;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.NewBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.NonExistentBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OfflineBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElectionStrategy.CONTROLLED_SHUTDOWN_ELECTION;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableBucketStateMachine}. */
class TableBucketStateMachineTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static CoordinatorContext coordinatorContext;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private CoordinatorRequestBatch coordinatorRequestBatch;
    private AutoPartitionManager autoPartitionManager;
    private LakeTableTieringManager lakeTableTieringManager;
    private CoordinatorMetadataCache serverMetadataCache;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() throws IOException {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_HOST, "localhost");
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        coordinatorContext = new TestCoordinatorContext();
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        coordinatorRequestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager,
                        event -> {
                            // do nothing
                        },
                        coordinatorContext);
        serverMetadataCache = new CoordinatorMetadataCache();
        autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        new MetadataManager(
                                zookeeperClient,
                                new Configuration(),
                                new LakeCatalogDynamicLoader(new Configuration(), null, true)),
                        new Configuration());
        lakeTableTieringManager = new LakeTableTieringManager();
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
                new LeaderAndIsr(0, 0, Arrays.asList(0, 1), 0, 0),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        zookeeperClient.registerLeaderAndIsr(
                new TableBucket(t1Id, 1),
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3), 0, 0),
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
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, fakeTablePath);
        coordinatorContext.updateBucketReplicaAssignment(tableBucket, Arrays.asList(0, 1, 2));
        coordinatorContext.putBucketState(tableBucket, NewBucket);
        coordinatorContext.setCoordinatorEpochAndZkVersion(
                0, ZkVersion.MATCH_ANY_VERSION.getVersion());
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
                                        TestingClientMetricGroup.newInstance(),
                                        false)),
                        coordinatorContext,
                        autoPartitionManager,
                        lakeTableTieringManager,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        new Configuration(),
                        Executors.newFixedThreadPool(
                                1, new ExecutorThreadFactory("test-coordinator-io")),
                        new MetadataManager(
                                zookeeperClient,
                                new Configuration(),
                                new LakeCatalogDynamicLoader(new Configuration(), null, true)));
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
    void testStateChangeForTabletServerControlledShutdown() {
        TableBucketStateMachine tableBucketStateMachine = createTableBucketStateMachine();
        long tableId = 7;
        TablePath fakeTablePath = TablePath.of("db1", "t2");
        TableBucket tb = new TableBucket(tableId, 0);

        // init coordinator context.
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        fakeTablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        coordinatorContext.putTablePath(tableId, fakeTablePath);
        coordinatorContext.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        coordinatorContext.putBucketState(tb, NewBucket);

        List<Integer> aliveServers = Arrays.asList(0, 1, 2);
        coordinatorContext.setLiveTabletServers(createServers(aliveServers));
        makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        // check state is online.
        tableBucketStateMachine.handleStateChange(Collections.singleton(tb), OnlineBucket);
        assertThat(coordinatorContext.getBucketState(tb)).isEqualTo(OnlineBucket);
        assertThat(coordinatorContext.liveTabletServerSet())
                .containsExactlyInAnyOrderElementsOf(aliveServers);
        assertThat(coordinatorContext.shuttingDownTabletServers()).isEmpty();
        assertThat(coordinatorContext.liveOrShuttingDownTabletServers())
                .containsExactlyInAnyOrderElementsOf(aliveServers);

        int oldLeader = coordinatorContext.getBucketLeaderAndIsr(tb).get().leader();
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
                Collections.singleton(tb), OnlineBucket, CONTROLLED_SHUTDOWN_ELECTION);
        assertThat(coordinatorContext.getBucketState(tb)).isEqualTo(OnlineBucket);
        assertThat(coordinatorContext.getBucketLeaderAndIsr(tb).get().leader())
                .isNotEqualTo(oldLeader);
    }

    private TableBucketStateMachine createTableBucketStateMachine() {
        return new TableBucketStateMachine(
                coordinatorContext, coordinatorRequestBatch, zookeeperClient);
    }
}
