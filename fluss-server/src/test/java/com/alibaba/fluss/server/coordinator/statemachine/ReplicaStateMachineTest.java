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

package com.alibaba.fluss.server.coordinator.statemachine;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.server.coordinator.CoordinatorChannelManager;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.coordinator.CoordinatorRequestBatch;
import com.alibaba.fluss.server.coordinator.CoordinatorTestUtils;
import com.alibaba.fluss.server.coordinator.TestCoordinatorChannelManager;
import com.alibaba.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import com.alibaba.fluss.server.entity.DeleteReplicaResultForBucket;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.NewReplica;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.OfflineReplica;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionStarted;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ReplicaStateMachine} . */
class ReplicaStateMachineTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @Test
    void testStartup() {
        CoordinatorContext coordinatorContext = new CoordinatorContext();

        // init coordinator server context with a table assignment
        TableBucket tableBucket = new TableBucket(1, 0);
        // bucket0 has two replicas, put them into context
        coordinatorContext.updateBucketReplicaAssignment(tableBucket, Arrays.asList(1, 2));
        // only server1 is alive
        coordinatorContext.setLiveTabletServers(createServers(new int[] {1}));

        // now, create the state machine with the context
        ReplicaStateMachine replicaStateMachine = createReplicaStateMachine(coordinatorContext);
        replicaStateMachine.startup();

        TableBucketReplica replica1 = new TableBucketReplica(tableBucket, 1);
        TableBucketReplica replica2 = new TableBucketReplica(tableBucket, 2);
        // replica1 should be online as the server is online
        // replica2 should be offline as the server is offline
        assertThat(coordinatorContext.getReplicaState(replica1)).isEqualTo(OnlineReplica);
        assertThat(coordinatorContext.getReplicaState(replica2)).isEqualTo(OfflineReplica);

        replicaStateMachine.shutdown();
    }

    @Test
    void testReplicaStateChange() {
        CoordinatorContext coordinatorContext = new CoordinatorContext();
        ReplicaStateMachine replicaStateMachine = createReplicaStateMachine(coordinatorContext);

        // test check valid replica state change
        long tableId = 1;
        TableBucket tableBucket = new TableBucket(tableId, 1);

        TableBucketReplica replica0 = new TableBucketReplica(tableBucket, 0);
        TableBucketReplica replica1 = new TableBucketReplica(tableBucket, 1);

        coordinatorContext.putReplicaState(replica0, ReplicaState.NonExistentReplica);
        coordinatorContext.putReplicaState(replica1, NewReplica);

        // replica0 is valid, replica1 is invalid
        Collection<TableBucketReplica> validReplicas =
                replicaStateMachine.checkValidReplicaStateChange(
                        Arrays.asList(replica0, replica1), OnlineReplica);
        assertThat(validReplicas).isEqualTo(Collections.singletonList(replica1));

        replicaStateMachine.handleStateChanges(Arrays.asList(replica0, replica1), OnlineReplica);
        // only replica1 is valid, and then replica1's state should be online
        assertThat(coordinatorContext.getReplicaState(replica0))
                .isEqualTo(ReplicaState.NonExistentReplica);
        assertThat(coordinatorContext.getReplicaState(replica1)).isEqualTo(OnlineReplica);
    }

    @Test
    void testDeleteReplicaStateChange() {
        Map<TableBucketReplica, Boolean> isReplicaDeleteSuccess = new HashMap<>();
        CoordinatorContext coordinatorContext = new CoordinatorContext();
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1)));
        // use a context that will return a gateway that always get success ack

        TestCoordinatorChannelManager testCoordinatorChannelManager =
                new TestCoordinatorChannelManager();
        ReplicaStateMachine replicaStateMachine =
                createReplicaStateMachine(
                        coordinatorContext, testCoordinatorChannelManager, isReplicaDeleteSuccess);
        CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        long tableId = 1;
        TableBucket tableBucket1 = new TableBucket(tableId, 1);
        TableBucketReplica b1Replica0 = new TableBucketReplica(tableBucket1, 0);
        TableBucketReplica b1Replica1 = new TableBucketReplica(tableBucket1, 1);
        TableBucket tableBucket2 = new TableBucket(tableId, 2);
        TableBucketReplica b2Replica0 = new TableBucketReplica(tableBucket2, 0);
        TableBucketReplica b2Replica1 = new TableBucketReplica(tableBucket2, 1);
        List<TableBucketReplica> replicas =
                Arrays.asList(b1Replica0, b1Replica1, b2Replica0, b2Replica1);
        coordinatorContext.putBucketLeaderAndIsr(tableBucket1, new LeaderAndIsr(0, 0));
        coordinatorContext.putBucketLeaderAndIsr(tableBucket2, new LeaderAndIsr(0, 0));

        toReplicaDeletionStartedState(replicaStateMachine, replicas);
        for (TableBucketReplica replica : isReplicaDeleteSuccess.keySet()) {
            assertThat(isReplicaDeleteSuccess.get(replica)).isTrue();
        }

        // now, we change a context that some gateway will return exception
        coordinatorContext = new CoordinatorContext();
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1)));
        coordinatorContext.putBucketLeaderAndIsr(tableBucket1, new LeaderAndIsr(0, 0));
        coordinatorContext.putBucketLeaderAndIsr(tableBucket2, new LeaderAndIsr(0, 0));

        // delete replica will fail for some gateway will return exception
        CoordinatorTestUtils.makeSendLeaderAndStopRequestFailContext(
                coordinatorContext,
                testCoordinatorChannelManager,
                new HashSet<>(Arrays.asList(0, 1)));

        isReplicaDeleteSuccess = new HashMap<>();
        replicaStateMachine =
                createReplicaStateMachine(
                        coordinatorContext, testCoordinatorChannelManager, isReplicaDeleteSuccess);

        // deletion should always fail
        toReplicaDeletionStartedState(replicaStateMachine, replicas);
        for (TableBucketReplica replica : replicas) {
            assertThat(isReplicaDeleteSuccess.get(replica)).isFalse();
        }
    }

    @Test
    void testOfflineReplicaShouldBeRemovedFromIsr() throws Exception {
        CoordinatorContext coordinatorContext = new CoordinatorContext();
        coordinatorContext.setLiveTabletServers(createServers(new int[] {0, 1, 2}));
        ReplicaStateMachine replicaStateMachine = createReplicaStateMachine(coordinatorContext);

        // put the replica to online
        long tableId = 1;
        TableBucket tableBucket = new TableBucket(tableId, 0);
        for (int i = 0; i < 3; i++) {
            TableBucketReplica replica = new TableBucketReplica(tableBucket, i);
            coordinatorContext.putReplicaState(replica, OnlineReplica);
        }
        // put leader and isr
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(0, 0, Arrays.asList(0, 1, 2), 0, 0);
        zookeeperClient.registerLeaderAndIsr(tableBucket, leaderAndIsr);
        coordinatorContext.putBucketLeaderAndIsr(tableBucket, leaderAndIsr);

        // set replica 1 to offline
        replicaStateMachine.handleStateChanges(
                Collections.singleton(new TableBucketReplica(tableBucket, 1)), OfflineReplica);
        leaderAndIsr = coordinatorContext.getBucketLeaderAndIsr(tableBucket).get();
        assertThat(leaderAndIsr).isEqualTo(new LeaderAndIsr(0, 0, Arrays.asList(0, 2), 0, 1));

        // set replica 2 to offline
        replicaStateMachine.handleStateChanges(
                Collections.singleton(new TableBucketReplica(tableBucket, 2)), OfflineReplica);
        leaderAndIsr = coordinatorContext.getBucketLeaderAndIsr(tableBucket).get();
        assertThat(leaderAndIsr)
                .isEqualTo(new LeaderAndIsr(0, 0, Collections.singletonList(0), 0, 2));

        // set replica 0 to offline, isr shouldn't be empty, leader should be NO_LEADER
        replicaStateMachine.handleStateChanges(
                Collections.singleton(new TableBucketReplica(tableBucket, 0)), OfflineReplica);
        leaderAndIsr = coordinatorContext.getBucketLeaderAndIsr(tableBucket).get();
        assertThat(leaderAndIsr)
                .isEqualTo(
                        new LeaderAndIsr(
                                LeaderAndIsr.NO_LEADER, 0, Collections.singletonList(0), 0, 3));
    }

    private void toReplicaDeletionStartedState(
            ReplicaStateMachine replicaStateMachine, Collection<TableBucketReplica> replicas) {
        replicaStateMachine.handleStateChanges(replicas, NewReplica);
        replicaStateMachine.handleStateChanges(replicas, OfflineReplica);
        replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionStarted);
    }

    private ReplicaStateMachine createReplicaStateMachine(CoordinatorContext coordinatorContext) {
        return new ReplicaStateMachine(
                coordinatorContext,
                new CoordinatorRequestBatch(
                        new CoordinatorChannelManager(
                                RpcClient.create(
                                        new Configuration(),
                                        TestingClientMetricGroup.newInstance())),
                        (event) -> {
                            // do nothing
                        }),
                zookeeperClient);
    }

    private ReplicaStateMachine createReplicaStateMachine(
            CoordinatorContext coordinatorContext,
            TestCoordinatorChannelManager testCoordinatorChannelManager,
            Map<TableBucketReplica, Boolean> isReplicaDeleteSuccess) {
        return new ReplicaStateMachine(
                coordinatorContext,
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager,
                        (event) -> {
                            if (event instanceof DeleteReplicaResponseReceivedEvent) {
                                // get replica delete success or not from
                                // DeleteReplicaResponseReceivedEvent
                                DeleteReplicaResponseReceivedEvent
                                        deleteReplicaResponseReceivedEvent =
                                                (DeleteReplicaResponseReceivedEvent) event;
                                List<DeleteReplicaResultForBucket> deleteReplicaResultForBuckets =
                                        deleteReplicaResponseReceivedEvent
                                                .getDeleteReplicaResults();
                                for (DeleteReplicaResultForBucket deleteReplicaResultForBucket :
                                        deleteReplicaResultForBuckets) {
                                    // set replica delete success or not
                                    isReplicaDeleteSuccess.put(
                                            deleteReplicaResultForBucket.getTableBucketReplica(),
                                            deleteReplicaResultForBucket.succeeded());
                                }
                            }
                        }),
                zookeeperClient);
    }

    private List<ServerInfo> createServers(int[] serverIds) {
        List<ServerInfo> servers = new ArrayList<>();
        for (int serverId : serverIds) {
            servers.add(
                    new ServerInfo(
                            serverId,
                            Endpoint.fromListenersString("CLIENT://host:23"),
                            ServerType.TABLET_SERVER));
        }
        return servers;
    }
}
