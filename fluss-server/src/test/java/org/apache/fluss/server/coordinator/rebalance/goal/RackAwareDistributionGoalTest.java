/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.rebalance.goal;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RackAwareDistributionGoal}. */
public class RackAwareDistributionGoalTest {
    @Test
    void testDiffWithRackAwareGoal() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(1, "rackA", false));
        servers.add(new ServerModel(2, "rackB", false));
        servers.add(new ServerModel(3, "rackA", false));
        servers.add(new ServerModel(4, "rackB", false));
        servers.add(new ServerModel(5, "rackA", false));
        servers.add(new ServerModel(6, "rackB", false));
        servers.add(new ServerModel(7, "rackA", false));
        servers.add(new ServerModel(8, "rackB", false));
        ClusterModel clusterModel = new ClusterModel(servers);

        TableBucket t1b0 = new TableBucket(1, 0);
        addBucket(clusterModel, t1b0, Arrays.asList(1, 4, 5, 8));

        RackAwareGoal goal = new RackAwareGoal();
        assertThatThrownBy(() -> goal.optimize(clusterModel, Collections.singleton(goal)))
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessage(
                        "[RackAwareGoal] Insufficient number of racks to distribute each replica (Current: 2, Needed: 4).");

        // No error.
        RackAwareDistributionGoal rackAwareDistributionGoal = new RackAwareDistributionGoal();
        rackAwareDistributionGoal.optimize(
                clusterModel, Collections.singleton(rackAwareDistributionGoal));
    }

    @Test
    void testLackOfSecondServerToPlaceReplica() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(1, "rackA", false));
        servers.add(new ServerModel(2, "rackB", false));
        servers.add(new ServerModel(3, "rackA", false));
        servers.add(new ServerModel(4, "rackA", false));
        ClusterModel clusterModel = new ClusterModel(servers);

        TableBucket t1b0 = new TableBucket(1, 0);
        addBucket(clusterModel, t1b0, Arrays.asList(1, 2, 3, 4));
        RackAwareDistributionGoal rackAwareDistributionGoal = new RackAwareDistributionGoal();
        assertThatThrownBy(
                        () ->
                                rackAwareDistributionGoal.optimize(
                                        clusterModel,
                                        Collections.singleton(rackAwareDistributionGoal)))
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessage(
                        "[RackAwareDistributionGoal] Bucket TableBucket{tableId=1, bucket=0} is not rack-aware. "
                                + "Servers ([ServerModel[id=1,rack=rackA,isOfflineTagged=false,replicaCount=1], "
                                + "ServerModel[id=2,rack=rackB,isOfflineTagged=false,replicaCount=1], "
                                + "ServerModel[id=3,rack=rackA,isOfflineTagged=false,replicaCount=1], "
                                + "ServerModel[id=4,rack=rackA,isOfflineTagged=false,replicaCount=1]]) "
                                + "and replicas per rack ({rackA=3, rackB=1}).");
    }

    @Test
    void testReplicaDistributionBalanceAcrossRack() {
        ClusterModel clusterModel = generateUnbalancedReplicaAcrossServerAndRack();

        RackAwareDistributionGoal goal = new RackAwareDistributionGoal();
        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<RebalancePlanForBucket> rebalancePlanForBuckets =
                goalOptimizer.doOptimizeOnce(clusterModel, Collections.singletonList(goal));
        assertThat(rebalancePlanForBuckets).hasSize(10);
        for (int i = 0; i < 10; i++) {
            RebalancePlanForBucket planForBucket = rebalancePlanForBuckets.get(i);
            assertThat(planForBucket.getNewLeader()).isEqualTo(4);
            assertThat(planForBucket.getNewReplicas()).isEqualTo(Arrays.asList(4, 5, 2, 3));
        }

        clusterModel = generateUnbalancedReplicaAcrossServerAndRack();
        // combine with ReplicaDistributionGoal, the replica distribution will be balanced across
        // server and rock.
        ReplicaDistributionGoal replicaDistributionGoal = new ReplicaDistributionGoal();
        goalOptimizer = new GoalOptimizer();
        rebalancePlanForBuckets =
                goalOptimizer.doOptimizeOnce(
                        clusterModel, Arrays.asList(goal, replicaDistributionGoal));
        assertThat(rebalancePlanForBuckets).hasSize(10);

        Map<Integer, Integer> serverIdToReplicaNumber =
                getServerIdToReplicaNumber(rebalancePlanForBuckets);
        assertThat(serverIdToReplicaNumber.values().stream().filter(n -> n > 6).count())
                .isEqualTo(0);
        assertThat(serverIdToReplicaNumber.values().stream().filter(n -> n < 4).count())
                .isEqualTo(0);
        assertThat(getRackIdToReplicaNumber(rebalancePlanForBuckets, clusterModel).values())
                .containsExactlyInAnyOrder(20, 20);
    }

    private ClusterModel generateUnbalancedReplicaAcrossServerAndRack() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", false));
        servers.add(new ServerModel(1, "rack0", false));
        servers.add(new ServerModel(2, "rack0", false));
        servers.add(new ServerModel(3, "rack0", false));
        servers.add(new ServerModel(4, "rack1", false));
        servers.add(new ServerModel(5, "rack1", false));
        servers.add(new ServerModel(6, "rack1", false));
        servers.add(new ServerModel(7, "rack1", false));

        ClusterModel clusterModel = new ClusterModel(servers);
        // all allocate in rack0, rack1, rack2.
        for (int i = 0; i < 10; i++) {
            TableBucket t1bi = new TableBucket(1, i);
            addBucket(clusterModel, t1bi, Arrays.asList(0, 1, 2, 3));
        }

        return clusterModel;
    }

    private Map<Integer, Integer> getServerIdToReplicaNumber(
            List<RebalancePlanForBucket> rebalancePlanForBuckets) {
        Map<Integer, Integer> serverIdToLeaderReplicaNumber = new HashMap<>();
        for (RebalancePlanForBucket planForBucket : rebalancePlanForBuckets) {
            List<Integer> newReplicas = planForBucket.getNewReplicas();
            for (Integer serverId : newReplicas) {
                serverIdToLeaderReplicaNumber.put(
                        serverId, serverIdToLeaderReplicaNumber.getOrDefault(serverId, 0) + 1);
            }
        }
        return serverIdToLeaderReplicaNumber;
    }

    private Map<String, Integer> getRackIdToReplicaNumber(
            List<RebalancePlanForBucket> rebalancePlanForBuckets, ClusterModel clusterModel) {
        Map<String, Integer> rackIdToLeaderReplicaNumber = new HashMap<>();
        for (RebalancePlanForBucket planForBucket : rebalancePlanForBuckets) {
            List<Integer> newReplicas = planForBucket.getNewReplicas();
            for (Integer serverId : newReplicas) {
                ServerModel server = clusterModel.server(serverId);
                rackIdToLeaderReplicaNumber.put(
                        server.rack(),
                        rackIdToLeaderReplicaNumber.getOrDefault(server.rack(), 0) + 1);
            }
        }
        return rackIdToLeaderReplicaNumber;
    }
}
