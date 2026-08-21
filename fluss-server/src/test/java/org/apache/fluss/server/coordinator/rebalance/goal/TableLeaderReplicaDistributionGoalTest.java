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

package org.apache.fluss.server.coordinator.rebalance.goal;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.ActionType;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableLeaderReplicaDistributionGoal}. */
public class TableLeaderReplicaDistributionGoalTest {

    @Test
    void testBalancesSkewedLeadersWithoutMovingReplicaSets() {
        ClusterModel cluster = cluster(false);
        for (int i = 0; i < 8; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(0, 1, 2, 3));
            addBucket(cluster, new TableBucket(2, i), Arrays.asList(1, 0, 2, 3));
        }
        Map<TableBucket, HashSet<Integer>> replicaSets = replicaSets(cluster);
        Map<TableBucket, Integer> leaders = cluster.getLeaderDistribution();

        new TableLeaderReplicaDistributionGoal().optimize(cluster, Collections.<Goal>emptySet());

        for (ServerModel server : cluster.servers()) {
            assertThat(server.numLeaderReplicas(1)).isBetween(1, 3);
            assertThat(server.numLeaderReplicas(2)).isBetween(1, 3);
        }
        assertThat(replicaSets(cluster)).isEqualTo(replicaSets);
        assertThat(cluster.getLeaderDistribution()).isNotEqualTo(leaders);
    }

    @Test
    void testMovesLeaderReplicasToNewServerAndBalancesPartitions() {
        ClusterModel cluster = cluster(false);
        for (int partition = 0; partition < 2; partition++) {
            for (int bucket = 0; bucket < 6; bucket++) {
                addBucket(
                        cluster, new TableBucket(1, (long) partition, bucket), Arrays.asList(0, 1));
            }
        }

        new TableLeaderReplicaDistributionGoal().optimize(cluster, Collections.<Goal>emptySet());

        assertThat(cluster.server(2).numLeaderReplicas(1)).isGreaterThan(0);
        assertThat(cluster.server(3).numLeaderReplicas(1)).isGreaterThan(0);
        for (ServerModel server : cluster.servers()) {
            assertThat(server.numLeaderReplicas(1)).isBetween(2, 4);
        }
    }

    @Test
    void testEvacuatesOfflineLeader() {
        ClusterModel cluster = cluster(true);
        for (int i = 0; i < 6; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(4, 0, 1));
        }

        new TableLeaderReplicaDistributionGoal().optimize(cluster, Collections.<Goal>emptySet());

        assertThat(cluster.server(4).numLeaderReplicas()).isZero();
    }

    @Test
    void testGracefullyDegradesWhenClusterLeaderGoalBlocksTableMoves() {
        ClusterModel cluster = cluster(false);
        for (int i = 0; i < 4; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(0, 1, 2, 3));
        }
        for (int i = 0; i < 4; i++) {
            addBucket(cluster, new TableBucket(2, i), Arrays.asList(1, 0));
            addBucket(cluster, new TableBucket(3, i), Arrays.asList(2, 0));
            addBucket(cluster, new TableBucket(4, i), Arrays.asList(3, 0));
        }

        LeaderReplicaDistributionGoal clusterLeaderGoal = new LeaderReplicaDistributionGoal();
        clusterLeaderGoal.optimize(cluster, Collections.<Goal>emptySet());
        TableLeaderReplicaDistributionGoal tableLeaderGoal =
                new TableLeaderReplicaDistributionGoal();
        tableLeaderGoal.optimize(cluster, Collections.<Goal>singleton(clusterLeaderGoal));

        // A standalone table cannot have every alive server at its upper limit: the upper limit
        // is derived from that table's average. Here the already-satisfied cluster leader window
        // is the realistic constraint that blocks the remaining table-level leadership movement.
        assertThat(cluster.server(0).numLeaderReplicas(1)).isGreaterThan(2);
        assertThat(cluster.server(0).numLeaderReplicas()).isGreaterThanOrEqualTo(3);
        for (ServerModel server : cluster.servers()) {
            assertThat(server.numLeaderReplicas()).isBetween(3, 5);
        }
    }

    @Test
    void testLeaderAndFollowerReplicaMovementAcceptanceAtBoundaries() {
        ClusterModel cluster = cluster(false);
        TableBucket leaderBucket = new TableBucket(1, 0);
        TableBucket followerBucket = new TableBucket(1, 1);
        addBucket(cluster, leaderBucket, Arrays.asList(0, 1));
        addBucket(cluster, followerBucket, Arrays.asList(1, 0));
        TableLeaderReplicaDistributionGoal goal = new TableLeaderReplicaDistributionGoal();
        goal.initGoalState(cluster);

        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        followerBucket, 0, 2, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isEqualTo(ActionAcceptance.ACCEPT);
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        leaderBucket, 0, 2, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isEqualTo(ActionAcceptance.ACCEPT);
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        leaderBucket, 0, 1, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isNotEqualTo(ActionAcceptance.ACCEPT);
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        leaderBucket, 0, 1, ActionType.LEADERSHIP_MOVEMENT),
                                cluster))
                .isNotEqualTo(ActionAcceptance.ACCEPT);
    }

    @Test
    void testRejectsLeadershipMoveOutOfServerAtLowerLimit() {
        // 8 leaders of table 1 over 4 alive servers: avg 2, lower 1, upper 3. Server 3 leads
        // exactly one bucket, so giving that leadership away would drop it below the lower limit.
        ClusterModel cluster = cluster(false);
        for (int bucket = 0; bucket < 3; bucket++) {
            addBucket(cluster, new TableBucket(1, bucket), Arrays.asList(0, 1, 3));
        }
        for (int bucket = 3; bucket < 7; bucket++) {
            addBucket(cluster, new TableBucket(1, bucket), Arrays.asList(1, 2, 0));
        }
        TableBucket boundaryBucket = new TableBucket(1, 7);
        addBucket(cluster, boundaryBucket, Arrays.asList(3, 2, 0));
        TableLeaderReplicaDistributionGoal goal = new TableLeaderReplicaDistributionGoal();
        goal.initGoalState(cluster);

        assertThat(cluster.server(3).numLeaderReplicas(1)).isEqualTo(1);
        assertThat(cluster.server(2).numLeaderReplicas(1)).isEqualTo(0);
        // Source at the lower limit is rejected for leadership movement ...
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        boundaryBucket, 3, 2, ActionType.LEADERSHIP_MOVEMENT),
                                cluster))
                .isNotEqualTo(ActionAcceptance.ACCEPT);
        // ... and for moving the leader replica itself.
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        boundaryBucket, 3, 1, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isNotEqualTo(ActionAcceptance.ACCEPT);
        // A source above the lower limit may still hand its leadership over.
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        new TableBucket(1, 3),
                                        1,
                                        2,
                                        ActionType.LEADERSHIP_MOVEMENT),
                                cluster))
                .isEqualTo(ActionAcceptance.ACCEPT);
    }

    private Map<TableBucket, HashSet<Integer>> replicaSets(ClusterModel cluster) {
        Map<TableBucket, HashSet<Integer>> result = new HashMap<>();
        for (Map.Entry<TableBucket, List<Integer>> entry :
                cluster.getReplicaDistribution().entrySet()) {
            result.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return result;
    }

    private ClusterModel cluster(boolean offlineServer) {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", false));
        servers.add(new ServerModel(1, "rack1", false));
        servers.add(new ServerModel(2, "rack2", false));
        servers.add(new ServerModel(3, "rack3", false));
        if (offlineServer) {
            servers.add(new ServerModel(4, "rack4", true));
        }
        return new ClusterModel(servers);
    }
}
