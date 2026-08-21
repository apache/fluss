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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.ActionType;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableReplicaDistributionGoal}. */
public class TableReplicaDistributionGoalTest {

    @Test
    void testBalancesSkewedTableWhileClusterIsBalanced() {
        ClusterModel cluster = cluster(false);
        for (int i = 0; i < 8; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(0, 1));
            addBucket(cluster, new TableBucket(2, i), Arrays.asList(2, 3));
        }

        new TableReplicaDistributionGoal().optimize(cluster, Collections.<Goal>emptySet());

        for (ServerModel server : cluster.servers()) {
            assertThat(server.numReplicas(1)).isBetween(3, 5);
            assertThat(server.numReplicas(2)).isBetween(3, 5);
        }
    }

    @Test
    void testMovesReplicaToNewServerAndBalancesPartitionedTable() {
        ClusterModel cluster = cluster(false);
        for (int partition = 0; partition < 3; partition++) {
            for (int bucket = 0; bucket < 4; bucket++) {
                addBucket(
                        cluster, new TableBucket(1, (long) partition, bucket), Arrays.asList(0, 1));
            }
        }

        new TableReplicaDistributionGoal().optimize(cluster, Collections.<Goal>emptySet());

        for (ServerModel server : cluster.servers()) {
            assertThat(server.numReplicas(1)).isBetween(5, 7);
        }
        assertThat(cluster.server(2).numReplicas(1)).isGreaterThan(0);
        assertThat(cluster.server(3).numReplicas(1)).isGreaterThan(0);
    }

    @Test
    void testEvacuatesOfflineServer() {
        ClusterModel cluster = cluster(true);
        for (int i = 0; i < 6; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(4, 0, 1));
        }

        new TableReplicaDistributionGoal().optimize(cluster, Collections.<Goal>emptySet());

        assertThat(cluster.server(4).numReplicas()).isZero();
        for (ServerModel server : cluster.aliveServers()) {
            assertThat(server.numReplicas(1)).isLessThanOrEqualTo(5);
        }
    }

    @Test
    void testDegradesGracefullyWhenNoLegitDestinationExists() {
        // A server can never be pushed above the upper limit of every alive server at once: if all
        // of them held at least upper(t) replicas then avg(t) >= upper(t), which contradicts
        // upper(t) = ceil(avg(t) * 1.09). Real "no headroom" therefore comes from placement, not
        // from counts: here every bucket already has a replica on every server, so
        // GoalUtils#legitMove rejects every destination and the offline server cannot be drained.
        ClusterModel cluster = cluster(true);
        for (int i = 0; i < 2; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(4, 0, 1, 2, 3));
        }

        List<RebalancePlanForBucket> plans =
                new GoalOptimizer()
                        .doOptimizeOnce(
                                cluster,
                                Collections.<Goal>singletonList(
                                        new TableReplicaDistributionGoal()));

        // The goal is soft: it gives up instead of throwing, and leaves the placement untouched.
        assertThat(plans).isEmpty();
        assertThat(cluster.server(4).numReplicas(1)).isEqualTo(2);
        for (ServerModel server : cluster.aliveServers()) {
            assertThat(server.numReplicas(1)).isEqualTo(2);
        }
    }

    @Test
    void testRejectsMoveOutOfServerAtLowerLimit() {
        // 8 replicas of table 1 over 4 alive servers: avg 2, lower 1, upper 3. Server 3 holds
        // exactly one replica, so moving it out would drop the server below the lower limit.
        ClusterModel cluster = cluster(false);
        addBucket(cluster, new TableBucket(1, 0), Arrays.asList(0, 1, 2));
        addBucket(cluster, new TableBucket(1, 1), Arrays.asList(0, 1, 2));
        TableBucket boundaryBucket = new TableBucket(1, 2);
        addBucket(cluster, boundaryBucket, Arrays.asList(3, 0));
        TableReplicaDistributionGoal goal = new TableReplicaDistributionGoal();
        goal.initGoalState(cluster);

        assertThat(cluster.server(3).numReplicas(1)).isEqualTo(1);
        // Source at the lower limit is rejected even though the destination has room.
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        boundaryBucket, 3, 1, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isNotEqualTo(ActionAcceptance.ACCEPT);
        // A source above the lower limit may move the same bucket to the same destination.
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(
                                        new TableBucket(1, 0), 2, 3, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isEqualTo(ActionAcceptance.ACCEPT);
    }

    @Test
    void testBoundaryAndSmallTableAcceptance() {
        ClusterModel cluster = cluster(false);
        TableBucket bucket = new TableBucket(1, 0);
        addBucket(cluster, bucket, Arrays.asList(0, 1));
        TableReplicaDistributionGoal goal = new TableReplicaDistributionGoal();
        goal.initGoalState(cluster);

        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(bucket, 0, 2, ActionType.LEADERSHIP_MOVEMENT),
                                cluster))
                .isEqualTo(ActionAcceptance.ACCEPT);
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(bucket, 0, 2, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isEqualTo(ActionAcceptance.ACCEPT);
        assertThat(
                        goal.actionAcceptance(
                                new RebalancingAction(bucket, 0, 1, ActionType.REPLICA_MOVEMENT),
                                cluster))
                .isNotEqualTo(ActionAcceptance.ACCEPT);
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
