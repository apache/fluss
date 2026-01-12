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
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GoalOptimizer}. */
public class GoalOptimizerTest {

    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        ServerModel server2 = new ServerModel(2, "rack2", false);
        ServerModel server3 = new ServerModel(3, "rack0", false);
        servers.add(server0);
        servers.add(server1);
        servers.add(server2);
        servers.add(server3);
    }

    @Test
    void testOptimize() {
        ClusterModel clusterModel = new ClusterModel(servers);
        // add buckets into clusterModel.
        addBucket(clusterModel, new TableBucket(0, 0), Arrays.asList(0, 1, 2));
        addBucket(clusterModel, new TableBucket(1, 0), Arrays.asList(0, 1, 2));
        addBucket(clusterModel, new TableBucket(2, 0), Arrays.asList(0, 1, 2));
        addBucket(clusterModel, new TableBucket(3, 0), Arrays.asList(0, 1, 2));

        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<Goal> goals = new ArrayList<>();
        goals.add(new ReplicaDistributionGoal());
        goals.add(new LeaderReplicaDistributionGoal());

        List<RebalancePlanForBucket> plans = goalOptimizer.doOptimizeOnce(clusterModel, goals);
        assertThat(plans).isNotEmpty();
    }

    @Test
    void testNodeJoin() {
        // Initial cluster with 3 nodes
        SortedSet<ServerModel> initialServers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        ServerModel server2 = new ServerModel(2, "rack2", false);
        initialServers.add(server0);
        initialServers.add(server1);
        initialServers.add(server2);

        // Add a new node (server 3)
        ServerModel server3 = new ServerModel(3, "rack0", false);
        initialServers.add(server3);
        ClusterModel newClusterModel = new ClusterModel(initialServers);
        // Re-add buckets to the new cluster model, still on 0, 1, 2
        for (int i = 0; i < 9; i++) {
            addBucket(newClusterModel, new TableBucket(i, 0), Arrays.asList(0, 1, 2));
        }

        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<Goal> goals = new ArrayList<>();
        goals.add(new ReplicaDistributionGoal());

        List<RebalancePlanForBucket> plans = goalOptimizer.doOptimizeOnce(newClusterModel, goals);

        // Expect some replicas to move to server 3
        boolean movedToNewNode =
                plans.stream()
                        .anyMatch(
                                plan ->
                                        plan.getNewReplicas().contains(3)
                                                && !plan.getOriginReplicas().contains(3));
        assertThat(movedToNewNode).isTrue();
    }

    @Test
    void testNodeLeave() {
        // Cluster with 4 nodes
        SortedSet<ServerModel> initialServers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        ServerModel server2 = new ServerModel(2, "rack2", false);
        ServerModel server3 = new ServerModel(3, "rack0", true); // Tagged as offline
        initialServers.add(server0);
        initialServers.add(server1);
        initialServers.add(server2);
        initialServers.add(server3);

        ClusterModel clusterModel = new ClusterModel(initialServers);
        // Add buckets, some on server 3
        addBucket(clusterModel, new TableBucket(0, 0), Arrays.asList(0, 1, 3));
        addBucket(clusterModel, new TableBucket(1, 0), Arrays.asList(0, 2, 3));

        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<Goal> goals = new ArrayList<>();
        goals.add(new ReplicaDistributionGoal());

        List<RebalancePlanForBucket> plans = goalOptimizer.doOptimizeOnce(clusterModel, goals);

        // Expect replicas on server 3 to be moved to other servers
        boolean movedFromOfflineNode =
                plans.stream()
                        .allMatch(
                                plan ->
                                        plan.getOriginReplicas().contains(3)
                                                && !plan.getNewReplicas().contains(3));
        assertThat(movedFromOfflineNode).isTrue();
    }

    @Test
    void testConstraintViolation() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        servers.add(server0);
        servers.add(server1);

        ClusterModel clusterModel = new ClusterModel(servers);
        // Node 0: 15 replicas
        // Node 1: 5 replicas
        // Total 20. Avg 10. Limit 11.
        for (int i = 0; i < 5; i++) {
            addBucket(clusterModel, new TableBucket(i, 0), Arrays.asList(0, 1));
        }
        for (int i = 5; i < 15; i++) {
            addBucket(clusterModel, new TableBucket(i, 0), Arrays.asList(0));
        }

        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<Goal> goals = new ArrayList<>();
        goals.add(new ReplicaDistributionGoal());

        List<RebalancePlanForBucket> plans = goalOptimizer.doOptimizeOnce(clusterModel, goals);

        // Verify that we have moves from 0 to 1.
        boolean movesFrom0To1 =
                plans.stream()
                        .anyMatch(
                                plan ->
                                        plan.getOriginReplicas().contains(0)
                                                && plan.getNewReplicas().contains(1)
                                                && !plan.getOriginReplicas().contains(1));
        assertThat(movesFrom0To1).isTrue();

        // Verify NO moves from 1 to 0.
        boolean movesFrom1To0 =
                plans.stream()
                        .anyMatch(
                                plan ->
                                        plan.getOriginReplicas().contains(1)
                                                && plan.getNewReplicas().contains(0)
                                                && !plan.getOriginReplicas().contains(0));
        assertThat(movesFrom1To0).isFalse();
    }
}
