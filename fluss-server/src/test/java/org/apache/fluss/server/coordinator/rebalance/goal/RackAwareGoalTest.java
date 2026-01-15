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
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RackAwareGoal}. */
public class RackAwareGoalTest {

    @Test
    void testReplicaNumExceedsRackNum() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", false));
        servers.add(new ServerModel(1, "rack1", false));
        // server2 offline.
        servers.add(new ServerModel(2, "rack2", true));
        ClusterModel clusterModel = new ClusterModel(servers);
        TableBucket t1b0 = new TableBucket(1, 0);
        addBucket(clusterModel, t1b0, Arrays.asList(0, 1, 2));

        RackAwareGoal goal = new RackAwareGoal();
        assertThatThrownBy(() -> goal.optimize(clusterModel, Collections.singleton(goal)))
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessage(
                        "[RackAwareGoal] Insufficient number of racks to distribute each replica (Current: 2, Needed: 3).");
    }

    @Test
    void testReplicaMove() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", false));
        servers.add(new ServerModel(1, "rack1", false));
        servers.add(new ServerModel(2, "rack2", false));
        servers.add(new ServerModel(3, "rack0", false));
        ClusterModel clusterModel = new ClusterModel(servers);

        TableBucket t1b0 = new TableBucket(1, 0);
        addBucket(clusterModel, t1b0, Arrays.asList(0, 1, 3));

        // check the follower will be moved to server2.
        RackAwareGoal goal = new RackAwareGoal();
        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<RebalancePlanForBucket> rebalancePlanForBuckets =
                goalOptimizer.doOptimizeOnce(clusterModel, Collections.singletonList(goal));
        assertThat(rebalancePlanForBuckets).hasSize(1);
        assertThat(rebalancePlanForBuckets.get(0))
                .isEqualTo(
                        new RebalancePlanForBucket(
                                t1b0, 0, 2, Arrays.asList(0, 1, 3), Arrays.asList(2, 1, 3)));
    }

    @Test
    void testReplicaDistributionNotBalanceAcrossRackAndServer() {
        // RackAwareGoal only requires that replicas of the same bucket cannot be distributed on
        // the same rack, but it does not care about the balance of replicas between racks, nor does
        // it care about the balance of replicas between servers.
        ClusterModel clusterModel = generateUnbalancedReplicaAcrossServerAndRack();
        RackAwareGoal goal = new RackAwareGoal();
        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<RebalancePlanForBucket> rebalancePlanForBuckets =
                goalOptimizer.doOptimizeOnce(clusterModel, Collections.singletonList(goal));
        assertThat(rebalancePlanForBuckets).hasSize(0);
    }

    @Test
    void testReplicaDistributionBalanceAcrossServer() {
        // the same input of `testReplicaDistributionNotBalanceAcrossRackAndServer`, if we combine
        // using RackAwareGoal and ReplicaDistributionGoal, the replica distribution will be
        // balanced across servers.
        ClusterModel clusterModel = generateUnbalancedReplicaAcrossServerAndRack();
        RackAwareGoal rackAwareGoal = new RackAwareGoal();
        ReplicaDistributionGoal replicaDistributionGoal = new ReplicaDistributionGoal();
        GoalOptimizer goalOptimizer = new GoalOptimizer();
        List<RebalancePlanForBucket> rebalancePlanForBuckets =
                goalOptimizer.doOptimizeOnce(
                        clusterModel, Arrays.asList(rackAwareGoal, replicaDistributionGoal));
        // Realance result(ReplicaNum) from server side: server0: 3, server1: 1, server2: 3,
        // server3: 1, server4: 1, server5: 1
        assertThat(rebalancePlanForBuckets).hasSize(1);
        assertThat(rebalancePlanForBuckets.get(0))
                .isEqualTo(
                        new RebalancePlanForBucket(
                                new TableBucket(1, 3),
                                0,
                                1,
                                Arrays.asList(0, 3, 5),
                                Arrays.asList(1, 3, 5)));
    }

    private ClusterModel generateUnbalancedReplicaAcrossServerAndRack() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", false));
        servers.add(new ServerModel(1, "rack0", false));
        servers.add(new ServerModel(2, "rack1", false));
        servers.add(new ServerModel(3, "rack1", false));
        servers.add(new ServerModel(4, "rack2", false));
        servers.add(new ServerModel(5, "rack3", false));
        ClusterModel clusterModel = new ClusterModel(servers);

        // For the following case, RackAwareGoal will not remove any replicas but the replica
        // distribution is not balanced not only in racks but also in servers.
        // t1b0 -> 0, 2, 4
        // t1b1 -> 0, 2, 5
        // t1b2 -> 0, 2, 4
        // t1b3 -> 0, 3, 5

        // Replica num from server side: server0: 4, server1: 0, server2: 3, server3: 1, server4: 1,
        // server5: 1
        // Replica num from rack side: rack0: 4, rack1: 4, rack2: 2, rack3: 2
        TableBucket t1b0 = new TableBucket(1, 0);
        addBucket(clusterModel, t1b0, Arrays.asList(0, 2, 4));
        TableBucket t1b1 = new TableBucket(1, 1);
        addBucket(clusterModel, t1b1, Arrays.asList(0, 2, 5));
        TableBucket t1b2 = new TableBucket(1, 2);
        addBucket(clusterModel, t1b2, Arrays.asList(0, 2, 4));
        TableBucket t1b3 = new TableBucket(1, 3);
        addBucket(clusterModel, t1b3, Arrays.asList(0, 3, 5));
        return clusterModel;
    }
}
