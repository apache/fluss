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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizerUtils.getDiff;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizerUtils.hasDiff;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test class for {@link GoalOptimizerUtils}. */
public class GoalOptimizerUtilsTest {

    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", true);
        ServerModel server1 = new ServerModel(1, "rack1", true);
        ServerModel server2 = new ServerModel(2, "rack2", true);
        ServerModel server3 = new ServerModel(3, "rack0", true);
        servers.add(server0);
        servers.add(server1);
        servers.add(server2);
        servers.add(server3);
    }

    @Test
    void testHasDiff() {
        ClusterModel clusterModel = new ClusterModel(servers);

        // add buckets into clusterModel.
        addBucket(clusterModel, new TableBucket(0, 0), Arrays.asList(0, 1, 2));
        addBucket(clusterModel, new TableBucket(1, 0), Arrays.asList(0, 1, 2));

        Map<TableBucket, List<Integer>> initialReplicaDistribution =
                clusterModel.getReplicaDistribution();
        Map<TableBucket, Integer> initialLeaderDistribution = clusterModel.getLeaderDistribution();
        assertThat(hasDiff(initialReplicaDistribution, initialLeaderDistribution, clusterModel))
                .isFalse();

        clusterModel.relocateLeadership(new TableBucket(0, 0), 0, 1);
        clusterModel.relocateReplica(new TableBucket(1, 0), 2, 3);
        assertThat(hasDiff(initialReplicaDistribution, initialLeaderDistribution, clusterModel))
                .isTrue();

        assertThatThrownBy(() -> hasDiff(new HashMap<>(), initialLeaderDistribution, clusterModel))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Initial and final replica distributions do not contain the same buckets.");
    }

    @Test
    void testGetDiff() {
        ClusterModel clusterModel = new ClusterModel(servers);

        // add buckets into clusterModel.
        addBucket(clusterModel, new TableBucket(0, 0), Arrays.asList(0, 1, 2));
        addBucket(clusterModel, new TableBucket(1, 0), Arrays.asList(0, 1, 2));

        Map<TableBucket, List<Integer>> initialReplicaDistribution =
                clusterModel.getReplicaDistribution();
        Map<TableBucket, Integer> initialLeaderDistribution = clusterModel.getLeaderDistribution();
        assertThat(hasDiff(initialReplicaDistribution, initialLeaderDistribution, clusterModel))
                .isFalse();

        clusterModel.relocateLeadership(new TableBucket(0, 0), 0, 1);
        clusterModel.relocateReplica(new TableBucket(1, 0), 2, 3);
        assertThat(hasDiff(initialReplicaDistribution, initialLeaderDistribution, clusterModel))
                .isTrue();

        List<RebalancePlanForBucket> diffPlan =
                getDiff(initialReplicaDistribution, initialLeaderDistribution, clusterModel);
        assertThat(diffPlan)
                .contains(
                        new RebalancePlanForBucket(
                                new TableBucket(0, 0),
                                0,
                                1,
                                Arrays.asList(0, 1, 2),
                                Arrays.asList(1, 0, 2)),
                        new RebalancePlanForBucket(
                                new TableBucket(1, 0),
                                0,
                                0,
                                Arrays.asList(0, 1, 2),
                                Arrays.asList(0, 1, 3)));
    }
}
