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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.assertGoalViolationNotRegressed;
import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.assertRebalanceInvariants;
import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.captureReplicaDistribution;
import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.violation;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that goals preserve the guarantees of goals optimized before them. */
public class GoalCombinationTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("goalChains")
    void testGoalChainPreservesMeasuredGoalViolations(
            String name, List<Goal> goals, boolean offline) {
        ClusterModel plannedCluster = combinedFixture(offline);
        Map<TableBucket, List<Integer>> plannedOriginal =
                captureReplicaDistribution(plannedCluster);
        List<RebalancePlanForBucket> plans =
                new GoalOptimizer().doOptimizeOnce(plannedCluster, goals);
        assertRebalanceInvariants(plannedOriginal, plannedCluster, plans);

        ClusterModel cluster = combinedFixture(offline);
        Map<TableBucket, List<Integer>> original = captureReplicaDistribution(cluster);
        Set<Goal> optimized = new HashSet<>();
        List<Integer> afterGoal = new ArrayList<>();
        for (Goal goal : goals) {
            goal.optimize(cluster, optimized);
            optimized.add(goal);
            afterGoal.add(violation(goal, cluster));
        }
        for (int i = 0; i < goals.size(); i++) {
            assertGoalViolationNotRegressed(goals.get(i), afterGoal.get(i), cluster);
        }
        assertRebalanceInvariants(original, cluster, new ArrayList<RebalancePlanForBucket>());
        if (offline) {
            assertThat(cluster.server(4).numReplicas()).isZero();
            assertThat(cluster.server(4).numLeaderReplicas()).isZero();
        }
    }

    @Test
    void testReplicaBeforeTableReplicaLeavesTableViolationWhileClusterWindowHolds() {
        ClusterModel cluster = wrongOrderFixture();
        Goal replica = new ReplicaDistributionGoal();
        Goal tableReplica = new TableReplicaDistributionGoal();
        Set<Goal> optimized = new HashSet<>();
        replica.optimize(cluster, optimized);
        optimized.add(replica);
        tableReplica.optimize(cluster, optimized);

        assertThat(violation(tableReplica, cluster)).isGreaterThan(0);
        assertThat(violation(replica, cluster)).isZero();
    }

    @Test
    void testRecommendedChainConvergesOnSecondRun() {
        ClusterModel cluster = convergenceFixture();
        List<Goal> goals = recommendedGoals();
        GoalOptimizer optimizer = new GoalOptimizer();
        optimizer.doOptimizeOnce(cluster, goals);
        assertThat(optimizer.doOptimizeOnce(cluster, recommendedGoals())).isEmpty();
    }

    private static Stream<Arguments> goalChains() {
        return Stream.of(
                Arguments.of("recommended", recommendedGoals(), false),
                Arguments.of(
                        "table replica then table leader",
                        Arrays.<Goal>asList(
                                new TableReplicaDistributionGoal(),
                                new TableLeaderReplicaDistributionGoal()),
                        false),
                Arguments.of(
                        "table leader then leader",
                        Arrays.<Goal>asList(
                                new TableLeaderReplicaDistributionGoal(),
                                new LeaderReplicaDistributionGoal()),
                        false),
                Arguments.of(
                        "leader then table leader",
                        Arrays.<Goal>asList(
                                new LeaderReplicaDistributionGoal(),
                                new TableLeaderReplicaDistributionGoal()),
                        false),
                Arguments.of(
                        "rack then table leader",
                        Arrays.<Goal>asList(
                                new RackAwareGoal(), new TableLeaderReplicaDistributionGoal()),
                        false),
                Arguments.of(
                        "rack then table replica",
                        Arrays.<Goal>asList(
                                new RackAwareGoal(), new TableReplicaDistributionGoal()),
                        false),
                Arguments.of(
                        "table leader then table replica",
                        Arrays.<Goal>asList(
                                new TableLeaderReplicaDistributionGoal(),
                                new TableReplicaDistributionGoal()),
                        false),
                Arguments.of(
                        "replica then table replica",
                        Arrays.<Goal>asList(
                                new ReplicaDistributionGoal(), new TableReplicaDistributionGoal()),
                        false),
                Arguments.of("full chain with offline server", recommendedGoals(), true));
    }

    private static List<Goal> recommendedGoals() {
        return Arrays.<Goal>asList(
                new RackAwareGoal(),
                new TableReplicaDistributionGoal(),
                new ReplicaDistributionGoal(),
                new TableLeaderReplicaDistributionGoal(),
                new LeaderReplicaDistributionGoal());
    }

    private ClusterModel combinedFixture(boolean offline) {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", false));
        servers.add(new ServerModel(1, "rack1", false));
        servers.add(new ServerModel(2, "rack2", false));
        servers.add(new ServerModel(3, "rack3", false));
        if (offline) {
            servers.add(new ServerModel(4, "rack4", true));
        }
        ClusterModel cluster = new ClusterModel(servers);
        for (int i = 0; i < 8; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(0, 1));
            addBucket(cluster, new TableBucket(2, i), Arrays.asList(2, 3));
        }
        for (int partition = 0; partition < 2; partition++) {
            for (int bucket = 0; bucket < 4; bucket++) {
                addBucket(
                        cluster,
                        new TableBucket(3, (long) partition, bucket),
                        offline ? Arrays.asList(4, 2) : Arrays.asList(0, 2));
            }
        }
        return cluster;
    }

    private ClusterModel wrongOrderFixture() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        for (int i = 0; i < 4; i++) {
            servers.add(new ServerModel(i, "rack" + i, false));
        }
        ClusterModel cluster = new ClusterModel(servers);
        for (int i = 0; i < 8; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(0));
            addBucket(cluster, new TableBucket(2, i), Arrays.asList(1, 2, 3));
        }
        return cluster;
    }

    private ClusterModel convergenceFixture() {
        SortedSet<ServerModel> servers = new TreeSet<>();
        for (int i = 0; i < 4; i++) {
            servers.add(new ServerModel(i, "rack" + i, false));
        }
        ClusterModel cluster = new ClusterModel(servers);
        for (int i = 0; i < 8; i++) {
            addBucket(cluster, new TableBucket(1, i), Arrays.asList(i % 2, 2 + i % 2));
            addBucket(cluster, new TableBucket(2, i), Arrays.asList((i + 1) % 2, 2 + (i + 1) % 2));
        }
        return cluster;
    }
}
