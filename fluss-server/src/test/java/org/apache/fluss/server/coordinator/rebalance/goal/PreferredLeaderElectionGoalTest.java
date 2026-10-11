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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PreferredLeaderElectionGoal}. */
class PreferredLeaderElectionGoalTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);
    private static final List<Integer> ASSIGNMENT = Arrays.asList(0, 1, 2);

    @Test
    void testGenerateLeaderOnlyPlanAndRemainIdempotent() {
        ClusterModel clusterModel = createClusterModel(false);
        addBucket(clusterModel, TABLE_BUCKET, 1, true);

        GoalOptimizer optimizer = new GoalOptimizer();
        List<RebalancePlanForBucket> plans =
                optimizer.doOptimizeOnce(
                        clusterModel, Collections.singletonList(new PreferredLeaderElectionGoal()));

        assertThat(plans)
                .containsExactly(
                        new RebalancePlanForBucket(TABLE_BUCKET, 1, 0, ASSIGNMENT, ASSIGNMENT));
        assertThat(clusterModel.getReplicaDistribution().get(TABLE_BUCKET))
                .containsExactlyElementsOf(ASSIGNMENT);
        assertThat(clusterModel.getLeaderDistribution().get(TABLE_BUCKET)).isEqualTo(0);

        assertThat(
                        optimizer.doOptimizeOnce(
                                clusterModel,
                                Collections.singletonList(new PreferredLeaderElectionGoal())))
                .isEmpty();
    }

    @Test
    void testSkipIneligiblePreferredLeaderWithoutFallback() {
        ClusterModel clusterModel = createClusterModel(false);
        addBucket(clusterModel, TABLE_BUCKET, 1, false);

        List<RebalancePlanForBucket> plans =
                new GoalOptimizer()
                        .doOptimizeOnce(
                                clusterModel,
                                Collections.singletonList(new PreferredLeaderElectionGoal()));

        assertThat(plans).isEmpty();
        assertThat(clusterModel.getLeaderDistribution().get(TABLE_BUCKET)).isEqualTo(1);
        assertThat(clusterModel.getReplicaDistribution().get(TABLE_BUCKET))
                .containsExactlyElementsOf(ASSIGNMENT);
    }

    @Test
    void testSkipOfflineTaggedPreferredLeader() {
        ClusterModel clusterModel = createClusterModel(true);
        addBucket(clusterModel, TABLE_BUCKET, 1, true);

        List<RebalancePlanForBucket> plans =
                new GoalOptimizer()
                        .doOptimizeOnce(
                                clusterModel,
                                Collections.singletonList(new PreferredLeaderElectionGoal()));

        assertThat(plans).isEmpty();
        assertThat(clusterModel.getLeaderDistribution().get(TABLE_BUCKET)).isEqualTo(1);
    }

    @Test
    void testRejectGoalCombination() {
        ClusterModel clusterModel = createClusterModel(false);

        assertThatThrownBy(
                        () ->
                                new GoalOptimizer()
                                        .doOptimizeOnce(
                                                clusterModel,
                                                Arrays.asList(
                                                        new PreferredLeaderElectionGoal(),
                                                        new LeaderReplicaDistributionGoal())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "PREFERRED_LEADER_ELECTION must be used as a standalone rebalance goal.");
    }

    private static ClusterModel createClusterModel(boolean preferredLeaderOfflineTagged) {
        SortedSet<ServerModel> servers = new TreeSet<>();
        servers.add(new ServerModel(0, "rack0", preferredLeaderOfflineTagged));
        servers.add(new ServerModel(1, "rack1", false));
        servers.add(new ServerModel(2, "rack2", false));
        return new ClusterModel(servers);
    }

    private static void addBucket(
            ClusterModel clusterModel,
            TableBucket tableBucket,
            int currentLeader,
            boolean preferredLeaderEligible) {
        for (int i = 0; i < ASSIGNMENT.size(); i++) {
            int replica = ASSIGNMENT.get(i);
            clusterModel.createReplica(
                    replica,
                    tableBucket,
                    i,
                    replica == currentLeader,
                    replica != ASSIGNMENT.get(0) || preferredLeaderEligible);
        }
    }
}
