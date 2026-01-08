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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizerUtils.getDiff;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizerUtils.hasDiff;

/** A class for optimizing goals in the given order of priority. */
public class GoalOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(GoalOptimizer.class);

    public List<RebalancePlanForBucket> doOptimizeOnce(
            ClusterModel clusterModel, List<Goal> goalsByPriority) {
        LOG.trace("Cluster before optimization is {}", clusterModel);
        Map<TableBucket, List<Integer>> initReplicaDistribution =
                clusterModel.getReplicaDistribution();
        Map<TableBucket, Integer> initLeaderDistribution = clusterModel.getLeaderDistribution();

        // Set of balancing proposals that will be applied to the given cluster state to satisfy
        // goals (leadership transfer AFTER bucket transfer.)
        Set<Goal> optimizedGoals = new HashSet<>();
        Map<TableBucket, List<Integer>> preOptimizedReplicaDistribution = null;
        Map<TableBucket, Integer> preOptimizedLeaderDistribution = null;
        for (Goal goal : goalsByPriority) {
            preOptimizedReplicaDistribution =
                    preOptimizedReplicaDistribution == null
                            ? initReplicaDistribution
                            : clusterModel.getReplicaDistribution();
            preOptimizedLeaderDistribution =
                    preOptimizedLeaderDistribution == null
                            ? initLeaderDistribution
                            : clusterModel.getLeaderDistribution();

            // executing the goal optimization.
            goal.optimize(clusterModel, optimizedGoals);
            optimizedGoals.add(goal);

            boolean hasDiff =
                    hasDiff(
                            preOptimizedReplicaDistribution,
                            preOptimizedLeaderDistribution,
                            clusterModel);
            LOG.info(
                    "[{}/{}] Generated {} proposals for {}",
                    optimizedGoals.size(),
                    goalsByPriority.size(),
                    hasDiff ? "some" : "no",
                    goal.name());
        }

        return getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    }
}
