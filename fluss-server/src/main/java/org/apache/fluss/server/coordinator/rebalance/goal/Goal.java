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

import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModelStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;

/** This is the interface of the optimization goals used for rebalance. */
public interface Goal {
    Logger LOG = LoggerFactory.getLogger(Goal.class);

    /**
     * Optimize the given cluster model as needed for this goal.
     *
     * <p>The method will be given a cluster model. The goal can try to optimize the cluster model
     * by performing some admin operations(e.g. move replicas or leader of tableBuckets).
     *
     * <p>During the optimization, the implementation should make sure that all the previously
     * optimized goals are still satisfied after this method completes its execution. The
     * implementation can use {@link #actionAcceptance(RebalancingAction, ClusterModel)} to check
     * whether an admin operation is allowed by a previously optimized goals.
     *
     * <p>The implementation of a soft goal should return a boolean indicating whether the goal has
     * been met after the optimization or not.
     */
    void optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals);

    /**
     * Check whether the given action is acceptable by this goal in the given state of the cluster.
     * An action is (1) accepted by a goal if it satisfies requirements of the goal, or (2) rejected
     * by a goal if it violates its requirements. The return value indicates whether the action is
     * accepted or why it is rejected.
     */
    ActionAcceptance actionAcceptance(RebalancingAction action, ClusterModel clusterModel);

    /**
     * Get an instance of {@link ClusterModelStatsComparator} for this goal.
     *
     * <p>The {@link ClusterModelStatsComparator#compare(ClusterModelStats, ClusterModelStats)}
     * method should give a preference between two {@link ClusterModelStats}.
     *
     * <p>The returned value must not be null.
     *
     * @return An instance of {@link ClusterModelStatsComparator} for this goal.
     */
    ClusterModelStatsComparator clusterModelStatsComparator();

    /**
     * Signal for finishing the process for rebalance. It is intended to mark the goal optimization
     * as finished and perform the memory clean up after the goal optimization.
     */
    void finish();

    /**
     * @return The name of this goal. Name of a goal provides an identification for the goal in
     *     human-readable format.
     */
    String name();

    /**
     * A comparator that compares two cluster model stats.
     *
     * <p>Note: this comparator imposes orderings that are inconsistent with equals.
     */
    interface ClusterModelStatsComparator extends Comparator<ClusterModelStats>, Serializable {

        /**
         * Compare two cluster model stats and determine which stats is preferred.
         *
         * @param stats1 the first stats
         * @param stats2 the second stats
         * @return Positive value if stats1 is preferred, 0 if the two stats are equally preferred,
         *     negative value if stats2 is preferred.
         */
        @Override
        int compare(ClusterModelStats stats1, ClusterModelStats stats2);

        /**
         * This is a method to get the reason for the last comparison. The implementation should at
         * least provide a reason when the last comparison returns negative value.
         *
         * @return A string that explains the result of last comparison.
         */
        String explainLastComparison();
    }
}
