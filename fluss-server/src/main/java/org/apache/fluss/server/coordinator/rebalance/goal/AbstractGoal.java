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

import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.ActionType;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModelStats;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.ActionType.LEADERSHIP_MOVEMENT;
import static org.apache.fluss.server.coordinator.rebalance.ActionType.REPLICA_MOVEMENT;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizerUtils.isProposalAcceptableForOptimizedGoals;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalUtils.legitMove;

/** An abstract class for goals. */
public abstract class AbstractGoal implements Goal {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGoal.class);
    protected boolean finished;
    protected boolean succeeded;

    public AbstractGoal() {
        finished = false;
        succeeded = true;
    }

    @Override
    public void optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals) {
        LOG.debug("Starting Optimizing for goal {}", name());
        // Initialize pre-optimized stats.
        ClusterModelStats statsBeforeOptimization = clusterModel.getClusterStats();
        LOG.trace("[PRE - {}] {}", name(), statsBeforeOptimization);
        finished = false;
        long goalStartTime = System.currentTimeMillis();
        initGoalState(clusterModel);
        SortedSet<ServerModel> offlineServers = clusterModel.offlineServers();

        while (!finished) {
            for (ServerModel server : serversToBalance(clusterModel)) {
                rebalanceForServer(server, clusterModel, optimizedGoals);
            }
            updateGoalState(clusterModel);
        }

        ClusterModelStats statsAfterOptimization = clusterModel.getClusterStats();
        LOG.trace("[POST - {}] {}", name(), statsAfterOptimization);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Finished optimization for {} in {}ms.",
                    name(),
                    System.currentTimeMillis() - goalStartTime);
        }
        LOG.trace("Cluster after optimization is {}", clusterModel);
        // The optimization cannot make stats worse unless the cluster has (1) offline servers for
        // replica move with replicas.
        if (offlineServers.isEmpty()) {
            ClusterModelStatsComparator comparator = clusterModelStatsComparator();
            // Throw exception when the stats before optimization is preferred.
            if (comparator.compare(statsAfterOptimization, statsBeforeOptimization) < 0) {
                // If a goal provides worse stats after optimization, that indicates an
                // implementation error with the goal.
                throw new IllegalStateException(
                        String.format(
                                "Optimization for goal %s failed because the optimized result is worse than before."
                                        + " Reason: %s.",
                                name(), comparator.explainLastComparison()));
            }
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    /**
     * Get sorted tabletServers that the rebalance process will go over to apply balancing actions
     * to replicas they contain.
     */
    protected SortedSet<ServerModel> serversToBalance(ClusterModel clusterModel) {
        return clusterModel.servers();
    }

    /** Initialize states that this goal requires. */
    protected abstract void initGoalState(ClusterModel clusterModel)
            throws RebalanceFailureException;

    /**
     * Rebalance the given tabletServers without violating the constraints of the current goal and
     * optimized goals.
     */
    protected abstract void rebalanceForServer(
            ServerModel server, ClusterModel clusterModel, Set<Goal> optimizedGoals)
            throws RebalanceFailureException;

    /** Update goal state after one round of rebalance. */
    protected abstract void updateGoalState(ClusterModel clusterModel)
            throws RebalanceFailureException;

    /**
     * Check if requirements of this goal are not violated if this action is applied to the given
     * cluster state, {@code false} otherwise.
     */
    protected abstract boolean selfSatisfied(ClusterModel clusterModel, RebalancingAction action);

    /**
     * Attempt to apply the given balancing action to the given replica in the given cluster. The
     * application considers the candidate tabletServers as the potential destination tabletServers
     * for replica movement or the location of followers for leadership transfer. If the movement
     * attempt succeeds, the function returns the server id of the destination, otherwise the
     * function returns null.
     */
    protected ServerModel maybeApplyBalancingAction(
            ClusterModel clusterModel,
            ReplicaModel replica,
            Collection<ServerModel> candidateServers,
            ActionType action,
            Set<Goal> optimizedGoals) {
        List<ServerModel> eligibleServers = new ArrayList<>(candidateServers);
        TableBucket tableBucket = replica.tableBucket();
        for (ServerModel server : eligibleServers) {
            RebalancingAction proposal =
                    new RebalancingAction(tableBucket, replica.server().id(), server.id(), action);
            // A replica should be moved if:
            // 0. The move is legit.
            // 1. The goal requirements are not violated if this action is applied to the given
            // cluster state.
            // 2. The movement is acceptable by the previously optimized goals.

            if (!legitMove(replica, server, clusterModel, action)) {
                LOG.trace("Replica move to server is not legit for {}.", proposal);
                continue;
            }

            if (!selfSatisfied(clusterModel, proposal)) {
                LOG.trace("Unable to self-satisfy proposal {}.", proposal);
                continue;
            }

            ActionAcceptance acceptance =
                    isProposalAcceptableForOptimizedGoals(optimizedGoals, proposal, clusterModel);
            LOG.trace(
                    "Trying to apply legit and self-satisfied action {}, actionAcceptance = {}",
                    proposal,
                    acceptance);
            if (acceptance == ACCEPT) {
                if (action == LEADERSHIP_MOVEMENT) {
                    clusterModel.relocateLeadership(
                            tableBucket, replica.server().id(), server.id());
                } else if (action == REPLICA_MOVEMENT) {
                    clusterModel.relocateReplica(tableBucket, replica.server().id(), server.id());
                }
                return server;
            }
        }
        return null;
    }
}
