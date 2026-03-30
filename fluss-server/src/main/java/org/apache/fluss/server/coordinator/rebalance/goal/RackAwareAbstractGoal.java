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
import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.ActionType;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.SortedSet;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.SERVER_REJECT;

/** An abstract class for rack-aware goals. */
public abstract class RackAwareAbstractGoal extends AbstractGoal {
    private static final Logger LOG = LoggerFactory.getLogger(RackAwareAbstractGoal.class);

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new GoalUtils.HardGoalStatsComparator();
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, RebalancingAction action) {
        return true;
    }

    /**
     * Check whether the given action is acceptable by this goal. The following actions are
     * acceptable:
     *
     * <ul>
     *   <li>All leadership moves
     *   <li>Replica moves that do not violate {@link
     *       #doesReplicaMoveViolateActionAcceptance(ClusterModel, ReplicaModel, ServerModel)}
     * </ul>
     *
     * @param action Action to be checked for acceptance.
     * @param clusterModel The state of the cluster.
     * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal, {@link
     *     ActionAcceptance#SERVER_REJECT} if the action is rejected due to violating rack awareness
     *     in the destination broker after moving source replica to destination broker.
     */
    @Override
    public ActionAcceptance actionAcceptance(RebalancingAction action, ClusterModel clusterModel) {
        switch (action.getActionType()) {
            case LEADERSHIP_MOVEMENT:
                return ACCEPT;
            case REPLICA_MOVEMENT:
                if (doesReplicaMoveViolateActionAcceptance(
                        clusterModel,
                        clusterModel
                                .server(action.getSourceServerId())
                                .replica(action.getTableBucket()),
                        clusterModel.server(action.getDestinationServerId()))) {
                    return SERVER_REJECT;
                }
                return ACCEPT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported rebalance action " + action.getActionType() + " is provided.");
        }
    }

    /**
     * Check whether the given replica move would violate the action acceptance for this rack aware
     * goal.
     *
     * @param clusterModel The state of the cluster.
     * @param sourceReplica Source replica
     * @param destServer Destination server to receive the given source replica.
     * @return {@code true} if the given replica move would violate action acceptance (i.e. the move
     *     is not acceptable), {@code false} otherwise.
     */
    protected abstract boolean doesReplicaMoveViolateActionAcceptance(
            ClusterModel clusterModel, ReplicaModel sourceReplica, ServerModel destServer);

    /**
     * Rebalance the given serverModel without violating the constraints of this rack aware goal and
     * optimized goals.
     *
     * @param serverModel Server to be balanced.
     * @param clusterModel The state of the cluster.
     * @param optimizedGoals Optimized goals.
     */
    protected void rebalanceForServer(
            ServerModel serverModel, ClusterModel clusterModel, Set<Goal> optimizedGoals)
            throws RebalanceFailureException {
        // TODO maybe use a sorted replicas set
        for (ReplicaModel replica : serverModel.replicas()) {
            if (!serverModel.isOfflineTagged()
                    && shouldKeepInTheCurrentServer(replica, clusterModel)) {
                continue;
            }
            // The relevant rack awareness condition is violated. Move replica to an eligible
            // serverModel
            SortedSet<ServerModel> eligibleServers =
                    rackAwareEligibleServers(replica, clusterModel);
            if (maybeApplyBalancingAction(
                            clusterModel,
                            replica,
                            eligibleServers,
                            ActionType.REPLICA_MOVEMENT,
                            optimizedGoals)
                    == null) {
                LOG.debug(
                        "Cannot move replica {} to any serverModel in {}",
                        replica,
                        eligibleServers);
            }
        }
    }

    /**
     * Check whether the given alive replica should stay in the current server or be moved to
     * another server to satisfy the specific requirements of the rack aware goal in the given
     * cluster state.
     *
     * @param replica An alive replica to check whether it should stay in the current server.
     * @param clusterModel The state of the cluster.
     * @return {@code true} if the given alive replica should stay in the current server, {@code
     *     false} otherwise.
     */
    protected abstract boolean shouldKeepInTheCurrentServer(
            ReplicaModel replica, ClusterModel clusterModel);

    /**
     * Get a list of eligible servers for moving the given replica in the given cluster to satisfy
     * the specific requirements of the rack aware goal.
     *
     * @param replica Replica for which a set of rack aware eligible servers are requested.
     * @param clusterModel The state of the cluster.
     * @return A list of rack aware eligible servers for the given replica in the given cluster.
     */
    protected abstract SortedSet<ServerModel> rackAwareEligibleServers(
            ReplicaModel replica, ClusterModel clusterModel);
}
