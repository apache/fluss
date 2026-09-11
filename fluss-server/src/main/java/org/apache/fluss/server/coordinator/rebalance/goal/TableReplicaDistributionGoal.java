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
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModelStats;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;
import org.apache.fluss.utils.MathUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.REPLICA_REJECT;
import static org.apache.fluss.utils.MathUtils.EPSILON;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Soft goal to distribute replicas of every table across tablet servers. */
public class TableReplicaDistributionGoal extends TableDistributionAbstractGoal {

    @Override
    public ActionAcceptance actionAcceptance(RebalancingAction action, ClusterModel clusterModel) {
        if (action.getActionType() == ActionType.LEADERSHIP_MOVEMENT) {
            return ACCEPT;
        }
        if (action.getActionType() != ActionType.REPLICA_MOVEMENT) {
            throw new IllegalArgumentException("Unsupported action type " + action.getActionType());
        }
        ServerModel sourceServer = clusterModel.server(action.getSourceServerId());
        ServerModel destinationServer = clusterModel.server(action.getDestinationServerId());
        checkNotNull(
                sourceServer, "Source server " + action.getSourceServerId() + " is not found.");
        checkNotNull(
                destinationServer,
                "Destination server " + action.getDestinationServerId() + " is not found.");
        long tableId = action.getTableBucket().getTableId();
        return destinationServer.numReplicas(tableId) + 1 <= upperLimit(tableId, destinationServer)
                        && (!isAlive(sourceServer)
                                || sourceServer.numReplicas(tableId) - 1
                                        >= lowerLimit(tableId, sourceServer))
                ? ACCEPT
                : REPLICA_REJECT;
    }

    @Override
    protected void rebalanceForServer(
            ServerModel server, ClusterModel clusterModel, Set<Goal> optimizedGoals)
            throws RebalanceFailureException {
        for (Long tableId : clusterModel.tables()) {
            int replicaCount = server.numReplicas(tableId);
            if (replicaCount > upperLimit(tableId, server) || server.isOfflineTagged()) {
                if (rebalanceByMovingReplicasOut(server, tableId, clusterModel, optimizedGoals)) {
                    tablesAboveRebalanceUpperLimit.add(tableId);
                }
            } else if (isAlive(server) && replicaCount < lowerLimit(tableId, server)) {
                if (rebalanceByMovingReplicasIn(server, tableId, clusterModel, optimizedGoals)) {
                    tablesBelowRebalanceLowerLimit.add(tableId);
                }
            }
        }
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new TableReplicaDistributionStatsComparator();
    }

    @Override
    Map<Long, Integer> numInterestedReplicasByTable(ClusterModel clusterModel) {
        return clusterModel.numReplicasByTable();
    }

    private boolean rebalanceByMovingReplicasOut(
            ServerModel sourceServer,
            long tableId,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals) {
        SortedSet<ServerModel> candidates =
                new TreeSet<>(
                        Comparator.comparingInt((ServerModel server) -> server.numReplicas(tableId))
                                .thenComparingInt(ServerModel::numReplicas)
                                .thenComparingInt(ServerModel::id));
        for (ServerModel server : clusterModel.aliveServers()) {
            if (server.numReplicas(tableId) < upperLimit(tableId, server)) {
                candidates.add(server);
            }
        }
        for (ReplicaModel replica : sourceServer.replicas(tableId)) {
            ServerModel destination =
                    maybeApplyBalancingAction(
                            clusterModel,
                            replica,
                            candidates,
                            ActionType.REPLICA_MOVEMENT,
                            optimizedGoals);
            if (destination != null) {
                if (sourceServer.numReplicas(tableId) <= upperLimit(tableId, sourceServer)) {
                    return false;
                }
                candidates.remove(destination);
                if (destination.numReplicas(tableId) < upperLimit(tableId, destination)) {
                    candidates.add(destination);
                }
            }
        }
        return sourceServer.numReplicas(tableId) > upperLimit(tableId, sourceServer);
    }

    private boolean rebalanceByMovingReplicasIn(
            ServerModel destinationServer,
            long tableId,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals) {
        PriorityQueue<ServerModel> sources =
                new PriorityQueue<>(
                        Comparator.comparingInt((ServerModel server) -> server.numReplicas(tableId))
                                .reversed()
                                .thenComparingInt(ServerModel::id));
        for (ServerModel sourceServer : clusterModel.servers()) {
            if (sourceServer.numReplicas(tableId) > lowerLimit(tableId, sourceServer)
                    || !isAlive(sourceServer)) {
                sources.add(sourceServer);
            }
        }
        List<ServerModel> candidate = Collections.singletonList(destinationServer);
        while (!sources.isEmpty()) {
            ServerModel sourceServer = sources.poll();
            for (ReplicaModel replica : sourceServer.replicas(tableId)) {
                ServerModel destination =
                        maybeApplyBalancingAction(
                                clusterModel,
                                replica,
                                candidate,
                                ActionType.REPLICA_MOVEMENT,
                                optimizedGoals);
                if (destination != null) {
                    if (destinationServer.numReplicas(tableId)
                            >= lowerLimit(tableId, destinationServer)) {
                        return false;
                    }
                    if (!sources.isEmpty()
                            && sourceServer.numReplicas(tableId)
                                    < sources.peek().numReplicas(tableId)) {
                        sources.add(sourceServer);
                        break;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Detects per-table regressions of the replica distribution.
     *
     * <p>This comparator is a regression detector rather than a general purpose total order: it
     * returns a negative value as soon as the standard deviation of any table got worse, and zero
     * otherwise. It never reports a preference for the post-optimization stats, because "preferred"
     * is not well defined when one table improves while another one stays equal. The only caller,
     * {@code AbstractGoal#optimize}, checks the negative branch alone.
     */
    private class TableReplicaDistributionStatsComparator implements ClusterModelStatsComparator {
        private String reasonForLastNegativeResult;

        @Override
        public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
            for (Map.Entry<Long, Double> entry : stats1.replicaStdDevByTable().entrySet()) {
                long tableId = entry.getKey();
                double postOptimizationStdDev = entry.getValue();
                double preOptimizationStdDev = stats2.replicaStdDevByTable().get(tableId);
                if (MathUtils.compare(preOptimizationStdDev, postOptimizationStdDev, EPSILON) < 0) {
                    reasonForLastNegativeResult =
                            String.format(
                                    "Violated %s for table %s. [Std Deviation of Replica Distribution] "
                                            + "post-optimization:%.3f pre-optimization:%.3f",
                                    name(), tableId, postOptimizationStdDev, preOptimizationStdDev);
                    return -1;
                }
            }
            return 0;
        }

        @Override
        public String explainLastComparison() {
            return reasonForLastNegativeResult;
        }
    }
}
