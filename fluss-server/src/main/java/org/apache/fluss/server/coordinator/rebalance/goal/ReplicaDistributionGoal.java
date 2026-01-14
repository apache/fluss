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
import org.apache.fluss.server.coordinator.rebalance.model.StatisticType;
import org.apache.fluss.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.REPLICA_REJECT;
import static org.apache.fluss.utils.MathUtils.EPSILON;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Soft goal to generate replica movement proposals to ensure that the number of replicas on each
 * server is.
 *
 * <ul>
 *   <li>Under: (the average number of replicas per server) * (1 + replica count balance percentage)
 *   <li>Above: (the average number of replicas per server) * Math.max(0, 1 - replica count balance
 *       percentage)
 * </ul>
 */
public class ReplicaDistributionGoal extends ReplicaDistributionAbstractGoal {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaDistributionGoal.class);

    // TODO configurable.
    /**
     * The maximum allowed extent of unbalance for replica leader replica distribution. For example,
     * 1.10 means the highest leader replica count of a server should not be 1.10x of average leader
     * replica count of all alive tabletServers.
     */
    private static final Double REPLICA_COUNT_REBALANCE_THRESHOLD = 1.10d;

    @Override
    public ActionAcceptance actionAcceptance(RebalancingAction action, ClusterModel clusterModel) {
        switch (action.getActionType()) {
            case LEADERSHIP_MOVEMENT:
                return ACCEPT;
            case REPLICA_MOVEMENT:
                ServerModel sourceServer = clusterModel.server(action.getSourceServerId());
                ServerModel destServer = clusterModel.server(action.getDestinationServerId());

                checkNotNull(
                        sourceServer,
                        "Source server " + action.getSourceServerId() + " is not found.");
                checkNotNull(
                        destServer,
                        "Destination server " + action.getDestinationServerId() + " is not found.");

                // Check that destination and source would not become unbalanced.
                return (isReplicaCountUnderBalanceUpperLimitAfterChange(
                                        destServer, destServer.numReplicas()))
                                && (!isAlive(sourceServer)
                                        || isReplicaCountAboveBalanceLowerLimitAfterChange(
                                                sourceServer, sourceServer.numReplicas()))
                        ? ACCEPT
                        : REPLICA_REJECT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported balancing action " + action.getActionType() + " is provided.");
        }
    }

    @Override
    protected void rebalanceForServer(
            ServerModel server, ClusterModel clusterModel, Set<Goal> optimizedGoals)
            throws RebalanceFailureException {
        LOG.debug(
                "Rebalancing server {} [limits] lower: {} upper: {}.",
                server.id(),
                rebalanceLowerLimit,
                rebalanceUpperLimit);
        int numReplicas = server.numReplicas();
        boolean isAlive = isAlive(server);

        boolean requireLessReplicas =
                numReplicas > rebalanceUpperLimit || !isAlive || server.isOfflineTagged();
        boolean requireMoreReplicas =
                isAlive && !server.isOfflineTagged() && numReplicas < rebalanceLowerLimit;
        if (!requireMoreReplicas && !requireLessReplicas) {
            // return if the server is already within the limit.
            return;
        }

        if (requireLessReplicas
                && rebalanceByMovingReplicasOut(server, clusterModel, optimizedGoals)) {
            serverIdsAboveRebalanceUpperLimit.add(server.id());
            LOG.debug(
                    "Failed to sufficiently decrease replica count in server {} with replica movements. "
                            + "Replicas number after remove: {}.",
                    server.id(),
                    server.numReplicas());
        }

        if (requireMoreReplicas
                && rebalanceByMovingReplicasIn(server, clusterModel, optimizedGoals)) {
            serverIdsBelowRebalanceLowerLimit.add(server.id());
            LOG.debug(
                    "Failed to sufficiently increase replica count in server {} with replica movements. "
                            + "Replicas number after remove: {}.",
                    server.id(),
                    server.numReplicas());
        }

        if (!serverIdsAboveRebalanceUpperLimit.contains(server.id())
                && !serverIdsBelowRebalanceLowerLimit.contains(server.id())) {
            LOG.debug(
                    "Successfully balanced replica count for server {} by moving replicas. "
                            + "Replicas number after remove: {}",
                    server.id(),
                    server.numReplicas());
        }
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new ReplicaDistributionGoalStatsComparator();
    }

    @Override
    int numInterestedReplicas(ClusterModel clusterModel) {
        return clusterModel.numReplicas();
    }

    @Override
    double balancePercentage() {
        return REPLICA_COUNT_REBALANCE_THRESHOLD;
    }

    private boolean rebalanceByMovingReplicasOut(
            ServerModel server, ClusterModel cluster, Set<Goal> optimizedGoals) {
        SortedSet<ServerModel> candidateServers =
                new TreeSet<>(
                        Comparator.comparingInt(ServerModel::numReplicas)
                                .thenComparingInt(ServerModel::id));

        candidateServers.addAll(
                cluster.aliveServers().stream()
                        .filter(b -> b.numReplicas() < rebalanceUpperLimit)
                        .collect(Collectors.toSet()));
        int balanceUpperLimitForSourceServer = isAlive(server) ? rebalanceUpperLimit : 0;

        // Now let's do the replica out operation.
        // TODO maybe use a sorted replicas set
        for (ReplicaModel replica : server.replicas()) {
            ServerModel b =
                    maybeApplyBalancingAction(
                            cluster,
                            replica,
                            candidateServers,
                            ActionType.REPLICA_MOVEMENT,
                            optimizedGoals);
            // Only check if we successfully moved something.
            if (b != null) {
                if (server.numReplicas() <= balanceUpperLimitForSourceServer) {
                    return false;
                }

                // Remove and reinsert the server so the order is correct.
                candidateServers.remove(b);
                if (b.numReplicas() < rebalanceUpperLimit) {
                    candidateServers.add(b);
                }
            }
        }

        return server.numReplicas() != 0;
    }

    private boolean rebalanceByMovingReplicasIn(
            ServerModel aliveDestServer, ClusterModel cluster, Set<Goal> optimizedGoals) {
        PriorityQueue<ServerModel> eligibleServers =
                new PriorityQueue<>(
                        (b1, b2) -> {
                            // Servers are sorted by (1) all replica count then (2) server id.
                            int resultByAllReplicas =
                                    Integer.compare(b2.numReplicas(), b1.numReplicas());
                            return resultByAllReplicas == 0
                                    ? Integer.compare(b1.id(), b2.id())
                                    : resultByAllReplicas;
                        });

        // Source server can be offline, alive.
        for (ServerModel sourceServer : cluster.servers()) {
            if (sourceServer.numReplicas() > rebalanceLowerLimit || !isAlive(sourceServer)) {
                eligibleServers.add(sourceServer);
            }
        }

        List<ServerModel> candidateServers = Collections.singletonList(aliveDestServer);
        while (!eligibleServers.isEmpty()) {
            ServerModel sourceServer = eligibleServers.poll();
            // TODO maybe use a sorted replicas set
            for (ReplicaModel replica : sourceServer.replicas()) {
                ServerModel b =
                        maybeApplyBalancingAction(
                                cluster,
                                replica,
                                candidateServers,
                                ActionType.REPLICA_MOVEMENT,
                                optimizedGoals);
                // Only need to check status if the action is taken. This will also handle the case
                // that the source server has nothing to move in. In that case we will never
                // re-enqueue that source server.
                if (b != null) {
                    if (aliveDestServer.numReplicas() >= rebalanceLowerLimit) {
                        // Note that the server passed to this method is always alive; hence, there
                        // is no need to check if it is dead.
                        return false;
                    }

                    if (!eligibleServers.isEmpty()) {
                        // If the source server has a lower number of replicas than the next server
                        // in the eligible server in the queue, we re-enqueue the source server and
                        // switch to the next server.
                        // TODO there maybe use source > eligibleServers.peek() to re-enqueue
                        if (sourceServer.numReplicas() < eligibleServers.peek().numReplicas()) {
                            eligibleServers.add(sourceServer);
                            break;
                        }
                    }
                }
            }
        }
        return true;
    }

    private class ReplicaDistributionGoalStatsComparator implements ClusterModelStatsComparator {
        private String reasonForLastNegativeResult;

        @Override
        public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
            // Standard deviation of number of replicas over servers not excluded for replica moves
            // must be less than the
            // pre-optimized stats.
            double stDev1 = stats1.replicaStats().get(StatisticType.ST_DEV).doubleValue();
            double stDev2 = stats2.replicaStats().get(StatisticType.ST_DEV).doubleValue();
            int result = MathUtils.compare(stDev2, stDev1, EPSILON);
            if (result < 0) {
                reasonForLastNegativeResult =
                        String.format(
                                "Violated %s. [Std Deviation of Replica Distribution] post-"
                                        + "optimization:%.3f pre-optimization:%.3f",
                                name(), stDev1, stDev2);
            }
            return result;
        }

        @Override
        public String explainLastComparison() {
            return reasonForLastNegativeResult;
        }
    }
}
