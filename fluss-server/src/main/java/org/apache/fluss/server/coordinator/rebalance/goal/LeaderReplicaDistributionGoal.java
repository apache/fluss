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
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
 * Soft goal to generate leadership movement and leader replica movement proposals to ensure that
 * the number of leader replicas on each server is.
 *
 * <ul>
 *   <li>Under: (the average number of leader replicas per server) * (1 + leader replica count
 *       balance percentage)
 *   <li>Above: (the average number of leader replicas per server) * Math.max(0, 1 - leader replica
 *       count balance percentage)
 * </ul>
 */
public class LeaderReplicaDistributionGoal extends ReplicaDistributionAbstractGoal {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderReplicaDistributionGoal.class);

    /**
     * The maximum allowed extent of unbalance for leader replica distribution. For example, 1.10
     * means the highest leader replica count of a server should not be 1.10x of average leader
     * replica count of all alive tabletServers.
     */
    private static final Double LEADER_REPLICA_COUNT_REBALANCE_THRESHOLD = 1.10d;

    @Override
    public ActionAcceptance actionAcceptance(RebalancingAction action, ClusterModel clusterModel) {
        ServerModel sourceServer = clusterModel.server(action.getSourceServerId());
        checkNotNull(
                sourceServer, "Source server " + action.getSourceServerId() + " is not found.");
        ReplicaModel sourceReplica = sourceServer.replica(action.getTableBucket());
        checkNotNull(sourceReplica, "Source replica " + action.getTableBucket() + " is not found.");
        ServerModel destServer = clusterModel.server(action.getDestinationServerId());
        switch (action.getActionType()) {
            case LEADERSHIP_MOVEMENT:
                return isLeaderMovementSatisfiable(sourceServer, destServer);
            case REPLICA_MOVEMENT:
                if (sourceReplica.isLeader()) {
                    return isLeaderMovementSatisfiable(sourceServer, destServer);
                }
                return ACCEPT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported action type " + action.getActionType());
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
        int numLeaderReplicas = server.numLeaderReplicas();
        boolean isAlive = isAlive(server);
        boolean requireLessLeaderReplicas =
                numLeaderReplicas > (isAlive ? rebalanceUpperLimit : 0) || server.isOfflineTagged();
        boolean requireMoreLeaderReplicas =
                isAlive && !server.isOfflineTagged() && numLeaderReplicas < rebalanceLowerLimit;
        // Update server ids over the balance limit for logging purposes.
        if (((requireLessLeaderReplicas
                        && rebalanceByMovingLeadershipOut(server, clusterModel, optimizedGoals)))
                && rebalanceByMovingReplicasOut(server, clusterModel, optimizedGoals)) {
            serverIdsAboveRebalanceUpperLimit.add(server.id());
            LOG.debug(
                    "Failed to sufficiently decrease leader replica count in server {}. Leader replicas: {}.",
                    server.id(),
                    server.numLeaderReplicas());
        } else if (requireMoreLeaderReplicas
                && rebalanceByMovingLeadershipIn(server, clusterModel, optimizedGoals)
                && rebalanceByMovingLeaderReplicasIn(server, clusterModel, optimizedGoals)) {
            serverIdsBelowRebalanceLowerLimit.add(server.id());
            LOG.debug(
                    "Failed to sufficiently increase leader replica count in server {}. Leader replicas: {}.",
                    server.id(),
                    server.numLeaderReplicas());
        }
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new LeaderReplicaDistributionGoalStatsComparator();
    }

    @Override
    int numInterestedReplicas(ClusterModel clusterModel) {
        return clusterModel.numLeaderReplicas();
    }

    @Override
    double balancePercentage() {
        return LEADER_REPLICA_COUNT_REBALANCE_THRESHOLD;
    }

    private ActionAcceptance isLeaderMovementSatisfiable(
            ServerModel sourceServer, ServerModel destServer) {
        return (isReplicaCountUnderBalanceUpperLimitAfterChange(
                                destServer, destServer.numLeaderReplicas())
                        && (!isAlive(sourceServer)
                                || isReplicaCountAboveBalanceLowerLimitAfterChange(
                                        sourceServer, sourceServer.numLeaderReplicas())))
                ? ACCEPT
                : REPLICA_REJECT;
    }

    private boolean rebalanceByMovingLeadershipOut(
            ServerModel server, ClusterModel cluster, Set<Goal> optimizedGoals) {
        // If the source server is excluded for replica move, set its upper limit to 0.
        int balanceUpperLimitForSourceServer = isAlive(server) ? rebalanceUpperLimit : 0;
        int numLeaderReplicas = server.numLeaderReplicas();
        for (ReplicaModel leader : new HashSet<>(server.leaderReplicas())) {
            BucketModel bucketModel = cluster.bucket(leader.tableBucket());
            checkNotNull(bucketModel, "Bucket " + leader.tableBucket() + " is not found.");
            Set<ServerModel> candidateServers =
                    bucketModel.bucketServers().stream()
                            .filter(b -> b != server)
                            .collect(Collectors.toSet());
            ServerModel b =
                    maybeApplyBalancingAction(
                            cluster,
                            leader,
                            candidateServers,
                            ActionType.LEADERSHIP_MOVEMENT,
                            optimizedGoals);
            // Only check if we successfully moved something.
            if (b != null) {
                if (--numLeaderReplicas <= balanceUpperLimitForSourceServer) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean rebalanceByMovingLeadershipIn(
            ServerModel server, ClusterModel cluster, Set<Goal> optimizedGoals) {
        int numLeaderReplicas = server.numLeaderReplicas();
        Set<ServerModel> candidateServers = Collections.singleton(server);
        for (ReplicaModel replica : server.replicas()) {
            if (replica.isLeader()) {
                continue;
            }

            BucketModel bucket = cluster.bucket(replica.tableBucket());
            checkNotNull(bucket, "Bucket " + replica.tableBucket() + " is not found.");
            ServerModel b =
                    maybeApplyBalancingAction(
                            cluster,
                            Objects.requireNonNull(bucket.leader()),
                            candidateServers,
                            ActionType.LEADERSHIP_MOVEMENT,
                            optimizedGoals);
            // Only check if we successfully moved something.
            if (b != null) {
                if (++numLeaderReplicas >= rebalanceLowerLimit) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean rebalanceByMovingReplicasOut(
            ServerModel server, ClusterModel cluster, Set<Goal> optimizedGoals) {
        // Get the eligible servers.
        SortedSet<ServerModel> candidateServers;
        candidateServers =
                new TreeSet<>(
                        Comparator.comparingInt(ServerModel::numLeaderReplicas)
                                .thenComparingInt(ServerModel::id));
        candidateServers.addAll(
                cluster.aliveServers().stream()
                        .filter(b -> b.numLeaderReplicas() < rebalanceUpperLimit)
                        .collect(Collectors.toSet()));

        int balanceUpperLimit = isAlive(server) ? rebalanceUpperLimit : 0;
        int numReplicas = server.numReplicas();
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
                if (--numReplicas <= balanceUpperLimit) {
                    return false;
                }
                // Remove and reinsert the server so the order is correct.
                candidateServers.remove(b);
                if (b.numLeaderReplicas() < rebalanceUpperLimit) {
                    candidateServers.add(b);
                }
            }
        }
        return true;
    }

    private boolean rebalanceByMovingLeaderReplicasIn(
            ServerModel server, ClusterModel clusterModel, Set<Goal> optimizedGoals) {
        PriorityQueue<ServerModel> eligibleServers =
                new PriorityQueue<>(
                        (b1, b2) -> {
                            int result =
                                    Integer.compare(b2.numLeaderReplicas(), b1.numLeaderReplicas());
                            return result == 0 ? Integer.compare(b1.id(), b2.id()) : result;
                        });

        for (ServerModel aliveServer : clusterModel.aliveServers()) {
            if (aliveServer.numLeaderReplicas() > rebalanceLowerLimit) {
                eligibleServers.add(aliveServer);
            }
        }
        List<ServerModel> candidateServers = Collections.singletonList(server);
        int numLeaderReplicas = server.numLeaderReplicas();
        while (!eligibleServers.isEmpty()) {
            ServerModel sourceServer = eligibleServers.poll();
            for (ReplicaModel replica : sourceServer.replicas()) {
                ServerModel b =
                        maybeApplyBalancingAction(
                                clusterModel,
                                replica,
                                candidateServers,
                                ActionType.REPLICA_MOVEMENT,
                                optimizedGoals);
                // Only need to check status if the action is taken. This will also handle the case
                // that the source server has nothing to move in. In that case we will never
                // reenqueue that source server.
                if (b != null) {
                    if (++numLeaderReplicas >= rebalanceLowerLimit) {
                        return false;
                    }
                    // If the source server has a lower number of replicas than the next
                    // server in the eligible server queue, we re-enqueue the source server and
                    // switch to the next server.
                    // TODO there maybe use source > eligibleServers.peek() to re-enqueue
                    if (!eligibleServers.isEmpty()
                            && sourceServer.numLeaderReplicas()
                                    < eligibleServers.peek().numLeaderReplicas()) {
                        eligibleServers.add(sourceServer);
                        break;
                    }
                }
            }
        }
        return true;
    }

    private class LeaderReplicaDistributionGoalStatsComparator
            implements ClusterModelStatsComparator {
        private String reasonForLastNegativeResult;

        @Override
        public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
            // Standard deviation of number of leader replicas over alive servers in the current
            // must be less than the pre-optimized stats.
            double stDev1 = stats1.leaderReplicaStats().get(StatisticType.ST_DEV).doubleValue();
            double stDev2 = stats2.leaderReplicaStats().get(StatisticType.ST_DEV).doubleValue();
            int result = MathUtils.compare(stDev2, stDev1, EPSILON);
            if (result < 0) {
                reasonForLastNegativeResult =
                        String.format(
                                "Violated %s. [Std Deviation of Leader Replica Distribution] post-"
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
