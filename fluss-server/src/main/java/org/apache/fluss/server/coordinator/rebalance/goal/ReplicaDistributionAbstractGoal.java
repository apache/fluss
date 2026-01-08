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
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalUtils.aliveServers;

/** An abstract class for goals that are based on the distribution of replicas. */
public abstract class ReplicaDistributionAbstractGoal extends AbstractGoal {
    private static final Logger LOG =
            LoggerFactory.getLogger(ReplicaDistributionAbstractGoal.class);
    private static final double BALANCE_MARGIN = 0.9;
    protected final Set<Integer> serverIdsAboveRebalanceUpperLimit;
    protected final Set<Integer> serverIdsBelowRebalanceLowerLimit;
    protected double avgReplicasOnAliveServer;
    protected int rebalanceUpperLimit;
    protected int rebalanceLowerLimit;
    // This is used to identify servers not excluded for replica moves.
    protected Set<Integer> aliveServers;

    public ReplicaDistributionAbstractGoal() {
        serverIdsAboveRebalanceUpperLimit = new HashSet<>();
        serverIdsBelowRebalanceLowerLimit = new HashSet<>();
    }

    private int rebalanceUpperLimit(double balancePercentage) {
        return (int)
                Math.ceil(
                        avgReplicasOnAliveServer
                                * (1 + adjustedRebalancePercentage(balancePercentage)));
    }

    private int rebalanceLowerLimit(double balancePercentage) {
        return (int)
                Math.floor(
                        avgReplicasOnAliveServer
                                * Math.max(
                                        0, (1 - adjustedRebalancePercentage(balancePercentage))));
    }

    private double adjustedRebalancePercentage(double rebalancePercentage) {
        return (rebalancePercentage - 1) * BALANCE_MARGIN;
    }

    boolean isReplicaCountUnderBalanceUpperLimitAfterChange(
            ServerModel server, int currentReplicaCount) {
        int serverBalanceUpperLimit = server.isOfflineTagged() ? 0 : rebalanceUpperLimit;
        return currentReplicaCount + 1 <= serverBalanceUpperLimit;
    }

    boolean isReplicaCountAboveBalanceLowerLimitAfterChange(
            ServerModel server, int currentReplicaCount) {
        int serverBalanceLowerLimit = server.isOfflineTagged() ? 0 : rebalanceLowerLimit;
        return currentReplicaCount - 1 >= serverBalanceLowerLimit;
    }

    @Override
    protected void initGoalState(ClusterModel clusterModel) throws RebalanceFailureException {
        aliveServers = aliveServers(clusterModel);
        if (aliveServers.isEmpty()) {
            throw new RebalanceFailureException(
                    String.format(
                            "[%s] All alive tabletServers are excluded from replica moves.",
                            name()));
        }

        // Initialize the average replicas on an alive server.
        avgReplicasOnAliveServer =
                numInterestedReplicas(clusterModel) / (double) aliveServers.size();

        rebalanceUpperLimit = rebalanceUpperLimit(balancePercentage());
        rebalanceLowerLimit = rebalanceLowerLimit(balancePercentage());
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, RebalancingAction action) {
        // Check that destination and source would not become unbalanced.
        return actionAcceptance(action, clusterModel) == ACCEPT;
    }

    @Override
    protected void updateGoalState(ClusterModel clusterModel) throws RebalanceFailureException {
        if (!serverIdsAboveRebalanceUpperLimit.isEmpty()) {
            LOG.debug(
                    "Replicas count on server ids:{} {} above the balance limit of {} after rebalance.",
                    serverIdsAboveRebalanceUpperLimit,
                    (serverIdsAboveRebalanceUpperLimit.size() > 1) ? "are" : "is",
                    rebalanceUpperLimit);
            serverIdsAboveRebalanceUpperLimit.clear();
            succeeded = false;
        }

        if (!serverIdsBelowRebalanceLowerLimit.isEmpty()) {
            LOG.debug(
                    "Replicas count on server ids:{} {} below the balance limit of {} after rebalance.",
                    serverIdsBelowRebalanceLowerLimit,
                    (serverIdsBelowRebalanceLowerLimit.size() > 1) ? "are" : "is",
                    rebalanceLowerLimit);
            serverIdsBelowRebalanceLowerLimit.clear();
            succeeded = false;
        }

        // TODO maybe need check offline server.

        finish();
    }

    abstract int numInterestedReplicas(ClusterModel clusterModel);

    /**
     * @return The requested balance threshold.
     */
    abstract double balancePercentage();

    protected boolean isAlive(ServerModel server) {
        return aliveServers.contains(server.id());
    }

    /** Whether bring replica in or out. */
    protected enum ChangeType {
        ADD,
        REMOVE
    }
}
