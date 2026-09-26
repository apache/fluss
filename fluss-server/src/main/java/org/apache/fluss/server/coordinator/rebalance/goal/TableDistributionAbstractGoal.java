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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalUtils.aliveServers;

/** An abstract base for goals that distribute replicas independently for each table. */
public abstract class TableDistributionAbstractGoal extends AbstractGoal {
    private static final Logger LOG = LoggerFactory.getLogger(TableDistributionAbstractGoal.class);
    private static final double BALANCE_MARGIN = 0.9;
    private static final double REBALANCE_THRESHOLD = 1.10d;

    protected final Set<Long> tablesAboveRebalanceUpperLimit;
    protected final Set<Long> tablesBelowRebalanceLowerLimit;
    protected final Map<Long, Integer> rebalanceUpperLimitByTable;
    protected final Map<Long, Integer> rebalanceLowerLimitByTable;
    protected Set<Integer> aliveServers;

    protected TableDistributionAbstractGoal() {
        tablesAboveRebalanceUpperLimit = new HashSet<>();
        tablesBelowRebalanceLowerLimit = new HashSet<>();
        rebalanceUpperLimitByTable = new HashMap<>();
        rebalanceLowerLimitByTable = new HashMap<>();
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
        rebalanceUpperLimitByTable.clear();
        rebalanceLowerLimitByTable.clear();
        Map<Long, Integer> numInterestedReplicasByTable =
                numInterestedReplicasByTable(clusterModel);
        for (Long tableId : clusterModel.tables()) {
            double average =
                    numInterestedReplicasByTable.getOrDefault(tableId, 0)
                            / (double) aliveServers.size();
            rebalanceUpperLimitByTable.put(tableId, rebalanceUpperLimit(average));
            rebalanceLowerLimitByTable.put(tableId, rebalanceLowerLimit(average));
        }
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, RebalancingAction action) {
        return actionAcceptance(action, clusterModel) == ACCEPT;
    }

    @Override
    protected void updateGoalState(ClusterModel clusterModel) {
        if (!tablesAboveRebalanceUpperLimit.isEmpty()) {
            LOG.debug(
                    "Tables {} remain above their rebalance upper limits after {}.",
                    tablesAboveRebalanceUpperLimit,
                    name());
            tablesAboveRebalanceUpperLimit.clear();
            succeeded = false;
        }
        if (!tablesBelowRebalanceLowerLimit.isEmpty()) {
            LOG.debug(
                    "Tables {} remain below their rebalance lower limits after {}.",
                    tablesBelowRebalanceLowerLimit,
                    name());
            tablesBelowRebalanceLowerLimit.clear();
            succeeded = false;
        }
        finish();
    }

    protected int upperLimit(long tableId, ServerModel server) {
        return server.isOfflineTagged() ? 0 : rebalanceUpperLimitByTable.get(tableId);
    }

    protected int lowerLimit(long tableId, ServerModel server) {
        return server.isOfflineTagged() ? 0 : rebalanceLowerLimitByTable.get(tableId);
    }

    protected boolean isAlive(ServerModel server) {
        return aliveServers.contains(server.id());
    }

    private int rebalanceUpperLimit(double average) {
        return (int) Math.ceil(average * (1 + adjustedRebalancePercentage()));
    }

    private int rebalanceLowerLimit(double average) {
        return (int) Math.floor(average * Math.max(0, 1 - adjustedRebalancePercentage()));
    }

    private double adjustedRebalancePercentage() {
        return (REBALANCE_THRESHOLD - 1) * BALANCE_MARGIN;
    }

    /**
     * Returns the number of replicas relevant to this goal for every table of the cluster, keyed by
     * table id. The counts of all tables are retrieved at once so that computing the balance limits
     * does not rescan the buckets of the cluster once per table.
     */
    abstract Map<Long, Integer> numInterestedReplicasByTable(ClusterModel clusterModel);
}
