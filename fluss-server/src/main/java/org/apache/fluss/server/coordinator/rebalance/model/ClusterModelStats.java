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

package org.apache.fluss.server.coordinator.rebalance.model;

import org.apache.fluss.annotation.VisibleForTesting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

/** A class that holds the statistics of the cluster for rebalance. */
public class ClusterModelStats {
    private final Map<StatisticType, Number> replicaStats;
    private final Map<StatisticType, Number> leaderReplicaStats;
    private int numServers;
    private int numReplicasInCluster;

    public ClusterModelStats() {
        replicaStats = new HashMap<>();
        leaderReplicaStats = new HashMap<>();

        numServers = 0;
        numReplicasInCluster = 0;
    }

    ClusterModelStats populate(ClusterModel clusterModel) {
        final SortedSet<ServerModel> servers = clusterModel.servers();
        final Set<ServerModel> aliveServers = clusterModel.aliveServers();
        this.numServers = servers.size();
        numForReplicas(clusterModel, servers, aliveServers);
        numForLeaderReplicas(servers, aliveServers);
        return this;
    }

    /** Generate statistics for replicas in the given cluster. */
    private void numForReplicas(
            ClusterModel clusterModel,
            SortedSet<ServerModel> servers,
            Set<ServerModel> aliveServers) {
        populateReplicaStats(ServerModel::numReplicas, replicaStats, servers, aliveServers);
        numReplicasInCluster = clusterModel.numReplicas();
    }

    /** Generate statistics for leader replicas in the given cluster. */
    private void numForLeaderReplicas(
            SortedSet<ServerModel> servers, Set<ServerModel> aliveServers) {
        populateReplicaStats(
                ServerModel::numLeaderReplicas, leaderReplicaStats, servers, aliveServers);
    }

    private void populateReplicaStats(
            Function<ServerModel, Integer> numInterestedReplicasFunc,
            Map<StatisticType, Number> interestedReplicaStats,
            SortedSet<ServerModel> servers,
            Set<ServerModel> aliveServers) {
        // Average, minimum, and maximum number of replicas of interest in servers.
        int maxInterestedReplicasInServer = 0;
        int minInterestedReplicasInServer = Integer.MAX_VALUE;
        int numInterestedReplicasInCluster = 0;
        for (ServerModel server : servers) {
            int numInterestedReplicasInServer = numInterestedReplicasFunc.apply(server);
            numInterestedReplicasInCluster += numInterestedReplicasInServer;
            maxInterestedReplicasInServer =
                    Math.max(maxInterestedReplicasInServer, numInterestedReplicasInServer);
            minInterestedReplicasInServer =
                    Math.min(minInterestedReplicasInServer, numInterestedReplicasInServer);
        }
        double avgInterestedReplicas =
                ((double) numInterestedReplicasInCluster) / aliveServers.size();

        // Standard deviation of replicas of interest in alive servers.
        double variance = 0.0;
        for (ServerModel broker : aliveServers) {
            variance +=
                    (Math.pow(
                                    (double) numInterestedReplicasFunc.apply(broker)
                                            - avgInterestedReplicas,
                                    2)
                            / aliveServers.size());
        }

        interestedReplicaStats.put(StatisticType.AVG, avgInterestedReplicas);
        interestedReplicaStats.put(StatisticType.MAX, maxInterestedReplicasInServer);
        interestedReplicaStats.put(StatisticType.MIN, minInterestedReplicasInServer);
        interestedReplicaStats.put(StatisticType.ST_DEV, Math.sqrt(variance));
    }

    public Map<StatisticType, Number> replicaStats() {
        return Collections.unmodifiableMap(replicaStats);
    }

    public Map<StatisticType, Number> leaderReplicaStats() {
        return Collections.unmodifiableMap(leaderReplicaStats);
    }

    @VisibleForTesting
    public int numServers() {
        return numServers;
    }

    @VisibleForTesting
    public int numReplicasInCluster() {
        return numReplicasInCluster;
    }
}
