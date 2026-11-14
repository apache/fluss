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
    private final Map<Long, Double> replicaStdDevByTable;
    private final Map<Long, Double> leaderReplicaStdDevByTable;
    private int numServers;
    private int numReplicasInCluster;

    public ClusterModelStats() {
        replicaStats = new HashMap<>();
        leaderReplicaStats = new HashMap<>();
        replicaStdDevByTable = new HashMap<>();
        leaderReplicaStdDevByTable = new HashMap<>();
        numServers = 0;
        numReplicasInCluster = 0;
    }

    ClusterModelStats populate(ClusterModel clusterModel) {
        final SortedSet<ServerModel> servers = clusterModel.servers();
        final Set<ServerModel> aliveServers = clusterModel.aliveServers();
        this.numServers = servers.size();
        numForReplicas(clusterModel, servers, aliveServers);
        numForLeaderReplicas(servers, aliveServers);
        populateTableStdDev(clusterModel, aliveServers);
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

    /**
     * Generate the per-table standard deviation of replica and leader replica counts over alive
     * servers.
     *
     * <p>Mirrors {@link #populateReplicaStats}: the average of a table divides the count over all
     * servers by the number of alive servers, while the variance only covers alive servers. The
     * accumulation is sparse, it only walks the tables that a server actually hosts, and adds
     * {@code average^2} once for every alive server that hosts no replica of the table.
     */
    private void populateTableStdDev(ClusterModel clusterModel, Set<ServerModel> aliveServers) {
        int numAliveServers = aliveServers.size();
        Map<Long, Double> replicaAverages =
                averages(clusterModel.numReplicasByTable(), numAliveServers);
        Map<Long, Double> leaderAverages =
                averages(clusterModel.numLeaderReplicasByTable(), numAliveServers);
        Map<Long, Double> replicaSquaredDeviations = new HashMap<>();
        Map<Long, Double> leaderSquaredDeviations = new HashMap<>();
        Map<Long, Integer> replicaHostingServers = new HashMap<>();
        Map<Long, Integer> leaderHostingServers = new HashMap<>();

        for (ServerModel server : aliveServers) {
            for (Long tableId : server.tables()) {
                accumulate(
                        replicaSquaredDeviations,
                        replicaHostingServers,
                        tableId,
                        server.numReplicas(tableId),
                        replicaAverages.getOrDefault(tableId, 0.0));
                accumulate(
                        leaderSquaredDeviations,
                        leaderHostingServers,
                        tableId,
                        server.numLeaderReplicas(tableId),
                        leaderAverages.getOrDefault(tableId, 0.0));
            }
        }

        for (Long tableId : clusterModel.tables()) {
            replicaStdDevByTable.put(
                    tableId,
                    stdDev(
                            replicaSquaredDeviations,
                            replicaHostingServers,
                            tableId,
                            replicaAverages.getOrDefault(tableId, 0.0),
                            numAliveServers));
            leaderReplicaStdDevByTable.put(
                    tableId,
                    stdDev(
                            leaderSquaredDeviations,
                            leaderHostingServers,
                            tableId,
                            leaderAverages.getOrDefault(tableId, 0.0),
                            numAliveServers));
        }
    }

    private static Map<Long, Double> averages(
            Map<Long, Integer> countByTable, int numAliveServers) {
        Map<Long, Double> averages = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : countByTable.entrySet()) {
            averages.put(entry.getKey(), ((double) entry.getValue()) / numAliveServers);
        }
        return averages;
    }

    private static void accumulate(
            Map<Long, Double> squaredDeviations,
            Map<Long, Integer> hostingServers,
            long tableId,
            int count,
            double average) {
        if (count == 0) {
            // A server without any replica of the table is covered by the average^2 term below.
            return;
        }
        squaredDeviations.merge(tableId, Math.pow(count - average, 2), Double::sum);
        hostingServers.merge(tableId, 1, Integer::sum);
    }

    private static double stdDev(
            Map<Long, Double> squaredDeviations,
            Map<Long, Integer> hostingServers,
            long tableId,
            double average,
            int numAliveServers) {
        double squaredDeviation = squaredDeviations.getOrDefault(tableId, 0.0);
        int hosting = hostingServers.getOrDefault(tableId, 0);
        double variance =
                (squaredDeviation + (numAliveServers - hosting) * Math.pow(average, 2))
                        / numAliveServers;
        return Math.sqrt(variance);
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

    /** Returns the standard deviation of replica counts for each table. */
    public Map<Long, Double> replicaStdDevByTable() {
        return Collections.unmodifiableMap(replicaStdDevByTable);
    }

    /** Returns the standard deviation of leader replica counts for each table. */
    public Map<Long, Double> leaderReplicaStdDevByTable() {
        return Collections.unmodifiableMap(leaderReplicaStdDevByTable);
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
