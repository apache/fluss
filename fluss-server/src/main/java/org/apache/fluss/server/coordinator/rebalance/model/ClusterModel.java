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

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A class that holds the information of the cluster for rebalance.The information including live
 * tabletServers, bucket distribution, tabletServer tag etc.
 *
 * <p>Currently, the clusterModel can only be created by a rebalance request. It's used as the input
 * of the GoalOptimizer to generate the rebalance plan for load rebalance.
 */
public class ClusterModel {
    // TODO ClusterModel can be implemented in incremental mode, dynamically modified when there are
    // events such as table create, table delete, server offline, etc. Currently designed to read
    // coordinatorContext and generate it directly

    private final Map<String, RackModel> racksById;
    private final Map<Integer, RackModel> serverIdToRack;
    private final Set<ServerModel> aliveServers;
    private final SortedSet<ServerModel> offlineServers;
    private final SortedSet<ServerModel> servers;
    private final Map<TableBucket, BucketModel> bucketsByTableBucket;

    public ClusterModel(SortedSet<ServerModel> servers) {
        this.servers = servers;
        this.bucketsByTableBucket = new HashMap<>();

        this.aliveServers = new HashSet<>();
        this.offlineServers = new TreeSet<>();
        for (ServerModel serverModel : servers) {
            if (!serverModel.isOfflineTagged()) {
                aliveServers.add(serverModel);
            } else {
                offlineServers.add(serverModel);
            }
        }

        this.racksById = new HashMap<>();
        this.serverIdToRack = new HashMap<>();
        for (ServerModel serverModel : servers) {
            RackModel rackModel = racksById.computeIfAbsent(serverModel.rack(), RackModel::new);
            rackModel.addServer(serverModel);
            serverIdToRack.put(serverModel.id(), rackModel);
        }
    }

    public SortedSet<ServerModel> offlineServers() {
        return offlineServers;
    }

    public SortedSet<ServerModel> servers() {
        return servers;
    }

    public Set<ServerModel> aliveServers() {
        return Collections.unmodifiableSet(aliveServers);
    }

    public @Nullable BucketModel bucket(TableBucket tableBucket) {
        return bucketsByTableBucket.get(tableBucket);
    }

    public RackModel rack(String rack) {
        return racksById.get(rack);
    }

    public @Nullable ServerModel server(int serverId) {
        RackModel rack = serverIdToRack.get(serverId);
        return rack == null ? null : rack.server(serverId);
    }

    /** Populate the analysis stats with this cluster. */
    public ClusterModelStats getClusterStats() {
        return (new ClusterModelStats()).populate(this);
    }

    public int numReplicas() {
        return bucketsByTableBucket.values().stream().mapToInt(p -> p.replicas().size()).sum();
    }

    public int numLeaderReplicas() {
        int numLeaderReplicas = 0;
        for (BucketModel bucket : bucketsByTableBucket.values()) {
            numLeaderReplicas += bucket.leader() != null ? 1 : 0;
        }
        return numLeaderReplicas;
    }

    public SortedMap<Long, List<BucketModel>> getBucketsByTable() {
        SortedMap<Long, List<BucketModel>> bucketsByTable = new TreeMap<>();
        for (Long tableId : tables()) {
            bucketsByTable.put(tableId, new ArrayList<>());
        }
        for (Map.Entry<TableBucket, BucketModel> entry : bucketsByTableBucket.entrySet()) {
            bucketsByTable.get(entry.getKey().getTableId()).add(entry.getValue());
        }
        return bucketsByTable;
    }

    public Set<Long> tables() {
        Set<Long> tables = new HashSet<>();

        for (RackModel rack : racksById.values()) {
            tables.addAll(rack.tables());
        }
        return tables;
    }

    /**
     * Get the distribution of replicas in the cluster at the point of call.
     *
     * @return A map from tableBucket to the list of replicas. the first element is the leader, the
     *     rest are followers.
     */
    public Map<TableBucket, List<Integer>> getReplicaDistribution() {
        Map<TableBucket, List<Integer>> replicaDistribution = new HashMap<>();
        for (Map.Entry<TableBucket, BucketModel> entry : bucketsByTableBucket.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            BucketModel bucket = entry.getValue();
            List<Integer> replicaIds =
                    bucket.replicas().stream()
                            .map(r -> r.server().id())
                            .collect(Collectors.toList());
            replicaDistribution.put(tableBucket, replicaIds);
        }
        return replicaDistribution;
    }

    public Map<TableBucket, Integer> getLeaderDistribution() {
        Map<TableBucket, Integer> leaderDistribution = new HashMap<>();
        for (Map.Entry<TableBucket, BucketModel> entry : bucketsByTableBucket.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            BucketModel bucket = entry.getValue();

            ReplicaModel replicaModel = bucket.leader();
            if (replicaModel == null) {
                continue;
            }

            leaderDistribution.put(tableBucket, replicaModel.server().id());
        }
        return leaderDistribution;
    }

    public void createReplica(int serverId, TableBucket tableBucket, int index, boolean isLeader) {
        ServerModel server = server(serverId);
        if (server == null) {
            throw new IllegalArgumentException("Server is not in the cluster.");
        }

        ReplicaModel replica = new ReplicaModel(tableBucket, server, isLeader);
        server.putReplica(tableBucket, replica);

        if (!bucketsByTableBucket.containsKey(tableBucket)) {
            bucketsByTableBucket.put(tableBucket, new BucketModel(tableBucket, offlineServers()));
        }

        BucketModel bucket = bucketsByTableBucket.get(tableBucket);
        if (isLeader) {
            bucket.addLeader(replica, index);
        } else {
            bucket.addFollower(replica, index);
        }
    }

    /**
     * Relocate leadership from source server to destination server.
     *
     * <ul>
     *   <li>1. Removes leadership from source replica.
     *   <li>2. Adds this leadership to the destination replica.
     *   <li>3. Updates the leader and list of followers of the bucket.
     * </ul>
     */
    public boolean relocateLeadership(
            TableBucket tableBucket, int sourceServerId, int desServerId) {
        // Sanity check to see if the source replica is the leader.
        BucketModel bucket = bucketsByTableBucket.get(tableBucket);
        ReplicaModel sourceReplica = bucket.replica(sourceServerId);
        if (!sourceReplica.isLeader()) {
            return false;
        }

        // Sanity check to see if the destination replica is a follower.
        ReplicaModel desReplica = bucket.replica(desServerId);
        if (desReplica.isLeader()) {
            throw new IllegalArgumentException(
                    "Cannot relocate leadership of bucket "
                            + tableBucket
                            + " from server "
                            + sourceServerId
                            + " to server "
                            + desServerId
                            + " because the destination replica is a leader.");
        }

        ServerModel sourceServer = server(sourceServerId);
        if (sourceServer == null) {
            throw new IllegalArgumentException("Source server is not in the cluster.");
        }
        sourceServer.makeFollower(tableBucket);

        ServerModel destServer = server(desServerId);
        if (destServer == null) {
            throw new IllegalArgumentException("Destination server is not in the cluster.");
        }
        destServer.makeLeader(tableBucket);

        // Update the leader and list of followers of the bucket.
        bucket.relocateLeadership(desReplica);
        return true;
    }

    /**
     * Relocate replica from source server to destination server.
     *
     * <ul>
     *   <li>1. Removes the replica from source server.
     *   <li>2. Set the server of the removed replica as the dest server
     *   <li>3. Add this replica to the dest server.
     * </ul>
     */
    public void relocateReplica(TableBucket tableBucket, int sourceServerId, int destServerId) {
        // Removes the replica from the source server.
        ReplicaModel replica = removeReplica(sourceServerId, tableBucket);
        if (replica == null) {
            throw new IllegalArgumentException("Replica is not in the cluster.");
        }

        // Updates the tabletServer of the removed replicas with dest server.
        replica.setServer(server(destServerId));

        // Add this replica back to destination rack and server.
        String rack = replica.server().rack();
        rack(rack).addReplica(replica);
    }

    private @Nullable ReplicaModel removeReplica(int serverId, TableBucket tableBucket) {
        ServerModel server = server(serverId);
        if (server == null) {
            return null;
        }
        return server.removeReplica(tableBucket);
    }

    @Override
    public String toString() {
        return String.format(
                "ClusterModel[serverCount=%s,bucketCount=%s,aliveServerCount=%s]",
                servers.size(), bucketsByTableBucket.size(), aliveServers.size());
    }
}
