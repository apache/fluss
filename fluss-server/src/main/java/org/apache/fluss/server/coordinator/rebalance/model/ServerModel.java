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
import org.apache.fluss.metadata.TablePartition;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A class that holds the information of the tabletServer for rebalance. */
public class ServerModel implements Comparable<ServerModel> {

    private final int serverId;
    private final boolean isOfflineTagged;
    private final String rack;
    private final Set<ReplicaModel> replicas;
    /** A map for tracking (tableId) -> (BucketId -> replica) for none-partitioned table. */
    private final Map<Long, Map<Integer, ReplicaModel>> tableReplicas;

    /** A map for tracking (tableId, partitionId) -> (BucketId -> replica) for partitioned table. */
    private final Map<TablePartition, Map<Integer, ReplicaModel>> tablePartitionReplicas;

    /** A map for tracking all replicas of each table, including all partitions. */
    private final Map<Long, Set<ReplicaModel>> replicasByTable;

    /** A map for tracking the number of leaders of each table. */
    private final Map<Long, Integer> numLeaderReplicasByTable;

    private int numLeaderReplicas = 0;

    public ServerModel(int serverId, String rack, boolean isOfflineTagged) {
        this.serverId = serverId;
        this.rack = rack;
        this.isOfflineTagged = isOfflineTagged;
        this.replicas = new HashSet<>();
        this.tableReplicas = new HashMap<>();
        this.tablePartitionReplicas = new HashMap<>();
        this.replicasByTable = new HashMap<>();
        this.numLeaderReplicasByTable = new HashMap<>();
    }

    public int id() {
        return serverId;
    }

    public String rack() {
        return rack;
    }

    public boolean isOfflineTagged() {
        return isOfflineTagged;
    }

    public Set<ReplicaModel> replicas() {
        return new HashSet<>(replicas);
    }

    /** Returns replicas of the given table on this server, across all partitions. */
    public Set<ReplicaModel> replicas(long tableId) {
        Set<ReplicaModel> replicasOfTable = replicasByTable.get(tableId);
        return replicasOfTable == null ? new HashSet<>() : new HashSet<>(replicasOfTable);
    }

    public int numReplicas() {
        return replicas.size();
    }

    /** Returns the number of replicas of the given table on this server. */
    public int numReplicas(long tableId) {
        Set<ReplicaModel> replicasOfTable = replicasByTable.get(tableId);
        return replicasOfTable == null ? 0 : replicasOfTable.size();
    }

    public Set<ReplicaModel> leaderReplicas() {
        return replicas.stream().filter(ReplicaModel::isLeader).collect(Collectors.toSet());
    }

    /** Returns leaders of the given table on this server, across all partitions. */
    public Set<ReplicaModel> leaderReplicas(long tableId) {
        return replicas(tableId).stream()
                .filter(ReplicaModel::isLeader)
                .collect(Collectors.toSet());
    }

    public int numLeaderReplicas() {
        return numLeaderReplicas;
    }

    /** Returns the number of leader replicas of the given table on this server. */
    public int numLeaderReplicas(long tableId) {
        Integer numLeaders = numLeaderReplicasByTable.get(tableId);
        return numLeaders == null ? 0 : numLeaders;
    }

    public Set<Long> tables() {
        return new HashSet<>(replicasByTable.keySet());
    }

    public void makeFollower(TableBucket tableBucket) {
        ReplicaModel replica = replica(tableBucket);
        if (replica != null && replica.isLeader()) {
            numLeaderReplicas--;
            decrementLeaderReplica(tableBucket.getTableId());
            replica.makeFollower();
        }
    }

    public void makeLeader(TableBucket tableBucket) {
        ReplicaModel replica = replica(tableBucket);
        if (replica != null && !replica.isLeader()) {
            numLeaderReplicas++;
            incrementLeaderReplica(tableBucket.getTableId());
            replica.makeLeader();
        }
    }

    public void putReplica(TableBucket tableBucket, ReplicaModel replica) {
        checkState(
                replica(tableBucket) == null,
                "Replica of bucket %s already exists on server %s.",
                tableBucket,
                serverId);
        replicas.add(replica);
        replica.setServer(this);
        long tableId = tableBucket.getTableId();
        replicasByTable.computeIfAbsent(tableId, k -> new HashSet<>()).add(replica);
        if (replica.isLeader()) {
            numLeaderReplicas++;
            incrementLeaderReplica(tableId);
        }

        if (tableBucket.getPartitionId() != null) {
            TablePartition tablePartition =
                    new TablePartition(tableId, tableBucket.getPartitionId());
            tablePartitionReplicas
                    .computeIfAbsent(tablePartition, k -> new HashMap<>())
                    .put(tableBucket.getBucket(), replica);
        } else {
            tableReplicas
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .put(tableBucket.getBucket(), replica);
        }
    }

    public @Nullable ReplicaModel replica(TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            Map<Integer, ReplicaModel> replicas = tableReplicas.get(tableBucket.getTableId());
            if (replicas == null) {
                return null;
            }

            return replicas.get(tableBucket.getBucket());
        } else {
            TablePartition tablePartition =
                    new TablePartition(tableBucket.getTableId(), tableBucket.getPartitionId());
            Map<Integer, ReplicaModel> replicas = tablePartitionReplicas.get(tablePartition);
            if (replicas == null) {
                return null;
            }
            return replicas.get(tableBucket.getBucket());
        }
    }

    public @Nullable ReplicaModel removeReplica(TableBucket tableBucket) {
        ReplicaModel removedReplica = replica(tableBucket);
        if (removedReplica == null) {
            return null;
        }

        long tableId = tableBucket.getTableId();
        if (removedReplica.isLeader()) {
            numLeaderReplicas--;
            decrementLeaderReplica(tableId);
        }
        replicas.remove(removedReplica);
        Set<ReplicaModel> tableReplicaSet = replicasByTable.get(tableId);
        tableReplicaSet.remove(removedReplica);
        if (tableReplicaSet.isEmpty()) {
            replicasByTable.remove(tableId);
        }

        if (tableBucket.getPartitionId() != null) {
            TablePartition tablePartition =
                    new TablePartition(tableId, tableBucket.getPartitionId());
            Map<Integer, ReplicaModel> partitionReplicas =
                    tablePartitionReplicas.get(tablePartition);
            partitionReplicas.remove(tableBucket.getBucket());
            if (partitionReplicas.isEmpty()) {
                tablePartitionReplicas.remove(tablePartition);
            }
        } else {
            Map<Integer, ReplicaModel> nonPartitionedReplicas = tableReplicas.get(tableId);
            nonPartitionedReplicas.remove(tableBucket.getBucket());
            if (nonPartitionedReplicas.isEmpty()) {
                tableReplicas.remove(tableId);
            }
        }
        return removedReplica;
    }

    private void incrementLeaderReplica(long tableId) {
        numLeaderReplicasByTable.merge(tableId, 1, Integer::sum);
    }

    private void decrementLeaderReplica(long tableId) {
        int numLeaders = numLeaderReplicasByTable.get(tableId) - 1;
        if (numLeaders == 0) {
            numLeaderReplicasByTable.remove(tableId);
        } else {
            numLeaderReplicasByTable.put(tableId, numLeaders);
        }
    }

    @Override
    public int compareTo(ServerModel o) {
        return Integer.compare(serverId, o.id());
    }

    @Override
    public String toString() {
        return String.format(
                "ServerModel[id=%s,rack=%s,isOfflineTagged=%s,replicaCount=%s]",
                serverId, rack, isOfflineTagged, replicas.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerModel that = (ServerModel) o;
        return serverId == that.serverId;
    }

    @Override
    public int hashCode() {
        return serverId;
    }
}
