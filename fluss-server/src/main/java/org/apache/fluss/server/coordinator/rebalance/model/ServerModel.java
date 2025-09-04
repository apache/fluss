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

/** A class that holds the information of the tabletServer for rebalance. */
public class ServerModel implements Comparable<ServerModel> {

    private final int serverId;
    private final boolean isAlive;
    private final String rack;
    private final Set<ReplicaModel> replicas;
    private final Set<ReplicaModel> leaderReplicas;
    /** A map for tracking (tableId) -> (BucketId -> replica) for none-partitioned table. */
    private final Map<Long, Map<Integer, ReplicaModel>> tableReplicas;

    /** A map for tracking (tableId, partitionId) -> (BucketId -> replica) for partitioned table. */
    private final Map<TablePartition, Map<Integer, ReplicaModel>> tablePartitionReplicas;

    public ServerModel(int serverId, String rack, boolean isAlive) {
        this.serverId = serverId;
        this.rack = rack;
        this.isAlive = isAlive;
        this.replicas = new HashSet<>();
        this.leaderReplicas = new HashSet<>();
        this.tableReplicas = new HashMap<>();
        this.tablePartitionReplicas = new HashMap<>();
    }

    public int id() {
        return serverId;
    }

    public String rack() {
        return rack;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public Set<ReplicaModel> replicas() {
        return new HashSet<>(replicas);
    }

    public Set<ReplicaModel> leaderReplicas() {
        return new HashSet<>(leaderReplicas);
    }

    public Set<Long> tables() {
        Set<Long> tables = new HashSet<>(tableReplicas.keySet());
        tablePartitionReplicas.keySet().forEach(t -> tables.add(t.getTableId()));
        return tables;
    }

    public void makeFollower(TableBucket tableBucket) {
        ReplicaModel replica = replica(tableBucket);
        if (replica != null) {
            replica.makeFollower();
            leaderReplicas.remove(replica);
        }
    }

    public void makeLeader(TableBucket tableBucket) {
        ReplicaModel replica = replica(tableBucket);
        if (replica != null) {
            replica.makeLeader();
            leaderReplicas.add(replica);
        }
    }

    public void putReplica(TableBucket tableBucket, ReplicaModel replica) {
        replicas.add(replica);
        replica.setServer(this);
        if (tableBucket.getPartitionId() != null) {
            TablePartition tablePartition =
                    new TablePartition(tableBucket.getTableId(), tableBucket.getPartitionId());
            tablePartitionReplicas
                    .computeIfAbsent(tablePartition, k -> new HashMap<>())
                    .put(tableBucket.getBucket(), replica);
        } else {
            tableReplicas
                    .computeIfAbsent(tableBucket.getTableId(), k -> new HashMap<>())
                    .put(tableBucket.getBucket(), replica);
        }

        if (replica.isLeader()) {
            leaderReplicas.add(replica);
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
        if (removedReplica != null) {
            replicas.remove(removedReplica);

            if (tableBucket.getPartitionId() != null) {
                TablePartition tablePartition =
                        new TablePartition(tableBucket.getTableId(), tableBucket.getPartitionId());
                Map<Integer, ReplicaModel> tablePartitionReplicas =
                        this.tablePartitionReplicas.get(tablePartition);
                if (tablePartitionReplicas != null) {
                    tablePartitionReplicas.remove(tableBucket.getBucket());

                    if (tablePartitionReplicas.isEmpty()) {
                        this.tablePartitionReplicas.remove(tablePartition);
                    }
                }
            } else {
                Map<Integer, ReplicaModel> tableReplicas =
                        this.tableReplicas.get(tableBucket.getTableId());
                if (tableReplicas != null) {
                    tableReplicas.remove(tableBucket.getBucket());

                    if (tableReplicas.isEmpty()) {
                        this.tableReplicas.remove(tableBucket.getTableId());
                    }
                }
            }

            if (removedReplica.isLeader()) {
                leaderReplicas.remove(removedReplica);
            }
        }

        return removedReplica;
    }

    @Override
    public int compareTo(ServerModel o) {
        return Integer.compare(serverId, o.id());
    }

    @Override
    public String toString() {
        return String.format(
                "ServerModel[id=%s,rack=%s,isAlive=%s,replicaCount=%s]",
                serverId, rack, isAlive, replicas.size());
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
