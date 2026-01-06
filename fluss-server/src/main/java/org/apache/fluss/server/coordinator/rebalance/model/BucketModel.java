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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A class that holds the information of the {@link TableBucket} for rebalance. */
public class BucketModel {
    private final TableBucket tableBucket;
    private final List<ReplicaModel> replicas;
    private @Nullable ReplicaModel leader;
    // Set of server which are unable to host replica of this replica (such as: the server are
    // offline).
    private final Set<ServerModel> ineligibleServers;

    public BucketModel(TableBucket tableBucket, Set<ServerModel> ineligibleServers) {
        this.tableBucket = tableBucket;
        this.replicas = new ArrayList<>();
        this.leader = null;
        this.ineligibleServers = ineligibleServers;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public @Nullable ReplicaModel leader() {
        return leader;
    }

    public List<ReplicaModel> replicas() {
        return replicas;
    }

    public Set<ServerModel> bucketServers() {
        Set<ServerModel> bucketServers = new HashSet<>();
        replicas.forEach(replica -> bucketServers.add(replica.server()));
        return bucketServers;
    }

    public boolean canAssignReplicaToServer(ServerModel candidateServer) {
        return !ineligibleServers.contains(candidateServer);
    }

    public ReplicaModel replica(long serverId) {
        for (ReplicaModel replica : replicas) {
            if (replica.server().id() == serverId) {
                return replica;
            }
        }

        throw new IllegalArgumentException(
                "Requested replica " + serverId + " is not a replica of bucket " + tableBucket);
    }

    public void addLeader(ReplicaModel leader, int index) {
        checkArgument(
                this.leader == null,
                String.format(
                        "Bucket %s already has a leader replica %s. Cannot add a new leader replica %s.",
                        tableBucket, this.leader, leader));

        checkArgument(
                leader.isLeader(),
                String.format(
                        "Inconsistent leadership information. Trying to set %s as the leader for bucket %s while "
                                + "the replica is not marked as a leader",
                        leader, tableBucket));

        this.leader = leader;
        replicas.add(index, leader);
    }

    public void addFollower(ReplicaModel follower, int index) {
        checkArgument(
                !follower.isLeader(),
                String.format(
                        "Inconsistent leadership information. Trying to set %s as the follower for bucket %s while "
                                + "the replica is marked as a leader",
                        follower, tableBucket));

        checkArgument(
                follower.tableBucket().equals(this.tableBucket),
                String.format(
                        "Inconsistent table bucket. Trying to add follower replica %s to tableBucket %s",
                        follower, tableBucket));

        // Add follower to list of followers
        replicas.add(index, follower);
    }

    void relocateLeadership(ReplicaModel prospectiveLeader) {
        int leaderPos = replicas.indexOf(prospectiveLeader);
        swapReplicaPositions(0, leaderPos);
        leader = prospectiveLeader;
    }

    private void swapReplicaPositions(int index1, int index2) {
        ReplicaModel replica1 = replicas.get(index1);
        ReplicaModel replica2 = replicas.get(index2);

        replicas.set(index2, replica1);
        replicas.set(index1, replica2);
    }
}
