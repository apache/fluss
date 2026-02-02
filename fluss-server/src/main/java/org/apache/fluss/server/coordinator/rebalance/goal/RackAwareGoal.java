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
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.RackModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Generate replica movement proposals to provide rack-aware replica distribution, which ensure that
 * all replicas of each bucket are assigned in a rack aware manner -- i.e. no more than one replica
 * of each bucket resides in the same rack.
 */
public class RackAwareGoal extends RackAwareAbstractGoal {

    @Override
    protected boolean doesReplicaMoveViolateActionAcceptance(
            ClusterModel clusterModel, ReplicaModel sourceReplica, ServerModel destServer) {
        // Destination server cannot be in a rack that violates rack awareness.
        BucketModel bucket = clusterModel.bucket(sourceReplica.tableBucket());
        checkNotNull(bucket, "Bucket for replica " + sourceReplica + " is not found");
        Set<ServerModel> bucketServers = bucket.bucketServers();
        bucketServers.remove(sourceReplica.server());

        // If destination server exists on any of the rack of other replicas, it violates the
        // rack-awareness
        return bucketServers.stream().map(ServerModel::rack).anyMatch(destServer.rack()::equals);
    }

    /**
     * This is a hard goal; hence, the proposals are not limited to dead server replicas in case of
     * self-healing. Sanity Check: There exists sufficient number of racks for achieving
     * rack-awareness.
     *
     * @param clusterModel The state of the cluster.
     */
    @Override
    protected void initGoalState(ClusterModel clusterModel) throws RebalanceFailureException {
        // Sanity Check: not enough racks to satisfy rack awareness.
        // Assumes number of racks doesn't exceed Integer.MAX_VALUE.
        int numAvailableRacks =
                (int)
                        clusterModel.racksContainServerWithoutOfflineTag().stream()
                                .map(RackModel::rack)
                                .distinct()
                                .count();
        if (clusterModel.maxReplicationFactor() > numAvailableRacks) {
            throw new RebalanceFailureException(
                    String.format(
                            "[%s] Insufficient number of racks to distribute each replica (Current: %d, Needed: %d).",
                            name(), numAvailableRacks, clusterModel.maxReplicationFactor()));
        }
    }

    /**
     * Update goal state. Sanity check: After completion of balancing, confirm that replicas of each
     * bucket reside at a separate rack.
     *
     * @param clusterModel The state of the cluster.
     */
    @Override
    protected void updateGoalState(ClusterModel clusterModel) throws RebalanceFailureException {
        finish();
    }

    /**
     * Get a list of rack aware eligible servers for the given replica in the given cluster. A
     * server is rack aware eligible for a given replica if the server resides in a rack where no
     * other server in the same rack contains a replica from the same bucket of the given replica.
     *
     * @param replica Replica for which a set of rack aware eligible servers are requested.
     * @param clusterModel The state of the cluster.
     * @return A list of rack aware eligible servers for the given replica in the given cluster.
     */
    @Override
    protected SortedSet<ServerModel> rackAwareEligibleServers(
            ReplicaModel replica, ClusterModel clusterModel) {
        // Populate bucket rackIds.
        BucketModel bucket = clusterModel.bucket(replica.tableBucket());
        checkNotNull(bucket, "Bucket for replica " + replica + " is not found");
        List<String> bucketRackIds =
                bucket.bucketServers().stream().map(ServerModel::rack).collect(Collectors.toList());

        // Remove rackId of the given replica, but if there is any other replica from the bucket
        // residing in the  same cluster, keep its rackId in the list.
        bucketRackIds.remove(replica.server().rack());

        SortedSet<ServerModel> rackAwareEligibleServers =
                new TreeSet<>(Comparator.comparingInt(ServerModel::id));
        for (ServerModel server : clusterModel.aliveServers()) {
            if (!bucketRackIds.contains(server.rack())) {
                rackAwareEligibleServers.add(server);
            }
        }
        // Return eligible servers.
        return rackAwareEligibleServers;
    }

    @Override
    protected boolean shouldKeepInTheCurrentServer(
            ReplicaModel replica, ClusterModel clusterModel) {
        // Rack awareness requires no more than one replica from a given bucket residing in any
        // rack in the cluster
        String myRackId = replica.server().rack();
        int myServerId = replica.serverId();
        BucketModel bucket = clusterModel.bucket(replica.tableBucket());
        checkNotNull(bucket, "Bucket for replica " + replica + " is not found");
        for (ServerModel bucketServer : bucket.bucketServers()) {
            if (myRackId.equals(bucketServer.rack()) && myServerId != bucketServer.id()) {
                return false;
            }
        }
        return true;
    }
}
