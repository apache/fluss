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
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.Collections.max;
import static java.util.Collections.min;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalUtils.aliveServers;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Generate replica movement proposals to evenly distribute replicas over alive racks not excluded
 * for replica moves.
 *
 * <p>This is a relaxed version of {@link RackAwareGoal}. Contrary to {@link RackAwareGoal}, as long
 * as replicas of each bucket can achieve a perfectly even distribution across the racks, this goal
 * lets placement of multiple replicas of a bucket into a single rack.
 *
 * <p>For example, suppose a table with 1 bucket has 4 replicas in a cluster with 2 racks. Then the
 * following distribution will be acceptable by this goal (but would be unacceptable by {@link
 * RackAwareGoal}):
 *
 * <pre>
 * Rack A                    | Rack B
 * -----------------------------------------------------
 * Server1-rack A  replica-0 | Server2-rack B
 * Server3-rack A            | Server4-rack B replica-1
 * Server5-rack A  replica-2 | Server6-rack B
 * Server7-rack A            | Server8-rack B replica-3
 * </pre>
 *
 * <p>However, this goal will yield an {@link RebalanceFailureException} for the same bucket in the
 * following cluster due to the lack of a second server to place a replica of this bucket in {@code
 * Rack B}:
 *
 * <pre>
 * Rack A                    | Rack B
 * -----------------------------------------------------
 * Server1-rack A  replica-0 | Server2-rack B replica-1
 * Server3-rack A  replica-3 |
 * Server5-rack A  replica-2 |
 * </pre>
 */
public class RackAwareDistributionGoal extends RackAwareAbstractGoal {

    private RackRebalanceLimit rackRebalanceLimit;
    private Set<Integer> serversAllowedReplicaMoves;

    @Override
    protected boolean doesReplicaMoveViolateActionAcceptance(
            ClusterModel clusterModel, ReplicaModel sourceReplica, ServerModel destServer) {
        String destinationRackId = destServer.rack();
        String sourceRackId = sourceReplica.server().rack();

        if (sourceRackId.equals(destinationRackId)) {
            // A replica move within the same rack cannot violate rack aware distribution.
            return false;
        }

        // The replica move shall not increase the replica distribution imbalance of the bucket
        // across racks.
        BucketModel bucket = clusterModel.bucket(sourceReplica.tableBucket());
        checkNotNull(bucket, "Bucket for replica " + sourceReplica + " is not found");
        Set<ServerModel> bucketServers = bucket.bucketServers();
        Map<String, Integer> numReplicasByRack = numBucketReplicasByRackId(bucketServers);

        // Once this goal is optimized, it is guaranteed to have 0 replicas on servers excluded for
        // replica moves.
        // Hence, no explicit check is necessary for verifying the replica source.
        return numReplicasByRack.getOrDefault(destinationRackId, 0)
                >= numReplicasByRack.getOrDefault(sourceRackId, 0);
    }

    @Override
    protected void initGoalState(ClusterModel clusterModel) throws RebalanceFailureException {
        serversAllowedReplicaMoves = aliveServers(clusterModel);
        if (serversAllowedReplicaMoves.isEmpty()) {
            throw new RebalanceFailureException(
                    String.format(
                            "[%s] All alive tabletServers are excluded from replica moves.",
                            name()));
        }

        rackRebalanceLimit =
                new RackRebalanceLimit(
                        clusterModel, clusterModel.racksContainServerWithoutOfflineTag().size());
    }

    /**
     * Update goal state. Sanity check: After completion of balancing, confirm that replicas of each
     * bucket are evenly distributed across the racks.
     *
     * @param clusterModel The state of the cluster.
     */
    @Override
    protected void updateGoalState(ClusterModel clusterModel) throws RebalanceFailureException {
        // One pass is sufficient to satisfy or alert impossibility of this goal.
        // Sanity check to confirm that replicas of each bucket are evenly distributed across the
        // racks
        ensureRackAwareDistribution(clusterModel);
        finish();
    }

    @Override
    public void finish() {
        super.finish();
        // Clean up the memory
        rackRebalanceLimit.clear();
    }

    /**
     * Violations of rack-aware distribution can be resolved with replica movements.
     *
     * @param server Server to be balanced.
     * @param clusterModel The state of the cluster.
     * @param optimizedGoals Optimized goals.
     */
    @Override
    protected void rebalanceForServer(
            ServerModel server, ClusterModel clusterModel, Set<Goal> optimizedGoals)
            throws RebalanceFailureException {
        rebalanceForServer(server, clusterModel, optimizedGoals, false);
    }

    /**
     * Get a list of eligible servers for moving the given replica in the given cluster to satisfy
     * the specific requirements of the rack aware goal. A server is rack aware distribution
     * eligible for a given replica if it is in a rack
     *
     * <ul>
     *   <li>that needs more replicas -- i.e. has fewer than base limit replicas
     *   <li>with at most as many as base limit replicas, and the number of racks with at least one
     *       more replica over the base limit is fewer than the allowed number of such racks
     * </ul>
     *
     * @param replica Replica for which a set of rack aware eligible servers are requested.
     * @param clusterModel The state of the cluster.
     * @return A list of rack aware eligible servers for the given replica in the given cluster.
     */
    @Override
    protected SortedSet<ServerModel> rackAwareEligibleServers(
            ReplicaModel replica, ClusterModel clusterModel) {
        return rackAwareEligibleServers(replica, clusterModel, Collections.emptyList());
    }

    /**
     * The same as {@link #rackAwareEligibleServers(ReplicaModel replica, ClusterModel
     * clusterModel)}, except that this accepts an extra comparator to sort after the original
     * rack-aware sort is performed. The method is intended to be used as a tool for
     * extension/customization.
     *
     * <p>For example, after the method sorts by number of replicas in the rack, if a customization
     * that "if possible, place it in zone-of-racks that has the least replica", one can try to
     * provide {@code Collections.singletonList(Comparator.comparingInt((Server b) ->
     * SomeZoneUtils.zoneOfRack(b.rack()).totalReplicas()))}
     *
     * @param replica Replica for which a set of rack aware eligible servers are requested.
     * @param clusterModel The state of the cluster.
     * @param extraSoftServerPriorityComparators List of comparators to sort eligible servers, after
     *     comparing the number of replicas in rack
     * @return A list of rack aware eligible servers for the given replica in the given cluster.
     */
    private SortedSet<ServerModel> rackAwareEligibleServers(
            ReplicaModel replica,
            ClusterModel clusterModel,
            List<Comparator<ServerModel>> extraSoftServerPriorityComparators) {
        BucketModel bucket = clusterModel.bucket(replica.tableBucket());
        checkNotNull(bucket, "Bucket for replica " + replica + " is not found");
        Set<ServerModel> bucketServers = bucket.bucketServers();
        Map<String, Integer> numReplicasByRack = numBucketReplicasByRackId(bucketServers);

        // Decrement the number of replicas in this rack.
        numReplicasByRack.merge(replica.server().rack(), -1, Integer::sum);

        int baseLimit = rackRebalanceLimit.baseLimitByRF(bucketServers.size());

        // A replica is allowed to be moved to a rack at the base limit only if the number of racks
        // with at least one more replica over the base limit is fewer than the allowed number of
        // such racks.
        // For example, suppose a bucket has 5 replicas in a cluster with 3 racks. In the ideal
        // distribution, 2 racks has 2 replicas and the other rack has 1 replica from the bucket.
        // Suppose that in the current distribution, Rack-1  has 1 offline and 1 online replica,
        // Rack-2 has 2 online replicas, and Rack-3 has 1 online replica. In this scenario,
        // we can place the offline replica to an alive broker in either Rack-1 or Rack-3, because
        // the cluster has only one rack with at least one more replica over the base limit and we
        // know that the ideal distribution allows 2 such racks.
        boolean canMoveToRacksAtBaseLimit = false;
        int numRacksWithOneMoreReplicaLimit =
                rackRebalanceLimit.numRacksWithOneMoreReplicaByRF(bucketServers.size());
        int numRacksWithAtLeastOneMoreReplica =
                (int) numReplicasByRack.values().stream().filter(r -> r > baseLimit).count();
        if (numRacksWithAtLeastOneMoreReplica < numRacksWithOneMoreReplicaLimit) {
            canMoveToRacksAtBaseLimit = true;
        }

        final Comparator<ServerModel> leastReplicasInRack =
                Comparator.comparingInt(
                        (ServerModel s) -> numReplicasByRack.getOrDefault(s.rack(), 0));
        final SortedSet<ServerModel> rackAwareDistributionEligibleServers =
                new TreeSet<>(
                        Stream.concat(
                                        // Prefer servers whose rack has fewer replicas from the
                                        // bucket first
                                        Stream.of(leastReplicasInRack),
                                        // Use the extra comparators as the sub-criteria for sorting
                                        extraSoftServerPriorityComparators.stream())
                                // Combine the comparators into one
                                .reduce(Comparator::thenComparing)
                                // While the stream won't be null, code check warns for direct
                                // `.get()` provide a default
                                .orElse(leastReplicasInRack)
                                // Compare serverID at the last
                                .thenComparingInt(ServerModel::id));

        for (ServerModel destServer : clusterModel.aliveServers()) {
            int numReplicasInThisRack = numReplicasByRack.getOrDefault(destServer.rack(), 0);
            if (numReplicasInThisRack < baseLimit
                    || (canMoveToRacksAtBaseLimit && numReplicasInThisRack == baseLimit)) {
                // Either the (1) destination rack needs more replicas or (2) the replica is allowed
                // to be moved to a rack at the base limit and the destination server is in a rack
                // at the base limit
                if (!bucketServers.contains(destServer)) {
                    rackAwareDistributionEligibleServers.add(destServer);
                }
            }
        }

        // Return eligible servers
        return rackAwareDistributionEligibleServers;
    }

    @Override
    protected boolean shouldKeepInTheCurrentServer(
            ReplicaModel replica, ClusterModel clusterModel) {
        if (isExcludedForReplicaMove(replica.server())) {
            // A replica in a server excluded for the replica moves must be relocated to another
            // server.
            return false;
        }
        // Rack aware distribution requires perfectly even distribution for replicas of each
        // bucket across the racks.
        // This permits placement of multiple replicas of a bucket into a single rack.
        BucketModel bucket = clusterModel.bucket(replica.tableBucket());
        checkNotNull(bucket, "Bucket for replica " + replica + " is not found");
        Set<ServerModel> bucketServers = bucket.bucketServers();
        int replicationFactor = bucketServers.size();
        Map<String, Integer> numReplicasByRack = numBucketReplicasByRackId(bucketServers);

        int baseLimit = rackRebalanceLimit.baseLimitByRF(replicationFactor);
        int numRacksWithOneMoreReplicaLimit =
                rackRebalanceLimit.numRacksWithOneMoreReplicaByRF(replicationFactor);
        int upperLimit = baseLimit + (numRacksWithOneMoreReplicaLimit == 0 ? 0 : 1);

        int numReplicasInThisRack = numReplicasByRack.get(replica.server().rack());
        if (numReplicasInThisRack <= baseLimit) {
            // Rack does not have extra replicas to give away
            return true;
        } else if (numReplicasInThisRack > upperLimit) {
            // The rack has extra replica(s) to give away
            return false;
        } else {
            // This is a rack either with an extra replica to give away or keep.
            int numRacksWithOneMoreReplica =
                    (int) numReplicasByRack.values().stream().filter(r -> r > baseLimit).count();
            // If the current number of racks with one more replica over the base limit are more
            // than the allowed number of such racks, then this rack has an extra replica to give
            // away, otherwise it keeps the replica.
            return numRacksWithOneMoreReplica <= numRacksWithOneMoreReplicaLimit;
        }
    }

    /**
     * Check whether the given server is excluded for replica moves. Such a server cannot receive
     * replicas, but can give them away.
     *
     * @param server Server to check for exclusion from replica moves.
     * @return {@code true} if the given server is excluded for replica moves, {@code false}
     *     otherwise.
     */
    private boolean isExcludedForReplicaMove(ServerModel server) {
        return !serversAllowedReplicaMoves.contains(server.id());
    }

    /**
     * Ensures that replicas of all buckets in the cluster satisfy rack-aware distribution.
     *
     * @param clusterModel A cluster model.
     */
    private void ensureRackAwareDistribution(ClusterModel clusterModel)
            throws RebalanceFailureException {
        for (ReplicaModel leader : clusterModel.leaderReplicas()) {
            BucketModel bucket = clusterModel.bucket(leader.tableBucket());
            checkNotNull(bucket, "Bucket for replica " + leader + " is not found");
            Set<ServerModel> bucketServers = bucket.bucketServers();
            Map<String, Integer> numReplicasByRack = numBucketReplicasByRackId(bucketServers);

            int maxNumReplicasInARack = max(numReplicasByRack.values());

            // Check if rack(s) have multiple replicas from the same bucket (i.e. otherwise the
            // distribution is rack-aware).
            if (maxNumReplicasInARack > 1) {
                // Check whether there are alive racks allowed replica moves with:
                // (1) no replicas despite having RF > number of alive racks allowed replica moves
                // (2) more replicas that they could have been placed into other racks.
                boolean someAliveRacksHaveNoReplicas =
                        numReplicasByRack.size() < rackRebalanceLimit.numAliveRacks();
                if (someAliveRacksHaveNoReplicas
                        || maxNumReplicasInARack - min(numReplicasByRack.values()) > 1) {
                    throw new RebalanceFailureException(
                            String.format(
                                    "[%s] Bucket %s is not rack-aware. Servers (%s) and replicas per rack (%s).",
                                    name(),
                                    leader.tableBucket(),
                                    bucketServers,
                                    numReplicasByRack));
                }
            }
        }
    }

    /**
     * Given the servers that host replicas of a bucket, retrieves a map containing number of
     * replicas by the id of the rack they reside in.
     *
     * @param bucketServers Servers that host replicas of some bucket
     * @return A map containing the number of replicas by rackId that these replicas reside in.
     */
    private Map<String, Integer> numBucketReplicasByRackId(Set<ServerModel> bucketServers) {
        Map<String, Integer> numBucketReplicasByRackId = new HashMap<>();
        for (ServerModel server : bucketServers) {
            numBucketReplicasByRackId.merge(server.rack(), 1, Integer::sum);
        }
        return numBucketReplicasByRackId;
    }

    /**
     * A wrapper to facilitate describing per-rack replica count limits for a bucket with the given
     * replication factor.
     *
     * <p>These limits are expressed in terms of the following:
     *
     * <ul>
     *   <li>{@link #baseLimitByRF}: The minimum number of replicas from the bucket with the given
     *       replication factor that each alive rack that is allowed replica moves must contain
     *   <li>{@link #numRacksWithOneMoreReplicaByRF}: The exact number of racks that are allowed
     *       replica moves must contain an additional replica (i.e. the base limit + 1) from the
     *       bucket with the given replication factor
     * </ul>
     *
     * <p>For example, for a given replication factor (RF), if the base limit is 1 and the number of
     * racks (that are allowed replica moves) with one more replica is 3, then 3 racks should have 2
     * replicas and each remaining rack should have 1 replica from the bucket with the given RF.
     */
    protected static class RackRebalanceLimit {
        private final int numAliveRacks;
        private final Map<Integer, Integer> baseLimitByRF;
        private final Map<Integer, Integer> numRacksWithOneMoreReplicaByRF;

        public RackRebalanceLimit(ClusterModel clusterModel, int numAliveRacks)
                throws RebalanceFailureException {
            this.numAliveRacks = numAliveRacks;
            if (this.numAliveRacks == 0) {
                // Handle the case when all alive racks are excluded from replica moves.
                throw new RebalanceFailureException(
                        "All alive racks are excluded from replica moves.");
            }
            int maxReplicationFactor = clusterModel.maxReplicationFactor();
            baseLimitByRF = new HashMap<>();
            numRacksWithOneMoreReplicaByRF = new HashMap<>();

            // Precompute the limits for each possible replication factor up to maximum replication
            // factor.
            for (int replicationFactor = 1;
                    replicationFactor <= maxReplicationFactor;
                    replicationFactor++) {
                int baseLimit = replicationFactor / this.numAliveRacks;
                baseLimitByRF.put(replicationFactor, baseLimit);
                numRacksWithOneMoreReplicaByRF.put(
                        replicationFactor, replicationFactor % this.numAliveRacks);
            }
        }

        public int numAliveRacks() {
            return numAliveRacks;
        }

        public Integer baseLimitByRF(int replicationFactor) {
            return baseLimitByRF.get(replicationFactor);
        }

        public Integer numRacksWithOneMoreReplicaByRF(int replicationFactor) {
            return numRacksWithOneMoreReplicaByRF.get(replicationFactor);
        }

        /** Clear balance limits. */
        public void clear() {
            baseLimitByRF.clear();
            numRacksWithOneMoreReplicaByRF.clear();
        }
    }
}
