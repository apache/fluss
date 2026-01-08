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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An util class for {@link GoalOptimizer}. */
public class GoalOptimizerUtils {

    /** Check whether the given proposal is acceptable for all the given optimized goals. */
    public static ActionAcceptance isProposalAcceptableForOptimizedGoals(
            Set<Goal> optimizedGoals, RebalancingAction action, ClusterModel cluster) {
        for (Goal goal : optimizedGoals) {
            ActionAcceptance acceptance = goal.actionAcceptance(action, cluster);
            if (acceptance != ACCEPT) {
                return acceptance;
            }
        }
        return ACCEPT;
    }

    /**
     * Get whether there is any diff represented by a set of rebalance plan to move from the initial
     * to final distribution.
     */
    public static boolean hasDiff(
            Map<TableBucket, List<Integer>> initialReplicaDistribution,
            Map<TableBucket, Integer> initialLeaderDistribution,
            ClusterModel optimizedCluster) {
        Map<TableBucket, List<Integer>> finalReplicaDistribution =
                optimizedCluster.getReplicaDistribution();
        sanityCheckReplicaDistribution(initialReplicaDistribution, finalReplicaDistribution);

        boolean hasDiff = false;
        for (Map.Entry<TableBucket, List<Integer>> entry : initialReplicaDistribution.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            List<Integer> initialReplicas = entry.getValue();
            List<Integer> finalReplicas = finalReplicaDistribution.get(tableBucket);

            if (!finalReplicas.equals(initialReplicas)) {
                hasDiff = true;
                break;
            } else {
                BucketModel bucket = optimizedCluster.bucket(tableBucket);
                checkNotNull(bucket, "Bucket is not in the cluster.");
                ReplicaModel finalLeaderReplica = bucket.leader();
                checkNotNull(finalLeaderReplica, "Leader replica is not in the bucket.");
                Integer finalLeader = finalLeaderReplica.server().id();
                if (!initialLeaderDistribution.get(tableBucket).equals(finalLeader)) {
                    hasDiff = true;
                    break;
                }
                // The bucket has no change.
            }
        }
        return hasDiff;
    }

    /**
     * Get the diff represented by the set of rebalance plan for bucket to move from initial to
     * final distribution.
     */
    public static List<RebalancePlanForBucket> getDiff(
            Map<TableBucket, List<Integer>> initialReplicaDistribution,
            Map<TableBucket, Integer> initialLeaderDistribution,
            ClusterModel optimizedCluster) {
        Map<TableBucket, List<Integer>> finalReplicaDistribution =
                optimizedCluster.getReplicaDistribution();
        sanityCheckReplicaDistribution(initialReplicaDistribution, finalReplicaDistribution);

        // Generate a set of rebalance plans to represent the diff between initial and final
        // distribution.
        List<RebalancePlanForBucket> diff = new ArrayList<>();
        for (Map.Entry<TableBucket, List<Integer>> entry : initialReplicaDistribution.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            List<Integer> initialReplicas = entry.getValue();
            List<Integer> finalReplicas = finalReplicaDistribution.get(tableBucket);
            BucketModel bucket = optimizedCluster.bucket(tableBucket);
            checkNotNull(bucket, "Bucket is not in the cluster.");
            ReplicaModel finalLeaderReplica = bucket.leader();
            checkNotNull(finalLeaderReplica, "Leader replica is not in the bucket.");
            int finalLeader = finalLeaderReplica.server().id();
            // The bucket has no change.
            if (finalReplicas.equals(initialReplicas)
                    && initialLeaderDistribution.get(tableBucket).equals(finalLeader)) {
                continue;
            }
            // We need to adjust the final server list order to ensure the final leader is the first
            // replica.
            if (finalLeader != finalReplicas.get(0)) {
                int leaderPos = finalReplicas.indexOf(finalLeader);
                finalReplicas.set(leaderPos, finalReplicas.get(0));
                finalReplicas.set(0, finalLeader);
            }
            diff.add(
                    new RebalancePlanForBucket(
                            tableBucket,
                            initialLeaderDistribution.get(tableBucket),
                            finalLeader,
                            initialReplicas,
                            finalReplicas));
        }
        return diff;
    }

    /**
     * Sanity check to ensure that initial and final replica distribution have exactly the same
     * buckets.
     */
    private static void sanityCheckReplicaDistribution(
            Map<TableBucket, List<Integer>> initialReplicaDistribution,
            Map<TableBucket, List<Integer>> finalReplicaDistribution) {
        // Sanity check to make sure that given distributions contain the same replicas.
        if (!initialReplicaDistribution.keySet().equals(finalReplicaDistribution.keySet())) {
            throw new IllegalArgumentException(
                    "Initial and final replica distributions do not contain the same buckets.");
        }
    }
}
