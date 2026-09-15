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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.goal.Goal;
import org.apache.fluss.server.coordinator.rebalance.goal.LeaderReplicaDistributionGoal;
import org.apache.fluss.server.coordinator.rebalance.goal.RackAwareGoal;
import org.apache.fluss.server.coordinator.rebalance.goal.TableLeaderReplicaDistributionGoal;
import org.apache.fluss.server.coordinator.rebalance.goal.TableReplicaDistributionGoal;
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Utilities shared by rebalance tests. */
public class RebalanceTestUtils {
    private static final double ADJUSTED_REBALANCE_PERCENTAGE = (1.10d - 1) * 0.9;

    public static void addBucket(
            ClusterModel clusterModel, TableBucket tb, List<Integer> replicas) {
        for (int i = 0; i < replicas.size(); i++) {
            clusterModel.createReplica(replicas.get(i), tb, i, i == 0);
        }
    }

    /** Captures the bucket-level state used to verify a rebalance result. */
    public static Map<TableBucket, List<Integer>> captureReplicaDistribution(
            ClusterModel clusterModel) {
        Map<TableBucket, List<Integer>> distribution = new HashMap<>();
        for (Map.Entry<TableBucket, List<Integer>> entry :
                clusterModel.getReplicaDistribution().entrySet()) {
            distribution.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return distribution;
    }

    /** Verifies bucket integrity and the contract of every emitted plan. */
    public static void assertRebalanceInvariants(
            Map<TableBucket, List<Integer>> originalDistribution,
            ClusterModel clusterModel,
            List<RebalancePlanForBucket> plans) {
        Map<TableBucket, List<Integer>> finalDistribution = clusterModel.getReplicaDistribution();
        assertThat(finalDistribution.keySet()).isEqualTo(originalDistribution.keySet());
        for (Map.Entry<TableBucket, List<Integer>> entry : originalDistribution.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            BucketModel bucket = clusterModel.bucket(tableBucket);
            assertThat(bucket).isNotNull();
            assertThat(bucket.replicas()).hasSize(entry.getValue().size());
            Set<Integer> replicaIds = new HashSet<>();
            int leaders = 0;
            for (ReplicaModel replica : bucket.replicas()) {
                assertThat(replicaIds.add(replica.server().id())).isTrue();
                if (replica.isLeader()) {
                    leaders++;
                    assertThat(bucket.leader()).isSameAs(replica);
                }
            }
            assertThat(leaders).isEqualTo(1);
            assertThat(bucket.leader()).isNotNull();
        }
        for (RebalancePlanForBucket plan : plans) {
            List<Integer> origin = plan.getOriginReplicas();
            List<Integer> updated = plan.getNewReplicas();
            assertThat(origin).hasSameSizeAs(updated);
            assertThat(new HashSet<>(origin)).hasSize(origin.size());
            assertThat(new HashSet<>(updated)).hasSize(updated.size());
            assertThat(origin.get(0)).isEqualTo(plan.getOriginalLeader());
            assertThat(updated.get(0)).isEqualTo(plan.getNewLeader());
            for (Integer serverId : updated) {
                assertThat(clusterModel.server(serverId).isOfflineTagged()).isFalse();
            }
            assertThat(origin).isNotEqualTo(updated);
        }
    }

    /** Asserts that a completed goal is preserved and an incomplete one is not made worse. */
    public static void assertGoalViolationNotRegressed(
            Goal goal, int violationAfterGoal, ClusterModel clusterModel) {
        int finalViolation = violation(goal, clusterModel);
        if (violationAfterGoal == 0) {
            assertThat(finalViolation).isZero();
        } else {
            assertThat(finalViolation).isLessThanOrEqualTo(violationAfterGoal);
        }
    }

    /** Calculates the goal-scoped violation metric used by goal-combination tests. */
    public static int violation(Goal goal, ClusterModel clusterModel) {
        if (goal instanceof RackAwareGoal) {
            int violations = 0;
            for (TableBucket tableBucket : clusterModel.getReplicaDistribution().keySet()) {
                Set<String> racks = new HashSet<>();
                for (ReplicaModel replica : clusterModel.bucket(tableBucket).replicas()) {
                    if (!racks.add(replica.server().rack())) {
                        violations++;
                        break;
                    }
                }
            }
            return violations;
        }
        boolean leaders =
                goal instanceof LeaderReplicaDistributionGoal
                        || goal instanceof TableLeaderReplicaDistributionGoal;
        boolean tableScoped =
                goal instanceof TableReplicaDistributionGoal
                        || goal instanceof TableLeaderReplicaDistributionGoal;
        int alive = clusterModel.aliveServers().size();
        int violations = 0;
        if (tableScoped) {
            for (Long tableId : clusterModel.tables()) {
                int total =
                        leaders
                                ? clusterModel.numLeaderReplicasByTable().get(tableId)
                                : clusterModel.numReplicasByTable().get(tableId);
                int lower = lowerLimit(total, alive);
                int upper = upperLimit(total, alive);
                for (ServerModel server : clusterModel.servers()) {
                    int count =
                            leaders
                                    ? server.numLeaderReplicas(tableId)
                                    : server.numReplicas(tableId);
                    violations +=
                            outsideWindow(
                                    count,
                                    server.isOfflineTagged() ? 0 : lower,
                                    server.isOfflineTagged() ? 0 : upper);
                }
            }
        } else {
            int total = leaders ? clusterModel.numLeaderReplicas() : clusterModel.numReplicas();
            int lower = lowerLimit(total, alive);
            int upper = upperLimit(total, alive);
            for (ServerModel server : clusterModel.servers()) {
                int count = leaders ? server.numLeaderReplicas() : server.numReplicas();
                violations +=
                        outsideWindow(
                                count,
                                server.isOfflineTagged() ? 0 : lower,
                                server.isOfflineTagged() ? 0 : upper);
            }
        }
        return violations;
    }

    private static int lowerLimit(int total, int aliveServers) {
        return (int)
                Math.floor(((double) total / aliveServers) * (1 - ADJUSTED_REBALANCE_PERCENTAGE));
    }

    private static int upperLimit(int total, int aliveServers) {
        return (int)
                Math.ceil(((double) total / aliveServers) * (1 + ADJUSTED_REBALANCE_PERCENTAGE));
    }

    private static int outsideWindow(int count, int lower, int upper) {
        return Math.max(lower - count, 0) + Math.max(count - upper, 0);
    }
}
