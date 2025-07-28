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

import org.apache.fluss.server.coordinator.rebalance.ActionAcceptance;
import org.apache.fluss.server.coordinator.rebalance.ReBalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModelStats;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import java.util.List;
import java.util.Set;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Soft goal to move the leaders to the first replica of each tableBucket. */
public class PreferredLeaderElectionGoal implements Goal {

    @Override
    public void optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals) {
        for (List<BucketModel> buckets : clusterModel.getBucketsByTable().values()) {
            for (BucketModel bucket : buckets) {
                for (int i = 0; i < bucket.replicas().size(); i++) {
                    // Only try to transfer the leadership to the first replica of the tabletBucket.
                    if (i > 0) {
                        break;
                    }
                    ReplicaModel r = bucket.replicas().get(i);
                    // Iterate over the replicas and ensure that (1) the leader is set to the first
                    // alive replica, and (2) the leadership is not transferred to a server excluded
                    // for leadership transfer.
                    ServerModel leaderCandidate = r.server();
                    ReplicaModel originLeader = bucket.leader();
                    checkNotNull(originLeader, "Leader replica is null.");
                    if (leaderCandidate.isAlive()) {
                        if (!r.isLeader()) {
                            clusterModel.relocateLeadership(
                                    r.tableBucket(),
                                    originLeader.server().id(),
                                    leaderCandidate.id());
                        }
                        break;
                    }
                }
            }
        }
    }

    @Override
    public ActionAcceptance actionAcceptance(ReBalancingAction action, ClusterModel clusterModel) {
        return ACCEPT;
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new ClusterModelStatsComparator() {
            @Override
            public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
                return 0;
            }

            @Override
            public String explainLastComparison() {
                return String.format("Comparison for the %s is irrelevant.", name());
            }
        };
    }

    @Override
    public void finish() {
        // do nothing.
    }

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public String name() {
        return PreferredLeaderElectionGoal.class.getSimpleName();
    }
}
