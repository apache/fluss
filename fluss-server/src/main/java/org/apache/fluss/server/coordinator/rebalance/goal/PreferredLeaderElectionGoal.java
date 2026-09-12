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
import org.apache.fluss.server.coordinator.rebalance.ActionType;
import org.apache.fluss.server.coordinator.rebalance.RebalancingAction;
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.ACCEPT;
import static org.apache.fluss.server.coordinator.rebalance.ActionAcceptance.REPLICA_REJECT;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalUtils.HardGoalStatsComparator;

/**
 * Goal to move each bucket's leadership to the first replica in its persisted assignment.
 *
 * <p>The goal never changes replica assignments and skips a bucket when its preferred replica is
 * not currently eligible to become leader.
 */
public class PreferredLeaderElectionGoal implements Goal {

    @Override
    public void optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals) {
        for (Map.Entry<Long, List<BucketModel>> entry :
                clusterModel.getBucketsByTable().entrySet()) {
            for (BucketModel bucket : entry.getValue()) {
                List<ReplicaModel> replicas = bucket.replicas();
                ReplicaModel leader = bucket.leader();
                if (replicas.isEmpty() || leader == null) {
                    continue;
                }

                ReplicaModel preferredLeader = replicas.get(0);
                if (leader.equals(preferredLeader)
                        || !preferredLeader.isLeaderEligible()
                        || preferredLeader.server().isOfflineTagged()) {
                    continue;
                }

                clusterModel.relocateLeadership(
                        bucket.tableBucket(), leader.serverId(), preferredLeader.serverId());
            }
        }
    }

    @Override
    public ActionAcceptance actionAcceptance(RebalancingAction action, ClusterModel clusterModel) {
        if (action.getActionType() != ActionType.LEADERSHIP_MOVEMENT) {
            return REPLICA_REJECT;
        }

        BucketModel bucket = clusterModel.bucket(action.getTableBucket());
        if (bucket == null || bucket.replicas().isEmpty()) {
            return REPLICA_REJECT;
        }

        return bucket.replicas().get(0).serverId() == action.getDestinationServerId()
                ? ACCEPT
                : REPLICA_REJECT;
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new HardGoalStatsComparator();
    }

    @Override
    public void finish() {}

    @Override
    public String name() {
        return getClass().getSimpleName();
    }
}
