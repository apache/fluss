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

import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.server.coordinator.rebalance.ActionType;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** An util class for {@link Goal}. */
public class GoalUtils {

    public static Goal getGoalByType(GoalType goalType) {
        switch (goalType) {
            case REPLICA_DISTRIBUTION:
                return new ReplicaDistributionGoal();
            case LEADER_DISTRIBUTION:
                return new LeaderReplicaDistributionGoal();
            default:
                throw new IllegalArgumentException("Unsupported goal type " + goalType);
        }
    }

    /**
     * Check whether the proposed action is legit. An action is legit if it is:
     *
     * <ul>
     *   <li>1. a replica movement across tabletServers, the dest server does not have a replica of
     *       the same bucket and is allowed to have a replica from the bucket
     *   <li>a leadership movement, the replica is a leader and the dest server has a follower of
     *       the same bucket
     * </ul>
     */
    public static boolean legitMove(
            ReplicaModel replica,
            ServerModel destServer,
            ClusterModel cluster,
            ActionType actionType) {
        switch (actionType) {
            case REPLICA_MOVEMENT:
                return cluster.bucket(replica.tableBucket()).canAssignReplicaToServer(destServer)
                        && destServer.replica(replica.tableBucket()) == null;
            case LEADERSHIP_MOVEMENT:
                return replica.isLeader() && destServer.replica(replica.tableBucket()) != null;
            default:
                return false;
        }
    }

    /**
     * Retrieve alive servers ids that are not excluded for replica moves. Returns a set to provide
     * constant time lookup guaranteed by a HashSet.
     */
    public static Set<Integer> aliveServers(ClusterModel cluster) {
        return cluster.aliveServers().stream()
                .map(ServerModel::id)
                .collect(Collectors.toCollection(HashSet::new));
    }
}
