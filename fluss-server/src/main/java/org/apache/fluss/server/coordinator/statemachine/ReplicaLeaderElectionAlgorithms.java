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

package org.apache.fluss.server.coordinator.statemachine;

import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine.ElectionResult;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** The algorithms to elect the replica leader. */
public class ReplicaLeaderElectionAlgorithms {

    /**
     * Init replica leader election when the bucket is new created.
     *
     * @param assignments the assignments
     * @param aliveReplicas the alive replicas
     * @param coordinatorEpoch the coordinator epoch
     * @return the election result
     */
    public static Optional<ElectionResult> initReplicaLeaderElection(
            List<Integer> assignments, List<Integer> aliveReplicas, int coordinatorEpoch) {
        // currently, we always use the first replica in assignment, which also in aliveReplicas and
        // isr as the leader replica.
        for (int assignment : assignments) {
            if (aliveReplicas.contains(assignment)) {
                return Optional.of(
                        new ElectionResult(
                                aliveReplicas,
                                new LeaderAndIsr(
                                        assignment, 0, aliveReplicas, coordinatorEpoch, 0)));
            }
        }

        return Optional.empty();
    }

    /**
     * Default replica leader election, like electing leader while leader offline.
     *
     * @param assignments the assignments
     * @param aliveReplicas the alive replicas
     * @param leaderAndIsr the original leaderAndIsr
     * @return the election result
     */
    public static Optional<ElectionResult> defaultReplicaLeaderElection(
            List<Integer> assignments, List<Integer> aliveReplicas, LeaderAndIsr leaderAndIsr) {
        // currently, we always use the first replica in assignment, which also in aliveReplicas and
        // isr as the leader replica.
        List<Integer> isr = leaderAndIsr.isr();
        for (int assignment : assignments) {
            if (aliveReplicas.contains(assignment) && isr.contains(assignment)) {
                return Optional.of(
                        new ElectionResult(
                                aliveReplicas, leaderAndIsr.newLeaderAndIsr(assignment, isr)));
            }
        }

        return Optional.empty();
    }

    /**
     * Controlled shutdown replica leader election.
     *
     * @param assignments the assignments
     * @param aliveReplicas the alive replicas
     * @param leaderAndIsr the original leaderAndIsr
     * @param shutdownTabletServers the shutdown tabletServers
     * @return the election result
     */
    public static Optional<ElectionResult> controlledShutdownReplicaLeaderElection(
            List<Integer> assignments,
            List<Integer> aliveReplicas,
            LeaderAndIsr leaderAndIsr,
            Set<Integer> shutdownTabletServers) {
        List<Integer> originIsr = leaderAndIsr.isr();
        Set<Integer> isrSet = new HashSet<>(originIsr);
        for (Integer id : assignments) {
            if (aliveReplicas.contains(id)
                    && isrSet.contains(id)
                    && !shutdownTabletServers.contains(id)) {
                Set<Integer> newAliveReplicas = new HashSet<>(aliveReplicas);
                newAliveReplicas.removeAll(shutdownTabletServers);
                List<Integer> newIsr =
                        originIsr.stream()
                                .filter(replica -> !shutdownTabletServers.contains(replica))
                                .collect(Collectors.toList());
                return Optional.of(
                        new ElectionResult(
                                new ArrayList<>(newAliveReplicas),
                                leaderAndIsr.newLeaderAndIsr(id, newIsr)));
            }
        }
        return Optional.empty();
    }
}
