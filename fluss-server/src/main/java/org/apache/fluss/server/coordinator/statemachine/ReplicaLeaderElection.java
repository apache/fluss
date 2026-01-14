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

/** The strategies to elect the replica leader. */
public abstract class ReplicaLeaderElection {

    // TODO refactor ReplicaLeaderElection into interface with method leaderElection, tace
    // by:https://github.com/apache/fluss/issues/2324

    /** The default replica leader election. */
    public static class DefaultLeaderElection extends ReplicaLeaderElection {
        /**
         * Default replica leader election, like electing leader while leader offline.
         *
         * @param assignments the assignments
         * @param aliveReplicas the alive replicas
         * @param leaderAndIsr the original leaderAndIsr
         * @return the election result
         */
        public Optional<ElectionResult> leaderElection(
                List<Integer> assignments, List<Integer> aliveReplicas, LeaderAndIsr leaderAndIsr) {
            // currently, we always use the first replica in assignment, which also in aliveReplicas
            // and
            // isr as the leader replica.
            List<Integer> isr = leaderAndIsr.isr();
            for (int assignment : assignments) {
                if (aliveReplicas.contains(assignment) && isr.contains(assignment)) {
                    return Optional.of(
                            new TableBucketStateMachine.ElectionResult(
                                    aliveReplicas, leaderAndIsr.newLeaderAndIsr(assignment, isr)));
                }
            }

            return Optional.empty();
        }
    }

    /** The controlled shutdown replica leader election. */
    public static class ControlledShutdownLeaderElection extends ReplicaLeaderElection {
        /**
         * Controlled shutdown replica leader election.
         *
         * @param assignments the assignments
         * @param aliveReplicas the alive replicas
         * @param leaderAndIsr the original leaderAndIsr
         * @param shutdownTabletServers the shutdown tabletServers
         * @return the election result
         */
        public Optional<ElectionResult> leaderElection(
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

    /** The reassignment replica leader election. */
    public static class ReassignmentLeaderElection extends ReplicaLeaderElection {
        private final List<Integer> newReplicas;

        public ReassignmentLeaderElection(List<Integer> newReplicas) {
            this.newReplicas = newReplicas;
        }

        public Optional<ElectionResult> leaderElection(
                List<Integer> liveReplicas, LeaderAndIsr leaderAndIsr) {
            // currently, we always use the first replica in targetReplicas, which also in
            // liveReplicas and isr as the leader replica. For bucket reassignment, the first
            // replica is the target leader replica.
            List<Integer> isr = leaderAndIsr.isr();
            for (int assignment : newReplicas) {
                if (liveReplicas.contains(assignment) && isr.contains(assignment)) {
                    return Optional.of(
                            new ElectionResult(
                                    liveReplicas, leaderAndIsr.newLeaderAndIsr(assignment, isr)));
                }
            }

            return Optional.empty();
        }
    }
}
