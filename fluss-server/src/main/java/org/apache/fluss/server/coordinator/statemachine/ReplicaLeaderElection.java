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
import java.util.Collections;
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
         * @param isPrimaryKeyTable whether this table bucket is primary key table
         * @return the election result
         */
        public Optional<ElectionResult> leaderElection(
                List<Integer> assignments,
                List<Integer> aliveReplicas,
                LeaderAndIsr leaderAndIsr,
                boolean isPrimaryKeyTable) {
            List<Integer> isr = leaderAndIsr.isr();
            // First we will filter out the assignment list to only contain the alive replicas and
            // isr.
            List<Integer> availableReplicas =
                    assignments.stream()
                            .filter(
                                    replica ->
                                            aliveReplicas.contains(replica)
                                                    && isr.contains(replica))
                            .collect(Collectors.toList());
            return electLeader(availableReplicas, aliveReplicas, leaderAndIsr, isPrimaryKeyTable);
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
         * @param isPrimaryKeyTable whether this table bucket is primary key table
         * @return the election result
         */
        public Optional<ElectionResult> leaderElection(
                List<Integer> assignments,
                List<Integer> aliveReplicas,
                LeaderAndIsr leaderAndIsr,
                Set<Integer> shutdownTabletServers,
                boolean isPrimaryKeyTable) {
            List<Integer> originIsr = leaderAndIsr.isr();
            Set<Integer> isrSet = new HashSet<>(originIsr);
            // Filter out available replicas: alive, in ISR, and not shutting down.
            List<Integer> availableReplicas =
                    assignments.stream()
                            .filter(
                                    replica ->
                                            aliveReplicas.contains(replica)
                                                    && isrSet.contains(replica)
                                                    && !shutdownTabletServers.contains(replica))
                            .collect(Collectors.toList());

            if (availableReplicas.isEmpty()) {
                return Optional.empty();
            }

            // For log table, simply use the first available replica as leader.
            if (!isPrimaryKeyTable) {
                List<Integer> newIsr =
                        originIsr.stream()
                                .filter(replica -> !shutdownTabletServers.contains(replica))
                                .collect(Collectors.toList());
                Set<Integer> newAliveReplicas = new HashSet<>(aliveReplicas);
                newAliveReplicas.removeAll(shutdownTabletServers);
                return Optional.of(
                        new ElectionResult(
                                new ArrayList<>(newAliveReplicas),
                                leaderAndIsr.newLeaderAndIsr(
                                        availableReplicas.get(0),
                                        newIsr,
                                        Collections.emptyList())));
            }

            // For PK table, elect leader and standby.
            LeaderAndStandby leaderAndStandby =
                    electLeaderAndStandbyForPkTable(availableReplicas, leaderAndIsr);

            Set<Integer> newAliveReplicas = new HashSet<>(aliveReplicas);
            newAliveReplicas.removeAll(shutdownTabletServers);
            List<Integer> newIsr =
                    originIsr.stream()
                            .filter(replica -> !shutdownTabletServers.contains(replica))
                            .collect(Collectors.toList());

            return Optional.of(
                    new ElectionResult(
                            new ArrayList<>(newAliveReplicas),
                            leaderAndIsr.newLeaderAndIsr(
                                    leaderAndStandby.leader,
                                    newIsr,
                                    leaderAndStandby.standbyReplicas)));
        }
    }

    /** The reassignment replica leader election. */
    public static class ReassignmentLeaderElection extends ReplicaLeaderElection {
        private final List<Integer> newReplicas;

        public ReassignmentLeaderElection(List<Integer> newReplicas) {
            this.newReplicas = newReplicas;
        }

        public Optional<ElectionResult> leaderElection(
                List<Integer> liveReplicas, LeaderAndIsr leaderAndIsr, boolean isPrimaryKeyTable) {
            // Currently, we always use the first replica in targetReplicas, which also in
            // liveReplicas and isr as the leader replica. For bucket reassignment, the first
            // replica is the target leader replica.
            List<Integer> isr = leaderAndIsr.isr();
            List<Integer> availableReplicas =
                    newReplicas.stream()
                            .filter(
                                    replica ->
                                            liveReplicas.contains(replica) && isr.contains(replica))
                            .collect(Collectors.toList());
            return electLeader(availableReplicas, liveReplicas, leaderAndIsr, isPrimaryKeyTable);
        }
    }

    // ------------------------------------------------------------------------
    //  Common election logic
    // ------------------------------------------------------------------------

    private static Optional<ElectionResult> electLeader(
            List<Integer> availableReplicas,
            List<Integer> aliveReplicas,
            LeaderAndIsr leaderAndIsr,
            boolean isPrimaryKeyTable) {
        if (availableReplicas.isEmpty()) {
            return Optional.empty();
        }

        List<Integer> isr = leaderAndIsr.isr();

        // For log table, simply use the first available replica as leader.
        if (!isPrimaryKeyTable) {
            return Optional.of(
                    new ElectionResult(
                            aliveReplicas,
                            leaderAndIsr.newLeaderAndIsr(
                                    availableReplicas.get(0), isr, Collections.emptyList())));
        }

        // For PK table, elect leader and standby.
        LeaderAndStandby leaderAndStandby =
                electLeaderAndStandbyForPkTable(availableReplicas, leaderAndIsr);
        return Optional.of(
                new ElectionResult(
                        aliveReplicas,
                        leaderAndIsr.newLeaderAndIsr(
                                leaderAndStandby.leader, isr, leaderAndStandby.standbyReplicas)));
    }

    /**
     * Elect leader and standby for PK table.
     *
     * <p>Election strategy:
     *
     * <ul>
     *   <li>If current standby exists and is available, promote it to leader
     *   <li>Otherwise, use the first available replica as leader
     *   <li>Select new standby from remaining available replicas (if any)
     * </ul>
     */
    private static LeaderAndStandby electLeaderAndStandbyForPkTable(
            List<Integer> availableReplicas, LeaderAndIsr leaderAndIsr) {
        int currentStandby = getCurrentStandby(leaderAndIsr);
        int newLeader;

        if (currentStandby != -1 && availableReplicas.contains(currentStandby)) {
            // Promote current standby to leader.
            newLeader = currentStandby;
        } else {
            // Use first available replica as leader.
            newLeader = availableReplicas.get(0);
        }

        // Find new standby from remaining replicas.
        List<Integer> standbyReplicas = findNewStandby(availableReplicas, newLeader);
        return new LeaderAndStandby(newLeader, standbyReplicas);
    }

    /** Get current standby replica ID, returns -1 if no standby exists. */
    private static int getCurrentStandby(LeaderAndIsr leaderAndIsr) {
        return leaderAndIsr.standbyReplicas().isEmpty()
                ? -1
                : leaderAndIsr.standbyReplicas().get(0);
    }

    /** Find new standby from available replicas, excluding the leader. */
    private static List<Integer> findNewStandby(
            List<Integer> availableReplicas, int excludeLeader) {
        return availableReplicas.stream()
                .filter(replica -> replica != excludeLeader)
                .findFirst()
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());
    }

    /** Internal class to hold leader and standby election result. */
    private static class LeaderAndStandby {
        final int leader;
        final List<Integer> standbyReplicas;

        LeaderAndStandby(int leader, List<Integer> standbyReplicas) {
            this.leader = leader;
            this.standbyReplicas = standbyReplicas;
        }
    }
}
