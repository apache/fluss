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

/** The algorithms to elect the replica leader. */
public class ReplicaLeaderElectionAlgorithms {

    /**
     * Init replica leader election when the bucket is new created.
     *
     * @param assignments the assignments
     * @param aliveReplicas the alive replicas
     * @param coordinatorEpoch the coordinator epoch
     * @param isPrimaryKeyTable whether this table bucket is primary key table
     * @return the election result
     */
    public static Optional<ElectionResult> initReplicaLeaderElection(
            List<Integer> assignments,
            List<Integer> aliveReplicas,
            int coordinatorEpoch,
            boolean isPrimaryKeyTable) {
        // First we will filter out the assignment list to only contain the alive replicas.
        List<Integer> availableReplicas =
                assignments.stream().filter(aliveReplicas::contains).collect(Collectors.toList());

        // If the assignment list is empty, we return empty.
        if (availableReplicas.isEmpty()) {
            return Optional.empty();
        }

        //  Then we will use the first replica in assignment as the leader replica.
        int leader = availableReplicas.get(0);

        // If this table is primaryKey table, we will use the second replica in assignment as the
        // standby if exists.
        List<Integer> standbyReplica = new ArrayList<>();
        if (isPrimaryKeyTable) {
            if (availableReplicas.size() > 1) {
                standbyReplica.add(availableReplicas.get(1));
            }
        }
        return Optional.of(
                new ElectionResult(
                        aliveReplicas,
                        new LeaderAndIsr(
                                leader, 0, aliveReplicas, standbyReplica, coordinatorEpoch, 0)));
    }

    /**
     * Default replica leader election, like electing leader while leader offline.
     *
     * @param assignments the assignments
     * @param aliveReplicas the alive replicas
     * @param leaderAndIsr the original leaderAndIsr
     * @param isPrimaryKeyTable whether this table bucket is primary key table
     * @return the election result
     */
    public static Optional<ElectionResult> defaultReplicaLeaderElection(
            List<Integer> assignments,
            List<Integer> aliveReplicas,
            LeaderAndIsr leaderAndIsr,
            boolean isPrimaryKeyTable) {
        List<Integer> isr = leaderAndIsr.isr();
        // First we will filter out the assignment list to only contain the alive replicas and isr.
        List<Integer> availableReplicas =
                assignments.stream()
                        .filter(replica -> aliveReplicas.contains(replica) && isr.contains(replica))
                        .collect(Collectors.toList());

        // If the assignment list is empty, we return empty.
        if (availableReplicas.isEmpty()) {
            return Optional.empty();
        }

        // For log table, we will use the first replica in assignment as the leader replica.
        if (!isPrimaryKeyTable) {
            int leader = availableReplicas.get(0);
            return Optional.of(
                    new ElectionResult(
                            aliveReplicas,
                            leaderAndIsr.newLeaderAndIsr(leader, isr, Collections.emptyList())));
        }

        int currentStandby =
                leaderAndIsr.standbyReplicas().isEmpty()
                        ? -1
                        : leaderAndIsr.standbyReplicas().get(0);
        int newLeader;
        int newStandby = -1;
        if (currentStandby != -1 && availableReplicas.contains(currentStandby)) {
            // Standby exists and is available: promote it to leader.
            newLeader = currentStandby;
            // Find a new standby for the candidate replica (not leader).
            List<Integer> candidatesForStandby =
                    availableReplicas.stream()
                            .filter(replica -> replica != newLeader)
                            .collect(Collectors.toList());
            if (!candidatesForStandby.isEmpty()) {
                newStandby = candidatesForStandby.get(0);
            }
        } else {
            // Standby does not exist or is not available: promote the first replica in assignment
            // to leader.
            newLeader = availableReplicas.get(0);
            // Find a new standby for the candidate replica (not leader).
            List<Integer> candidatesForStandby =
                    availableReplicas.stream()
                            .filter(replica -> replica != newLeader)
                            .collect(Collectors.toList());
            if (!candidatesForStandby.isEmpty()) {
                newStandby = candidatesForStandby.get(0);
            }
        }

        if (newStandby == -1) {
            newStandby =
                    availableReplicas.stream()
                            .filter(replica -> replica != newLeader)
                            .collect(Collectors.toList())
                            .get(0);
        }

        LeaderAndIsr newLeaderAndIsr =
                leaderAndIsr.newLeaderAndIsr(newLeader, isr, Collections.singletonList(newStandby));
        return Optional.of(new ElectionResult(aliveReplicas, newLeaderAndIsr));
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
            Set<Integer> shutdownTabletServers,
            boolean isPrimaryKeyTable) {
        List<Integer> originIsr = leaderAndIsr.isr();
        Set<Integer> isrSet = new HashSet<>(originIsr);
        // First we will filter out the assignment list to only contain the alive replicas, isr, and
        // not is shutdownTabletServers set.
        List<Integer> availableReplicas =
                assignments.stream()
                        .filter(
                                replica ->
                                        aliveReplicas.contains(replica)
                                                && isrSet.contains(replica)
                                                && !shutdownTabletServers.contains(replica))
                        .collect(Collectors.toList());
        // If the assignment list is empty, we return empty.
        if (availableReplicas.isEmpty()) {
            return Optional.empty();
        }

        int currentStandby =
                leaderAndIsr.standbyReplicas().isEmpty()
                        ? -1
                        : leaderAndIsr.standbyReplicas().get(0);
        int newLeader;
        int newStandby = -1;
        if (!isPrimaryKeyTable) {
            // For log table, we will use the first replica in availableReplicas as the leader
            // replica.
            newLeader = availableReplicas.get(0);
        } else if (currentStandby != -1 && availableReplicas.contains(currentStandby)) {
            // For pk table, Standby exists and is available: promote it to leader.
            newLeader = currentStandby;
            // Find a new standby for the candidate replica (not leader).
            List<Integer> candidatesForStandby =
                    availableReplicas.stream()
                            .filter(replica -> replica != newLeader)
                            .collect(Collectors.toList());
            if (!candidatesForStandby.isEmpty()) {
                newStandby = candidatesForStandby.get(0);
            }
        } else {
            // Standby does not exist or is not available: promote the first replica in assignment
            // to leader.
            newLeader = availableReplicas.get(0);
            // Find a new standby for the candidate replica (not leader).
            List<Integer> candidatesForStandby =
                    availableReplicas.stream()
                            .filter(replica -> replica != newLeader)
                            .collect(Collectors.toList());
            if (!candidatesForStandby.isEmpty()) {
                newStandby = candidatesForStandby.get(0);
            }
        }

        Set<Integer> newAliveReplicas = new HashSet<>(aliveReplicas);
        newAliveReplicas.removeAll(shutdownTabletServers);
        List<Integer> newIsr =
                originIsr.stream()
                        .filter(replica -> !shutdownTabletServers.contains(replica))
                        .collect(Collectors.toList());

        if (newStandby == -1) {
            newStandby =
                    availableReplicas.stream()
                            .filter(replica -> replica != newLeader)
                            .collect(Collectors.toList())
                            .get(0);
        }

        return Optional.of(
                new ElectionResult(
                        new ArrayList<>(newAliveReplicas),
                        leaderAndIsr.newLeaderAndIsr(
                                newLeader, newIsr, Collections.singletonList(newStandby))));
    }
}
