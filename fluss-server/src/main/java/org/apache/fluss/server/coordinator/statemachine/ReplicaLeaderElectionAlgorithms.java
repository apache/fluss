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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** The algorithms to elect the replica leader. */
public class ReplicaLeaderElectionAlgorithms {
    public static Optional<LeaderElectionResult> defaultReplicaLeaderElection(
            List<Integer> assignments, List<Integer> aliveReplicas, List<Integer> isr) {
        // currently, we always use the first replica in assignment, which also in aliveReplicas and
        // isr as the leader replica.
        for (int assignment : assignments) {
            if (aliveReplicas.contains(assignment) && isr.contains(assignment)) {
                return Optional.of(new LeaderElectionResult(assignment, isr));
            }
        }

        return Optional.empty();
    }

    public static Optional<LeaderElectionResult> controlledShutdownReplicaLeaderElection(
            List<Integer> assignments,
            List<Integer> isr,
            List<Integer> aliveReplicas,
            Set<Integer> shutdownTabletServers) {
        Set<Integer> isrSet = new HashSet<>(isr);
        for (Integer id : assignments) {
            if (aliveReplicas.contains(id)
                    && isrSet.contains(id)
                    && !shutdownTabletServers.contains(id)) {
                return Optional.of(
                        new LeaderElectionResult(
                                id,
                                isr.stream()
                                        .filter(replica -> !shutdownTabletServers.contains(replica))
                                        .collect(Collectors.toList())));
            }
        }
        return Optional.empty();
    }

    /** The result of replica leader election for different election algorithm. */
    public static final class LeaderElectionResult {
        private final int leader;
        private final List<Integer> newIsr;

        public LeaderElectionResult(int leader, List<Integer> newIsr) {
            this.leader = leader;
            this.newIsr = newIsr;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getNewIsr() {
            return newIsr;
        }
    }
}
