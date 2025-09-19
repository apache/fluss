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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElectionAlgorithms.controlledShutdownReplicaLeaderElection;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElectionAlgorithms.defaultReplicaLeaderElection;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElectionAlgorithms.initReplicaLeaderElection;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ReplicaLeaderElectionAlgorithms}. */
public class ReplicaLeaderElectionAlgorithmsTest {

    @Test
    void testInitReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Collections.singletonList(4);

        Optional<ElectionResult> leaderElectionResultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(4);
    }

    @Test
    void testDefaultReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr = new LeaderAndIsr(4, 0, Arrays.asList(2, 4), 0, 0);

        Optional<ElectionResult> leaderElectionResultOpt =
                defaultReplicaLeaderElection(assignments, liveReplicas, originLeaderAndIsr);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(2);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 4);
    }

    @Test
    void testControlledShutdownReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr = new LeaderAndIsr(2, 0, Arrays.asList(2, 4), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(2);

        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(4);
    }

    @Test
    void testControlledShutdownReplicaLeaderElectionLastIsrShuttingDown() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Collections.singletonList(2), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(2);

        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers);
        assertThat(leaderElectionResultOpt).isEmpty();
    }

    @Test
    void testControlledShutdownPartitionLeaderElectionAllIsrSimultaneouslyShutdown() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr = new LeaderAndIsr(2, 0, Arrays.asList(2, 4), 0, 0);
        Set<Integer> shutdownTabletServers = new HashSet<>(Arrays.asList(2, 4));

        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers);
        assertThat(leaderElectionResultOpt).isEmpty();
    }
}
