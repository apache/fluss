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
                initReplicaLeaderElection(assignments, liveReplicas, 0, false);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas()).isEmpty();

        // Test primary key table.
        assignments = Arrays.asList(2, 4);
        liveReplicas = Arrays.asList(2, 4);

        leaderElectionResultOpt = initReplicaLeaderElection(assignments, liveReplicas, 0, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(2);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(4);
    }

    @Test
    void testInitReplicaLeaderElectionForPkTable() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);

        Optional<ElectionResult> leaderElectionResultOpt =
                initReplicaLeaderElection(assignments, liveReplicas, 0, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(2);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(4);
    }

    @Test
    void testDefaultReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(4, 0, Arrays.asList(2, 4), Collections.emptyList(), 0, 0);

        Optional<ElectionResult> leaderElectionResultOpt =
                defaultReplicaLeaderElection(assignments, liveReplicas, originLeaderAndIsr, false);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(2);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testDefaultReplicaLeaderElectionForPkTable() {
        List<Integer> assignments = Arrays.asList(2, 3, 4);
        List<Integer> liveReplicas = Arrays.asList(3, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3, 4), Collections.emptyList(), 0, 0);

        // first, test origin leaderAndIsr don't have standby replica.
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultReplicaLeaderElection(assignments, liveReplicas, originLeaderAndIsr, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(3);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(4);

        // second. test origin leaderAndIsr has standby replica.
        originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3, 4), Collections.singletonList(4), 0, 0);
        leaderElectionResultOpt =
                defaultReplicaLeaderElection(assignments, liveReplicas, originLeaderAndIsr, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(3);

        // third. test no enough live replicas.
        assignments = Arrays.asList(2, 3, 4);
        liveReplicas = Collections.singletonList(4);
        originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3, 4), Collections.emptyList(), 0, 0);
        leaderElectionResultOpt =
                defaultReplicaLeaderElection(assignments, liveReplicas, originLeaderAndIsr, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(2, 3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testControlledShutdownReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 4), Collections.emptyList(), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(2);

        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments,
                        liveReplicas,
                        originLeaderAndIsr,
                        shutdownTabletServers,
                        false);
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
                new LeaderAndIsr(2, 0, Collections.singletonList(2), Collections.emptyList(), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(2);

        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments,
                        liveReplicas,
                        originLeaderAndIsr,
                        shutdownTabletServers,
                        false);
        assertThat(leaderElectionResultOpt).isEmpty();
    }

    @Test
    void testControlledShutdownLeaderElectionAllIsrSimultaneouslyShutdown() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 4), Collections.emptyList(), 0, 0);
        Set<Integer> shutdownTabletServers = new HashSet<>(Arrays.asList(2, 4));

        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments,
                        liveReplicas,
                        originLeaderAndIsr,
                        shutdownTabletServers,
                        false);
        assertThat(leaderElectionResultOpt).isEmpty();
    }

    @Test
    void testControlledShutdownReplicaLeaderElectionForPkTable() {
        List<Integer> assignments = Arrays.asList(2, 3, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 3, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3, 4), Collections.emptyList(), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(2);
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(3);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(4);

        originLeaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(2, 3, 4), Collections.singletonList(4), 0, 0);
        leaderElectionResultOpt =
                controlledShutdownReplicaLeaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(3);
    }
}
