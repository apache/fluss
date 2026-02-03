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

import org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElection.ControlledShutdownLeaderElection;
import org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElection.DefaultLeaderElection;
import org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElection.ReassignmentLeaderElection;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine.ElectionResult;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for different implement of {@link ReplicaLeaderElection}. */
public class ReplicaLeaderElectionTest {

    @Test
    void testDefaultReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(4, 0, Arrays.asList(2, 4), Collections.emptyList(), 0, 0);

        DefaultLeaderElection defaultLeaderElection = new DefaultLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, false);
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
        DefaultLeaderElection defaultLeaderElection = new DefaultLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, true);
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
        defaultLeaderElection = new DefaultLeaderElection();
        leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, true);
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
        defaultLeaderElection = new DefaultLeaderElection();
        leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, true);
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

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
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

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
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

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
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
        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
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
        controlledShutdownLeaderElection = new ControlledShutdownLeaderElection();
        leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLiveReplicas()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(4);
        assertThat(leaderElectionResult.getLeaderAndIsr().isr()).containsExactlyInAnyOrder(3, 4);
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(3);
    }

    @Test
    void testReassignBucketLeaderElection() {
        List<Integer> targetReplicas = Arrays.asList(1, 2, 3);
        ReassignmentLeaderElection reassignmentLeaderElection =
                new ReassignmentLeaderElection(targetReplicas);
        List<Integer> liveReplicas = Arrays.asList(1, 2, 3);
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(1, 0, Arrays.asList(1, 2, 3), Collections.emptyList(), 0, 0);
        Optional<ElectionResult> leaderOpt =
                reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr, false);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get().getLeaderAndIsr().leader()).isEqualTo(1);

        targetReplicas = Arrays.asList(1, 2, 3);
        reassignmentLeaderElection = new ReassignmentLeaderElection(targetReplicas);
        liveReplicas = Arrays.asList(2, 3);
        leaderAndIsr = new LeaderAndIsr(1, 0, Arrays.asList(2, 3), Collections.emptyList(), 0, 0);
        leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr, false);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get().getLeaderAndIsr().leader()).isEqualTo(2);

        targetReplicas = Arrays.asList(1, 2, 3);
        reassignmentLeaderElection = new ReassignmentLeaderElection(targetReplicas);
        liveReplicas = Arrays.asList(1, 2);
        leaderAndIsr =
                new LeaderAndIsr(2, 1, Collections.emptyList(), Collections.emptyList(), 0, 1);
        leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr, false);
        assertThat(leaderOpt).isNotPresent();
    }

    @Test
    void testDefaultLeaderElectionWithSingleReplica() {
        // Test that single replica election doesn't throw IndexOutOfBoundsException
        List<Integer> assignments = Collections.singletonList(1);
        List<Integer> liveReplicas = Collections.singletonList(1);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(1, 0, Collections.singletonList(1), Collections.emptyList(), 0, 0);

        DefaultLeaderElection defaultLeaderElection = new DefaultLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(1);
        // With only one replica, standby should be empty
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testControlledShutdownWithSingleRemainingReplica() {
        // Test controlled shutdown when only one replica remains available
        List<Integer> assignments = Arrays.asList(1, 2);
        List<Integer> liveReplicas = Arrays.asList(1, 2);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(1, 0, Arrays.asList(1, 2), Collections.singletonList(2), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(1);

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        // Standby (2) should be promoted to leader
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(2);
        // With only one remaining replica, standby should be empty
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testReassignBucketLeaderElectionForPkTable() {
        // Test reassignment election for PK table
        List<Integer> targetReplicas = Arrays.asList(1, 2, 3);
        ReassignmentLeaderElection reassignmentLeaderElection =
                new ReassignmentLeaderElection(targetReplicas);
        List<Integer> liveReplicas = Arrays.asList(1, 2, 3);
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(2, 0, Arrays.asList(1, 2, 3), Collections.singletonList(3), 0, 0);
        Optional<ElectionResult> leaderOpt =
                reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr, true);
        assertThat(leaderOpt).isPresent();
        // Standby (3) should be promoted to leader
        assertThat(leaderOpt.get().getLeaderAndIsr().leader()).isEqualTo(3);
        // A new standby should be assigned from remaining replicas
        assertThat(leaderOpt.get().getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(1);
    }

    @Test
    void testReassignBucketLeaderElectionForPkTableWithSingleReplica() {
        // Test reassignment election for PK table with only one replica
        List<Integer> targetReplicas = Collections.singletonList(1);
        ReassignmentLeaderElection reassignmentLeaderElection =
                new ReassignmentLeaderElection(targetReplicas);
        List<Integer> liveReplicas = Collections.singletonList(1);
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(1, 0, Collections.singletonList(1), Collections.emptyList(), 0, 0);
        Optional<ElectionResult> leaderOpt =
                reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr, true);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get().getLeaderAndIsr().leader()).isEqualTo(1);
        // With only one replica, standby should be empty
        assertThat(leaderOpt.get().getLeaderAndIsr().standbyReplicas()).isEmpty();
    }

    @Test
    void testDefaultLeaderElectionWithStandbyUnavailable() {
        // Test when current standby is not in available replicas
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Arrays.asList(1, 2); // standby (3) is not alive
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(1, 0, Arrays.asList(1, 2, 3), Collections.singletonList(3), 0, 0);

        DefaultLeaderElection defaultLeaderElection = new DefaultLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        // Should select first available replica as leader since standby is unavailable
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(1);
        // New standby should be selected from remaining available replicas
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(2);
    }

    @Test
    void testDefaultLeaderElectionWithNoAvailableReplicas() {
        // Test when no replicas are available
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Collections.emptyList();
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(1, 0, Arrays.asList(1, 2, 3), Collections.emptyList(), 0, 0);

        DefaultLeaderElection defaultLeaderElection = new DefaultLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, true);
        assertThat(leaderElectionResultOpt).isEmpty();
    }

    @Test
    void testControlledShutdownForPkTableWithStandbyShuttingDown() {
        // Test when the current standby is shutting down
        List<Integer> assignments = Arrays.asList(1, 2, 3);
        List<Integer> liveReplicas = Arrays.asList(1, 2, 3);
        LeaderAndIsr originLeaderAndIsr =
                new LeaderAndIsr(1, 0, Arrays.asList(1, 2, 3), Collections.singletonList(2), 0, 0);
        Set<Integer> shutdownTabletServers = Collections.singleton(2); // standby is shutting down

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers, true);
        assertThat(leaderElectionResultOpt.isPresent()).isTrue();
        ElectionResult leaderElectionResult = leaderElectionResultOpt.get();
        // Leader (1) should remain leader since standby (2) is shutting down
        assertThat(leaderElectionResult.getLeaderAndIsr().leader()).isEqualTo(1);
        // New standby should be selected from remaining replicas
        assertThat(leaderElectionResult.getLeaderAndIsr().standbyReplicas())
                .containsExactlyInAnyOrder(3);
    }
}
