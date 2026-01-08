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
        LeaderAndIsr originLeaderAndIsr = new LeaderAndIsr(4, 0, Arrays.asList(2, 4), 0, 0);

        DefaultLeaderElection defaultLeaderElection = new DefaultLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                defaultLeaderElection.leaderElection(assignments, liveReplicas, originLeaderAndIsr);
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

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
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

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers);
        assertThat(leaderElectionResultOpt).isEmpty();
    }

    @Test
    void testControlledShutdownPartitionLeaderElectionAllIsrSimultaneouslyShutdown() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Arrays.asList(2, 4);
        LeaderAndIsr originLeaderAndIsr = new LeaderAndIsr(2, 0, Arrays.asList(2, 4), 0, 0);
        Set<Integer> shutdownTabletServers = new HashSet<>(Arrays.asList(2, 4));

        ControlledShutdownLeaderElection controlledShutdownLeaderElection =
                new ControlledShutdownLeaderElection();
        Optional<ElectionResult> leaderElectionResultOpt =
                controlledShutdownLeaderElection.leaderElection(
                        assignments, liveReplicas, originLeaderAndIsr, shutdownTabletServers);
        assertThat(leaderElectionResultOpt).isEmpty();
    }

    @Test
    void testReassignBucketLeaderElection() {
        List<Integer> targetReplicas = Arrays.asList(1, 2, 3);
        ReassignmentLeaderElection reassignmentLeaderElection =
                new ReassignmentLeaderElection(targetReplicas);
        List<Integer> liveReplicas = Arrays.asList(1, 2, 3);
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(1, 0, Arrays.asList(1, 2, 3), 0, 0);
        Optional<ElectionResult> leaderOpt =
                reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get().getLeaderAndIsr().leader()).isEqualTo(1);

        targetReplicas = Arrays.asList(1, 2, 3);
        reassignmentLeaderElection = new ReassignmentLeaderElection(targetReplicas);
        liveReplicas = Arrays.asList(2, 3);
        leaderAndIsr = new LeaderAndIsr(1, 0, Arrays.asList(2, 3), 0, 0);
        leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get().getLeaderAndIsr().leader()).isEqualTo(2);

        targetReplicas = Arrays.asList(1, 2, 3);
        reassignmentLeaderElection = new ReassignmentLeaderElection(targetReplicas);
        liveReplicas = Arrays.asList(1, 2);
        leaderAndIsr = new LeaderAndIsr(2, 1, Collections.emptyList(), 0, 1);
        leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, leaderAndIsr);
        assertThat(leaderOpt).isNotPresent();
    }
}
