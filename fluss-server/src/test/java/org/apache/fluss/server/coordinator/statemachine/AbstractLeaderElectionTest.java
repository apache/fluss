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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for different implement of {@link AbstractLeaderElection}. */
public class AbstractLeaderElectionTest {

    @Test
    void testDefaultReplicaLeaderElection() {
        List<Integer> assignments = Arrays.asList(2, 4);
        List<Integer> liveReplicas = Collections.singletonList(4);
        List<Integer> isr = Arrays.asList(2, 4);

        Optional<Integer> leaderOpt =
                new DefaultLeaderElection().leaderElection(assignments, liveReplicas, isr);
        assertThat(leaderOpt).hasValue(4);
    }

    @Test
    void testReassignBucketLeaderElection() {
        List<Integer> targetReplicas = Arrays.asList(1, 2, 3);
        ReassignmentLeaderElection reassignmentLeaderElection =
                new ReassignmentLeaderElection(targetReplicas);
        List<Integer> liveReplicas = Arrays.asList(1, 2, 3);
        List<Integer> isr = Arrays.asList(1, 2, 3);
        Optional<Integer> leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, isr);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get()).isEqualTo(1);

        targetReplicas = Arrays.asList(1, 2, 3);
        reassignmentLeaderElection = new ReassignmentLeaderElection(targetReplicas);
        liveReplicas = Arrays.asList(2, 3);
        isr = Arrays.asList(2, 3);
        leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, isr);
        assertThat(leaderOpt).isPresent();
        assertThat(leaderOpt.get()).isEqualTo(2);

        targetReplicas = Arrays.asList(1, 2, 3);
        reassignmentLeaderElection = new ReassignmentLeaderElection(targetReplicas);
        liveReplicas = Arrays.asList(1, 2);
        isr = Collections.emptyList();
        leaderOpt = reassignmentLeaderElection.leaderElection(liveReplicas, isr);
        assertThat(leaderOpt).isNotPresent();
    }
}
