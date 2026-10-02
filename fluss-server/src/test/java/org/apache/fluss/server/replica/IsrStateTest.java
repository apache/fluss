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

package org.apache.fluss.server.replica;

import org.apache.fluss.server.replica.IsrState.CommittedIsrState;
import org.apache.fluss.server.replica.IsrState.PendingExpandIsrState;
import org.apache.fluss.server.replica.IsrState.PendingShrinkIsrState;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the {@code equals}/{@code hashCode} contract of the {@link IsrState} implementations. */
class IsrStateTest {

    private static final List<Integer> ISR = Arrays.asList(1, 2, 3);
    private static final List<Integer> STANDBY = Collections.singletonList(4);

    private static LeaderAndIsr leaderAndIsr() {
        return new LeaderAndIsr(1, 0, Arrays.asList(1, 2, 3), Collections.singletonList(4), 0, 0);
    }

    @Test
    void testCommittedIsrStateEqualsAndHashCode() {
        CommittedIsrState state1 = new CommittedIsrState(ISR, STANDBY);
        CommittedIsrState state2 = new CommittedIsrState(Arrays.asList(1, 2, 3), STANDBY);

        assertThat(state1).isEqualTo(state2);
        assertThat(state1).hasSameHashCodeAs(state2);
    }

    @Test
    void testCommittedIsrStateHashCodeDiffersForDifferentIsr() {
        CommittedIsrState state1 = new CommittedIsrState(ISR, STANDBY);
        CommittedIsrState state2 = new CommittedIsrState(Arrays.asList(1, 2), STANDBY);

        assertThat(state1).isNotEqualTo(state2);
        assertThat(state1.hashCode()).isNotEqualTo(state2.hashCode());
    }

    @Test
    void testPendingExpandIsrStateEqualsAndHashCode() {
        CommittedIsrState committed = new CommittedIsrState(ISR, STANDBY);
        PendingExpandIsrState state1 =
                new PendingExpandIsrState(5, leaderAndIsr(), new CommittedIsrState(ISR, STANDBY));
        PendingExpandIsrState state2 = new PendingExpandIsrState(5, leaderAndIsr(), committed);

        assertThat(state1).isEqualTo(state2);
        assertThat(state1).hasSameHashCodeAs(state2);
    }

    @Test
    void testPendingExpandIsrStateHashCodeDiffersForDifferentReplica() {
        CommittedIsrState committed = new CommittedIsrState(ISR, STANDBY);
        PendingExpandIsrState state1 = new PendingExpandIsrState(5, leaderAndIsr(), committed);
        PendingExpandIsrState state2 = new PendingExpandIsrState(6, leaderAndIsr(), committed);

        assertThat(state1).isNotEqualTo(state2);
        assertThat(state1.hashCode()).isNotEqualTo(state2.hashCode());
    }

    @Test
    void testPendingShrinkIsrStateEqualsAndHashCode() {
        CommittedIsrState committed = new CommittedIsrState(ISR, STANDBY);
        PendingShrinkIsrState state1 =
                new PendingShrinkIsrState(
                        Collections.singletonList(3),
                        leaderAndIsr(),
                        new CommittedIsrState(ISR, STANDBY));
        PendingShrinkIsrState state2 =
                new PendingShrinkIsrState(Collections.singletonList(3), leaderAndIsr(), committed);

        assertThat(state1).isEqualTo(state2);
        assertThat(state1).hasSameHashCodeAs(state2);
    }

    @Test
    void testPendingShrinkIsrStateHashCodeDiffersForDifferentOutOfSyncReplicas() {
        CommittedIsrState committed = new CommittedIsrState(ISR, STANDBY);
        PendingShrinkIsrState state1 =
                new PendingShrinkIsrState(Collections.singletonList(3), leaderAndIsr(), committed);
        PendingShrinkIsrState state2 =
                new PendingShrinkIsrState(Arrays.asList(2, 3), leaderAndIsr(), committed);

        assertThat(state1).isNotEqualTo(state2);
        assertThat(state1.hashCode()).isNotEqualTo(state2.hashCode());
    }

    @Test
    void testEqualStatesDeduplicateInHashSet() {
        Set<IsrState> states = new HashSet<>();
        states.add(new CommittedIsrState(ISR, STANDBY));
        states.add(new CommittedIsrState(Arrays.asList(1, 2, 3), STANDBY));

        assertThat(states).hasSize(1);
    }
}
