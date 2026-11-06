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

package org.apache.fluss.flink.tiering.committer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ConsecutiveCommitFailureCounter}. */
class ConsecutiveCommitFailureCounterTest {

    @Test
    void testCountsUntilTableCommits() {
        ConsecutiveCommitFailureCounter counter = new ConsecutiveCommitFailureCounter(10);

        assertThat(counter.recordFailure(1L)).isEqualTo(1);
        assertThat(counter.recordFailure(1L)).isEqualTo(2);
        assertThat(counter.recordFailure(2L)).isEqualTo(1);
        assertThat(counter.getFailures(1L)).isEqualTo(2);

        counter.recordSuccess(1L);
        assertThat(counter.getFailures(1L)).isZero();
        assertThat(counter.getFailures(2L)).isEqualTo(1);
        assertThat(counter.size()).isEqualTo(1);
        assertThat(counter.recordFailure(1L)).isEqualTo(1);

        // A table that never failed has nothing to reset.
        counter.recordSuccess(3L);
        assertThat(counter.size()).isEqualTo(2);
    }

    @Test
    void testEvictsTableThatFailedLeastRecently() {
        ConsecutiveCommitFailureCounter counter = new ConsecutiveCommitFailureCounter(2);
        counter.recordFailure(1L);
        counter.recordFailure(2L);
        // Table 1 failed again, so table 2 is now the one that failed least recently, e.g. as it
        // was dropped.
        counter.recordFailure(1L);

        assertThat(counter.recordFailure(3L)).isEqualTo(1);
        assertThat(counter.size()).isEqualTo(2);
        assertThat(counter.getFailures(1L)).isEqualTo(2);
        assertThat(counter.getFailures(2L)).isZero();
        assertThat(counter.getFailures(3L)).isEqualTo(1);

        // An evicted table that fails again restarts its count.
        assertThat(counter.recordFailure(2L)).isEqualTo(1);
        assertThat(counter.getFailures(1L)).isZero();
    }

    @Test
    void testRejectsNonPositiveMaxTables() {
        assertThatThrownBy(() -> new ConsecutiveCommitFailureCounter(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxTables must be positive");
    }
}
