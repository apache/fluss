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

package org.apache.fluss.flink.action.orphan.build;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaxKnownIdsTrackerTest {

    @Test
    void initialValuesAreNegativeOne() {
        MaxKnownIdsTracker t = new MaxKnownIdsTracker();
        assertThat(t.maxKnownTableId()).isEqualTo(-1L);
        assertThat(t.maxKnownPartitionId()).isEqualTo(-1L);
    }

    @Test
    void observeTableIdMonotonicallyIncreases() {
        MaxKnownIdsTracker t = new MaxKnownIdsTracker();
        t.observeTableId(5L);
        assertThat(t.maxKnownTableId()).isEqualTo(5L);
        t.observeTableId(3L);
        assertThat(t.maxKnownTableId()).isEqualTo(5L); // never decreases
        t.observeTableId(10L);
        assertThat(t.maxKnownTableId()).isEqualTo(10L);
    }

    @Test
    void observePartitionIdMonotonicallyIncreases() {
        MaxKnownIdsTracker t = new MaxKnownIdsTracker();
        t.observePartitionId(7L);
        t.observePartitionId(2L);
        assertThat(t.maxKnownPartitionId()).isEqualTo(7L);
    }

    @Test
    void independentTracking() {
        MaxKnownIdsTracker t = new MaxKnownIdsTracker();
        t.observeTableId(100L);
        assertThat(t.maxKnownPartitionId()).isEqualTo(-1L);
    }
}
