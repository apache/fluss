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

package org.apache.fluss.cluster.rebalance;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GoalType}. */
public class GoalTypeTest {

    @Test
    void testValueOfIntForAllGoalTypes() {
        assertThat(GoalType.valueOf(0)).isEqualTo(GoalType.REPLICA_DISTRIBUTION);
        assertThat(GoalType.valueOf(1)).isEqualTo(GoalType.LEADER_DISTRIBUTION);
        assertThat(GoalType.valueOf(2)).isEqualTo(GoalType.RACK_AWARE);
        assertThat(GoalType.valueOf(3)).isEqualTo(GoalType.TABLE_REPLICA_DISTRIBUTION);
        assertThat(GoalType.valueOf(4)).isEqualTo(GoalType.TABLE_LEADER_DISTRIBUTION);

        for (GoalType goalType : GoalType.values()) {
            assertThat(GoalType.valueOf(goalType.value)).isEqualTo(goalType);
        }
    }

    @Test
    void testFromNameForAllGoalTypes() {
        for (GoalType goalType : GoalType.values()) {
            assertThat(GoalType.fromName(goalType.name().toLowerCase())).isEqualTo(goalType);
        }
    }
}
