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

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Arrays;

/**
 * The type of goal to optimize.
 *
 * @since 0.9
 */
@PublicEvolving
public enum GoalType {
    /**
     * Goal to generate replica movement tasks to ensure that the number of replicas on each
     * tabletServer is near balanced.
     */
    REPLICA_DISTRIBUTION_GOAL(0),

    /**
     * Goal to generate leadership movement and leader replica movement tasks to ensure that the
     * number of leader replicas on each tabletServer is near balanced.
     */
    LEADER_DISTRIBUTION_GOAL(1);

    public final int value;

    GoalType(int value) {
        this.value = value;
    }

    public static GoalType valueOf(int value) {
        if (value == REPLICA_DISTRIBUTION_GOAL.value) {
            return REPLICA_DISTRIBUTION_GOAL;
        } else if (value == LEADER_DISTRIBUTION_GOAL.value) {
            return LEADER_DISTRIBUTION_GOAL;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Value %s must be one of %s", value, Arrays.asList(GoalType.values())));
        }
    }
}
