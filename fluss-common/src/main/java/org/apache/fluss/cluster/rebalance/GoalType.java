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
    REPLICA_DISTRIBUTION(0),

    /**
     * Goal to generate leadership movement and leader replica movement tasks to ensure that the
     * number of leader replicas on each tabletServer is near balanced.
     */
    LEADER_DISTRIBUTION(1),

    /**
     * Goal to distribute replicas of each bucket across distinct racks. This does not balance
     * leader counts across racks.
     */
    RACK_AWARE(2),

    /**
     * Goal to bring replica counts independently for each table within a per-server tolerance
     * window, subject to higher-priority goals. This does not guarantee equal counts.
     */
    TABLE_REPLICA_DISTRIBUTION(3),

    /**
     * Goal to bring leader counts independently for each table within a per-server tolerance
     * window, subject to replica placement and higher-priority goals. This does not guarantee equal
     * counts per server or per rack.
     */
    TABLE_LEADER_DISTRIBUTION(4);

    public final int value;

    GoalType(int value) {
        this.value = value;
    }

    public static GoalType valueOf(int value) {
        if (value == REPLICA_DISTRIBUTION.value) {
            return REPLICA_DISTRIBUTION;
        } else if (value == LEADER_DISTRIBUTION.value) {
            return LEADER_DISTRIBUTION;
        } else if (value == RACK_AWARE.value) {
            return RACK_AWARE;
        } else if (value == TABLE_REPLICA_DISTRIBUTION.value) {
            return TABLE_REPLICA_DISTRIBUTION;
        } else if (value == TABLE_LEADER_DISTRIBUTION.value) {
            return TABLE_LEADER_DISTRIBUTION;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Value %s must be one of %s", value, Arrays.asList(GoalType.values())));
        }
    }

    public static GoalType fromName(String name) {
        return valueOf(name.toUpperCase());
    }
}
