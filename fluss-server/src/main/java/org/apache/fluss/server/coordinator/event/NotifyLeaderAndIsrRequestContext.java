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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.server.coordinator.rebalance.RebalanceExecutionKey;

import javax.annotation.Nullable;

import java.util.Objects;

/** The coordinator and bucket epochs of a sent NotifyLeaderAndIsr request. */
public final class NotifyLeaderAndIsrRequestContext {

    private final int coordinatorEpoch;
    private final int leader;
    private final int leaderEpoch;
    private final int bucketEpoch;
    private final @Nullable RebalanceExecutionKey rebalanceExecutionKey;

    public NotifyLeaderAndIsrRequestContext(
            int coordinatorEpoch, int leader, int leaderEpoch, int bucketEpoch) {
        this(coordinatorEpoch, leader, leaderEpoch, bucketEpoch, null);
    }

    public NotifyLeaderAndIsrRequestContext(
            int coordinatorEpoch,
            int leader,
            int leaderEpoch,
            int bucketEpoch,
            @Nullable RebalanceExecutionKey rebalanceExecutionKey) {
        this.coordinatorEpoch = coordinatorEpoch;
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.bucketEpoch = bucketEpoch;
        this.rebalanceExecutionKey = rebalanceExecutionKey;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getLeader() {
        return leader;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public int getBucketEpoch() {
        return bucketEpoch;
    }

    public @Nullable RebalanceExecutionKey getRebalanceExecutionKey() {
        return rebalanceExecutionKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NotifyLeaderAndIsrRequestContext that = (NotifyLeaderAndIsrRequestContext) o;
        return coordinatorEpoch == that.coordinatorEpoch
                && leader == that.leader
                && leaderEpoch == that.leaderEpoch
                && bucketEpoch == that.bucketEpoch
                && Objects.equals(rebalanceExecutionKey, that.rebalanceExecutionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                coordinatorEpoch, leader, leaderEpoch, bucketEpoch, rebalanceExecutionKey);
    }

    @Override
    public String toString() {
        return "NotifyLeaderAndIsrRequestContext{"
                + "coordinatorEpoch="
                + coordinatorEpoch
                + ", leader="
                + leader
                + ", leaderEpoch="
                + leaderEpoch
                + ", bucketEpoch="
                + bucketEpoch
                + ", rebalanceExecutionKey="
                + rebalanceExecutionKey
                + '}';
    }
}
