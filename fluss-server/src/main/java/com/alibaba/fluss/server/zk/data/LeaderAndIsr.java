/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * The leadership and ISR information of a bucket stored in {@link ZkData.LeaderAndIsrZNode}.
 *
 * @see LeaderAndIsrJsonSerde for json serialization and deserialization.
 */
public class LeaderAndIsr {

    public static final int INITIAL_LEADER_EPOCH = 0;
    public static final int INITIAL_BUCKET_EPOCH = 0;
    public static final int NO_LEADER = -1;

    /** The leader replica id. */
    private final int leader;

    /** The epoch of leader replica. */
    private final int leaderEpoch;

    /** The latest inSyncReplica collection. */
    private final List<Integer> isr;

    /** The coordinator epoch. */
    private final int coordinatorEpoch;

    /**
     * The epoch of the state of the bucket (i.e., the leader and isr information). The epoch is a
     * monotonically increasing value which is incremented after every leaderAndIsr change.
     */
    private final int bucketEpoch;

    public LeaderAndIsr(int leader, int coordinatorEpoch) {
        this(
                leader,
                INITIAL_LEADER_EPOCH,
                new ArrayList<>(),
                coordinatorEpoch,
                INITIAL_BUCKET_EPOCH);
    }

    public LeaderAndIsr(
            int leader, int leaderEpoch, List<Integer> isr, int coordinatorEpoch, int bucketEpoch) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = checkNotNull(isr);
        this.coordinatorEpoch = coordinatorEpoch;
        this.bucketEpoch = bucketEpoch;
    }

    public LeaderAndIsr newLeaderAndIsr(int newLeader, List<Integer> newIsr) {
        return new LeaderAndIsr(newLeader, leaderEpoch, newIsr, coordinatorEpoch, bucketEpoch + 1);
    }

    public int leader() {
        return leader;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public List<Integer> isr() {
        return isr;
    }

    public int[] isrArray() {
        return isr.stream().mapToInt(Integer::intValue).toArray();
    }

    public int bucketEpoch() {
        return bucketEpoch;
    }

    public boolean equalsAllowStalePartitionEpoch(LeaderAndIsr other) {
        return leader == other.leader
                && leaderEpoch == other.leaderEpoch
                && coordinatorEpoch == other.coordinatorEpoch
                && isr.equals(other.isr)
                && bucketEpoch <= other.bucketEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeaderAndIsr that = (LeaderAndIsr) o;
        return leader == that.leader
                && leaderEpoch == that.leaderEpoch
                && coordinatorEpoch == that.coordinatorEpoch
                && bucketEpoch == that.bucketEpoch
                && Objects.equals(isr, that.isr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leader, leaderEpoch, isr, coordinatorEpoch, bucketEpoch);
    }

    @Override
    public String toString() {
        return "LeaderAndIsr{"
                + "leader="
                + leader
                + ", leaderEpoch="
                + leaderEpoch
                + ", isr="
                + isr
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + ", bucketEpoch="
                + bucketEpoch
                + '}';
    }
}
