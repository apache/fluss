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

package org.apache.fluss.server.zk.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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

    /** The latest standbyReplica collection. */
    private final List<Integer> standbyReplicas;

    /** The latest inSyncStandby replica collection. * */
    private final List<Integer> issr;

    /**
     * The epoch of the state of the bucket (i.e., the leader and isr information). The epoch is a
     * monotonically increasing value which is incremented after every leaderAndIsr change.
     */
    private final int bucketEpoch;

    private LeaderAndIsr(
            int leader,
            int leaderEpoch,
            List<Integer> isr,
            int coordinatorEpoch,
            int bucketEpoch,
            List<Integer> standbyReplicas,
            List<Integer> issr) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = checkNotNull(isr);
        this.coordinatorEpoch = coordinatorEpoch;
        this.bucketEpoch = bucketEpoch;
        this.standbyReplicas = checkNotNull(standbyReplicas);
        this.issr = checkNotNull(issr);
    }

    public LeaderAndIsr newLeaderAndIsr(int newLeader, List<Integer> newIsr) {
        return new LeaderAndIsr(
                newLeader,
                leaderEpoch,
                newIsr,
                coordinatorEpoch,
                bucketEpoch + 1,
                Collections.emptyList(),
                Collections.emptyList());
    }

    public LeaderAndIsr newLeaderAndIsr(List<Integer> standbyReplicas, List<Integer> issr) {
        return new LeaderAndIsr(
                leader, leaderEpoch, isr, coordinatorEpoch, bucketEpoch + 1, standbyReplicas, issr);
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

    public List<Integer> standbyList() {
        return standbyReplicas;
    }

    public List<Integer> issr() {
        return issr;
    }

    public int[] standbyArray() {
        return standbyReplicas.stream().mapToInt(Integer::intValue).toArray();
    }

    public int[] issrArray() {
        return issr.stream().mapToInt(Integer::intValue).toArray();
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
                && Objects.equals(isr, that.isr)
                && Objects.equals(standbyReplicas, that.standbyReplicas)
                && Objects.equals(issr, that.issr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                leader, leaderEpoch, isr, coordinatorEpoch, bucketEpoch, standbyReplicas, issr);
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
                + ", standbyReplicas="
                + standbyReplicas
                + ", issr="
                + issr
                + '}';
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link LeaderAndIsr}. */
    public static class Builder {
        private int leader;
        private int leaderEpoch;
        private List<Integer> isr;
        private int coordinatorEpoch;
        private int bucketEpoch;
        private List<Integer> standbyReplicas;
        private List<Integer> issr;

        public Builder() {
            this.leader = NO_LEADER;
            this.leaderEpoch = INITIAL_LEADER_EPOCH;
            this.isr = new ArrayList<>();
            this.coordinatorEpoch = INITIAL_COORDINATOR_EPOCH;
            this.bucketEpoch = INITIAL_BUCKET_EPOCH;
            this.standbyReplicas = new ArrayList<>();
            this.issr = new ArrayList<>();
        }

        public Builder leader(int leader) {
            this.leader = leader;
            return this;
        }

        public Builder leaderEpoch(int leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
            return this;
        }

        public Builder isr(List<Integer> isr) {
            this.isr = isr;
            return this;
        }

        public Builder coordinatorEpoch(int coordinatorEpoch) {
            this.coordinatorEpoch = coordinatorEpoch;
            return this;
        }

        public Builder bucketEpoch(int bucketEpoch) {
            this.bucketEpoch = bucketEpoch;
            return this;
        }

        public Builder standbyReplicas(List<Integer> hotStandbyReplicas) {
            this.standbyReplicas = hotStandbyReplicas;
            return this;
        }

        public Builder issr(List<Integer> issr) {
            this.issr = issr;
            return this;
        }

        public LeaderAndIsr build() {
            return new LeaderAndIsr(
                    leader, leaderEpoch, isr, coordinatorEpoch, bucketEpoch, standbyReplicas, issr);
        }
    }
}
