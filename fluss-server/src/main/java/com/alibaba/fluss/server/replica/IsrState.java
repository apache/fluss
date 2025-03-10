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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import java.util.ArrayList;
import java.util.List;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** The interface to present bucket's isr states. */
public interface IsrState {

    /** Only get the inSync replicas which have been committed to ZK. */
    List<Integer> isr();

    /**
     * This set may include un-committed ISR members following an isr expansion. This "effective"
     * ISR is used for advancing the high watermark as well as determining which replicas are
     * required for acks=all produceLog/putKv requests.
     */
    List<Integer> maximalIsr();

    /** Indicates if we have an AdjustIsr request inflight. */
    boolean isInflight();

    /** Class to represent the committed isr state of a {@link TableBucket}. */
    class CommittedIsrState implements IsrState {
        private final List<Integer> isr;

        public CommittedIsrState(List<Integer> isr) {
            this.isr = isr;
        }

        @Override
        public List<Integer> isr() {
            return isr;
        }

        @Override
        public List<Integer> maximalIsr() {
            return isr;
        }

        @Override
        public boolean isInflight() {
            return false;
        }

        @Override
        public String toString() {
            return "CommittedIsrState{" + "isr=" + isr + '}';
        }
    }

    /**
     * Interface to represent the pending isr state of a {@link TableBucket}, which means isr state
     * is changing.
     */
    interface PendingIsrState extends IsrState {
        /**
         * Get the last committed isr state of this bucket.
         *
         * @return the last committed state of this bucket
         */
        CommittedIsrState lastCommittedState();

        /**
         * Get the sending leader and isr of this bucket.
         *
         * @return the leader and isr of the bucket
         */
        LeaderAndIsr sentLeaderAndIsr();
    }

    /** The pending isr state change happened while expanding isr. */
    class PendingExpandIsrState implements PendingIsrState {

        private final int newInSyncReplicaId;
        private final LeaderAndIsr sentLeaderAndIsr;
        private final CommittedIsrState lastCommittedState;

        public PendingExpandIsrState(
                int newInSyncReplicaId,
                LeaderAndIsr sentLeaderAndIsr,
                CommittedIsrState lastCommittedState) {
            this.newInSyncReplicaId = newInSyncReplicaId;
            this.sentLeaderAndIsr = sentLeaderAndIsr;
            this.lastCommittedState = lastCommittedState;
        }

        @Override
        public CommittedIsrState lastCommittedState() {
            return lastCommittedState;
        }

        @Override
        public LeaderAndIsr sentLeaderAndIsr() {
            return sentLeaderAndIsr;
        }

        @Override
        public List<Integer> isr() {
            return lastCommittedState.isr();
        }

        @Override
        public List<Integer> maximalIsr() {
            ArrayList<Integer> maximalIsr = new ArrayList<>(lastCommittedState.isr());
            maximalIsr.add(newInSyncReplicaId);
            return maximalIsr;
        }

        @Override
        public boolean isInflight() {
            return true;
        }

        @Override
        public String toString() {
            return "PendingExpandIsrState{"
                    + "newInSyncReplicaId="
                    + newInSyncReplicaId
                    + ", sentLeaderAndIsr="
                    + sentLeaderAndIsr
                    + ", lastCommittedState="
                    + lastCommittedState
                    + '}';
        }
    }

    /** The pending isr state change happened while shrinking isr. */
    class PendingShrinkIsrState implements PendingIsrState {

        private final List<Integer> outOfSyncReplicaIds;
        private final LeaderAndIsr sentLeaderAndIsr;
        private final CommittedIsrState lastCommittedState;

        public PendingShrinkIsrState(
                List<Integer> outOfSyncReplicaIds,
                LeaderAndIsr sentLeaderAndIsr,
                CommittedIsrState lastCommittedState) {
            this.outOfSyncReplicaIds = outOfSyncReplicaIds;
            this.sentLeaderAndIsr = sentLeaderAndIsr;
            this.lastCommittedState = lastCommittedState;
        }

        @Override
        public CommittedIsrState lastCommittedState() {
            return lastCommittedState;
        }

        @Override
        public LeaderAndIsr sentLeaderAndIsr() {
            return sentLeaderAndIsr;
        }

        @Override
        public List<Integer> isr() {
            return lastCommittedState.isr();
        }

        @Override
        public List<Integer> maximalIsr() {
            return lastCommittedState.isr();
        }

        @Override
        public boolean isInflight() {
            return true;
        }

        @Override
        public String toString() {
            return "PendingShrinkIsrState{"
                    + "outOfSyncReplicaIds="
                    + outOfSyncReplicaIds
                    + ", sentLeaderAndIsr="
                    + sentLeaderAndIsr
                    + ", lastCommittedState="
                    + lastCommittedState
                    + '}';
        }
    }
}
