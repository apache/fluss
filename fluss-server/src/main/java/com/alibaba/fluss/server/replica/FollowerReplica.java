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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.log.LogOffsetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** The replica to store the state of remote follower replica. */
final class FollowerReplica {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerReplica.class);

    private final int followerId;
    private final TableBucket tableBucket;
    private final AtomicReference<FollowerReplicaState> followerReplicaState =
            new AtomicReference<>(FollowerReplicaState.EMPTY);

    FollowerReplica(int followerId, TableBucket tableBucket) {
        this.followerId = followerId;
        this.tableBucket = tableBucket;
    }

    public int getFollowerId() {
        return followerId;
    }

    FollowerReplicaState stateSnapshot() {
        return followerReplicaState.get();
    }

    /**
     * If the FetchLogRequest reads up to the log end offset of the leader when the current fetch
     * log request is received, set `lastCaughtUpTimeMs` to the time when the current fetch log
     * request was received.
     *
     * <p>Else if the FetchLogRequest reads up to the log end offset of the leader when the previous
     * fetch log request was received, set `lastCaughtUpTimeMs` to the time when the previous fetch
     * log request was received.
     *
     * <p>This is needed to enforce the semantics of ISR, i.e. a replica is in ISR if and only if it
     * lags behind leader's LEO by at most `replicaLagTimeMaxMs`. These semantics allow a follower
     * to be added to the ISR even if the offset of its fetch request is always smaller than the
     * leader's LEO, which can happen if small produceLog/putKv requests are received at high
     * frequency.
     */
    void updateFetchState(
            LogOffsetMetadata followerFetchOffsetMetadata,
            long followerFetchTimeMs,
            long leaderEndOffset) {
        followerReplicaState.updateAndGet(
                currentReplicaState -> {
                    long lastCaughtUpTimeMs = currentReplicaState.getLastCaughtUpTimeMs();
                    if (followerFetchOffsetMetadata.getMessageOffset() >= leaderEndOffset) {
                        lastCaughtUpTimeMs =
                                Math.max(
                                        currentReplicaState.getLastCaughtUpTimeMs(),
                                        followerFetchTimeMs);
                    } else if (followerFetchOffsetMetadata.getMessageOffset()
                            >= currentReplicaState.getLastFetchLeaderLogEndOffset()) {
                        lastCaughtUpTimeMs =
                                Math.max(
                                        currentReplicaState.getLastCaughtUpTimeMs(),
                                        currentReplicaState.getLastFetchTimeMs());
                    }
                    return new FollowerReplicaState(
                            followerFetchOffsetMetadata,
                            Math.max(
                                    leaderEndOffset,
                                    currentReplicaState.getLastFetchLeaderLogEndOffset()),
                            followerFetchTimeMs,
                            lastCaughtUpTimeMs);
                });
    }

    /**
     * When the leader is elected or re-elected, the state of the follower is reinitialized
     * accordingly.
     */
    void resetFollowerReplicaState(
            long currentTimeMs,
            long leaderEndOffset,
            boolean isNewLeader,
            boolean isFollowerInSync) {
        followerReplicaState.updateAndGet(
                currentReplicaState -> {
                    // When the leader is elected or re-elected, the follower's last caught up time
                    // is set to the current time if the follower is in the ISR, else to 0. The
                    // latter is done to ensure that the high watermark is not hold back
                    // unnecessarily for a follower which is not in the ISR anymore.
                    long lastCaughtUpTimeMs = isFollowerInSync ? currentTimeMs : 0L;
                    if (isNewLeader) {
                        return new FollowerReplicaState(
                                LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
                                -1L,
                                0L,
                                lastCaughtUpTimeMs);
                    } else {
                        return new FollowerReplicaState(
                                currentReplicaState.getLogEndOffsetMetadata(),
                                leaderEndOffset,
                                isFollowerInSync ? currentTimeMs : 0L,
                                lastCaughtUpTimeMs);
                    }
                });
        LOG.trace("Reset state of follower replica to  {}", this);
    }

    @Override
    public String toString() {
        FollowerReplicaState state = followerReplicaState.get();
        return "FollowerReplica{"
                + "followerId="
                + followerId
                + ", tableBucket="
                + tableBucket
                + ", followerReplicaState="
                + state
                + '}';
    }

    /** Follower replica state. */
    static class FollowerReplicaState {
        /**
         * The log end offset value of remote follower replica. This value can only update by the
         * follower fetch.
         */
        private final LogOffsetMetadata logEndOffsetMetadata;

        /**
         * The log end offset value at the time the leader received the last FetchRequest from this
         * follower. This is used to determine the lastCaughtUpTimeMs of the follower. It is reset
         * by the leader when a LeaderAndIsr request is received and might be reset when the leader
         * appends a record to its log.
         */
        private final long lastFetchLeaderLogEndOffset;

        /**
         * The time when the leader received the last FetchRequest from this follower. This is used
         * to determine the lastCaughtUpTimeMs of the follower.
         */
        private final long lastFetchTimeMs;

        /**
         * lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest
         * from this follower >= the LEO of leader at time t. This is used to determine the lag of
         * this follower and ISR of this replica.
         */
        private final long lastCaughtUpTimeMs;

        static final FollowerReplicaState EMPTY =
                new FollowerReplicaState(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, 0L, 0L, 0L);

        FollowerReplicaState(
                LogOffsetMetadata logEndOffsetMetadata,
                long lastFetchLeaderLogEndOffset,
                long lastFetchTimeMs,
                long lastCaughtUpTimeMs) {
            this.logEndOffsetMetadata = logEndOffsetMetadata;
            this.lastFetchLeaderLogEndOffset = lastFetchLeaderLogEndOffset;
            this.lastFetchTimeMs = lastFetchTimeMs;
            this.lastCaughtUpTimeMs = lastCaughtUpTimeMs;
        }

        LogOffsetMetadata getLogEndOffsetMetadata() {
            return logEndOffsetMetadata;
        }

        /** Returns the current log end offset of the follower replica. */
        long getLogEndOffset() {
            return logEndOffsetMetadata.getMessageOffset();
        }

        long getLastCaughtUpTimeMs() {
            return lastCaughtUpTimeMs;
        }

        long getLastFetchLeaderLogEndOffset() {
            return lastFetchLeaderLogEndOffset;
        }

        long getLastFetchTimeMs() {
            return lastFetchTimeMs;
        }

        /**
         * Returns true when the follower replica is considered as "caught-up". A follower replica
         * is considered "caught-up" when its log end offset is equals to the log end offset of the
         * leader OR when its last caught up time minus the current time is smaller than the max
         * replica lag.
         */
        boolean isCaughtUp(long leaderEndOffset, long currentTimeMs, long replicaMaxLagMs) {
            return leaderEndOffset == getLogEndOffset()
                    || currentTimeMs - lastCaughtUpTimeMs <= replicaMaxLagMs;
        }

        @Override
        public String toString() {
            return "FollowerReplicaState{"
                    + "logEndOffsetMetadata="
                    + logEndOffsetMetadata
                    + ", lastFetchLeaderLogEndOffset="
                    + lastFetchLeaderLogEndOffset
                    + ", lastFetchTimeMs="
                    + lastFetchTimeMs
                    + ", lastCaughtUpTimeMs="
                    + lastCaughtUpTimeMs
                    + '}';
        }
    }
}
