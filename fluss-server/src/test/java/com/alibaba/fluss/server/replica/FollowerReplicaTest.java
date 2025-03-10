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
import com.alibaba.fluss.server.log.LogOffsetMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.server.log.LocalLog.UNKNOWN_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FollowerReplica}. */
final class FollowerReplicaTest {
    private FollowerReplica followerReplica;

    @BeforeEach
    void setup() {
        int followerId = 0;
        followerReplica = new FollowerReplica(followerId, new TableBucket(DATA1_TABLE_ID, 0));
    }

    @Test
    void testInitialState() {
        assertFollowerReplicaState(UNKNOWN_OFFSET, 0L, 0L, 0L);
    }

    @Test
    void testUpdateFetchState() {
        long fetchTime1 = updateFetchState(5L, 10L);
        assertFollowerReplicaState(5L, 0L, 10L, fetchTime1);

        long fetchTime2 = updateFetchState(10L, 15L);
        assertFollowerReplicaState(10L, fetchTime1, 15L, fetchTime2);

        long fetchTime3 = updateFetchState(15L, 15L);
        assertFollowerReplicaState(15L, fetchTime3, 15L, fetchTime3);
    }

    @Test
    void testRestReplicaStateWhenLeaderIsrReelectedAndReplicaIsInSync() {
        updateFetchState(10L, 10L);
        long resetTime1 = resetReplicaState(11L, false, true);
        assertFollowerReplicaState(10L, resetTime1, 11L, resetTime1);
    }

    @Test
    void testRestReplicaStateWhenLeaderIsrReelectedAndReplicaIsNotInSync() {
        updateFetchState(10L, 10L);
        resetReplicaState(11L, false, false);
        assertFollowerReplicaState(10L, 0L, 11L, 0L);
    }

    @Test
    void testRestReplicaStateWhenNewLeaderIsrElectedAndReplicaIsInSync() {
        updateFetchState(10L, 10L);
        long resetTime1 = resetReplicaState(11L, true, true);
        assertFollowerReplicaState(-1L, resetTime1, -1L, 0L);
    }

    @Test
    void testRestReplicaStateWhenNewLeaderIsrElectedAndReplicaIsNotInSync() {
        updateFetchState(10L, 10L);
        resetReplicaState(11L, true, false);
        assertFollowerReplicaState(-1L, 0L, -1L, 0L);
    }

    @Test
    void testIsCaughtUpWhenReplicaIsCaughtUpToLogEnd() {
        long nowTime = System.currentTimeMillis();
        long replicaMaxLagTime = 5000L;
        assertThat(isCaughtUp(10L, nowTime, replicaMaxLagTime)).isFalse();
        updateFetchState(nowTime, 10L, 10L);
        assertThat(isCaughtUp(10L, nowTime, replicaMaxLagTime)).isTrue();
        nowTime += (replicaMaxLagTime + 1);
        assertThat(isCaughtUp(10L, nowTime, replicaMaxLagTime)).isTrue();
    }

    @Test
    void testIsCaughtUpWhenReplicaIsNotCaughtUpToLogEnd() {
        long nowTime = System.currentTimeMillis();
        long replicaMaxLagTime = 5000L;
        assertThat(isCaughtUp(10L, nowTime, replicaMaxLagTime)).isFalse();
        updateFetchState(nowTime, 5L, 10L);
        assertThat(isCaughtUp(10L, nowTime, replicaMaxLagTime)).isFalse();
        updateFetchState(nowTime, 10L, 15L);
        assertThat(isCaughtUp(16L, nowTime, replicaMaxLagTime)).isTrue();
        nowTime += (replicaMaxLagTime + 1);
        assertThat(isCaughtUp(16L, nowTime, replicaMaxLagTime)).isFalse();
    }

    private void assertFollowerReplicaState(
            long logEndOffset,
            long lastCaughtUpTimeMs,
            long lastFetchLeaderLogEndOffset,
            long lastFetchTimeMs) {
        FollowerReplica.FollowerReplicaState followerReplicaState = followerReplica.stateSnapshot();
        assertThat(followerReplicaState.getLogEndOffset()).isEqualTo(logEndOffset);
        assertThat(followerReplicaState.getLastCaughtUpTimeMs()).isEqualTo(lastCaughtUpTimeMs);
        assertThat(followerReplicaState.getLastFetchLeaderLogEndOffset())
                .isEqualTo(lastFetchLeaderLogEndOffset);
        assertThat(followerReplicaState.getLastFetchTimeMs()).isEqualTo(lastFetchTimeMs);
    }

    private long updateFetchState(long followerFetchOffset, long leaderEndOffset) {
        long currentTimeMillis = System.currentTimeMillis();
        followerReplica.updateFetchState(
                new LogOffsetMetadata(followerFetchOffset), currentTimeMillis, leaderEndOffset);
        return currentTimeMillis;
    }

    private void updateFetchState(
            long currentTimeMs, long followerFetchOffset, long leaderEndOffset) {
        followerReplica.updateFetchState(
                new LogOffsetMetadata(followerFetchOffset), currentTimeMs, leaderEndOffset);
    }

    private long resetReplicaState(
            long leaderEndOffset, boolean isNewLeader, boolean isFollowerInSync) {
        long currentTimeMillis = System.currentTimeMillis();
        followerReplica.resetFollowerReplicaState(
                currentTimeMillis, leaderEndOffset, isNewLeader, isFollowerInSync);
        return currentTimeMillis;
    }

    private boolean isCaughtUp(long leaderEndOffset, long nowTimeMs, long replicaMaxLagTime) {
        return followerReplica
                .stateSnapshot()
                .isCaughtUp(leaderEndOffset, nowTimeMs, replicaMaxLagTime);
    }
}
