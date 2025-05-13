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

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.exception.FencedLeaderEpochException;
import com.alibaba.fluss.exception.UnknownLeaderEpochException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.OffsetForLeaderEpochRequest;

import java.util.Objects;

/** The data for request {@link OffsetForLeaderEpochRequest}. */
public class OffsetForLeaderEpochData {

    private final TableBucket tableBucket;

    /**
     * An epoch used to fence clients/followers with old metadata. If the leaderEpoch provided by
     * the client or follower is larger than the current leaderEpoch known to the tabletServer, then
     * the {@link UnknownLeaderEpochException} will be thrown. If the leaderEpoch provided by the
     * client or follower is smaller than the current epoch known to the requested tabletServer,
     * then the {@link FencedLeaderEpochException} will be thrown.
     */
    private final int currentLeaderEpoch;

    /** The epoch to look up an offset for. */
    private final int leaderEpoch;

    public OffsetForLeaderEpochData(
            TableBucket tableBucket, int currentLeaderEpoch, int leaderEpoch) {
        this.tableBucket = tableBucket;
        this.currentLeaderEpoch = currentLeaderEpoch;
        this.leaderEpoch = leaderEpoch;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public int getCurrentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetForLeaderEpochData that = (OffsetForLeaderEpochData) o;
        return currentLeaderEpoch == that.currentLeaderEpoch
                && leaderEpoch == that.leaderEpoch
                && tableBucket.equals(that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, currentLeaderEpoch, leaderEpoch);
    }

    @Override
    public String toString() {
        return "OffsetForLeaderEpochData{"
                + "tableBucket="
                + tableBucket
                + ", currentLeaderEpoch="
                + currentLeaderEpoch
                + ", leaderEpoch="
                + leaderEpoch
                + '}';
    }
}
