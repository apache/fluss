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

package com.alibaba.fluss.server.log;

import javax.annotation.Nullable;

import java.util.OptionalLong;

/** List offsets param. */
public class ListOffsetsParam {
    /**
     * Earliest offset type. If the list offsets request come from client, it represents listing the
     * lowest offset of LocalLogStartOffset and RemoteLogStartOffset. Otherwise, the request come
     * from follower, it represents listing LocalLogStartOffset.
     */
    public static final int EARLIEST_OFFSET_TYPE = 0;
    /**
     * Latest offset type. If the list offsets request come from client, it represents listing
     * HighWatermark. otherwise, the request come from follower, it represents listing
     * LocalLogEndOffset.
     */
    public static final int LATEST_OFFSET_TYPE = 1;

    /**
     * Timestamp offset type. It means list offset based on the startTimestamp. If this type is
     * setting, the {@link #startTimestamp} cannot be null.
     */
    public static final int TIMESTAMP_OFFSET_TYPE = 2;

    /** The offset type indicate the end offset snapshot when become leader. */
    public static final int LEADER_END_OFFSET_SNAPSHOT_TYPE = 3;

    private final int followerServerId;
    private final Integer offsetType;
    private @Nullable final Long startTimestamp;

    public ListOffsetsParam(
            int followerServerId, Integer offsetType, @Nullable Long startTimestamp) {
        this.followerServerId = followerServerId;
        this.offsetType = offsetType;
        this.startTimestamp = startTimestamp;
    }

    public int getFollowerServerId() {
        return followerServerId;
    }

    public int getOffsetType() {
        return offsetType;
    }

    public OptionalLong startTimestamp() {
        return startTimestamp == null ? OptionalLong.empty() : OptionalLong.of(startTimestamp);
    }

    @Override
    public String toString() {
        return "ListOffsetsParam{"
                + "followerServerId="
                + followerServerId
                + ", offsetType="
                + offsetType
                + ", startTimestamp="
                + startTimestamp
                + '}';
    }
}
