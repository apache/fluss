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

package com.alibaba.fluss.rpc.entity;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.OffsetForLeaderEpochRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;

/** Result of {@link OffsetForLeaderEpochRequest} for each table bucket. */
public class EpochAndLogEndOffsetForBucket extends ResultForBucket {

    /** The leaderEpoch of this tableBucket. */
    private final int leaderEpoch;

    /** The logEndOffset of this leaderEpoch. */
    private final long logEndOffset;

    public EpochAndLogEndOffsetForBucket(
            TableBucket tableBucket, int leaderEpoch, long logEndOffset) {
        this(tableBucket, leaderEpoch, logEndOffset, ApiError.NONE);
    }

    public EpochAndLogEndOffsetForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1, -1L, error);
    }

    public EpochAndLogEndOffsetForBucket(
            TableBucket tableBucket, int leaderEpoch, long logEndOffset, ApiError error) {
        super(tableBucket, error);
        this.leaderEpoch = leaderEpoch;
        this.logEndOffset = logEndOffset;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }
}
