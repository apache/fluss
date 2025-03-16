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

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;

/** The data for request {@link NotifyRemoteLogOffsetsRequest}. */
public class NotifyRemoteLogOffsetsData {
    private final TableBucket tableBucket;
    private final long remoteLogStartOffset;
    private final long remoteLogEndOffset;
    private final int coordinatorEpoch;

    public NotifyRemoteLogOffsetsData(
            TableBucket tableBucket,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            int coordinatorEpoch) {
        this.tableBucket = tableBucket;
        this.remoteLogStartOffset = remoteLogStartOffset;
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getRemoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public String toString() {
        return "NotifyRemoteLogOffsetsData{"
                + "tableBucket="
                + tableBucket
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + '}';
    }
}
