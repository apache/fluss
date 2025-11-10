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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;

import javax.annotation.Nullable;

/** The data for request {@link NotifyRemoteLogOffsetsRequest}. */
public class NotifyKvSnapshotOffsetData {
    private final TableBucket tableBucket;
    private final long minRetainOffset;
    private final int coordinatorEpoch;
    private final @Nullable Long snapshotId;

    public NotifyKvSnapshotOffsetData(
            TableBucket tableBucket,
            long minRetainOffset,
            int coordinatorEpoch,
            @Nullable Long snapshotId) {
        this.tableBucket = tableBucket;
        this.minRetainOffset = minRetainOffset;
        this.coordinatorEpoch = coordinatorEpoch;
        this.snapshotId = snapshotId;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getMinRetainOffset() {
        return minRetainOffset;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Nullable
    public Long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return "NotifyRemoteLogOffsetsData{"
                + "tableBucket="
                + tableBucket
                + ", minRetainOffset="
                + minRetainOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + ", snapshotId="
                + snapshotId
                + '}';
    }
}
