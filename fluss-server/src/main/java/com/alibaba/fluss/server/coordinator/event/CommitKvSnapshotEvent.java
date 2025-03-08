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

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotResponse;
import com.alibaba.fluss.server.entity.CommitKvSnapshotData;

import java.util.concurrent.CompletableFuture;

/** An event for receiving the request of commiting a completed snapshot to coordinator server. */
public class CommitKvSnapshotEvent implements FencedCoordinatorEvent {

    private final CommitKvSnapshotData commitKvSnapshotData;

    private final CompletableFuture<CommitKvSnapshotResponse> respCallback;

    public CommitKvSnapshotEvent(
            CommitKvSnapshotData commitKvSnapshotData,
            CompletableFuture<CommitKvSnapshotResponse> respCallback) {
        this.commitKvSnapshotData = commitKvSnapshotData;
        this.respCallback = respCallback;
    }

    public CommitKvSnapshotData getAddCompletedSnapshotData() {
        return commitKvSnapshotData;
    }

    public CompletableFuture<CommitKvSnapshotResponse> getRespCallback() {
        return respCallback;
    }

    @Override
    public TableBucket getTableBucket() {
        return commitKvSnapshotData.getCompletedSnapshot().getTableBucket();
    }

    @Override
    public int getCoordinatorEpoch() {
        return commitKvSnapshotData.getCoordinatorEpoch();
    }

    @Override
    public int getBucketLeaderEpoch() {
        return commitKvSnapshotData.getBucketLeaderEpoch();
    }
}
