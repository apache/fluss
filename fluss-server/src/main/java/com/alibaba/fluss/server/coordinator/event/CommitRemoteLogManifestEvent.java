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

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import com.alibaba.fluss.server.entity.CommitRemoteLogManifestData;

import java.util.concurrent.CompletableFuture;

/** An event for receiving the request of updating remote log metadata to coordinator server. */
public class CommitRemoteLogManifestEvent implements FencedCoordinatorEvent {
    private final CommitRemoteLogManifestData commitRemoteLogManifestData;
    private final CompletableFuture<CommitRemoteLogManifestResponse> respCallback;

    public CommitRemoteLogManifestEvent(
            CommitRemoteLogManifestData commitRemoteLogManifestData,
            CompletableFuture<CommitRemoteLogManifestResponse> respCallback) {
        this.commitRemoteLogManifestData = commitRemoteLogManifestData;
        this.respCallback = respCallback;
    }

    public CommitRemoteLogManifestData getCommitRemoteLogManifestData() {
        return commitRemoteLogManifestData;
    }

    public CompletableFuture<CommitRemoteLogManifestResponse> getRespCallback() {
        return respCallback;
    }

    @Override
    public TableBucket getTableBucket() {
        return commitRemoteLogManifestData.getTableBucket();
    }

    @Override
    public int getCoordinatorEpoch() {
        return commitRemoteLogManifestData.getCoordinatorEpoch();
    }

    @Override
    public int getBucketLeaderEpoch() {
        return commitRemoteLogManifestData.getBucketLeaderEpoch();
    }
}
