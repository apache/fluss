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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import org.apache.fluss.server.entity.CommitLakeTableSnapshotData;

import java.util.concurrent.CompletableFuture;

/** An event for receiving the request of commit lakehouse data to coordinator server. */
public class CommitLakeTableSnapshotEvent implements CoordinatorEvent {

    private final CommitLakeTableSnapshotData commitLakeTableSnapshotData;

    private final CompletableFuture<CommitLakeTableSnapshotResponse> respCallback;

    public CommitLakeTableSnapshotEvent(
            CommitLakeTableSnapshotData commitLakeTableSnapshotData,
            CompletableFuture<CommitLakeTableSnapshotResponse> respCallback) {
        this.commitLakeTableSnapshotData = commitLakeTableSnapshotData;
        this.respCallback = respCallback;
    }

    public CommitLakeTableSnapshotData getCommitLakeTableSnapshotData() {
        return commitLakeTableSnapshotData;
    }

    public CompletableFuture<CommitLakeTableSnapshotResponse> getRespCallback() {
        return respCallback;
    }
}
