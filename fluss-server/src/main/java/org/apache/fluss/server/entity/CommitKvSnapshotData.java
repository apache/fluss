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

import org.apache.fluss.rpc.messages.CommitKvSnapshotRequest;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;

/** The data for request {@link CommitKvSnapshotRequest}. */
public class CommitKvSnapshotData {

    /** The completed snapshot to be added. */
    private final CompletedSnapshot completedSnapshot;

    /** The coordinator epoch when the snapshot is triggered. */
    private final int coordinatorEpoch;

    /** The leader epoch of the bucket when the snapshot is triggered. */
    private final int bucketLeaderEpoch;

    public CommitKvSnapshotData(
            CompletedSnapshot completedSnapshot, int coordinatorEpoch, int bucketLeaderEpoch) {
        this.completedSnapshot = completedSnapshot;
        this.coordinatorEpoch = coordinatorEpoch;
        this.bucketLeaderEpoch = bucketLeaderEpoch;
    }

    public CompletedSnapshot getCompletedSnapshot() {
        return completedSnapshot;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getBucketLeaderEpoch() {
        return bucketLeaderEpoch;
    }
}
