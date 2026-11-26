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

import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Publishes the validated snapshot and the bucket's next role in the same transaction. */
public final class CommitBulkLoadBucketEvent implements CoordinatorEventWithResult<Void> {
    public final CompletedSnapshot snapshot;
    public final VersionedData<BulkLoadTransaction> transaction;
    public final VersionedData<BulkLoadTarget> target;
    public final int tableVersion;
    public final @Nullable Integer partitionVersion;
    public final int coordinatorEpochVersion;
    public final CompletedSnapshotStore snapshotStore;
    private final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

    public CommitBulkLoadBucketEvent(
            CompletedSnapshot snapshot,
            VersionedData<BulkLoadTransaction> transaction,
            VersionedData<BulkLoadTarget> target,
            int tableVersion,
            @Nullable Integer partitionVersion,
            int coordinatorEpochVersion,
            CompletedSnapshotStore snapshotStore) {
        this.snapshotStore = snapshotStore;
        this.snapshot = snapshot;
        this.transaction = transaction;
        this.target = target;
        this.tableVersion = tableVersion;
        this.partitionVersion = partitionVersion;
        this.coordinatorEpochVersion = coordinatorEpochVersion;
    }

    @Override
    public CompletableFuture<Void> getResultFuture() {
        return resultFuture;
    }
}
