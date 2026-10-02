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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Per-bucket active KV snapshot ids for a table or partition scope. The active set is the union of
 * retained snapshots and lease-pinned (still-in-use) snapshots reported by the coordinator.
 *
 * @since 1.0
 */
@PublicEvolving
public final class ActiveKvSnapshots {

    private final long tableId;
    @Nullable private final Long partitionId;
    private final Map<Integer, Set<Long>> snapshotIdsByBucket;

    public ActiveKvSnapshots(
            long tableId, @Nullable Long partitionId, Map<Integer, Set<Long>> snapshotIdsByBucket) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        Map<Integer, Set<Long>> copy = new HashMap<>();
        for (Map.Entry<Integer, Set<Long>> entry : snapshotIdsByBucket.entrySet()) {
            copy.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
        }
        this.snapshotIdsByBucket = Collections.unmodifiableMap(copy);
    }

    public long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    public Map<Integer, Set<Long>> getSnapshotIdsByBucket() {
        return snapshotIdsByBucket;
    }
}
