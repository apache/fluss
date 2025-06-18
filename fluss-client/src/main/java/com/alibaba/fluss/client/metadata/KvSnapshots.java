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

package com.alibaba.fluss.client.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A class representing the kv snapshots of a table or a partition. It contains multiple snapshots
 * for each kv tablet (bucket).
 *
 * @since 0.6
 */
@PublicEvolving
public class KvSnapshots {

    private final long tableId;

    @Nullable private final Long partitionId;

    // map from bucket id to the latest snapshot id,
    // null or empty if there is no snapshot for this bucket
    private final Map<Integer, Long> snapshotIds;

    // map from bucket id to the log offset to read after the snapshot,
    // null or empty if there is no snapshot for this bucket,
    // then read from EARLIEST offset
    private final Map<Integer, Long> logOffsets;

    public KvSnapshots(
            long tableId,
            @Nullable Long partitionId,
            Map<Integer, Long> snapshotIds,
            Map<Integer, Long> logOffsets) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.snapshotIds = snapshotIds;
        this.logOffsets = logOffsets;
    }

    public long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    public Set<Integer> getBucketIds() {
        return snapshotIds.keySet();
    }

    /**
     * Get the latest snapshot id for this kv tablet (bucket), or empty if there are no snapshots.
     */
    public OptionalLong getSnapshotId(int bucketId) {
        Long snapshotId = snapshotIds.get(bucketId);
        return snapshotId == null ? OptionalLong.empty() : OptionalLong.of(snapshotId);
    }

    /**
     * Get the log offset to read after the snapshot for this kv tablet (bucket), or empty if there
     * are no snapshots.
     */
    public OptionalLong getLogOffset(int bucketId) {
        Long logOffset = logOffsets.get(bucketId);
        return logOffset == null ? OptionalLong.empty() : OptionalLong.of(logOffset);
    }
}
