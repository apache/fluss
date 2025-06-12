/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.committer;

import com.alibaba.fluss.utils.types.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The lake already committed snapshot, containing the lake snapshot id and the bucket end offsets
 * in this snapshot.
 */
public class CommittedLakeSnapshot {

    private final long lakeSnapshotId;
    // <partition_name, bucket> -> log offset, partition_name will be null if it's not a
    // partition bucket
    private final Map<Tuple2<String, Integer>, Long> logEndOffsets = new HashMap<>();

    public CommittedLakeSnapshot(long lakeSnapshotId) {
        this.lakeSnapshotId = lakeSnapshotId;
    }

    public long getLakeSnapshotId() {
        return lakeSnapshotId;
    }

    public void addBucket(int bucketId, long offset) {
        logEndOffsets.put(Tuple2.of(null, bucketId), offset);
    }

    public void addPartitionBucket(String partitionName, int bucketId, long offset) {
        logEndOffsets.put(Tuple2.of(partitionName, bucketId), offset);
    }

    public Map<Tuple2<String, Integer>, Long> getLogEndOffsets() {
        return logEndOffsets;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommittedLakeSnapshot that = (CommittedLakeSnapshot) o;
        return lakeSnapshotId == that.lakeSnapshotId
                && Objects.equals(logEndOffsets, that.logEndOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lakeSnapshotId, logEndOffsets);
    }

    @Override
    public String toString() {
        return "CommittedLakeSnapshot{"
                + "lakeSnapshotId="
                + lakeSnapshotId
                + ", logEndOffsets="
                + logEndOffsets
                + '}';
    }
}
