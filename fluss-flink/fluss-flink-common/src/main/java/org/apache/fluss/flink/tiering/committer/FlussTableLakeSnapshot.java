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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.types.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** A lake snapshot for a Fluss table. */
class FlussTableLakeSnapshot {

    private final long tableId;

    private final long lakeSnapshotId;

    // <table_bucket, partition_name> -> log end offsets,
    // if the bucket is not of a partition, the partition_name is null
    private final Map<Tuple2<TableBucket, String>, Long> logEndOffsets;

    // <table_bucket, partition_name> -> max timestamps,
    // if the bucket is not of a partition, the partition_name is null
    private final Map<Tuple2<TableBucket, String>, Long> maxTimestamps;

    FlussTableLakeSnapshot(long tableId, long lakeSnapshotId) {
        this.tableId = tableId;
        this.lakeSnapshotId = lakeSnapshotId;
        this.logEndOffsets = new HashMap<>();
        this.maxTimestamps = new HashMap<>();
    }

    public long tableId() {
        return tableId;
    }

    public long lakeSnapshotId() {
        return lakeSnapshotId;
    }

    public Set<Tuple2<TableBucket, String>> tablePartitionBuckets() {
        return logEndOffsets.keySet();
    }

    public void addBucketOffsetAndTimestamp(TableBucket bucket, long offset, long timestamp) {
        logEndOffsets.put(Tuple2.of(bucket, null), offset);
        maxTimestamps.put(Tuple2.of(bucket, null), timestamp);
    }

    public void addPartitionBucketOffsetAndTimestamp(
            TableBucket bucket, String partitionName, long offset, long timestamp) {
        logEndOffsets.put(Tuple2.of(bucket, partitionName), offset);
        maxTimestamps.put(Tuple2.of(bucket, partitionName), timestamp);
    }

    public long getLogEndOffset(Tuple2<TableBucket, String> bucketPartition) {
        return logEndOffsets.get(bucketPartition);
    }

    public long getMaxTimestamp(Tuple2<TableBucket, String> bucketPartition) {
        return maxTimestamps.get(bucketPartition);
    }

    @Override
    public String toString() {
        return "FlussTableLakeSnapshot{"
                + "tableId="
                + tableId
                + ", lakeSnapshotId="
                + lakeSnapshotId
                + ", logEndOffsets="
                + logEndOffsets
                + ", maxTimestamps="
                + maxTimestamps
                + '}';
    }
}
