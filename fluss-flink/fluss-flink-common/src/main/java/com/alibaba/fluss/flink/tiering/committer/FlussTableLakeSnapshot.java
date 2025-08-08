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

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.types.Tuple2;

import java.util.HashMap;
import java.util.Map;

/** A lake snapshot for a Fluss table. */
class FlussTableLakeSnapshot {

    private final long tableId;

    private final long lakeSnapshotId;

    // <table_bucket, partition_name> -> log end offsets,
    // if the bucket is not of a partition, the partition_name is null
    private final Map<Tuple2<TableBucket, String>, Long> logEndOffsets;

    FlussTableLakeSnapshot(long tableId, long lakeSnapshotId) {
        this.tableId = tableId;
        this.lakeSnapshotId = lakeSnapshotId;
        this.logEndOffsets = new HashMap<>();
    }

    public long tableId() {
        return tableId;
    }

    public long lakeSnapshotId() {
        return lakeSnapshotId;
    }

    public Map<Tuple2<TableBucket, String>, Long> logEndOffsets() {
        return logEndOffsets;
    }

    public void addBucketOffset(TableBucket bucket, long offset) {
        logEndOffsets.put(Tuple2.of(bucket, null), offset);
    }

    public void addPartitionBucketOffset(TableBucket bucket, String partitionName, long offset) {
        logEndOffsets.put(Tuple2.of(bucket, partitionName), offset);
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
                + '}';
    }
}
