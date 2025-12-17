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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** A lake snapshot for a Fluss table. */
class FlussTableLakeSnapshot {

    private final long tableId;

    private final long lakeSnapshotId;

    // table_bucket -> log end offsets,
    private final Map<TableBucket, Long> logEndOffsets;

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

    public Set<TableBucket> tableBuckets() {
        return logEndOffsets.keySet();
    }

    public void addBucketOffset(TableBucket bucket, long offset) {
        logEndOffsets.put(bucket, offset);
    }

    public long getLogEndOffset(TableBucket bucket) {
        return logEndOffsets.get(bucket);
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
