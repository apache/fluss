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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.metadata.TableBucket;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** The snapshot info for a table. */
public class LakeTableSnapshot {

    // the last committed snapshot id in lake
    private final long snapshotId;
    private final long tableId;

    // the log offset of the bucket

    // mapping from bucket id to log start/end offset,
    // will be null if log offset is unknown such as reading the snapshot of primary key table
    private final Map<TableBucket, Long> bucketLogStartOffset;
    private final Map<TableBucket, Long> bucketLogEndOffset;

    public LakeTableSnapshot(
            long snapshotId,
            long tableId,
            Map<TableBucket, Long> bucketLogStartOffset,
            Map<TableBucket, Long> bucketLogEndOffset) {
        this.snapshotId = snapshotId;
        this.tableId = tableId;
        this.bucketLogStartOffset = bucketLogStartOffset;
        this.bucketLogEndOffset = bucketLogEndOffset;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getTableId() {
        return tableId;
    }

    public void putLogStartOffset(TableBucket tableBucket, Long logStartOffset) {
        bucketLogStartOffset.put(tableBucket, logStartOffset);
    }

    public void putLogEndOffset(TableBucket tableBucket, Long logEndOffset) {
        bucketLogEndOffset.put(tableBucket, logEndOffset);
    }

    public Optional<Long> getLogStartOffset(TableBucket tableBucket) {
        return Optional.ofNullable(bucketLogStartOffset.get(tableBucket));
    }

    public Optional<Long> getLogEndOffset(TableBucket tableBucket) {
        return Optional.ofNullable(bucketLogEndOffset.get(tableBucket));
    }

    public Map<TableBucket, Long> getBucketLogEndOffset() {
        return bucketLogEndOffset;
    }

    public Map<TableBucket, Long> getBucketLogStartOffset() {
        return bucketLogStartOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LakeTableSnapshot)) {
            return false;
        }
        LakeTableSnapshot that = (LakeTableSnapshot) o;
        return snapshotId == that.snapshotId
                && tableId == that.tableId
                && Objects.equals(bucketLogStartOffset, that.bucketLogStartOffset)
                && Objects.equals(bucketLogEndOffset, that.bucketLogEndOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, tableId, bucketLogStartOffset, bucketLogEndOffset);
    }

    @Override
    public String toString() {
        return "LakeTableSnapshot{"
                + "snapshotId="
                + snapshotId
                + ", tableId="
                + tableId
                + ", bucketLogStartOffset="
                + bucketLogStartOffset
                + ", bucketLogEndOffset="
                + bucketLogEndOffset
                + '}';
    }
}
