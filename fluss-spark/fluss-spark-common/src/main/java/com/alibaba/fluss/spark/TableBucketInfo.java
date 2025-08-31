/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark;

import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The table bucket info can be used to first read the snapshot file (snapshotId exists) and then
 * switch to read the cdc log from a specified offset with {Map[TableBucketInfo, Long]}.
 */
public class TableBucketInfo implements Serializable {
    private final TableBucket tableBucket;
    private final String partitionName;
    private Long snapshotId;

    public TableBucketInfo() {
        this(null, null, null);
    }

    public TableBucketInfo(
            TableBucket tableBucket, @Nullable String partitionName, @Nullable Long snapshotId) {
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.snapshotId = snapshotId;
    }

    public TableBucketInfo(TableBucket tableBucket) {
        this(tableBucket, null, null);
    }

    public TableBucketInfo(TableBucket tableBucket, String partitionName) {
        this(tableBucket, partitionName, null);
    }

    public boolean isBatch() {
        return snapshotId != null;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public Long getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(Long snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    public String toString() {
        return "TableBucketInfo{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", snapshotId="
                + snapshotId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableBucketInfo that = (TableBucketInfo) o;
        return Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(partitionName, that.partitionName)
                && Objects.equals(snapshotId, that.snapshotId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, partitionName, snapshotId);
    }
}
