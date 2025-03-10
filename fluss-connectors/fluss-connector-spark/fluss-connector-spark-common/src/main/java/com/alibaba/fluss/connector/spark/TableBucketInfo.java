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

package com.alibaba.fluss.connector.spark;

import com.alibaba.fluss.metadata.TableBucket;

public class TableBucketInfo {
    private TableBucket tableBucket;
    private String partitionName;
    private long snapshotId;

    public TableBucketInfo(TableBucket tableBucket, String partitionName, long snapshotId) {
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.snapshotId = snapshotId;
    }

    public TableBucketInfo(TableBucket tableBucket) {
        this(tableBucket, null, 0);
    }

    public TableBucketInfo(TableBucket tableBucket, String partitionName) {
        this(tableBucket, partitionName, 0);
    }

    public boolean isBatch() {
        return snapshotId != 0;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getSnapshotId() {
        return snapshotId;
    }
}
