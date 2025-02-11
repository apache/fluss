/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.metadata.TableBucket;

import java.util.Map;

/**
 * A class representing the lake snapshot information of a table. It contains:
 * <li>The lake storage info
 * <li>The snapshot id and the log offset for each bucket.
 *
 * @since 0.3
 */
@PublicEvolving
public class LakeSnapshot {

    private final LakeStorageInfo lakeStorageInfo;

    private final long snapshotId;

    // the specific log offset of the snapshot
    private final Map<TableBucket, Long> tableBucketsOffset;

    public LakeSnapshot(
            LakeStorageInfo lakeStorageInfo,
            long snapshotId,
            Map<TableBucket, Long> tableBucketsOffset) {
        this.lakeStorageInfo = lakeStorageInfo;
        this.snapshotId = snapshotId;
        this.tableBucketsOffset = tableBucketsOffset;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public LakeStorageInfo getLakeStorageInfo() {
        return lakeStorageInfo;
    }

    public Map<TableBucket, Long> getTableBucketsOffset() {
        return tableBucketsOffset;
    }
}
