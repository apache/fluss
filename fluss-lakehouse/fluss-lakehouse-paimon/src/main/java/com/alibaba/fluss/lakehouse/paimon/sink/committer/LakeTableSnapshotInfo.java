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

package com.alibaba.fluss.lakehouse.paimon.sink.committer;

import com.alibaba.fluss.metadata.TableBucket;

import java.util.Map;

/** The snapshots of tables that has been written to. */
public class LakeTableSnapshotInfo {

    // tableId -> snapshotId
    Map<Long, Long> snapshotIdByTableId;

    // tableId -> <bucket, log start offset>
    Map<Long, Map<TableBucket, Long>> logStartOffsetByTableId;
    // tableId -> <bucket, log end offset>
    Map<Long, Map<TableBucket, Long>> logEndOffsetByTableId;

    public LakeTableSnapshotInfo(
            Map<Long, Long> snapshotIdByTableId,
            Map<Long, Map<TableBucket, Long>> logStartOffsetByTableId,
            Map<Long, Map<TableBucket, Long>> logEndOffsetByTableId) {
        this.snapshotIdByTableId = snapshotIdByTableId;
        this.logStartOffsetByTableId = logStartOffsetByTableId;
        this.logEndOffsetByTableId = logEndOffsetByTableId;
    }

    public Map<Long, Long> getSnapshotIdByTableId() {
        return snapshotIdByTableId;
    }

    public Map<Long, Map<TableBucket, Long>> getLogStartOffsetByTableId() {
        return logStartOffsetByTableId;
    }

    public Map<Long, Map<TableBucket, Long>> getLogEndOffsetByTableId() {
        return logEndOffsetByTableId;
    }
}
