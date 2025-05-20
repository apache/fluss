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

package com.alibaba.fluss.flink.tiering.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The table split for tiering service. It's used to describe the snapshot data of a KV table
 * bucket.
 */
public class TieringKvSplit extends TieringSplit {

    private static final String TIERING_KV_SPLIT_PREFIX = "tiering-kv-split-";

    /** The snapshot id. It's used to identify the snapshot for a kv bucket. */
    private final long snapshotId;

    /** The log offset corresponding to the KV table bucket snapshot finished. */
    private final long logOffsetOfSnapshot;

    /** The records to skip when reading the snapshot. */
    private final long recordsToSkip;

    public TieringKvSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long logOffsetOfSnapshot,
            long recordsToSkip) {
        super(tablePath, tableBucket, partitionName);
        this.snapshotId = snapshotId;
        this.logOffsetOfSnapshot = logOffsetOfSnapshot;
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_KV_SPLIT_PREFIX, this.tableBucket);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getLogOffsetOfSnapshot() {
        return logOffsetOfSnapshot;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public String toString() {
        return "TieringKvSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", snapshotId="
                + snapshotId
                + ", logOffsetOfSnapshot="
                + logOffsetOfSnapshot
                + ", recordsToSkip="
                + recordsToSkip
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringKvSplit)) {
            return false;
        }
        TieringKvSplit that = (TieringKvSplit) object;
        return snapshotId == that.snapshotId
                && logOffsetOfSnapshot == that.logOffsetOfSnapshot
                && recordsToSkip == that.recordsToSkip;
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, logOffsetOfSnapshot, recordsToSkip);
    }
}
