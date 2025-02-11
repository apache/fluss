/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.split;

import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The hybrid split for first reading the snapshot files and then switch to read the cdc log from a
 * specified offset.
 *
 * <p>Only used for primary key table which will be of snapshot phase and incremental phase of
 * reading.
 */
public class HybridSnapshotLogSplit extends SnapshotSplit {

    private static final String HYBRID_SPLIT_PREFIX = "hybrid-snapshot-log-";
    private final boolean isSnapshotFinished;
    private final long logStartingOffset;

    public HybridSnapshotLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long logStartingOffset) {
        this(tableBucket, partitionName, snapshotId, 0, false, logStartingOffset);
    }

    public HybridSnapshotLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long recordsToSkip,
            boolean isSnapshotFinished,
            long logStartingOffset) {
        super(tableBucket, partitionName, snapshotId, recordsToSkip);
        this.isSnapshotFinished = isSnapshotFinished;
        this.logStartingOffset = logStartingOffset;
    }

    public long getLogStartingOffset() {
        return logStartingOffset;
    }

    public boolean isSnapshotFinished() {
        return isSnapshotFinished;
    }

    @Override
    public String splitId() {
        return toSplitId(HYBRID_SPLIT_PREFIX, tableBucket);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HybridSnapshotLogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        HybridSnapshotLogSplit that = (HybridSnapshotLogSplit) o;
        return isSnapshotFinished == that.isSnapshotFinished
                && logStartingOffset == that.logStartingOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isSnapshotFinished, logStartingOffset);
    }

    @Override
    public String toString() {
        return "HybridSnapshotLogSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + "', snapshotId="
                + snapshotId
                + "isSnapshotFinished="
                + isSnapshotFinished
                + ", logStartingOffset="
                + logStartingOffset
                + ", recordsToSkip="
                + recordsToSkip
                + '}';
    }
}
