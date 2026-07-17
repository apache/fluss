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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The hybrid split for first reading the snapshot files and then switch to read the cdc log from a
 * specified offset.
 *
 * <p>Only used for primary key table which will be of snapshot phase and incremental phase of
 * reading.
 */
public class HybridSnapshotLogSplit extends SnapshotSplit {

    public static final long NO_SNAPSHOT_ID = -1L;

    private static final String HYBRID_SPLIT_PREFIX = "hybrid-snapshot-log-";
    private final boolean isSnapshotFinished;
    private final long logStartingOffset;
    private final long logStoppingOffset;
    private final boolean isBatch;

    public HybridSnapshotLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long recordsToSkip,
            boolean isSnapshotFinished,
            long logStartingOffset,
            long logStoppingOffset,
            boolean isBatch) {
        super(tableBucket, partitionName, snapshotId, recordsToSkip);
        checkArgument(
                !isBatch || logStoppingOffset >= 0,
                "Batch hybrid snapshot log split must have a non-negative stopping offset.");
        this.isSnapshotFinished = isSnapshotFinished;
        this.logStartingOffset = logStartingOffset;
        this.logStoppingOffset = logStoppingOffset;
        this.isBatch = isBatch;
    }

    public long getLogStartingOffset() {
        return logStartingOffset;
    }

    public Optional<Long> getLogStoppingOffset() {
        return logStoppingOffset >= 0 ? Optional.of(logStoppingOffset) : Optional.empty();
    }

    public boolean isBatch() {
        return isBatch;
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
                && logStartingOffset == that.logStartingOffset
                && logStoppingOffset == that.logStoppingOffset
                && isBatch == that.isBatch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                isSnapshotFinished,
                logStartingOffset,
                logStoppingOffset,
                isBatch);
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
                + ", isSnapshotFinished="
                + isSnapshotFinished
                + ", logStartingOffset="
                + logStartingOffset
                + ", logStoppingOffset="
                + logStoppingOffset
                + ", isBatch="
                + isBatch
                + ", recordsToSkip="
                + recordsToSkip
                + '}';
    }
}
