/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The result of a lake commit operation, containing the committed snapshot ID and the readable
 * snapshot information.
 *
 * <p>For most implementations, the readable snapshot is the same as the committed snapshot, and the
 * readable log offsets are the same as the tiered offsets from TieringCommitOperator.
 *
 * <p>For Paimon DV tables, the readable snapshot will be different from the committed snapshot, and
 * the log end offsets will be different as well (based on compaction status).
 *
 * @since 0.9
 */
@PublicEvolving
public class LakeCommitResult {

    // -1 to enforce to keep all previous snapshots
    public static final Long KEEP_ALL_PREVIOUS = -1L;

    // The snapshot ID that was just committed
    private final long committedSnapshotId;

    private final boolean committedIsReadable;

    // The earliest snapshot ID to keep, null means not to keep any previous snapshot
    @Nullable private final Long earliestSnapshotIDToKeep;

    // the readable snapshot, null if
    // 1: the readable snapshot is unknown,
    // 2: committedIsReadable is true, committed snapshot is just also readable
    @Nullable private final ReadableSnapshot readableSnapshot;

    private LakeCommitResult(
            long committedSnapshotId,
            boolean committedIsReadable,
            @Nullable ReadableSnapshot readableSnapshot,
            @Nullable Long earliestSnapshotIDToKeep) {
        this.committedSnapshotId = committedSnapshotId;
        this.committedIsReadable = committedIsReadable;
        this.readableSnapshot = readableSnapshot;
        this.earliestSnapshotIDToKeep = earliestSnapshotIDToKeep;
    }

    public static LakeCommitResult committedIsReadable(long committedSnapshotId) {
        return new LakeCommitResult(committedSnapshotId, true, null, null);
    }

    public static LakeCommitResult unknownReadableSnapshot(long committedSnapshotId) {
        return new LakeCommitResult(committedSnapshotId, false, null, KEEP_ALL_PREVIOUS);
    }

    public static LakeCommitResult withReadableSnapshot(
            long committedSnapshotId,
            // the readable snapshot id
            long readableSnapshotId,
            // the tiered log end offset for readable snapshot
            Map<TableBucket, Long> tieredLogEndOffsets,
            // the readable log end offset for readable snapshot
            Map<TableBucket, Long> readableLogEndOffsets,
            @Nullable Long earliestSnapshotIDToKeep) {
        return new LakeCommitResult(
                committedSnapshotId,
                false,
                new ReadableSnapshot(
                        readableSnapshotId, tieredLogEndOffsets, readableLogEndOffsets),
                earliestSnapshotIDToKeep);
    }

    public long getCommittedSnapshotId() {
        return committedSnapshotId;
    }

    public boolean committedIsReadable() {
        return committedIsReadable;
    }

    @Nullable
    public ReadableSnapshot getReadableSnapshot() {
        return readableSnapshot;
    }

    /**
     * Gets the earliest snapshot ID to keep.
     *
     * @return the earliest snapshot ID to keep
     */
    @Nullable
    public Long getEarliestSnapshotIDToKeep() {
        return earliestSnapshotIDToKeep;
    }

    /**
     * Represents the information about a snapshot that is considered "readable".
     *
     * <p>In lake storage, a snapshot might be physically committed but not yet fully consistent for
     * reading (e.g., due to data invisible in level0 for Paimon DV tables). This class tracks both
     * the tiered offsets (what's been uploaded) and the readable offsets (what can be safely
     * queried).
     */
    public static class ReadableSnapshot {

        private final long readableSnapshotId;
        /**
         * The log end offsets that have been tiered to the lake storage for this snapshot. These
         * represent the physical data boundaries in the lake.
         */
        private final Map<TableBucket, Long> tieredLogEndOffsets;

        /**
         * The log end offsets that are actually visible/readable for this snapshot. For some table
         * types (like Paimon DV), this might lag behind {@link #tieredLogEndOffsets} until specific
         * compaction tasks complete.
         */
        private final Map<TableBucket, Long> readableLogEndOffsets;

        public ReadableSnapshot(
                Long readableSnapshotId,
                Map<TableBucket, Long> tieredLogEndOffsets,
                Map<TableBucket, Long> readableLogEndOffsets) {
            this.readableSnapshotId = readableSnapshotId;
            this.tieredLogEndOffsets = tieredLogEndOffsets;
            this.readableLogEndOffsets = readableLogEndOffsets;
        }

        public Long getReadableSnapshotId() {
            return readableSnapshotId;
        }

        public Map<TableBucket, Long> getTieredLogEndOffsets() {
            return tieredLogEndOffsets;
        }

        public Map<TableBucket, Long> getReadableLogEndOffsets() {
            return readableLogEndOffsets;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReadableSnapshot that = (ReadableSnapshot) o;
            return readableSnapshotId == that.readableSnapshotId
                    && Objects.equals(tieredLogEndOffsets, that.tieredLogEndOffsets)
                    && Objects.equals(readableLogEndOffsets, that.readableLogEndOffsets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(readableSnapshotId, tieredLogEndOffsets, readableLogEndOffsets);
        }
    }
}
