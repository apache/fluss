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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The LakeCommitter interface for committing write results. It extends the AutoCloseable interface
 * to ensure resources are released after use.
 *
 * @param <WriteResult> the type of the write result
 * @param <CommittableT> the type of the committable object
 * @since 0.7
 */
@PublicEvolving
public interface LakeCommitter<WriteResult, CommittableT> extends AutoCloseable {

    /**
     * The property key used to store the file path of lake table bucket offsets in snapshot
     * properties.
     */
    String FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY = "fluss-offsets";

    /**
     * Converts a list of write results to a committable object.
     *
     * @param writeResults the list of write results
     * @return the committable object
     * @throws IOException if an I/O error occurs
     */
    CommittableT toCommittable(List<WriteResult> writeResults) throws IOException;

    /**
     * Commits the given committable object.
     *
     * @param committable the committable object
     * @param context the snapshot properties and tiered log end offsets for this commit
     * @return the commit result, which always includes the latest committed snapshot ID and may
     *     optionally contain distinct readable snapshot information if the physical tiered offsets
     *     do not yet represent a consistent readable state (e.g., in Paimon DV tables where the
     *     tiered log records may still in level0 which is not readable).
     * @throws IOException if an I/O error occurs
     */
    LakeCommitResult commit(CommittableT committable, CommitContext context) throws IOException;

    /** Context for committing a lake snapshot. */
    final class CommitContext {
        private final Map<String, String> snapshotProperties;
        private final Map<TableBucket, Long> tieredLogEndOffsets;

        /**
         * Creates a commit context.
         *
         * @param snapshotProperties the properties stored in the lake snapshot
         * @param tieredLogEndOffsets the previous lake snapshot offsets merged with this commit's
         *     log end offsets
         */
        public CommitContext(
                Map<String, String> snapshotProperties,
                Map<TableBucket, Long> tieredLogEndOffsets) {
            this.snapshotProperties = checkNotNull(snapshotProperties);
            this.tieredLogEndOffsets = checkNotNull(tieredLogEndOffsets);
        }

        /** Returns the properties stored in the lake snapshot. */
        public Map<String, String> snapshotProperties() {
            return snapshotProperties;
        }

        /** Returns the tiered log end offsets. */
        public Map<TableBucket, Long> tieredLogEndOffsets() {
            return tieredLogEndOffsets;
        }
    }

    /**
     * Aborts the given committable object.
     *
     * @param committable the committable object
     * @throws IOException if an I/O error occurs
     */
    void abort(CommittableT committable) throws IOException;

    /**
     * Get missing lake snapshot that has been committed to lake but didn't commit to fluss.
     *
     * @param latestLakeSnapshotIdOfFluss the latest lake snapshot id in fluss, used to judge which
     *     lake snapshot is missing.
     * @return the missing lake snapshot, returns null if no any missing snapshot found
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException;
}
