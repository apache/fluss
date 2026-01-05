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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** The data for request {@link CommitLakeTableSnapshotRequest}. */
public class CommitLakeTableSnapshotsData {

    private final Map<Long, CommitLakeTableSnapshot> commitLakeTableSnapshotByTableId;

    private CommitLakeTableSnapshotsData(
            Map<Long, CommitLakeTableSnapshot> commitLakeTableSnapshotByTableId) {
        this.commitLakeTableSnapshotByTableId =
                Collections.unmodifiableMap(commitLakeTableSnapshotByTableId);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link CommitLakeTableSnapshotsData}. */
    public static class Builder {
        private final Map<Long, CommitLakeTableSnapshot> snapshotMap = new HashMap<>();

        /**
         * Add a table snapshot entry.
         *
         * @param tableId the table ID
         * @param lakeTableSnapshot the lake table snapshot (for V1 format, can be null in V2)
         * @param tableMaxTieredTimestamps the max tiered timestamps for metrics (can be null when
         *     tiered timestamps is unknown)
         * @param lakeSnapshotMetadata the lake snapshot metadata (for V2 format, can be null in v1)
         * @param earliestSnapshotIDToKeep the earliest snapshot ID to keep (can be null in v1 or
         *     not to keep previous snapshot)
         */
        public void addTableSnapshot(
                long tableId,
                @Nullable LakeTableSnapshot lakeTableSnapshot,
                @Nullable Map<TableBucket, Long> tableMaxTieredTimestamps,
                @Nullable LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata,
                @Nullable Long earliestSnapshotIDToKeep) {
            snapshotMap.put(
                    tableId,
                    new CommitLakeTableSnapshot(
                            lakeTableSnapshot,
                            tableMaxTieredTimestamps != null
                                    ? tableMaxTieredTimestamps
                                    : Collections.emptyMap(),
                            lakeSnapshotMetadata,
                            earliestSnapshotIDToKeep));
        }

        /**
         * Build the {@link CommitLakeTableSnapshotsData} instance.
         *
         * @return the built instance
         */
        public CommitLakeTableSnapshotsData build() {
            return new CommitLakeTableSnapshotsData(snapshotMap);
        }
    }

    public Map<Long, CommitLakeTableSnapshot> getCommitLakeTableSnapshotByTableId() {
        return commitLakeTableSnapshotByTableId;
    }

    // Backward compatibility methods
    public Map<Long, LakeTableSnapshot> getLakeTableSnapshot() {
        return commitLakeTableSnapshotByTableId.entrySet().stream()
                .filter(entry -> entry.getValue().lakeTableSnapshot != null)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, entry -> entry.getValue().lakeTableSnapshot));
    }

    public Map<Long, Map<TableBucket, Long>> getTableMaxTieredTimestamps() {
        return commitLakeTableSnapshotByTableId.entrySet().stream()
                .filter(
                        entry ->
                                entry.getValue().tableMaxTieredTimestamps != null
                                        && !entry.getValue().tableMaxTieredTimestamps.isEmpty())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().tableMaxTieredTimestamps));
    }

    public Map<Long, LakeTable.LakeSnapshotMetadata> getLakeTableSnapshotMetadatas() {
        return commitLakeTableSnapshotByTableId.entrySet().stream()
                .filter(entry -> entry.getValue().lakeSnapshotMetadata != null)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, entry -> entry.getValue().lakeSnapshotMetadata));
    }

    /**
     * Data container for a single table's lake snapshot commit.
     *
     * <p>This class bridges legacy V1 reporting (used for metrics) and the V2 snapshot metadata
     * persistence required for the tiering service.
     */
    public static class CommitLakeTableSnapshot {

        /**
         * Since 0.9, this field is only used to allow the coordinator to send requests to tablet
         * servers, enabling tablet servers to report metrics about synchronized log end offsets. In
         * the future, we plan to have the tiering service directly report metrics, and this field
         * will be removed.
         */
        @Nullable private final LakeTableSnapshot lakeTableSnapshot;

        /**
         * Since 0.9, this field is only used to allow the coordinator to send requests to tablet
         * servers, enabling tablet servers to report metrics about max tiered timestamps. In the
         * future, we plan to have the tiering service directly report metrics, and this field will
         * be removed.
         */
        @Nullable private final Map<TableBucket, Long> tableMaxTieredTimestamps;

        // the following field only non-empty since 0.9
        @Nullable private final LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata;

        // The earliest snapshot ID to keep for DV tables. Null for non-DV tables.
        @Nullable private final Long earliestSnapshotIDToKeep;

        public CommitLakeTableSnapshot(
                @Nullable LakeTableSnapshot lakeTableSnapshot,
                @Nullable Map<TableBucket, Long> tableMaxTieredTimestamps,
                @Nullable LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata,
                @Nullable Long earliestSnapshotIDToKeep) {
            this.lakeTableSnapshot = lakeTableSnapshot;
            this.tableMaxTieredTimestamps = tableMaxTieredTimestamps;
            this.lakeSnapshotMetadata = lakeSnapshotMetadata;
            this.earliestSnapshotIDToKeep = earliestSnapshotIDToKeep;
        }

        @Nullable
        public LakeTableSnapshot getLakeTableSnapshot() {
            return lakeTableSnapshot;
        }

        @Nullable
        public LakeTable.LakeSnapshotMetadata getLakeSnapshotMetadata() {
            return lakeSnapshotMetadata;
        }

        @Nullable
        public Long getEarliestSnapshotIDToKeep() {
            return earliestSnapshotIDToKeep;
        }
    }
}
