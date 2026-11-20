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

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.metrics.registry.MetricRegistry.LOG;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents lake table snapshot information stored in {@link ZkData.LakeTableZNode}.
 *
 * <p>This class supports two storage formats:
 *
 * <ul>
 *   <li>Version 1 (legacy): Contains the full {@link LakeTableSnapshot} data directly
 *   <li>Version 2 (current): Contains a list of lake snapshot, recording the metadata file path for
 *       different lake snapshots, with actual metadata storing in file to reduce zk pressure
 * </ul>
 *
 * @see LakeTableJsonSerde for JSON serialization and deserialization
 */
public class LakeTable {

    // Version 2 (current):
    // a list of lake snapshot metadata, record the metadata for different lake snapshots
    @Nullable private final List<LakeSnapshotMetadata> lakeSnapshotMetadatas;

    // Version 1 (legacy): the full lake table snapshot info stored in ZK, will be null in version2
    @Nullable private final LakeTableSnapshot lakeTableSnapshot;

    /**
     * Creates a LakeTable from a LakeTableSnapshot (version 1 format).
     *
     * @param lakeTableSnapshot the snapshot data
     */
    public LakeTable(LakeTableSnapshot lakeTableSnapshot) {
        this(lakeTableSnapshot, null);
    }

    /**
     * Creates a LakeTable with a lake snapshot metadata (version 2 format).
     *
     * @param lakeSnapshotMetadata the metadata containing the file path to the snapshot data
     */
    public LakeTable(LakeSnapshotMetadata lakeSnapshotMetadata) {
        this(null, Collections.singletonList(lakeSnapshotMetadata));
    }

    /**
     * Creates a LakeTable with a list of lake snapshot metadata (version 2 format).
     *
     * @param lakeSnapshotMetadatas the list of lake snapshot metadata
     */
    public LakeTable(List<LakeSnapshotMetadata> lakeSnapshotMetadatas) {
        this(null, lakeSnapshotMetadatas);
    }

    private LakeTable(
            @Nullable LakeTableSnapshot lakeTableSnapshot,
            List<LakeSnapshotMetadata> lakeSnapshotMetadatas) {
        this.lakeTableSnapshot = lakeTableSnapshot;
        this.lakeSnapshotMetadatas = lakeSnapshotMetadatas;
    }

    @Nullable
    public LakeSnapshotMetadata getLatestLakeSnapshotMetadata() {
        if (lakeSnapshotMetadatas == null || lakeSnapshotMetadatas.isEmpty()) {
            return null;
        }
        // Pick the entry with the largest commitTimestamp to handle out-of-order
        // commits. When readable/tiered snapshots arrive in wrong order, use
        // commitTimestamp to identify the real latest snapshot. Legacy entries
        // without timestamp fall back to list order for compatibility.
        LakeSnapshotMetadata best = lakeSnapshotMetadatas.get(0);
        for (int i = 1; i < lakeSnapshotMetadatas.size(); i++) {
            LakeSnapshotMetadata cur = lakeSnapshotMetadatas.get(i);
            if (cur.getCommitTimestamp() >= best.getCommitTimestamp()) {
                best = cur;
            }
        }
        return best;
    }

    @Nullable
    private LakeSnapshotMetadata getLakeSnapshotMetadata(long snapshotId) {
        if (lakeSnapshotMetadatas != null) {
            for (LakeSnapshotMetadata lakeSnapshotMetadata : lakeSnapshotMetadatas) {
                if (lakeSnapshotMetadata.snapshotId == snapshotId) {
                    return lakeSnapshotMetadata;
                }
            }
        }
        return null;
    }

    @Nullable
    public List<LakeSnapshotMetadata> getLakeSnapshotMetadatas() {
        return lakeSnapshotMetadatas;
    }

    @Nullable
    public LakeTableSnapshot getLakeTableSnapshot() {
        return lakeTableSnapshot;
    }

    /**
     * Get or read the latest table snapshot for the lake table.
     *
     * <p>If this LakeTable was created from a LakeTableSnapshot (version 1), returns it directly.
     * Otherwise, reads the snapshot data from the lake snapshot file.
     *
     * @return the LakeTableSnapshot
     */
    public LakeTableSnapshot getOrReadLatestTableSnapshot() throws IOException {
        if (lakeTableSnapshot != null) {
            return lakeTableSnapshot;
        }
        return toLakeTableSnapshot(getLatestLakeSnapshotMetadata());
    }

    /**
     * Gets the table snapshot for the given ID.
     *
     * <p>Returns the cached snapshot if the ID matches; otherwise, reads and reconstructs it from
     * lake metadata.
     *
     * @return the {@link LakeTableSnapshot}, or null if not found.
     */
    @Nullable
    public LakeTableSnapshot getOrReadTableSnapshot(long snapshotId) throws IOException {
        // only happen in v1
        if (lakeTableSnapshot != null && lakeTableSnapshot.getSnapshotId() == snapshotId) {
            return lakeTableSnapshot;
        }
        LakeSnapshotMetadata lakeSnapshotMetadata = getLakeSnapshotMetadata(snapshotId);
        if (lakeSnapshotMetadata == null) {
            return null;
        }
        return toLakeTableSnapshot(lakeSnapshotMetadata);
    }

    /**
     * Gets the latest readable table snapshot. *
     *
     * <p>Searches metadata in reverse order to find the newest snapshot with valid readable
     * offsets.
     *
     * @return the latest readable {@link LakeTableSnapshot}, or null if none exist.
     */
    @Nullable
    public LakeTableSnapshot getOrReadLatestReadableTableSnapshot() throws IOException {
        // only happen in v1
        if (lakeTableSnapshot != null) {
            return lakeTableSnapshot;
        }

        // when fluss cluster upgrade, tiering service not upgrade,
        // flink connector upgrade, and call getOrReadLatestReadableTableSnapshot
        // will always get null.
        // todo: do we need to consider such case?
        //
        // Sort by commit_timestamp to get the latest readable snapshot regardless
        // of physical list order. Handles out-of-order RPCs and maintains
        // compatibility with legacy entries that lack timestamps.
        List<LakeSnapshotMetadata> snapshots = checkNotNull(lakeSnapshotMetadatas);
        Integer[] order = new Integer[snapshots.size()];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        Arrays.sort(
                order,
                (i, j) -> {
                    int cmp =
                            Long.compare(
                                    snapshots.get(j).getCommitTimestamp(),
                                    snapshots.get(i).getCommitTimestamp());
                    // tie on timestamp: later original index first
                    return cmp != 0 ? cmp : Integer.compare(j, i);
                });
        for (int idx : order) {
            LakeSnapshotMetadata snapshotMetadata = snapshots.get(idx);
            if (snapshotMetadata.readableOffsetsFilePath != null) {
                return toLakeTableSnapshot(
                        snapshotMetadata.snapshotId, snapshotMetadata.readableOffsetsFilePath);
            }
        }
        return null;
    }

    private LakeTableSnapshot toLakeTableSnapshot(LakeSnapshotMetadata lakeSnapshotMetadata)
            throws IOException {
        FsPath tieredOffsetsFilePath = checkNotNull(lakeSnapshotMetadata).tieredOffsetsFilePath;
        return toLakeTableSnapshot(lakeSnapshotMetadata.snapshotId, tieredOffsetsFilePath);
    }

    private LakeTableSnapshot toLakeTableSnapshot(long snapshotId, FsPath offsetFilePath)
            throws IOException {
        FSDataInputStream inputStream = offsetFilePath.getFileSystem().open(offsetFilePath);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(inputStream, outputStream, true);
            Map<TableBucket, Long> logOffsets =
                    TableBucketOffsets.fromJsonBytes(outputStream.toByteArray()).getOffsets();
            return new LakeTableSnapshot(snapshotId, logOffsets);
        }
    }

    /** The lake snapshot metadata entry stored in zk lake table. */
    public static class LakeSnapshotMetadata {
        // Sentinel value for unknown commit timestamps in legacy entries.
        public static final long UNKNOWN_COMMIT_TIMESTAMP = 0L;

        private final long snapshotId;

        // the file path to file storing the tiered offsets,
        // it points a file storing LakeTableSnapshot which includes tiered offsets
        private final FsPath tieredOffsetsFilePath;

        // the file path to file storing the readable offsets,
        // will be null if we don't now the readable offsets for this snapshot
        @Nullable private final FsPath readableOffsetsFilePath;

        // Server-side timestamp to determine the real latest snapshot regardless of list order.
        // Legacy entries without timestamp fall back to list order.
        private final long commitTimestamp;

        // Legacy constructor kept for backward compatibility.
        public LakeSnapshotMetadata(
                long snapshotId,
                FsPath tieredOffsetsFilePath,
                @Nullable FsPath readableOffsetsFilePath) {
            this(
                    snapshotId,
                    tieredOffsetsFilePath,
                    readableOffsetsFilePath,
                    UNKNOWN_COMMIT_TIMESTAMP);
        }

        public LakeSnapshotMetadata(
                long snapshotId,
                FsPath tieredOffsetsFilePath,
                @Nullable FsPath readableOffsetsFilePath,
                long commitTimestamp) {
            this.snapshotId = snapshotId;
            this.tieredOffsetsFilePath = tieredOffsetsFilePath;
            this.readableOffsetsFilePath = readableOffsetsFilePath;
            this.commitTimestamp = commitTimestamp;
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public FsPath getTieredOffsetsFilePath() {
            return tieredOffsetsFilePath;
        }

        public FsPath getReadableOffsetsFilePath() {
            return readableOffsetsFilePath;
        }

        public long getCommitTimestamp() {
            return commitTimestamp;
        }

        public void discard() {
            if (tieredOffsetsFilePath != null) {
                delete(tieredOffsetsFilePath);
            }
            if (readableOffsetsFilePath != null
                    && readableOffsetsFilePath != tieredOffsetsFilePath) {
                delete(readableOffsetsFilePath);
            }
        }

        private void delete(FsPath fsPath) {
            try {
                FileSystem fileSystem = fsPath.getFileSystem();
                if (fileSystem.exists(fsPath)) {
                    fileSystem.delete(fsPath, false);
                }
            } catch (IOException e) {
                LOG.warn("Error deleting filePath at {}", fsPath, e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LakeSnapshotMetadata)) {
                return false;
            }
            LakeSnapshotMetadata that = (LakeSnapshotMetadata) o;
            return snapshotId == that.snapshotId
                    && commitTimestamp == that.commitTimestamp
                    && Objects.equals(tieredOffsetsFilePath, that.tieredOffsetsFilePath)
                    && Objects.equals(readableOffsetsFilePath, that.readableOffsetsFilePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    snapshotId, tieredOffsetsFilePath, readableOffsetsFilePath, commitTimestamp);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LakeTable)) {
            return false;
        }
        LakeTable lakeTable = (LakeTable) o;
        return Objects.equals(lakeSnapshotMetadatas, lakeTable.lakeSnapshotMetadatas)
                && Objects.equals(lakeTableSnapshot, lakeTable.lakeTableSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lakeSnapshotMetadatas, lakeTableSnapshot);
    }

    @Override
    public String toString() {
        return "LakeTable{"
                + "lakeSnapshotMetadatas="
                + lakeSnapshotMetadatas
                + ", lakeTableSnapshot="
                + lakeTableSnapshot
                + '}';
    }
}
