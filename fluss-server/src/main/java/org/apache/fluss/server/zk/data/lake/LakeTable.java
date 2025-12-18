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
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
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
        if (lakeSnapshotMetadatas != null && !lakeSnapshotMetadatas.isEmpty()) {
            return lakeSnapshotMetadatas.get(0);
        }
        return null;
    }

    @Nullable
    public List<LakeSnapshotMetadata> getLakeSnapshotMetadatas() {
        return lakeSnapshotMetadatas;
    }

    /**
     * Get the latest table snapshot for the lake table.
     *
     * <p>If this LakeTable was created from a LakeTableSnapshot (version 1), returns it directly.
     * Otherwise, reads the snapshot data from the lake snapshot file.
     *
     * @return the LakeTableSnapshot
     */
    public LakeTableSnapshot getLatestTableSnapshot() throws Exception {
        if (lakeTableSnapshot != null) {
            return lakeTableSnapshot;
        }
        FsPath tieredOffsetsFilePath =
                checkNotNull(getLatestLakeSnapshotMetadata()).tieredOffsetsFilePath;
        FSDataInputStream inputStream =
                tieredOffsetsFilePath.getFileSystem().open(tieredOffsetsFilePath);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(inputStream, outputStream, true);
            return LakeTableSnapshotJsonSerde.fromJson(outputStream.toByteArray());
        }
    }

    /** The lake snapshot metadata entry stored in zk lake table. */
    public static class LakeSnapshotMetadata {
        private final long snapshotId;

        // the file path to file storing the tiered offsets,
        // it points a file storing LakeTableSnapshot which includes tiered offsets
        private final FsPath tieredOffsetsFilePath;

        // the file path to file storing the readable offsets,
        // will be null if we don't now the readable offsets for this snapshot
        @Nullable private final FsPath readableOffsetsFilePath;

        public LakeSnapshotMetadata(
                long snapshotId,
                FsPath tieredOffsetsFilePath,
                @Nullable FsPath readableOffsetsFilePath) {
            this.snapshotId = snapshotId;
            this.tieredOffsetsFilePath = tieredOffsetsFilePath;
            this.readableOffsetsFilePath = readableOffsetsFilePath;
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
                    && Objects.equals(tieredOffsetsFilePath, that.tieredOffsetsFilePath)
                    && Objects.equals(readableOffsetsFilePath, that.readableOffsetsFilePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, tieredOffsetsFilePath, readableOffsetsFilePath);
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
