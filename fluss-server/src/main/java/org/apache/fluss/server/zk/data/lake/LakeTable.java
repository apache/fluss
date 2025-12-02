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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents lake table snapshot information stored in {@link ZkData.LakeTableZNode}.
 *
 * <p>This class supports two storage formats:
 *
 * <ul>
 *   <li>Version 1 (legacy): Contains the full {@link LakeTableSnapshot} data directly
 *   <li>Version 2 (current): Contains only the file paths point to the file storing {@link
 *       LakeTableSnapshot}.
 * </ul>
 *
 * @see LakeTableJsonSerde for JSON serialization and deserialization
 */
public class LakeTable {

    // Version 2 (current):
    // the pointer to the file storing the latest known LakeTableSnapshot, will be null in
    // version1
    @Nullable private final FsPath lakeTableLatestSnapshotFileHandle;

    // the pointer to the file storing the latest known compacted LakeTableSnapshot, will be null in
    // version1 or no any known compacted LakeTableSnapshot
    // todo: support tiering service commit lake table latest compacted snapshot to Fluss
    @Nullable private final FsPath lakeTableLatestCompactedSnapshotFileHandle = null;

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
     * Creates a LakeTable with a metadata file path (version 2 format).
     *
     * @param lakeTableSnapshotFileHandle the path to the metadata file containing the snapshot data
     */
    public LakeTable(@Nullable FsPath lakeTableSnapshotFileHandle) {
        this(null, lakeTableSnapshotFileHandle);
    }

    private LakeTable(
            @Nullable LakeTableSnapshot lakeTableSnapshot,
            @Nullable FsPath lakeTableSnapshotFileHandle) {
        this.lakeTableSnapshot = lakeTableSnapshot;
        this.lakeTableLatestSnapshotFileHandle = lakeTableSnapshotFileHandle;
    }

    @Nullable
    public FsPath getLakeTableLatestSnapshotFileHandle() {
        return lakeTableLatestSnapshotFileHandle;
    }

    /**
     * Converts this LakeTable to a LakeTableSnapshot.
     *
     * <p>If this LakeTable was created from a LakeTableSnapshot (version 1), returns it directly.
     * Otherwise, reads the snapshot data from the lake snapshot file.
     *
     * @return the LakeTableSnapshot
     */
    public LakeTableSnapshot toLakeTableSnapshot() throws Exception {
        if (lakeTableSnapshot != null) {
            return lakeTableSnapshot;
        }
        FSDataInputStream inputStream =
                checkNotNull(getLakeTableLatestSnapshotFileHandle())
                        .getFileSystem()
                        .open(getLakeTableLatestSnapshotFileHandle());
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(inputStream, outputStream, true);
            return LakeTableSnapshotJsonSerde.fromJson(outputStream.toByteArray());
        }
    }
}
