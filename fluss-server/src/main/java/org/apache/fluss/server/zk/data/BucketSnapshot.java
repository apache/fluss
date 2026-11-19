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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The one snapshot information of a table bucket stored in {@link ZkData.BucketSnapshotIdZNode}.
 *
 * @see BucketSnapshotJsonSerde for json serialization and deserialization.
 */
public class BucketSnapshot {

    private final long snapshotId;
    private final long logOffset;
    @Nullable private final Long syncIndexPushedOffset;
    private final String metadataPath;

    public BucketSnapshot(long snapshotId, long logOffset, String metadataPath) {
        this(snapshotId, logOffset, metadataPath, null);
    }

    public BucketSnapshot(
            long snapshotId,
            long logOffset,
            String metadataPath,
            @Nullable Long syncIndexPushedOffset) {
        this.snapshotId = snapshotId;
        this.metadataPath = metadataPath;
        this.logOffset = logOffset;
        this.syncIndexPushedOffset = syncIndexPushedOffset;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public String getMetadataPath() {
        return metadataPath;
    }

    public long getLogOffset() {
        return logOffset;
    }

    @Nullable
    public Long getSyncIndexPushedOffset() {
        return syncIndexPushedOffset;
    }

    public CompletedSnapshotHandle toCompletedSnapshotHandle() {
        return new CompletedSnapshotHandle(
                snapshotId, new FsPath(metadataPath), logOffset, syncIndexPushedOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketSnapshot that = (BucketSnapshot) o;
        return snapshotId == that.snapshotId
                && logOffset == that.logOffset
                && Objects.equals(metadataPath, that.metadataPath)
                && Objects.equals(syncIndexPushedOffset, that.syncIndexPushedOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, logOffset, metadataPath, syncIndexPushedOffset);
    }
}
