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

import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** The split for snapshot. It's used to describe a snapshot of a table bucket. */
public abstract class SnapshotSplit extends SourceSplitBase {

    /** The records to skip when reading the snapshot. */
    protected final long recordsToSkip;

    /** The snapshot files to read for reading the snapshot. */
    private final List<FsPathAndFileName> snapshotFiles;

    public SnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            List<FsPathAndFileName> snapshotFiles,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.snapshotFiles = snapshotFiles;
        this.recordsToSkip = recordsToSkip;
    }

    public SnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            List<FsPathAndFileName> snapshotFiles) {
        this(tableBucket, partitionName, snapshotFiles, 0);
    }

    public List<FsPathAndFileName> getSnapshotFiles() {
        return snapshotFiles;
    }

    public long recordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SnapshotSplit that = (SnapshotSplit) o;
        return recordsToSkip == that.recordsToSkip
                && Objects.equals(snapshotFiles, that.snapshotFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), recordsToSkip, snapshotFiles);
    }

    @Override
    public String toString() {
        return "SnapshotSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", recordsToSkip="
                + recordsToSkip
                + ", snapshotFiles="
                + snapshotFiles
                + '}';
    }
}
