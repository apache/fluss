/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.lakehouse.paimon.split;

import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.flink.source.FileStoreSourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;

/** A split for reading a snapshot of paimon. */
public class PaimonSnapshotSplit extends SourceSplitBase {

    public static final byte PAIMON_SNAPSHOT_SPLIT_KIND = -1;

    private final FileStoreSourceSplit fileStoreSourceSplit;

    public PaimonSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            FileStoreSourceSplit fileStoreSourceSplit) {
        super(tableBucket, partitionName);
        this.fileStoreSourceSplit = fileStoreSourceSplit;
    }

    @Override
    public String splitId() {
        return toSplitId("paimon-snapshot-" + fileStoreSourceSplit.splitId() + "-", tableBucket);
    }

    @Override
    public byte splitKind() {
        return PAIMON_SNAPSHOT_SPLIT_KIND;
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    public FileStoreSourceSplit getFileStoreSourceSplit() {
        return fileStoreSourceSplit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonSnapshotSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PaimonSnapshotSplit that = (PaimonSnapshotSplit) o;
        return Objects.equals(fileStoreSourceSplit, that.fileStoreSourceSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fileStoreSourceSplit);
    }

    @Override
    public String toString() {
        return "PaimonSnapshotSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", fileStoreSourceSplit="
                + fileStoreSourceSplit
                + '}';
    }
}
