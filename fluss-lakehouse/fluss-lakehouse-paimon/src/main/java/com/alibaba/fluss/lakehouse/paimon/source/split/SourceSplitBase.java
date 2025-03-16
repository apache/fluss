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

package com.alibaba.fluss.lakehouse.paimon.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;

/** A base source split for {@link SnapshotSplit} and {@link LogSplit}. . */
public abstract class SourceSplitBase implements SourceSplit {

    protected final TablePath tablePath;
    protected final TableBucket tableBucket;

    @Nullable private final String partitionName;

    public SourceSplitBase(
            TablePath tablePath, TableBucket tableBucket, @Nullable String partitionName) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        if ((tableBucket.getPartitionId() == null && partitionName != null)
                || (tableBucket.getPartitionId() != null && partitionName == null)) {
            throw new IllegalArgumentException(
                    "Partition name and partition id must be both null or both not null.");
        }
    }

    protected static String toSplitId(String splitPrefix, TableBucket tableBucket) {
        if (tableBucket.getPartitionId() != null) {
            return splitPrefix
                    + tableBucket.getTableId()
                    + "-p"
                    + tableBucket.getPartitionId()
                    + "-"
                    + tableBucket.getBucket();
        } else {
            return splitPrefix + tableBucket.getTableId() + "-" + tableBucket.getBucket();
        }
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public final HybridSnapshotLogSplit asHybridSnapshotLogSplit() {
        return (HybridSnapshotLogSplit) this;
    }

    /** Casts this split into a {@link LogSplit}. */
    public final LogSplit asLogSplit() {
        return (LogSplit) this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceSplitBase)) {
            return false;
        }
        SourceSplitBase that = (SourceSplitBase) o;
        return Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket, partitionName);
    }
}
