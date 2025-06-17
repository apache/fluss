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

package com.alibaba.fluss.flink.source.split;

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;

/** A base source split for {@link SnapshotSplit} and {@link LogSplit}. . */
public abstract class SourceSplitBase implements SourceSplit {

    public static final byte HYBRID_SNAPSHOT_SPLIT_FLAG = 1;
    public static final byte LOG_SPLIT_FLAG = 2;

    protected final TableBucket tableBucket;

    @Nullable protected final String partitionName;

    public SourceSplitBase(TableBucket tableBucket, @Nullable String partitionName) {
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

    /** Checks whether this split is a log split. */
    public final boolean isLogSplit() {
        return getClass() == LogSplit.class;
    }

    public boolean isLakeSplit() {
        return false;
    }

    /** Checks whether this split is a hybrid snapshot and log split. */
    public final boolean isHybridSnapshotLogSplit() {
        return getClass() == HybridSnapshotLogSplit.class;
    }

    /** Casts this split into a {@link HybridSnapshotLogSplit}. */
    public final HybridSnapshotLogSplit asHybridSnapshotLogSplit() {
        return (HybridSnapshotLogSplit) this;
    }

    /** Casts this split into a {@link LogSplit}. */
    public final LogSplit asLogSplit() {
        return (LogSplit) this;
    }

    protected byte splitKind() {
        if (isHybridSnapshotLogSplit()) {
            return HYBRID_SNAPSHOT_SPLIT_FLAG;
        } else if (isLogSplit()) {
            return LOG_SPLIT_FLAG;
        } else {
            throw new IllegalArgumentException("Unsupported split kind for " + getClass());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceSplitBase that = (SourceSplitBase) o;
        return Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableBucket);
    }
}
