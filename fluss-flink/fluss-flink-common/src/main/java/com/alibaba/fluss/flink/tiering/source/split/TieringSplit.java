/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/** The base table split for tiering service. */
public abstract class TieringSplit implements SourceSplit {

    public static final byte TIERING_KV_SPLIT_FLAG = 1;
    public static final byte TIERING_LOG_SPLIT_FLAG = 2;

    protected final TablePath tablePath;
    protected final TableBucket tableBucket;
    @Nullable protected final String partitionName;

    public TieringSplit(
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

    /** Checks whether this split is a kv split to tier. */
    public final boolean isTieringKvSplit() {
        return getClass() == TieringKvSplit.class;
    }

    /** Casts this split into a {@link TieringKvSplit}. */
    public TieringKvSplit asTieringKvSplit() {
        return (TieringKvSplit) this;
    }

    /** Checks whether this split is a log split to tier. */
    public final boolean isTieringLogSplit() {
        return getClass() == TieringLogSplit.class;
    }

    /** Casts this split into a {@link TieringLogSplit}. */
    public TieringLogSplit asTieringLogSplit() {
        return (TieringLogSplit) this;
    }

    protected byte splitKind() {
        if (isTieringKvSplit()) {
            return TIERING_KV_SPLIT_FLAG;
        } else if (isTieringLogSplit()) {
            return TIERING_LOG_SPLIT_FLAG;
        } else {
            throw new IllegalArgumentException("Unsupported split kind for " + getClass());
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

    public TablePath getTablePath() {
        return tablePath;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }
}
