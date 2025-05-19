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

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;

/** The table split for tiering service. It's used to describe the log data of a table bucket. */
public class TieringSplit implements SourceSplit {

    private static final String TIERING_SPLIT_PREFIX = "tiering-split-";

    private final TablePath tablePath;
    protected final TableBucket tableBucket;
    @Nullable protected final String partitionName;
    private final long startingOffset;
    private final long stoppingOffset;

    public TieringSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        if ((tableBucket.getPartitionId() == null && partitionName != null)
                || (tableBucket.getPartitionId() != null && partitionName == null)) {
            throw new IllegalArgumentException(
                    "Partition name and partition id must be both null or both not null.");
        }
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public long getStoppingOffset() {
        return stoppingOffset;
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

    public String splitId() {
        if (tableBucket.getPartitionId() != null) {
            return TIERING_SPLIT_PREFIX
                    + tableBucket.getTableId()
                    + "-p"
                    + tableBucket.getPartitionId()
                    + "-"
                    + tableBucket.getBucket();
        } else {
            return TIERING_SPLIT_PREFIX + tableBucket.getTableId() + "-" + tableBucket.getBucket();
        }
    }

    @Override
    public String toString() {
        return "TieringSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringSplit)) {
            return false;
        }
        TieringSplit that = (TieringSplit) object;
        return startingOffset == that.startingOffset
                && stoppingOffset == that.stoppingOffset
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket, partitionName, startingOffset, stoppingOffset);
    }
}
