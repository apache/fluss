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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;

/** The base table split for tiering service. */
public abstract class TieringSplit implements SourceSplit {

    public static final byte TIERING_SNAPSHOT_SPLIT_FLAG = 1;
    public static final byte TIERING_LOG_SPLIT_FLAG = 2;

    protected static final int UNKNOWN_NUMBER_OF_SPLITS = -1;

    protected final TablePath tablePath;
    protected final TableBucket tableBucket;
    @Nullable protected final String partitionName;

    // the total number of splits in one round of tiering
    protected final int numberOfSplits;

    public TieringSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            int numberOfSplits) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        if ((tableBucket.getPartitionId() == null && partitionName != null)
                || (tableBucket.getPartitionId() != null && partitionName == null)) {
            throw new IllegalArgumentException(
                    "Partition name and partition id must be both null or both not null.");
        }
        this.numberOfSplits = numberOfSplits;
    }

    /** Checks whether this split is a primary key table split to tier. */
    public final boolean isTieringSnapshotSplit() {
        return getClass() == TieringSnapshotSplit.class;
    }

    /** Casts this split into a {@link TieringSnapshotSplit}. */
    public TieringSnapshotSplit asTieringSnapshotSplit() {
        return (TieringSnapshotSplit) this;
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
        if (isTieringSnapshotSplit()) {
            return TIERING_SNAPSHOT_SPLIT_FLAG;
        } else if (isTieringLogSplit()) {
            return TIERING_LOG_SPLIT_FLAG;
        } else {
            throw new IllegalArgumentException("Unsupported split kind for " + getClass());
        }
    }

    public int getNumberOfSplits() {
        return numberOfSplits;
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

    public abstract TieringSplit copy(int numberOfSplits);

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringSplit)) {
            return false;
        }
        TieringSplit that = (TieringSplit) object;
        return Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(partitionName, that.partitionName)
                && numberOfSplits == that.numberOfSplits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket, partitionName, numberOfSplits);
    }
}
