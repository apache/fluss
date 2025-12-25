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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The table split for tiering service. It's used to describe the snapshot data of a primary key
 * table bucket.
 */
public class TieringSnapshotSplit extends TieringSplit {

    private static final String TIERING_SNAPSHOT_SPLIT_PREFIX = "tiering-snapshot-split-";

    /** The snapshot id. It's used to identify the snapshot for a primary key table bucket. */
    private final long snapshotId;

    /** The log offset corresponding to the primary key table bucket snapshot finished. */
    private final long logOffsetOfSnapshot;

    public TieringSnapshotSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long logOffsetOfSnapshot,
            int numberOfSplits) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                snapshotId,
                logOffsetOfSnapshot,
                numberOfSplits,
                false);
    }

    public TieringSnapshotSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long logOffsetOfSnapshot,
            int numberOfSplits,
            boolean forceIgnore) {
        super(tablePath, tableBucket, partitionName, forceIgnore, numberOfSplits);
        this.snapshotId = snapshotId;
        this.logOffsetOfSnapshot = logOffsetOfSnapshot;
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_SNAPSHOT_SPLIT_PREFIX, this.tableBucket);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getLogOffsetOfSnapshot() {
        return logOffsetOfSnapshot;
    }

    @Override
    public String toString() {
        return "TieringSnapshotSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", numberOfSplits="
                + numberOfSplits
                + ", forceIgnore="
                + forceIgnore
                + ", snapshotId="
                + snapshotId
                + ", logOffsetOfSnapshot="
                + logOffsetOfSnapshot
                + '}';
    }

    @Override
    public TieringSnapshotSplit copy(int numberOfSplits) {
        return new TieringSnapshotSplit(
                tablePath,
                tableBucket,
                partitionName,
                snapshotId,
                logOffsetOfSnapshot,
                numberOfSplits,
                forceIgnore);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringSnapshotSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        TieringSnapshotSplit that = (TieringSnapshotSplit) object;
        return snapshotId == that.snapshotId && logOffsetOfSnapshot == that.logOffsetOfSnapshot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), snapshotId, logOffsetOfSnapshot);
    }
}
