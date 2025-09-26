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

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

/** A split for reading a snapshot of lake. */
public class LakeSnapshotSplit extends SourceSplitBase {

    public static final byte LAKE_SNAPSHOT_SPLIT_KIND = -1;

    private final LakeSplit lakeSplit;

    private final long recordsToSkip;

    private final int splitIndex;

    public LakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex) {
        this(tableBucket, partitionName, lakeSplit, splitIndex, 0);
    }

    public LakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.lakeSplit = lakeSplit;
        this.splitIndex = splitIndex;
        this.recordsToSkip = recordsToSkip;
    }

    public LakeSplit getLakeSplit() {
        return lakeSplit;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    public int getSplitIndex() {
        return splitIndex;
    }

    @Override
    public String splitId() {
        return toSplitId(
                        "lake-snapshot-",
                        new TableBucket(
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                lakeSplit.bucket()))
                + "-"
                + splitIndex;
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    public byte splitKind() {
        return LAKE_SNAPSHOT_SPLIT_KIND;
    }

    @Override
    public String toString() {
        return "LakeSnapshotSplit{"
                + "lakeSplit="
                + lakeSplit
                + ", recordsToSkip="
                + recordsToSkip
                + ", splitIndex="
                + splitIndex
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + '}';
    }
}
