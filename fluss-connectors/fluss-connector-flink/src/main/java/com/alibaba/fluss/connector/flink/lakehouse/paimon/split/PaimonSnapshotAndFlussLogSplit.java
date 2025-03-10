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

package com.alibaba.fluss.connector.flink.lakehouse.paimon.split;

import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.flink.source.FileStoreSourceSplit;

import javax.annotation.Nullable;

import java.util.Optional;

/** A split mixing Paimon snapshot and Fluss log. */
public class PaimonSnapshotAndFlussLogSplit extends SourceSplitBase {

    public static final byte PAIMON_SNAPSHOT_FLUSS_LOG_SPLIT_KIND = -2;

    // may be null when no snapshot data for the bucket
    @Nullable private final FileStoreSourceSplit snapshotSplit;

    /** The records to skip when reading the splits. */
    private final long recordsToSkip;

    private final long startingOffset;
    private final long stoppingOffset;

    public PaimonSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable FileStoreSourceSplit snapshotSplit,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, null, snapshotSplit, startingOffset, stoppingOffset, 0);
    }

    public PaimonSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable FileStoreSourceSplit snapshotSplit,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, partitionName, snapshotSplit, startingOffset, stoppingOffset, 0);
    }

    public PaimonSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable FileStoreSourceSplit snapshotSplit,
            long startingOffset,
            long stoppingOffset,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.snapshotSplit = snapshotSplit;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.recordsToSkip = recordsToSkip;
    }

    public PaimonSnapshotAndFlussLogSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new PaimonSnapshotAndFlussLogSplit(
                getTableBucket(),
                getPartitionName(),
                snapshotSplit,
                startingOffset,
                stoppingOffset,
                recordsToSkip);
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    public byte splitKind() {
        return PAIMON_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
    }

    @Override
    public String splitId() {
        return toSplitId("paimon-hybrid-snapshot-log-", tableBucket);
    }

    @Nullable
    public FileStoreSourceSplit getSnapshotSplit() {
        return snapshotSplit;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    @Override
    public String toString() {
        return "PaimonSnapshotAndFlussLogSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", snapshotSplit="
                + snapshotSplit
                + ", recordsToSkip="
                + recordsToSkip
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + '}';
    }
}
