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

package org.apache.fluss.client.tiering;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

/** The table split for tiering service. It's used to describe the log data of a table bucket. */
public class TieringLogSplit extends TieringSplit {

    private static final long serialVersionUID = 1L;

    private static final String TIERING_LOG_SPLIT_PREFIX = "tiering-log-split-";

    /** The log start offset of a bucket that is unknown. */
    public static final long UNKNOWN_LOG_START_OFFSET = -1L;

    private final long startingOffset;
    private final long stoppingOffset;
    // the bucket's log start offset when the split was generated, only known for a split that reads
    // from the earliest offset; it estimates the split's backlog and doesn't change what is read
    private final long logStartOffset;

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                UNKNOWN_NUMBER_OF_SPLITS);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                false);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits,
            int splitIndex,
            long tieringRoundTimestamp) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                false,
                splitIndex,
                tieringRoundTimestamp);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits,
            boolean skipCurrentRound) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                skipCurrentRound,
                UNKNOWN_SPLIT_INDEX,
                UNKNOWN_TIERING_ROUND_TIMESTAMP);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits,
            boolean skipCurrentRound,
            int splitIndex,
            long tieringRoundTimestamp) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                skipCurrentRound,
                splitIndex,
                tieringRoundTimestamp,
                UNKNOWN_LOG_START_OFFSET);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits,
            boolean skipCurrentRound,
            int splitIndex,
            long tieringRoundTimestamp,
            long logStartOffset) {
        super(
                tablePath,
                tableBucket,
                partitionName,
                numberOfSplits,
                skipCurrentRound,
                splitIndex,
                tieringRoundTimestamp);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.logStartOffset = logStartOffset;
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_LOG_SPLIT_PREFIX, this.tableBucket);
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public long getStoppingOffset() {
        return stoppingOffset;
    }

    /**
     * Returns the bucket's log start offset when the split was generated, or {@link
     * #UNKNOWN_LOG_START_OFFSET}. Only known for a split that reads from the earliest offset.
     */
    public long getLogStartOffset() {
        return logStartOffset;
    }

    /** Returns a copy of this split with the bucket's log start offset. */
    public TieringLogSplit withLogStartOffset(long logStartOffset) {
        return new TieringLogSplit(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                skipCurrentRound,
                splitIndex,
                tieringRoundTimestamp,
                logStartOffset);
    }

    @Override
    public String toString() {
        return "TieringLogSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", numberOfSplits="
                + numberOfSplits
                + ", skipCurrentRound="
                + skipCurrentRound
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", logStartOffset="
                + logStartOffset
                + ", splitIndex="
                + splitIndex
                + ", tieringRoundTimestamp="
                + tieringRoundTimestamp
                + '}';
    }

    @Override
    public TieringLogSplit copy(int numberOfSplits, int splitIndex, long tieringRoundTimestamp) {
        return new TieringLogSplit(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                skipCurrentRound,
                splitIndex,
                tieringRoundTimestamp,
                logStartOffset);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringLogSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        TieringLogSplit that = (TieringLogSplit) object;
        return startingOffset == that.startingOffset
                && stoppingOffset == that.stoppingOffset
                && logStartOffset == that.logStartOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startingOffset, stoppingOffset, logStartOffset);
    }
}
