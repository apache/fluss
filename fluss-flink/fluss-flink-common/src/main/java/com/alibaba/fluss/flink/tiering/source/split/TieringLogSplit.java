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

package com.alibaba.fluss.flink.tiering.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

/** The table split for tiering service. It's used to describe the log data of a table bucket. */
public class TieringLogSplit extends TieringSplit {

    private static final String TIERING_LOG_SPLIT_PREFIX = "tiering-log-split-";

    private final long startingOffset;
    private final long stoppingOffset;

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
        super(tablePath, tableBucket, partitionName, numberOfSplits);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
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
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", numberOfSplits="
                + numberOfSplits
                + '}';
    }

    @Override
    public TieringLogSplit copy(int numberOfSplits) {
        return new TieringLogSplit(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits);
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
        return startingOffset == that.startingOffset && stoppingOffset == that.stoppingOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startingOffset, stoppingOffset);
    }
}
