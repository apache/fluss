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

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A Flink-specific wrapper for {@link TieringSplit} that implements Flink's {@link SourceSplit}
 * interface. This adapter allows the engine-agnostic {@link TieringSplit} to be used within Flink's
 * source framework.
 */
public class FlinkTieringSplit implements SourceSplit {

    private final TieringSplit tieringSplit;

    public FlinkTieringSplit(TieringSplit tieringSplit) {
        this.tieringSplit = tieringSplit;
    }

    @Override
    public String splitId() {
        return tieringSplit.splitId();
    }

    /** Returns the underlying engine-agnostic {@link TieringSplit}. */
    public TieringSplit unwrap() {
        return tieringSplit;
    }

    /** Checks whether this split is a primary key table split to tier. */
    public boolean isTieringSnapshotSplit() {
        return tieringSplit.isTieringSnapshotSplit();
    }

    /** Checks whether this split is a log split to tier. */
    public boolean isTieringLogSplit() {
        return tieringSplit.isTieringLogSplit();
    }

    /** Casts the underlying split into a {@link TieringSnapshotSplit}. */
    public TieringSnapshotSplit asTieringSnapshotSplit() {
        return tieringSplit.asTieringSnapshotSplit();
    }

    /** Casts the underlying split into a {@link TieringLogSplit}. */
    public TieringLogSplit asTieringLogSplit() {
        return tieringSplit.asTieringLogSplit();
    }

    /**
     * Marks this split to skip reading data in the current round. Once called, the split will not
     * be processed and data reading will be skipped.
     */
    public void skipCurrentRound() {
        tieringSplit.skipCurrentRound();
    }

    /**
     * Returns whether this split should skip tiering data in the current round of tiering.
     *
     * @return true if the split should skip tiering data, false otherwise
     */
    public boolean shouldSkipCurrentRound() {
        return tieringSplit.shouldSkipCurrentRound();
    }

    public byte splitKind() {
        return tieringSplit.splitKind();
    }

    public int getNumberOfSplits() {
        return tieringSplit.getNumberOfSplits();
    }

    public int getSplitIndex() {
        return tieringSplit.getSplitIndex();
    }

    public boolean isFirstSplit() {
        return tieringSplit.isFirstSplit();
    }

    public long getTieringRoundTimestamp() {
        return tieringSplit.getTieringRoundTimestamp();
    }

    public TablePath getTablePath() {
        return tieringSplit.getTablePath();
    }

    public TableBucket getTableBucket() {
        return tieringSplit.getTableBucket();
    }

    @Nullable
    public String getPartitionName() {
        return tieringSplit.getPartitionName();
    }

    public FlinkTieringSplit copy(int numberOfSplits) {
        return new FlinkTieringSplit(tieringSplit.copy(numberOfSplits));
    }

    public FlinkTieringSplit copy(int numberOfSplits, int splitIndex, long tieringRoundTimestamp) {
        return new FlinkTieringSplit(
                tieringSplit.copy(numberOfSplits, splitIndex, tieringRoundTimestamp));
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof FlinkTieringSplit)) {
            return false;
        }
        FlinkTieringSplit that = (FlinkTieringSplit) object;
        return Objects.equals(tieringSplit, that.tieringSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tieringSplit);
    }

    @Override
    public String toString() {
        return "FlinkTieringSplit{" + tieringSplit + '}';
    }
}
