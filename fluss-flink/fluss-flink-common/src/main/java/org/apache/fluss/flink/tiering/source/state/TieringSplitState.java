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

package org.apache.fluss.flink.tiering.source.state;

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;

/**
 * The state of a {@link FlinkTieringSplit}.
 *
 * <p>Note: The tiering service adopts a stateless design and does not store any progress
 * information in state during checkpoints. All splits are re-requested from the Fluss cluster in
 * case of failover.
 */
public class TieringSplitState {

    protected final FlinkTieringSplit flinkTieringSplit;

    public TieringSplitState(FlinkTieringSplit flinkTieringSplit) {
        this.flinkTieringSplit = flinkTieringSplit;
    }

    public FlinkTieringSplit toSourceSplit() {
        TieringSplit tieringSplit = flinkTieringSplit.unwrap();
        if (tieringSplit.isTieringSnapshotSplit()) {
            final TieringSnapshotSplit split = tieringSplit.asTieringSnapshotSplit();
            return new FlinkTieringSplit(
                    new TieringSnapshotSplit(
                            split.getTablePath(),
                            split.getTableBucket(),
                            split.getPartitionName(),
                            split.getSnapshotId(),
                            split.getLogOffsetOfSnapshot(),
                            split.getNumberOfSplits(),
                            split.shouldSkipCurrentRound(),
                            split.getSplitIndex(),
                            split.getTieringRoundTimestamp()));
        } else {
            final TieringLogSplit split = tieringSplit.asTieringLogSplit();
            return new FlinkTieringSplit(
                    new TieringLogSplit(
                            split.getTablePath(),
                            split.getTableBucket(),
                            split.getPartitionName(),
                            split.getStartingOffset(),
                            split.getStoppingOffset(),
                            split.getNumberOfSplits(),
                            split.shouldSkipCurrentRound(),
                            split.getSplitIndex(),
                            split.getTieringRoundTimestamp()));
        }
    }
}
