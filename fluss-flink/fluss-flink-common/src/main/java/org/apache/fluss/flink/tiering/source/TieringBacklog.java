/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSplit;

import java.util.Collection;

/**
 * Estimates the backlog of tiering splits, in log records.
 *
 * <p>The tiering source reports its backlog as {@code pendingRecords} and its progress as {@code
 * numRecordsIn}, both counted as log offsets consumed, so that autoscalers can compare them. A log
 * split is exact: the offsets between its starting and stopping offset. A snapshot split, the first
 * round of a primary key bucket, is estimated by the log offset of its snapshot, an upper bound
 * since updates of the same key collapse in the snapshot.
 */
@Internal
public final class TieringBacklog {

    private TieringBacklog() {}

    /** Returns the estimated number of log records of the split still to be tiered. */
    public static long estimate(TieringSplit split) {
        if (split.shouldSkipCurrentRound()) {
            return 0L;
        }
        if (split.isTieringLogSplit()) {
            TieringLogSplit logSplit = split.asTieringLogSplit();
            // a negative starting offset means the earliest offset, which is unknown here
            return remaining(
                    Math.max(logSplit.getStartingOffset(), 0L), logSplit.getStoppingOffset());
        }
        return Math.max(split.asTieringSnapshotSplit().getLogOffsetOfSnapshot(), 0L);
    }

    /** Returns the estimated number of log records of the splits still to be tiered. */
    public static long estimate(Collection<? extends TieringSplit> splits) {
        long backlog = 0L;
        for (TieringSplit split : splits) {
            backlog += estimate(split);
        }
        return backlog;
    }

    /** Returns the number of offsets from {@code nextOffset} up to {@code stoppingOffset}. */
    static long remaining(long nextOffset, long stoppingOffset) {
        return Math.max(stoppingOffset - nextOffset, 0L);
    }
}
