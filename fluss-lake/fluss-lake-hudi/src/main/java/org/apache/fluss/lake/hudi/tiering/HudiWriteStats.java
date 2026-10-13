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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Hudi write stats that are sufficient for committing one Hudi instant. */
public class HudiWriteStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<HoodieWriteStat> writeStats;
    private final long totalErrorRecords;

    public HudiWriteStats(List<HoodieWriteStat> writeStats, long totalErrorRecords) {
        checkArgument(
                totalErrorRecords >= 0,
                "Hudi total error records must not be negative, but is %s.",
                totalErrorRecords);
        this.writeStats = copyWriteStats(writeStats);
        this.totalErrorRecords = totalErrorRecords;
    }

    public static HudiWriteStats fromWriteStatuses(List<WriteStatus> writeStatuses) {
        checkNotNull(writeStatuses, "Hudi write statuses must not be null.");

        List<HoodieWriteStat> stats = new ArrayList<>(writeStatuses.size());
        long totalErrorRecords = 0L;
        for (WriteStatus writeStatus : writeStatuses) {
            checkNotNull(writeStatus, "Hudi write status must not be null.");
            stats.add(checkNotNull(writeStatus.getStat(), "Hudi write stat must not be null."));
            totalErrorRecords += writeStatus.getTotalErrorRecords();
        }
        return new HudiWriteStats(stats, totalErrorRecords);
    }

    public List<HoodieWriteStat> getWriteStats() {
        return writeStats;
    }

    public long getTotalErrorRecords() {
        return totalErrorRecords;
    }

    public HudiWriteStats merge(HudiWriteStats other) {
        checkNotNull(other, "Hudi write stats to merge must not be null.");

        List<HoodieWriteStat> mergedStats =
                new ArrayList<>(writeStats.size() + other.writeStats.size());
        mergedStats.addAll(writeStats);
        mergedStats.addAll(other.writeStats);
        return new HudiWriteStats(mergedStats, totalErrorRecords + other.totalErrorRecords);
    }

    private static List<HoodieWriteStat> copyWriteStats(List<HoodieWriteStat> writeStats) {
        if (writeStats == null || writeStats.isEmpty()) {
            return Collections.emptyList();
        }

        List<HoodieWriteStat> copiedStats = new ArrayList<>(writeStats.size());
        for (HoodieWriteStat writeStat : writeStats) {
            copiedStats.add(checkNotNull(writeStat, "Hudi write stat must not be null."));
        }
        return Collections.unmodifiableList(copiedStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiWriteStats)) {
            return false;
        }
        HudiWriteStats that = (HudiWriteStats) o;
        return totalErrorRecords == that.totalErrorRecords
                && Objects.equals(writeStats, that.writeStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writeStats, totalErrorRecords);
    }

    @Override
    public String toString() {
        return "HudiWriteStats{"
                + "writeStats="
                + writeStats
                + ", totalErrorRecords="
                + totalErrorRecords
                + '}';
    }
}
