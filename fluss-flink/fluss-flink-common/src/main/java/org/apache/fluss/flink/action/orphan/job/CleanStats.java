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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;

/**
 * Per-task cleanup statistics emitted by each {@link ScanAndCleanFunction} subtask. The scalar
 * counters are accumulated by {@link StatsAggregateOperator} via simple addition.
 */
@Internal
public final class CleanStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CleanupCounters counters;
    private final RuleSummary rules = new RuleSummary();

    public RuleSummary rules() {
        return rules;
    }

    private final ScopeCoverageStats scopeCoverage;
    private final boolean scopeSummary;

    public CleanStats(long scanned, long deleted, long deleteFailures, long bytesReclaimed) {
        this(scanned, deleted, 0L, deleteFailures, bytesReclaimed);
    }

    public CleanStats(
            long scanned,
            long deleted,
            long emptyDirsRemoved,
            long deleteFailures,
            long bytesReclaimed) {
        this(
                new CleanupCounters(
                        scanned,
                        deleted,
                        emptyDirsRemoved,
                        bytesReclaimed,
                        deleted,
                        emptyDirsRemoved,
                        deleteFailures,
                        bytesReclaimed));
    }

    public CleanStats(CleanupCounters counters) {
        this(counters, ScopeCoverageStats.empty(), false);
    }

    public CleanStats(CleanupCounters counters, ScopeCoverageStats scopeCoverage) {
        this(counters, scopeCoverage, false);
    }

    private CleanStats(
            CleanupCounters counters, ScopeCoverageStats scopeCoverage, boolean scopeSummary) {
        this.counters = counters;
        this.scopeCoverage = scopeCoverage;
        this.scopeSummary = scopeSummary;
    }

    public static CleanStats scopeSummary(ScopeCoverageStats scopeCoverage) {
        return new CleanStats(CleanupCounters.empty(), scopeCoverage, true);
    }

    public static CleanStats empty() {
        return new CleanStats(0L, 0L, 0L, 0L);
    }

    public long scanned() {
        return counters.scannedFiles();
    }

    public long deleted() {
        return counters.deletedFiles() + counters.emptyDirsRemoved();
    }

    public long emptyDirsRemoved() {
        return counters.emptyDirsRemoved();
    }

    public long deleteFailures() {
        return counters.deleteFailures();
    }

    public long bytesReclaimed() {
        return counters.bytesReclaimed();
    }

    public CleanupCounters counters() {
        return counters;
    }

    public ScopeCoverageStats scopeCoverage() {
        return scopeCoverage;
    }

    public boolean isScopeSummary() {
        return scopeSummary;
    }
}
