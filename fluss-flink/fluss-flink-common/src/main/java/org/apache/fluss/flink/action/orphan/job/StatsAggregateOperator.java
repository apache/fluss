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
import org.apache.fluss.flink.action.orphan.audit.ResultAuditLogger;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;

/**
 * Stage 3 of the orphan files cleanup job. Runs at parallelism=1 to aggregate per-subtask {@link
 * CleanStats} records.
 *
 * <p>Implemented as a custom operator (not ProcessFunction) because {@code ProcessOperator} does
 * not implement {@link BoundedOneInput} — the {@code endInput()} callback would never fire.
 *
 * <p>Scalar counters are accumulated into longs and the final summary is emitted in {@link
 * #endInput()}.
 */
@Internal
public final class StatsAggregateOperator extends AbstractStreamOperator<CleanStats>
        implements OneInputStreamOperator<CleanStats, CleanStats>, BoundedOneInput {

    private static final long serialVersionUID = 2L;

    private final boolean dryRun;
    private final ResultAuditLogger resultAudit;
    private transient RuleSummary rules;
    private transient long tasksCompleted;

    private transient CleanupCounters counters;
    private transient ScopeCoverageStats scopeCoverage;
    private transient long scopeSummaryMarkers;

    public StatsAggregateOperator(boolean dryRun) {
        this(dryRun, new ResultAuditLogger(Collections.emptyMap()));
    }

    public StatsAggregateOperator(boolean dryRun, ResultAuditLogger resultAudit) {
        this.resultAudit = resultAudit;
        this.dryRun = dryRun;
    }

    @Override
    public void open() throws Exception {
        super.open();
        counters = CleanupCounters.empty();
        rules = new RuleSummary();
        tasksCompleted = 0;
        scopeCoverage = ScopeCoverageStats.empty();
        scopeSummaryMarkers = 0L;
    }

    @Override
    public void processElement(StreamRecord<CleanStats> element) {
        rules.add(element.getValue().rules());
        if (!element.getValue().isScopeSummary()) {
            tasksCompleted++;
        }
        counters = counters.add(element.getValue().counters());
        scopeCoverage.add(element.getValue().scopeCoverage());
        if (element.getValue().isScopeSummary()) {
            scopeSummaryMarkers++;
        }
    }

    @Override
    public void endInput() {
        validateScopeSummaryMarkers(scopeSummaryMarkers);
        CleanStats finalStats = new CleanStats(counters, scopeCoverage);

        resultAudit.summary(counters, scopeCoverage, rules, tasksCompleted, dryRun);
        finalStats.rules().add(rules);

        output.collect(new StreamRecord<>(finalStats));
    }

    static void validateScopeSummaryMarkers(long markers) {
        if (markers != 1L) {
            throw new IllegalStateException(
                    "Expected exactly one scope summary marker, but received " + markers);
        }
    }
}
