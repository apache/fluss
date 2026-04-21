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

package org.apache.fluss.microbench.report;

import java.util.ArrayList;
import java.util.List;

/** Compares key metrics between two {@link PerfReport} instances. */
public class ReportDiffer {

    /** Threshold (percentage) below which a change is considered neutral. */
    private static final double NEUTRAL_THRESHOLD = 1.0;

    private ReportDiffer() {}

    /** Produces a {@link DiffResult} comparing baseline to target. */
    public static DiffResult diff(PerfReport baseline, PerfReport target) {
        List<DiffResult.MetricComparison> comparisons = new ArrayList<>();

        // Per-phase comparisons (match by phase name).
        for (PhaseResult basePhase : baseline.phaseResults()) {
            if (basePhase.warmupOnly()) {
                continue;
            }
            PhaseResult targetPhase = findPhase(target, basePhase.phaseName());
            if (targetPhase == null) {
                continue;
            }

            comparisons.add(
                    compare(
                            basePhase.phaseName() + ".opsPerSecond",
                            basePhase.opsPerSecond(),
                            targetPhase.opsPerSecond(),
                            MetricDirection.HIGHER_BETTER));
            comparisons.add(
                    compare(
                            basePhase.phaseName() + ".p50Nanos",
                            basePhase.p50Nanos(),
                            targetPhase.p50Nanos(),
                            MetricDirection.LOWER_BETTER));
            comparisons.add(
                    compare(
                            basePhase.phaseName() + ".p95Nanos",
                            basePhase.p95Nanos(),
                            targetPhase.p95Nanos(),
                            MetricDirection.LOWER_BETTER));
            comparisons.add(
                    compare(
                            basePhase.phaseName() + ".p99Nanos",
                            basePhase.p99Nanos(),
                            targetPhase.p99Nanos(),
                            MetricDirection.LOWER_BETTER));
        }

        // Resource peak comparisons from pre-computed summary
        ReportSummary baseSummary = baseline.summary();
        ReportSummary targetSummary = target.summary();
        if (baseSummary != null && targetSummary != null) {
            comparisons.add(
                    compare(
                            "maxClientHeapUsed",
                            baseSummary.peakClientHeapBytes(),
                            targetSummary.peakClientHeapBytes(),
                            MetricDirection.LOWER_BETTER));
            comparisons.add(
                    compare(
                            "maxClientRss",
                            baseSummary.peakClientRssBytes(),
                            targetSummary.peakClientRssBytes(),
                            MetricDirection.LOWER_BETTER));
            comparisons.add(
                    compare(
                            "maxServerRss",
                            baseSummary.peakServerRssBytes(),
                            targetSummary.peakServerRssBytes(),
                            MetricDirection.LOWER_BETTER));
            comparisons.add(
                    compare(
                            "serverGcCount",
                            baseSummary.peakServerGcCount(),
                            targetSummary.peakServerGcCount(),
                            MetricDirection.LOWER_BETTER));
            comparisons.add(
                    compare(
                            "serverGcTimeMs",
                            baseSummary.peakServerGcTimeMs(),
                            targetSummary.peakServerGcTimeMs(),
                            MetricDirection.LOWER_BETTER));
        }

        return new DiffResult(comparisons);
    }

    private static PhaseResult findPhase(PerfReport report, String name) {
        return report.phaseResults().stream()
                .filter(pr -> pr.phaseName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private static DiffResult.MetricComparison compare(
            String name, double baselineValue, double targetValue, MetricDirection direction) {
        double changePct = 0.0;
        if (baselineValue != 0.0) {
            changePct = ((targetValue - baselineValue) / baselineValue) * 100.0;
        } else if (targetValue != 0.0) {
            changePct = targetValue > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }

        String verdict;
        if (Double.isInfinite(changePct)) {
            if (direction == MetricDirection.HIGHER_BETTER) {
                verdict = changePct > 0 ? "improvement" : "regression";
            } else {
                verdict = changePct < 0 ? "improvement" : "regression";
            }
        } else if (Math.abs(changePct) < NEUTRAL_THRESHOLD) {
            verdict = "neutral";
        } else if (direction == MetricDirection.HIGHER_BETTER) {
            verdict = changePct > 0 ? "improvement" : "regression";
        } else {
            // lower_better
            verdict = changePct < 0 ? "improvement" : "regression";
        }

        return new DiffResult.MetricComparison(
                name, baselineValue, targetValue, changePct, direction, verdict);
    }
}
