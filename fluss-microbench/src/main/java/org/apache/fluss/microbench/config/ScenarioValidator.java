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

package org.apache.fluss.microbench.config;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.TimeUtils.parseDuration;

/** Validates a {@link ScenarioConfig} for semantic correctness after YAML parsing. */
public class ScenarioValidator {

    private static final long MIN_RECORDS = 100_000;
    private static final Duration MIN_DURATION = Duration.ofSeconds(30);

    private static final Set<PhaseType> PK_ONLY_PHASES =
            Set.of(PhaseType.LOOKUP, PhaseType.PREFIX_LOOKUP);
    private static final Set<String> RBM_TYPES = Set.of("RBM32", "RBM64");

    public static List<String> validate(ScenarioConfig config) {
        List<String> errors = new ArrayList<>();
        validateTable(config.table(), errors);
        validateWorkload(config.table(), config.workload(), errors);
        validateData(config.table(), config.data(), errors);
        return errors;
    }

    private static void validateTable(TableConfig table, List<String> errors) {
        if (table == null) {
            return;
        }
        boolean hasPk = table.hasPrimaryKey();
        boolean hasAgg = table.columns().stream().anyMatch(c -> c.agg() != null);
        String mergeEngine = table.mergeEngine();

        if (hasAgg && !hasPk) {
            errors.add("table: columns with agg require primary-key");
        }
        if (hasAgg && !"AGGREGATION".equals(mergeEngine)) {
            errors.add("table: columns with agg require merge-engine: AGGREGATION");
        }
        if ("VERSIONED".equals(mergeEngine) && hasAgg) {
            errors.add("table: VERSIONED merge-engine does not support agg columns");
        }
        if ("VERSIONED".equals(mergeEngine)) {
            Map<String, String> props = table.properties();
            if (props == null || !props.containsKey("table.merge-engine.versioned.ver-column")) {
                errors.add(
                        "table: VERSIONED merge-engine requires "
                                + "'table.merge-engine.versioned.ver-column' in properties");
            }
        }
        if (mergeEngine != null && !hasPk) {
            errors.add("table: merge-engine requires primary-key");
        }

        Set<String> pkSet = hasPk ? Set.copyOf(table.primaryKey()) : Set.of();
        for (ColumnConfig col : table.columns()) {
            if (col.agg() != null && pkSet.contains(col.name())) {
                errors.add(
                        "table.columns: agg column '" + col.name() + "' cannot be in primary-key");
            }
            if (col.agg() != null) {
                String fn = col.agg().function();
                if (RBM_TYPES.contains(fn) && !"BYTES".equals(col.type())) {
                    errors.add(
                            "table.columns: "
                                    + fn
                                    + " requires type BYTES, got "
                                    + col.type()
                                    + " on column '"
                                    + col.name()
                                    + "'");
                }
            }
        }
    }

    private static void validateWorkload(
            TableConfig table, List<WorkloadPhaseConfig> phases, List<String> errors) {
        if (table == null || phases == null) {
            return;
        }

        // Check for duplicate phase names
        Set<String> phaseNames = new HashSet<>();
        for (WorkloadPhaseConfig phase : phases) {
            if (!phaseNames.add(phase.phase())) {
                errors.add(
                        "Duplicate phase name: '"
                                + phase.phase()
                                + "'. Each phase must have a unique name for reliable diff comparison.");
            }
        }

        boolean hasPk = table.hasPrimaryKey();
        for (int i = 0; i < phases.size(); i++) {
            WorkloadPhaseConfig phase = phases.get(i);
            String prefix = "workload[" + i + "]: ";

            if (phase.records() == null && phase.duration() == null) {
                errors.add(prefix + "must specify either records or duration");
            }

            if (phase.records() != null && phase.records() < MIN_RECORDS) {
                errors.add(
                        prefix
                                + "records must be >= "
                                + MIN_RECORDS
                                + " (got "
                                + phase.records()
                                + "). Performance results require sufficient data volume.");
            }

            if (phase.duration() != null) {
                try {
                    Duration d = parseDuration(phase.duration().trim());
                    if (d.compareTo(MIN_DURATION) < 0) {
                        errors.add(
                                prefix
                                        + "duration must be >= "
                                        + MIN_DURATION.toSeconds()
                                        + "s (got "
                                        + phase.duration()
                                        + "). Performance results require sufficient run time.");
                    }
                } catch (Exception e) {
                    // Duration parsing errors handled elsewhere
                }
            }

            if (!hasPk && PK_ONLY_PHASES.contains(parsePhaseType(phase.phase()))) {
                errors.add(
                        prefix
                                + phase.phase()
                                + " is not supported for log tables (no primary-key)");
            }

            if (PhaseType.MIXED == parsePhaseType(phase.phase())) {
                if (phase.mix() == null || phase.mix().isEmpty()) {
                    errors.add(prefix + "mixed phase requires mix map");
                } else {
                    int sum = phase.mix().values().stream().mapToInt(Integer::intValue).sum();
                    if (sum != 100) {
                        errors.add(prefix + "mix percentages must sum to 100, got " + sum);
                    }
                    if (!hasPk) {
                        for (String key : phase.mix().keySet()) {
                            if (PK_ONLY_PHASES.contains(parsePhaseType(key))) {
                                errors.add(
                                        prefix
                                                + "mix contains '"
                                                + key
                                                + "' which is not supported for log tables");
                            }
                        }
                    }
                }
            }

            // Validate warmup vs records ratio and effective per-thread records.
            // These checks only apply to record-count-based phases. Duration-based
            // phases (no records field) may have large warmup values to cover JIT C2
            // compilation without being constrained by a record budget.
            if (phase.records() != null && phase.warmup() != null) {
                long warmup = parseWarmupValue(phase.warmup(), phase.records());
                if (warmup >= phase.records() / 2) {
                    errors.add(
                            prefix
                                    + "warmup ("
                                    + warmup
                                    + ") must be less than half of records ("
                                    + phase.records()
                                    + ")");
                }
                int threads = phase.threads() != null ? phase.threads() : 1;
                long effectivePerThread = (phase.records() - warmup) / threads;
                if (effectivePerThread < 10000) {
                    errors.add(
                            prefix
                                    + "effective records per thread ("
                                    + effectivePerThread
                                    + ") must be >= 10,000 for reliable results");
                }
            }
        }
    }

    private static void validateData(TableConfig table, DataConfig data, List<String> errors) {
        // TODO: Add generator-vs-column-type compatibility checks once custom generator
        //       plugins are supported. Currently, WriteExecutor.inferGeneratorType() handles
        //       auto-selection based on column data type, so missing generators are not errors.
        if (table == null || data == null) {
            return;
        }
    }

    @Nullable
    private static PhaseType parsePhaseType(String phase) {
        try {
            return PhaseType.fromString(phase);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parses a warmup value (absolute number or percentage like "10%") into an absolute record
     * count.
     */
    private static long parseWarmupValue(String warmup, long totalRecords) {
        if (warmup == null || warmup.isEmpty()) {
            return 0;
        }
        warmup = warmup.trim();
        if (warmup.endsWith("%")) {
            double pct = Double.parseDouble(warmup.substring(0, warmup.length() - 1)) / 100.0;
            return (long) (totalRecords * pct);
        }
        // Time-based warmup (e.g. "60s", "1m") — not comparable to record counts
        try {
            return Long.parseLong(warmup);
        } catch (NumberFormatException e) {
            return 0; // time-based warmup, skip record-count validation
        }
    }
}
