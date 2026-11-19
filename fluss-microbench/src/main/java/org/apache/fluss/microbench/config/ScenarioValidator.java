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

import org.apache.fluss.config.MemorySize;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.TimeUtils.parseDuration;

/** Validates a scenario after YAML parsing. */
public final class ScenarioValidator {

    private ScenarioValidator() {}

    public static List<String> validate(ScenarioConfig config) {
        List<String> errors = new ArrayList<>();
        for (String field : config.parseWarnings()) {
            errors.add("unknown YAML field: " + field);
        }
        if (config.client() != null && config.client().properties() != null) {
            String writerBuffer =
                    config.client().properties().get("client.writer.buffer.memory-size");
            if (writerBuffer != null) {
                try {
                    if (MemorySize.parseBytes(writerBuffer) <= 256L * 1024 * 1024) {
                        errors.add("client.writer.buffer.memory-size must exceed 256mb");
                    }
                } catch (IllegalArgumentException e) {
                    errors.add("invalid client.writer.buffer.memory-size: " + writerBuffer);
                }
            }
        }
        TableConfig table = config.table();
        List<WorkloadPhaseConfig> workload = config.workload();
        if (table == null || table.columns() == null || table.columns().isEmpty()) {
            errors.add("table.columns must not be empty");
        }
        if (workload == null || workload.isEmpty()) {
            errors.add("workload must not be empty");
        }
        if (!errors.isEmpty()) {
            return errors;
        }

        boolean hasPk = table.hasPrimaryKey();
        boolean hasAgg = table.columns().stream().anyMatch(column -> column.agg() != null);
        String mergeEngine = table.mergeEngine();
        if (hasAgg && (!hasPk || !"AGGREGATION".equals(mergeEngine))) {
            errors.add("aggregation columns require a primary key and AGGREGATION merge engine");
        }
        if (mergeEngine != null && !hasPk) {
            errors.add("merge-engine requires primary-key");
        }
        if ("VERSIONED".equals(mergeEngine)) {
            Map<String, String> properties = table.properties();
            if (hasAgg) {
                errors.add("VERSIONED merge-engine does not support aggregation columns");
            }
            if (properties == null
                    || !properties.containsKey("table.merge-engine.versioned.ver-column")) {
                errors.add(
                        "VERSIONED merge-engine requires table.merge-engine.versioned.ver-column");
            }
        }
        Set<String> primaryKeys =
                hasPk ? new HashSet<>(table.primaryKey()) : java.util.Collections.emptySet();
        for (ColumnConfig column : table.columns()) {
            if (column.agg() == null) {
                continue;
            }
            if (primaryKeys.contains(column.name())) {
                errors.add("aggregation column cannot be in primary-key: " + column.name());
            }
            String function = column.agg().function();
            if (("RBM32".equals(function) || "RBM64".equals(function))
                    && !"BYTES".equals(column.type())) {
                errors.add(function + " requires BYTES column: " + column.name());
            }
        }

        Set<String> names = new HashSet<>();
        for (int i = 0; i < workload.size(); i++) {
            WorkloadPhaseConfig phase = workload.get(i);
            String prefix = "workload[" + i + "]: ";
            if (!names.add(phase.phase())) {
                errors.add(prefix + "duplicate phase: " + phase.phase());
            }
            PhaseType type = parsePhaseType(phase.phase());
            if (type == null) {
                errors.add(prefix + "unknown phase: " + phase.phase());
                continue;
            }
            if (phase.records() == null && phase.duration() == null) {
                errors.add(prefix + "records or duration is required");
            }
            if (phase.records() != null && phase.records() < 100_000) {
                errors.add(prefix + "records must be >= 100000");
            }
            if (phase.duration() != null) {
                try {
                    if (parseDuration(phase.duration().trim()).compareTo(Duration.ofSeconds(30))
                            < 0) {
                        errors.add(prefix + "duration must be >= 30s");
                    }
                } catch (Exception e) {
                    errors.add(prefix + "invalid duration: " + phase.duration());
                }
            }
            if (!hasPk && type == PhaseType.LOOKUP) {
                errors.add(prefix + "lookup requires a primary key table");
            }
            if (type == PhaseType.MIXED) {
                Map<String, Integer> mix = phase.mix();
                if (mix == null || mix.isEmpty()) {
                    errors.add(prefix + "mixed phase requires mix map");
                } else {
                    int sum = mix.values().stream().mapToInt(Integer::intValue).sum();
                    if (sum != 100) {
                        errors.add(prefix + "mix percentages must sum to 100");
                    }
                    if (!hasPk
                            && mix.keySet().stream()
                                    .anyMatch(key -> parsePhaseType(key) == PhaseType.LOOKUP)) {
                        errors.add(prefix + "lookup in mix requires a primary key table");
                    }
                }
            }
            if (phase.records() != null && phase.warmup() != null) {
                long warmup = warmupRecords(phase.warmup(), phase.records());
                if (warmup >= phase.records() / 2) {
                    errors.add(prefix + "warmup must be less than half of records");
                }
                int threads = phase.threads() == null ? 1 : phase.threads();
                if ((phase.records() - warmup) / threads < 10_000) {
                    errors.add(prefix + "effective records per thread must be >= 10000");
                }
            }
        }
        return errors;
    }

    private static PhaseType parsePhaseType(String name) {
        try {
            return PhaseType.fromString(name);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static long warmupRecords(String value, long records) {
        String warmup = value.trim();
        if (warmup.endsWith("%")) {
            return (long)
                    (records * Double.parseDouble(warmup.substring(0, warmup.length() - 1)) / 100);
        }
        try {
            return Long.parseLong(warmup);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
