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

package org.apache.fluss.microbench.engine;

import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.datagen.DataGeneratorFactory;
import org.apache.fluss.microbench.datagen.FieldGenerator;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.TimeUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/** Shared utilities for phase executors. */
public final class ExecutorUtils {

    static final String DEFAULT_DATABASE = "fluss";

    /**
     * Cache for normalized column type strings to avoid repeated trim().toUpperCase() on hot path.
     */
    private static final ConcurrentHashMap<String, String> NORMALIZED_TYPE_CACHE =
            MapUtils.newConcurrentHashMap();

    private ExecutorUtils() {}

    @Nullable
    static Duration parseDuration(@Nullable String durationStr) {
        if (durationStr == null || durationStr.isEmpty()) {
            return null;
        }
        return TimeUtils.parseDuration(durationStr.trim());
    }

    static long parseWarmupOps(String warmup, long totalOps, int threads) {
        if (warmup == null || warmup.isEmpty()) {
            return 0;
        }
        warmup = warmup.trim();
        // Time-based warmup (e.g. "60s", "1m") is handled by parseWarmupDuration
        if (isTimeBased(warmup)) {
            return 0;
        }
        if (warmup.endsWith("%")) {
            double pct = Double.parseDouble(warmup.substring(0, warmup.length() - 1)) / 100.0;
            return (long) (totalOps * pct);
        }
        // Absolute warmup is global; divide by threads to get per-thread warmup
        return Long.parseLong(warmup) / Math.max(threads, 1);
    }

    /**
     * Parses a time-based warmup duration (e.g. "60s", "1m", "90s"). Returns null if the warmup
     * string is not time-based (i.e. it's a record count or percentage).
     */
    @Nullable
    static Duration parseWarmupDuration(@Nullable String warmup) {
        if (warmup == null || warmup.isEmpty()) {
            return null;
        }
        warmup = warmup.trim();
        if (!isTimeBased(warmup)) {
            return null;
        }
        return TimeUtils.parseDuration(warmup);
    }

    private static boolean isTimeBased(String value) {
        return value.endsWith("s")
                || value.endsWith("m")
                || value.endsWith("h")
                || value.endsWith("ms")
                || value.endsWith("min");
    }

    static long resolveKeyMin(List<Long> keyRange, long fallback) {
        return (keyRange != null && keyRange.size() >= 1) ? keyRange.get(0) : fallback;
    }

    static long resolveKeyMax(List<Long> keyRange, long fallback) {
        return (keyRange != null && keyRange.size() >= 2) ? keyRange.get(1) : fallback;
    }

    static boolean isExpired(long startNanos, Duration duration) {
        return (System.nanoTime() - startNanos) >= duration.toNanos();
    }

    static void throttleIfNeeded(Long rateLimit, long opsCount, long startNanos)
            throws InterruptedException {
        if (rateLimit == null || rateLimit <= 0) {
            return;
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        long expectedMs = opsCount * 1000 / rateLimit;
        if (expectedMs > elapsedMs) {
            Thread.sleep(expectedMs - elapsedMs);
        }
    }

    public static FieldGenerator[] buildGenerators(
            List<ColumnConfig> columns, DataConfig dataConfig) {
        long seed = resolveSeed(dataConfig);
        Map<String, Map<String, Object>> genConfigs =
                dataConfig.generators() != null ? dataConfig.generators() : Collections.emptyMap();

        FieldGenerator[] generators = new FieldGenerator[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ColumnConfig col = columns.get(i);
            Map<String, Object> genConfig =
                    genConfigs.getOrDefault(col.name(), Collections.emptyMap());
            String type = genConfig.containsKey("type") ? (String) genConfig.get("type") : null;
            if (type == null) {
                type = inferGeneratorType(col.type());
            }
            // The genConfig map IS the params (type/start/end/min/max etc are all at the same
            // level)
            generators[i] = DataGeneratorFactory.create(type, genConfig, seed + i);
        }
        return generators;
    }

    public static GenericRow buildRow(
            FieldGenerator[] generators, List<ColumnConfig> columns, long index) {
        GenericRow row = new GenericRow(columns.size());
        for (int c = 0; c < columns.size(); c++) {
            Object value = generators[c].generate(index);
            value = coerceType(value, columns.get(c).type());
            row.setField(c, value);
        }
        return row;
    }

    /** Builds a lookup key row from the specified column indexes. */
    public static GenericRow buildKeyRow(
            FieldGenerator[] generators,
            List<ColumnConfig> columns,
            int[] keyIndexes,
            long keyIndex) {
        GenericRow keyRow = new GenericRow(keyIndexes.length);
        for (int k = 0; k < keyIndexes.length; k++) {
            Object val = generators[keyIndexes[k]].generate(keyIndex);
            val = coerceType(val, columns.get(keyIndexes[k]).type());
            keyRow.setField(k, val);
        }
        return keyRow;
    }

    /**
     * Estimates the serialized byte size of a row based on column types and values. This is an
     * approximation used for throughput byte counting — not exact wire format size, but close
     * enough for meaningful MB/s reporting.
     */
    public static int estimateRowBytes(GenericRow row, List<ColumnConfig> columns) {
        int size = 0;
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object val = row.getField(i);
            if (val == null) {
                continue;
            }
            if (val instanceof BinaryString) {
                size += ((BinaryString) val).getSizeInBytes();
            } else if (val instanceof byte[]) {
                size += ((byte[]) val).length;
            } else if (val instanceof Long) {
                size += 8;
            } else if (val instanceof Integer) {
                size += 4;
            } else if (val instanceof Double) {
                size += 8;
            } else if (val instanceof Float) {
                size += 4;
            } else if (val instanceof Short) {
                size += 2;
            } else if (val instanceof Byte) {
                size += 1;
            } else if (val instanceof Boolean) {
                size += 1;
            } else {
                size += 8; // fallback estimate
            }
        }
        return size;
    }

    /**
     * Coerces generator output to the type expected by Fluss. Generators may return Integer for
     * sequential/random-int, but BIGINT columns require Long.
     */
    public static Object coerceType(Object value, String columnType) {
        if (value == null || columnType == null) {
            return value;
        }
        if (value instanceof String) {
            return BinaryString.fromString((String) value);
        }
        String upper =
                NORMALIZED_TYPE_CACHE.computeIfAbsent(columnType, k -> k.trim().toUpperCase());
        if (upper.equals("BIGINT") && value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        if ((upper.equals("FLOAT")) && value instanceof Double) {
            return ((Double) value).floatValue();
        }
        if ((upper.startsWith("INT") || upper.equals("TINYINT") || upper.equals("SMALLINT"))
                && value instanceof Long) {
            return ((Long) value).intValue();
        }
        return value;
    }

    static String inferGeneratorType(String typeStr) {
        if (typeStr == null) {
            return "random-string";
        }
        String upper = typeStr.trim().toUpperCase();
        if (upper.startsWith("INT") || upper.equals("TINYINT") || upper.equals("SMALLINT")) {
            return "random-int";
        } else if (upper.equals("BIGINT")) {
            return "random-long";
        } else if (upper.equals("FLOAT")) {
            return "random-float";
        } else if (upper.equals("DOUBLE")) {
            return "random-double";
        } else if (upper.equals("BOOLEAN")) {
            return "random-boolean";
        } else if (upper.startsWith("BINARY") || upper.startsWith("BYTES")) {
            return "random-bytes";
        } else if (upper.startsWith("TIMESTAMP")) {
            return "timestamp-now";
        } else {
            return "random-string";
        }
    }

    static int[] resolvePkIndexes(List<String> pkColumns, List<ColumnConfig> columns) {
        int[] indexes = new int[pkColumns.size()];
        for (int i = 0; i < pkColumns.size(); i++) {
            String pkCol = pkColumns.get(i);
            boolean found = false;
            for (int j = 0; j < columns.size(); j++) {
                if (columns.get(j).name().equals(pkCol)) {
                    indexes[i] = j;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException(
                        "Primary key column '" + pkCol + "' not found in table columns");
            }
        }
        return indexes;
    }

    static long resolveSeed(DataConfig dataConfig) {
        return dataConfig.seed() != null ? dataConfig.seed() : 42L;
    }

    static Random createSeededRandom(DataConfig dataConfig, long startIndex) {
        return new Random(resolveSeed(dataConfig) ^ startIndex);
    }

    /** Estimates key byte size from column types for throughput counting. */
    static int estimateKeyBytes(List<ColumnConfig> columns, int[] indexes) {
        int bytes = 0;
        for (int index : indexes) {
            String colType = columns.get(index).type().trim().toUpperCase();
            if (colType.equals("BIGINT")) {
                bytes += 8;
            } else if (colType.startsWith("INT")
                    || colType.equals("TINYINT")
                    || colType.equals("SMALLINT")) {
                bytes += 4;
            } else {
                bytes += 16;
            }
        }
        return Math.max(bytes, 1);
    }

    /**
     * Builds a {@link LookupExecutor.LookupContext} from common executor parameters. Shared by
     * {@link LookupExecutor} and {@link PrefixLookupExecutor}.
     */
    static LookupExecutor.LookupContext buildLookupContext(
            DataConfig dataConfig,
            WorkloadPhaseConfig phaseConfig,
            List<ColumnConfig> columns,
            int[] keyIndexes,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex) {
        FieldGenerator[] generators = buildGenerators(columns, dataConfig);
        long warmupOps =
                parseWarmupOps(
                        phaseConfig.warmup(),
                        endIndex - startIndex,
                        phaseConfig.threads() != null ? phaseConfig.threads() : 1);
        Long rateLimit = phaseConfig.rateLimit();
        Duration duration = parseDuration(phaseConfig.duration());
        long keyMin = resolveKeyMin(phaseConfig.keyRange(), startIndex);
        long keyMax = resolveKeyMax(phaseConfig.keyRange(), endIndex);
        Random random = createSeededRandom(dataConfig, startIndex);
        int keyBytes = estimateKeyBytes(columns, keyIndexes);
        return new LookupExecutor.LookupContext(
                generators,
                columns,
                keyIndexes,
                latencyRecorder,
                throughputCounter,
                startIndex,
                endIndex,
                warmupOps,
                rateLimit,
                duration,
                keyMin,
                keyMax,
                keyBytes,
                random);
    }

    /** Estimates row byte size from column type definitions for throughput counting. */
    public static int estimateRowBytesFromSchema(List<ColumnConfig> columns) {
        int size = 0;
        for (ColumnConfig col : columns) {
            String upper = col.type().trim().toUpperCase();
            if (upper.equals("BIGINT")) {
                size += 8;
            } else if (upper.startsWith("INT")
                    || upper.equals("TINYINT")
                    || upper.equals("SMALLINT")) {
                size += 4;
            } else if (upper.equals("DOUBLE")) {
                size += 8;
            } else if (upper.equals("FLOAT")) {
                size += 4;
            } else if (upper.equals("BOOLEAN")) {
                size += 1;
            } else if (upper.startsWith("STRING") || upper.startsWith("VARCHAR")) {
                size += 64;
            } else if (upper.startsWith("BINARY") || upper.startsWith("BYTES")) {
                size += 64;
            } else {
                size += 16;
            }
        }
        return Math.max(size, 1);
    }
}
