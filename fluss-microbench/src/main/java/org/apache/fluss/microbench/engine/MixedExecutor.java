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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.PhaseType;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.datagen.FieldGenerator;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.row.GenericRow;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executor for mixed workload phases. Each operation is independently selected by weighted random
 * from the configured {@code mix} percentages, sharing a single phase clock and rate limiter.
 *
 * <p>Unlike other executors, MixedExecutor opens a single Table and pre-creates all needed
 * resources (writers, lookupers) upfront, reusing them across operations. This avoids the overhead
 * of per-operation Table open/close which would dominate latency measurements.
 */
public class MixedExecutor implements PhaseExecutor {

    @FunctionalInterface
    private interface MixedOp {
        void execute(long index) throws Exception;
    }

    /** Shared context for all mixed operation lambdas, avoiding 15-parameter method signatures. */
    private static class MixedOpContext {
        final Table table;
        final TableConfig tableConfig;
        final WorkloadPhaseConfig phaseConfig;
        final FieldGenerator[] generators;
        final List<ColumnConfig> columns;
        final boolean hasPrimaryKey;
        final LatencyRecorder latencyRecorder;
        final ThroughputCounter throughputCounter;
        final long warmupOps;
        final AtomicReference<Throwable> asyncError;
        final Random random;
        final long startIndex;
        final long endIndex;
        final List<Runnable> flushCallbacks;
        final List<Closeable> closeables;

        MixedOpContext(
                Table table,
                TableConfig tableConfig,
                WorkloadPhaseConfig phaseConfig,
                FieldGenerator[] generators,
                List<ColumnConfig> columns,
                boolean hasPrimaryKey,
                LatencyRecorder latencyRecorder,
                ThroughputCounter throughputCounter,
                long warmupOps,
                AtomicReference<Throwable> asyncError,
                Random random,
                long startIndex,
                long endIndex,
                List<Runnable> flushCallbacks,
                List<Closeable> closeables) {
            this.table = table;
            this.tableConfig = tableConfig;
            this.phaseConfig = phaseConfig;
            this.generators = generators;
            this.columns = columns;
            this.hasPrimaryKey = hasPrimaryKey;
            this.latencyRecorder = latencyRecorder;
            this.throughputCounter = throughputCounter;
            this.warmupOps = warmupOps;
            this.asyncError = asyncError;
            this.random = random;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.flushCallbacks = flushCallbacks;
            this.closeables = closeables;
        }
    }

    @Override
    public void execute(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phaseConfig,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex)
            throws Exception {

        Map<String, Integer> mix = phaseConfig.mix();
        if (mix == null || mix.isEmpty()) {
            throw new IllegalArgumentException("Mixed executor requires a non-empty 'mix' config");
        }

        // Build weighted entries (sorted for deterministic ordering across JVM runs)
        List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(mix.entrySet());
        sortedEntries.sort(Map.Entry.comparingByKey());
        String[] phases = new String[sortedEntries.size()];
        int[] cumulativeWeights = new int[sortedEntries.size()];
        int totalWeight = 0;
        int idx = 0;
        for (Map.Entry<String, Integer> entry : sortedEntries) {
            phases[idx] = entry.getKey();
            totalWeight += entry.getValue();
            cumulativeWeights[idx] = totalWeight;
            idx++;
        }

        Random random = ExecutorUtils.createSeededRandom(dataConfig, startIndex);

        long warmupOps =
                ExecutorUtils.parseWarmupOps(
                        phaseConfig.warmup(),
                        endIndex - startIndex,
                        phaseConfig.threads() != null ? phaseConfig.threads() : 1);
        Long rateLimit = phaseConfig.rateLimit();
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());

        List<ColumnConfig> columns = tableConfig.columns();
        FieldGenerator[] generators = ExecutorUtils.buildGenerators(columns, dataConfig);
        boolean hasPrimaryKey = tableConfig.hasPrimaryKey();

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());

        try (Table table = conn.getTable(tablePath)) {
            // Pre-create operation lambdas that reuse the single Table and its resources.
            // Flush callbacks are collected so buffered writes can be flushed after the loop.
            AtomicReference<Throwable> asyncError = new AtomicReference<>();
            List<Runnable> flushCallbacks = new ArrayList<>();
            List<Closeable> closeables = new ArrayList<>();
            MixedOpContext ctx =
                    new MixedOpContext(
                            table,
                            tableConfig,
                            phaseConfig,
                            generators,
                            columns,
                            hasPrimaryKey,
                            latencyRecorder,
                            throughputCounter,
                            warmupOps,
                            asyncError,
                            random,
                            startIndex,
                            endIndex,
                            flushCallbacks,
                            closeables);
            MixedOp[] ops = new MixedOp[phases.length];
            for (int i = 0; i < phases.length; i++) {
                ops[i] = createOp(phases[i], ctx);
            }

            try {
                long startTime = System.nanoTime();
                long opsCount = 0;

                for (long i = startIndex; i < endIndex; i++) {
                    if (duration != null && ExecutorUtils.isExpired(startTime, duration)) {
                        break;
                    }

                    // Check for async write errors
                    Throwable err = asyncError.get();
                    if (err != null) {
                        throw err instanceof Exception
                                ? (Exception) err
                                : new RuntimeException(err);
                    }

                    // Weighted random selection
                    int roll = random.nextInt(totalWeight);
                    int selected = 0;
                    for (int w = 0; w < cumulativeWeights.length; w++) {
                        if (roll < cumulativeWeights[w]) {
                            selected = w;
                            break;
                        }
                    }

                    ops[selected].execute(i);

                    opsCount++;
                    ExecutorUtils.throttleIfNeeded(rateLimit, opsCount, startTime);
                }

                // Flush any buffered writes
                for (Runnable flush : flushCallbacks) {
                    flush.run();
                }

                // Final async error check
                Throwable err = asyncError.get();
                if (err != null) {
                    throw err instanceof Exception ? (Exception) err : new RuntimeException(err);
                }
            } finally {
                // Close writers and lookupers created during op setup
                for (Closeable c : closeables) {
                    try {
                        c.close();
                    } catch (Exception ignored) {
                        // best-effort cleanup
                    }
                }
            }
        }
    }

    private MixedOp createOp(String phase, MixedOpContext ctx) {
        PhaseType type = PhaseType.fromString(phase);
        switch (type) {
            case WRITE:
                return createWriteOp(ctx);

            case LOOKUP:
                return createLookupOp(ctx);

            case PREFIX_LOOKUP:
                return createPrefixLookupOp(ctx);

            case SCAN:
                throw new IllegalArgumentException("SCAN is not supported in mixed workload");

            case MIXED:
                throw new IllegalArgumentException("MIXED cannot be nested");

            default:
                throw new IllegalArgumentException("Unknown phase type in mix: " + phase);
        }
    }

    private MixedOp createWriteOp(MixedOpContext ctx) {
        WriteExecutor.AsyncWriteOp writeOp;
        Runnable flushOp;

        if (ctx.hasPrimaryKey) {
            UpsertWriter writer = ctx.table.newUpsert().createWriter();
            writeOp = writer::upsert;
            flushOp =
                    () -> {
                        try {
                            writer.flush();
                        } catch (Exception e) {
                            ctx.asyncError.compareAndSet(null, e);
                        }
                    };
        } else {
            AppendWriter writer = ctx.table.newAppend().createWriter();
            writeOp = writer::append;
            flushOp =
                    () -> {
                        try {
                            writer.flush();
                        } catch (Exception e) {
                            ctx.asyncError.compareAndSet(null, e);
                        }
                    };
        }
        ctx.flushCallbacks.add(flushOp);

        return index -> {
            GenericRow row = ExecutorUtils.buildRow(ctx.generators, ctx.columns, index);
            int rowBytes = ExecutorUtils.estimateRowBytes(row, ctx.columns);
            long opStart = System.nanoTime();
            boolean record = (index - ctx.startIndex + 1) > ctx.warmupOps;
            writeOp.write(row)
                    .whenComplete(
                            (result, error) -> {
                                if (error != null) {
                                    ctx.asyncError.compareAndSet(null, error);
                                    return;
                                }
                                if (record) {
                                    ctx.latencyRecorder.record(System.nanoTime() - opStart);
                                    ctx.throughputCounter.record(rowBytes);
                                }
                            });
        };
    }

    /**
     * Builds a MixedOp lambda that performs async lookups using the given lookuper and key
     * configuration. Shared by {@link #createLookupOp} and {@link #createPrefixLookupOp}.
     */
    private MixedOp buildLookupLambda(
            Lookuper lookuper,
            int[] keyIndexes,
            int keyBytes,
            long keyMin,
            long keyMax,
            MixedOpContext ctx) {
        return index -> {
            long keyIndex = keyMin + (long) (ctx.random.nextDouble() * (keyMax - keyMin));
            GenericRow keyRow =
                    ExecutorUtils.buildKeyRow(ctx.generators, ctx.columns, keyIndexes, keyIndex);

            long opStart = System.nanoTime();
            boolean record = (index - ctx.startIndex + 1) > ctx.warmupOps;

            lookuper.lookup(keyRow)
                    .whenComplete(
                            (result, error) -> {
                                if (error != null) {
                                    ctx.asyncError.compareAndSet(null, error);
                                    return;
                                }
                                if (record) {
                                    ctx.latencyRecorder.record(System.nanoTime() - opStart);
                                    ctx.throughputCounter.record(keyBytes);
                                }
                            });
        };
    }

    private MixedOp createLookupOp(MixedOpContext ctx) {
        List<String> pkColumns = ctx.tableConfig.primaryKey();
        if (!ctx.tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Lookup in mix requires a primary key table");
        }
        int[] pkIndexes = ExecutorUtils.resolvePkIndexes(pkColumns, ctx.columns);
        long keyMin = ExecutorUtils.resolveKeyMin(ctx.phaseConfig.keyRange(), ctx.startIndex);
        long keyMax = ExecutorUtils.resolveKeyMax(ctx.phaseConfig.keyRange(), ctx.endIndex);
        Lookuper lookuper = ctx.table.newLookup().createLookuper();
        int keyBytes = ExecutorUtils.estimateKeyBytes(ctx.columns, pkIndexes);
        return buildLookupLambda(lookuper, pkIndexes, keyBytes, keyMin, keyMax, ctx);
    }

    private MixedOp createPrefixLookupOp(MixedOpContext ctx) {
        List<String> pkColumns = ctx.tableConfig.primaryKey();
        if (!ctx.tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Prefix lookup in mix requires a primary key table");
        }
        int prefixLength =
                ctx.phaseConfig.keyPrefixLength() != null
                        ? ctx.phaseConfig.keyPrefixLength()
                        : pkColumns.size();
        List<String> prefixColumns = pkColumns.subList(0, prefixLength);
        int[] prefixIndexes = ExecutorUtils.resolvePkIndexes(prefixColumns, ctx.columns);
        long keyMin = ExecutorUtils.resolveKeyMin(ctx.phaseConfig.keyRange(), ctx.startIndex);
        long keyMax = ExecutorUtils.resolveKeyMax(ctx.phaseConfig.keyRange(), ctx.endIndex);
        Lookuper lookuper = ctx.table.newLookup().lookupBy(prefixColumns).createLookuper();
        int keyBytes = ExecutorUtils.estimateKeyBytes(ctx.columns, prefixIndexes);
        return buildLookupLambda(lookuper, prefixIndexes, keyBytes, keyMin, keyMax, ctx);
    }
}
