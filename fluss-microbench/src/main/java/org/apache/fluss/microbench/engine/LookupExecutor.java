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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.datagen.FieldGenerator;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.row.GenericRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executor for lookup workload phases. Uses async pipelined lookups (matching the Flink connector's
 * async lookup pattern) to maximize throughput. A semaphore limits the number of in-flight lookups
 * to prevent unbounded memory growth.
 */
public class LookupExecutor implements PhaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(LookupExecutor.class);

    /** Maximum number of concurrent in-flight lookups per thread. */
    private static final int MAX_IN_FLIGHT = 256;

    /** Context object for async lookup loop parameters. */
    static class LookupContext {
        final FieldGenerator[] generators;
        final List<ColumnConfig> columns;
        final int[] keyIndexes;
        final LatencyRecorder latencyRecorder;
        final ThroughputCounter throughputCounter;
        final long startIndex;
        final long endIndex;
        final long warmupOps;
        final Long rateLimit;
        final Duration duration;
        final long keyMin;
        final long keyMax;
        final int keyBytes;
        final Random random;

        LookupContext(
                FieldGenerator[] generators,
                List<ColumnConfig> columns,
                int[] keyIndexes,
                LatencyRecorder latencyRecorder,
                ThroughputCounter throughputCounter,
                long startIndex,
                long endIndex,
                long warmupOps,
                Long rateLimit,
                Duration duration,
                long keyMin,
                long keyMax,
                int keyBytes,
                Random random) {
            this.generators = generators;
            this.columns = columns;
            this.keyIndexes = keyIndexes;
            this.latencyRecorder = latencyRecorder;
            this.throughputCounter = throughputCounter;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.warmupOps = warmupOps;
            this.rateLimit = rateLimit;
            this.duration = duration;
            this.keyMin = keyMin;
            this.keyMax = keyMax;
            this.keyBytes = keyBytes;
            this.random = random;
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

        List<ColumnConfig> columns = tableConfig.columns();
        List<String> pkColumns = tableConfig.primaryKey();
        if (!tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Lookup requires a primary key table");
        }

        int[] pkIndexes = ExecutorUtils.resolvePkIndexes(pkColumns, columns);
        LookupContext ctx =
                ExecutorUtils.buildLookupContext(
                        dataConfig,
                        phaseConfig,
                        columns,
                        pkIndexes,
                        latencyRecorder,
                        throughputCounter,
                        startIndex,
                        endIndex);

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            asyncLookupLoop(lookuper, ctx);
        }
    }

    /**
     * Async pipelined lookup loop. Fires lookups without blocking, records latency in completion
     * callbacks. Uses a semaphore to cap in-flight requests.
     */
    static void asyncLookupLoop(Lookuper lookuper, LookupContext ctx) {
        long startTime = System.nanoTime();
        long opsCount = 0;
        Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
        AtomicReference<Throwable> asyncError = new AtomicReference<>();

        try {
            for (long i = ctx.startIndex; i < ctx.endIndex; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    LOG.info("Lookup interrupted after {} ops, exiting gracefully", opsCount);
                    break;
                }
                Throwable err = asyncError.get();
                if (err != null) {
                    LOG.warn(
                            "Lookup stopped after {} ops due to async error: {}",
                            opsCount,
                            err.getMessage());
                    break;
                }
                if (ctx.duration != null && ExecutorUtils.isExpired(startTime, ctx.duration)) {
                    break;
                }

                long keyIndex =
                        ctx.keyMin + (long) (ctx.random.nextDouble() * (ctx.keyMax - ctx.keyMin));
                GenericRow keyRow =
                        ExecutorUtils.buildKeyRow(
                                ctx.generators, ctx.columns, ctx.keyIndexes, keyIndex);

                opsCount++;
                final boolean shouldRecord = opsCount > ctx.warmupOps;
                long opStart = System.nanoTime();

                // Acquire permit — blocks if MAX_IN_FLIGHT lookups are pending
                inFlight.acquire();

                lookuper.lookup(keyRow)
                        .whenComplete(
                                (result, error) -> {
                                    inFlight.release();
                                    if (error != null) {
                                        asyncError.compareAndSet(null, error);
                                        return;
                                    }
                                    if (shouldRecord) {
                                        ctx.latencyRecorder.record(System.nanoTime() - opStart);
                                        ctx.throughputCounter.record(ctx.keyBytes);
                                    }
                                });

                ExecutorUtils.throttleIfNeeded(ctx.rateLimit, opsCount, startTime);
            }
        } catch (InterruptedException e) {
            LOG.info("Lookup interrupted after {} ops", opsCount);
            Thread.currentThread().interrupt();
        }

        // Wait for all in-flight lookups to complete
        try {
            inFlight.acquire(MAX_IN_FLIGHT);
            inFlight.release(MAX_IN_FLIGHT);
        } catch (InterruptedException e) {
            LOG.info(
                    "Lookup drain interrupted, {} lookups may be in-flight",
                    MAX_IN_FLIGHT - inFlight.availablePermits());
            Thread.currentThread().interrupt();
        }
    }
}
