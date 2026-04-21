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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executor for write workload phases. Handles both PK tables (UpsertWriter) and Log tables
 * (AppendWriter).
 *
 * <p>Uses async pipelined writes for throughput: records are fired without blocking, latency is
 * measured via completion callbacks, and flush() is called at the end to ensure all writes
 * complete.
 */
public class WriteExecutor implements PhaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(WriteExecutor.class);

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
        boolean hasPrimaryKey = tableConfig.hasPrimaryKey();

        FieldGenerator[] generators = ExecutorUtils.buildGenerators(columns, dataConfig);

        long warmupOps =
                ExecutorUtils.parseWarmupOps(
                        phaseConfig.warmup(),
                        endIndex - startIndex,
                        phaseConfig.threads() != null ? phaseConfig.threads() : 1);
        Duration warmupDuration = ExecutorUtils.parseWarmupDuration(phaseConfig.warmup());
        Long rateLimit = phaseConfig.rateLimit();
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());

        try (Table table = conn.getTable(tablePath)) {
            if (hasPrimaryKey) {
                UpsertWriter writer = table.newUpsert().createWriter();
                try {
                    writeLoop(
                            writer::upsert,
                            writer::flush,
                            generators,
                            columns,
                            latencyRecorder,
                            throughputCounter,
                            startIndex,
                            endIndex,
                            warmupOps,
                            warmupDuration,
                            rateLimit,
                            duration,
                            "Upsert");
                } finally {
                    writer.flush();
                }
            } else {
                AppendWriter writer = table.newAppend().createWriter();
                try {
                    writeLoop(
                            writer::append,
                            writer::flush,
                            generators,
                            columns,
                            latencyRecorder,
                            throughputCounter,
                            startIndex,
                            endIndex,
                            warmupOps,
                            warmupDuration,
                            rateLimit,
                            duration,
                            "Append");
                } finally {
                    writer.flush();
                }
            }
        }
    }

    @FunctionalInterface
    interface AsyncWriteOp {
        java.util.concurrent.CompletableFuture<?> write(GenericRow row);
    }

    private void writeLoop(
            AsyncWriteOp writeOp,
            Runnable flush,
            FieldGenerator[] generators,
            List<ColumnConfig> columns,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex,
            long warmupOps,
            Duration warmupDuration,
            Long rateLimit,
            Duration duration,
            String writerType)
            throws Exception {

        long startTime = System.nanoTime();
        long opsCount = 0;
        AtomicReference<Throwable> asyncError = new AtomicReference<>();

        // Compute absolute deadline: warmup + measurement duration from startTime
        // This avoids per-thread startTime reset issues in multi-threaded mode
        long warmupNanos = warmupDuration != null ? warmupDuration.toNanos() : 0;
        long warmupDeadlineNanos = warmupDuration != null ? startTime + warmupNanos : 0;
        long totalDeadlineNanos =
                duration != null ? startTime + warmupNanos + duration.toNanos() : Long.MAX_VALUE;
        boolean hasTimeWarmup = warmupDuration != null;

        try {
            for (long i = startIndex; i < endIndex; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    LOG.info(
                            "{} write interrupted after {} ops, exiting gracefully",
                            writerType,
                            opsCount);
                    break;
                }
                checkAsyncError(asyncError);
                long now = System.nanoTime();
                if (duration != null && now >= totalDeadlineNanos) {
                    break;
                }

                GenericRow row = ExecutorUtils.buildRow(generators, columns, i);
                final int rowBytes = ExecutorUtils.estimateRowBytes(row, columns);
                long opStart = System.nanoTime();
                opsCount++;

                // Past warmup? Record this op.
                final boolean record;
                if (hasTimeWarmup) {
                    record = opStart >= warmupDeadlineNanos;
                } else {
                    record = opsCount > warmupOps;
                }

                writeOp.write(row)
                        .whenComplete(
                                (result, error) -> {
                                    if (error != null) {
                                        asyncError.compareAndSet(null, error);
                                        return;
                                    }
                                    if (record) {
                                        latencyRecorder.record(System.nanoTime() - opStart);
                                        throughputCounter.record(rowBytes);
                                    }
                                });

                ExecutorUtils.throttleIfNeeded(rateLimit, opsCount, startTime);
            }
        } catch (Exception e) {
            LOG.warn(
                    "{} write loop stopped after {} ops: {}", writerType, opsCount, e.getMessage());
        }

        // Best-effort flush: wait for pending writes to complete.
        // If interrupted (phase ending), log and return — don't crash the test.
        try {
            flush.run();
            checkAsyncError(asyncError);
        } catch (Exception e) {
            LOG.info(
                    "{} flush completed with {} after {} ops (pending writes may be lost)",
                    writerType,
                    e.getClass().getSimpleName(),
                    opsCount);
        }
    }

    private static void checkAsyncError(AtomicReference<Throwable> asyncError) throws Exception {
        Throwable error = asyncError.get();
        if (error != null) {
            if (error instanceof Exception) {
                throw (Exception) error;
            }
            throw new RuntimeException(error);
        }
    }
}
