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
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataTypeParser;
import org.apache.fluss.microbench.config.PhaseType;
import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.report.EnvironmentSnapshot;
import org.apache.fluss.microbench.report.PerfReport;
import org.apache.fluss.microbench.report.PhaseResult;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs the configured phases against a Fluss cluster and collects their results. */
public class YamlDrivenEngine {

    private static final Logger LOG = LoggerFactory.getLogger(YamlDrivenEngine.class);
    /** Runs the benchmark against an independently started Fluss cluster. */
    public PerfReport run(ScenarioConfig config, String bootstrapServers) throws Exception {
        EnvironmentSnapshot environment = EnvironmentSnapshot.capture();
        List<PhaseResult> results = new ArrayList<>();
        String error = null;
        try {
            Configuration clientConfig = new Configuration();
            clientConfig.setString(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(), "512mb");
            if (config.client() != null && config.client().properties() != null) {
                config.client().properties().forEach(clientConfig::setString);
            }
            clientConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
            try (Connection connection = ConnectionFactory.createConnection(clientConfig)) {
                createTable(connection, config);
                waitForTableReady(connection, config.table().name(), 30_000);
                for (WorkloadPhaseConfig phase : config.workload()) {
                    results.add(executePhaseWithRetry(connection, config, phase));
                }
            }
        } catch (Exception e) {
            LOG.error("Benchmark failed", e);
            error = e.getMessage();
        }
        return PerfReport.build(
                config, results, environment, error == null ? "complete" : "partial", error);
    }

    private void createTable(Connection conn, ScenarioConfig config) throws Exception {
        TableConfig tableConfig = config.table();
        try (Admin admin = conn.getAdmin()) {
            admin.createDatabase(ExecutorUtils.DEFAULT_DATABASE, DatabaseDescriptor.EMPTY, true)
                    .get();

            Schema.Builder schemaBuilder = Schema.newBuilder();
            for (ColumnConfig col : tableConfig.columns()) {
                DataType dataType = DataTypeParser.parse(col.type());
                if (col.agg() != null && col.agg().function() != null) {
                    AggFunctionType aggType = AggFunctionType.fromString(col.agg().function());
                    if (aggType == null) {
                        throw new IllegalArgumentException(
                                "Unknown aggregation function: " + col.agg().function());
                    }
                    AggFunction aggFunction = AggFunctions.of(aggType, col.agg().args());
                    schemaBuilder.column(col.name(), dataType, aggFunction);
                } else {
                    schemaBuilder.column(col.name(), dataType);
                }
            }
            if (tableConfig.hasPrimaryKey()) {
                schemaBuilder.primaryKey(tableConfig.primaryKey());
            }
            Schema schema = schemaBuilder.build();

            TableDescriptor.Builder descriptorBuilder = TableDescriptor.builder().schema(schema);
            if (tableConfig.buckets() != null) {
                List<String> bucketKeys =
                        tableConfig.bucketKeys() != null
                                ? tableConfig.bucketKeys()
                                : Collections.emptyList();
                descriptorBuilder.distributedBy(tableConfig.buckets(), bucketKeys);
            }
            if (tableConfig.mergeEngine() != null && !tableConfig.mergeEngine().isEmpty()) {
                MergeEngineType mergeEngineType =
                        MergeEngineType.fromString(tableConfig.mergeEngine());
                descriptorBuilder.property(ConfigOptions.TABLE_MERGE_ENGINE, mergeEngineType);
            }
            if (tableConfig.properties() != null) {
                descriptorBuilder.properties(tableConfig.properties());
            }
            if (tableConfig.logFormat() != null && !tableConfig.logFormat().isEmpty()) {
                descriptorBuilder.property(
                        ConfigOptions.TABLE_LOG_FORMAT,
                        LogFormat.valueOf(tableConfig.logFormat().toUpperCase()));
            }
            if (tableConfig.kvFormat() != null && !tableConfig.kvFormat().isEmpty()) {
                descriptorBuilder.property(
                        ConfigOptions.TABLE_KV_FORMAT,
                        KvFormat.valueOf(tableConfig.kvFormat().toUpperCase()));
            }

            TableDescriptor descriptor = descriptorBuilder.build();
            TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());
            admin.createTable(tablePath, descriptor, false).get();
            LOG.info("Created table {}", tablePath);
        }
    }

    private void waitForTableReady(Connection conn, String tableName, long timeoutMs)
            throws Exception {
        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableName);
        long deadline = System.currentTimeMillis() + timeoutMs;

        try (Admin admin = conn.getAdmin()) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    TableInfo info = admin.getTableInfo(tablePath).get(1, TimeUnit.SECONDS);
                    if (info.getNumBuckets() > 0) {
                        LOG.info("Table {} ready with {} buckets", tablePath, info.getNumBuckets());
                        return;
                    }
                } catch (Exception e) {
                    LOG.debug("Waiting for table ready: {}", e.getMessage());
                }
                Thread.sleep(500);
            }
        }
        throw new TimeoutException("Table " + tablePath + " not ready after " + timeoutMs + "ms");
    }

    private PhaseResult executePhaseWithRetry(
            Connection conn, ScenarioConfig config, WorkloadPhaseConfig phase) throws Exception {
        int maxRetries = phase.maxRetries() != null ? phase.maxRetries() : 0;
        for (int attempt = 1; ; attempt++) {
            try {
                return executePhase(conn, config, phase);
            } catch (Exception e) {
                if (attempt > maxRetries) {
                    throw e;
                }
                LOG.warn(
                        "Phase '{}' failed on attempt {}/{}, retrying: {}",
                        phase.phase(),
                        attempt,
                        maxRetries,
                        e.getMessage());
                Thread.sleep(2000);
            }
        }
    }

    private PhaseResult executePhase(
            Connection conn, ScenarioConfig config, WorkloadPhaseConfig phase) throws Exception {

        int threads = phase.threads() != null ? phase.threads() : 1;
        long totalRecords = phase.records() != null ? phase.records() : Long.MAX_VALUE;

        LatencyRecorder latencyRecorder = new LatencyRecorder();
        ThroughputCounter throughputCounter = new ThroughputCounter();

        PhaseExecutor executor = createExecutor(phase.phase());

        long startNanos = System.nanoTime();

        if (threads == 1) {
            executor.execute(
                    conn,
                    config.table(),
                    config.data(),
                    phase,
                    latencyRecorder,
                    throughputCounter,
                    0,
                    totalRecords);
        } else {
            boolean isScan = PhaseType.fromString(phase.phase()) == PhaseType.SCAN;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                long perThread = totalRecords / threads;
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    long start = t * perThread;
                    long end = (t == threads - 1) ? totalRecords : start + perThread;
                    // For scan phases, each thread needs its own executor with the
                    // correct threadIndex for bucket partitioning.
                    PhaseExecutor threadExecutor = isScan ? new ScanExecutor(t) : executor;
                    futures.add(
                            pool.submit(
                                    () -> {
                                        try {
                                            threadExecutor.execute(
                                                    conn,
                                                    config.table(),
                                                    config.data(),
                                                    phase,
                                                    latencyRecorder,
                                                    throughputCounter,
                                                    start,
                                                    end);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        return null;
                                    }));
                }
                Duration phaseDuration = ExecutorUtils.parseDuration(phase.duration());
                Duration warmupDuration = ExecutorUtils.parseWarmupDuration(phase.warmup());
                long warmupMs = warmupDuration != null ? warmupDuration.toMillis() : 0;
                long futureTimeoutMs =
                        phaseDuration != null
                                ? phaseDuration.toMillis() + warmupMs + 60_000
                                : TimeUnit.HOURS.toMillis(1);
                for (Future<?> f : futures) {
                    try {
                        f.get(futureTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        LOG.warn(
                                "Phase '{}' thread timed out after {}ms, proceeding with partial results",
                                phase.phase(),
                                futureTimeoutMs);
                    } catch (ExecutionException e) {
                        LOG.warn(
                                "Phase '{}' thread failed: {}",
                                phase.phase(),
                                e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    }
                }
            } finally {
                pool.shutdown();
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.warn("Thread pool did not terminate within 60s, forcing shutdown");
                    pool.shutdownNow();
                    pool.awaitTermination(10, TimeUnit.SECONDS);
                }
            }
        }

        long elapsedNanos = System.nanoTime() - startNanos;

        // Subtract time-based warmup from elapsed time so that opsPerSecond
        // reflects only the measurement window, not warmup + measurement.
        // Only applies to write phases — other executors use count-based warmup
        // and don't support time-based warmup.
        Duration warmupDuration = ExecutorUtils.parseWarmupDuration(phase.warmup());
        PhaseType phaseType = PhaseType.fromString(phase.phase());
        if (warmupDuration != null && phaseType == PhaseType.WRITE) {
            long warmupNanos = warmupDuration.toNanos();
            elapsedNanos = Math.max(elapsedNanos - warmupNanos, 1);
        }

        return PhaseResult.from(phase.phase(), latencyRecorder, throughputCounter, elapsedNanos);
    }

    private static PhaseExecutor createExecutor(String phaseType) {
        PhaseType type = PhaseType.fromString(phaseType);
        switch (type) {
            case WRITE:
                return new WriteExecutor();
            case LOOKUP:
                return new LookupExecutor();
            case SCAN:
                return new ScanExecutor();
            case MIXED:
                return new MixedExecutor();
            default:
                throw new IllegalArgumentException("Unknown phase type: " + phaseType);
        }
    }
}
