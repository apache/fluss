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
import org.apache.fluss.metrics.registry.MetricRegistryImpl;
import org.apache.fluss.microbench.TeeOutputStream;
import org.apache.fluss.microbench.cluster.ForkedClusterManager;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataTypeParser;
import org.apache.fluss.microbench.config.PhaseType;
import org.apache.fluss.microbench.config.SamplingConfig;
import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.report.EnvironmentSnapshot;
import org.apache.fluss.microbench.report.PerfReport;
import org.apache.fluss.microbench.report.PhaseResult;
import org.apache.fluss.microbench.sampling.ClientMetricSampler;
import org.apache.fluss.microbench.sampling.ResourceSampler;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Core orchestrator that ties together cluster management, table creation, phase execution,
 * resource sampling, and report generation — all driven by a YAML {@link ScenarioConfig}.
 *
 * <p>Supports two modes:
 *
 * <ul>
 *   <li>{@link #run} — Forked mode: server runs in a child JVM process for process-level resource
 *       isolation
 *   <li>{@link #runEmbedded} — Embedded mode: server runs in the same JVM (used by integration
 *       tests)
 * </ul>
 */
public class YamlDrivenEngine {

    private static final Logger LOG = LoggerFactory.getLogger(YamlDrivenEngine.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // -------------------------------------------------------------------------
    //  Forked mode (CLI default) — server in child JVM process
    // -------------------------------------------------------------------------

    /**
     * Runs the benchmark with the server in a forked child JVM process. This provides process-level
     * resource isolation for accurate sampling.
     *
     * @param config the scenario configuration
     * @param jsonLinesOut optional stream for json-lines progress events
     * @param runOutputDir optional directory for run artifacts (run.log, server logs)
     */
    public PerfReport run(
            ScenarioConfig config, @Nullable PrintStream jsonLinesOut, @Nullable Path runOutputDir)
            throws Exception {
        EnvironmentSnapshot env = EnvironmentSnapshot.capture();
        List<PhaseResult> phaseResults = new ArrayList<>();
        String status = "complete";
        String error = null;

        // Set up TeeOutputStream for run.log if runOutputDir is provided
        PrintStream originalErr = System.err;
        OutputStream runLogStream = null;
        if (runOutputDir != null) {
            Files.createDirectories(runOutputDir);
            runLogStream = new FileOutputStream(runOutputDir.resolve("run.log").toFile());
            System.setErr(
                    new PrintStream(new TeeOutputStream(originalErr, runLogStream), true, "UTF-8"));
        }

        boolean nmtEnabled =
                config.sampling() != null
                        && config.sampling().isNmtEnabled() != null
                        && config.sampling().isNmtEnabled();
        ForkedClusterManager cluster = new ForkedClusterManager(config.cluster(), nmtEnabled);
        ResourceSampler resourceSampler = null;
        Connection conn = null;
        MetricRegistryImpl metricRegistry = null;

        try {
            // 1. Fork server as child JVM process
            Path logDir = runOutputDir != null ? runOutputDir.resolve("logs") : null;
            cluster.start(logDir);
            LOG.info("Forked cluster started (PID={})", cluster.getServerPid());

            // 2. Create client connection with metric sampling
            Configuration clientConf = cluster.getClientConfig();
            if (config.client() != null && config.client().properties() != null) {
                config.client().properties().forEach(clientConf::setString);
            }
            metricRegistry =
                    new MetricRegistryImpl(
                            Collections.singletonList(ClientMetricSampler.instance()));
            conn = ConnectionFactory.createConnection(clientConf, metricRegistry);

            // 3. Create table and wait for readiness
            long tableId = createTable(conn, config);
            waitForTableReady(conn, config.table().name(), 30_000);
            LOG.info("Table created and ready (id={})", tableId);

            // 4. Start dual-process resource sampling
            SamplingConfig samplingConfig =
                    config.sampling() != null ? config.sampling() : new SamplingConfig();
            resourceSampler = new ResourceSampler(samplingConfig);
            resourceSampler.start(cluster.getServerPid(), cluster.getMetricsFile().toPath());

            // 5. Execute phases sequentially
            executePhases(conn, config, phaseResults, jsonLinesOut);

        } catch (Throwable e) {
            LOG.error("Engine run failed", e);
            status = "partial";
            error = e.getMessage();
        } finally {
            if (resourceSampler != null) {
                resourceSampler.stop();
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close connection", e);
                }
            }
            cluster.stop();
            if (metricRegistry != null) {
                metricRegistry.closeAsync();
            }
            // Restore original stderr and close run.log
            if (runLogStream != null) {
                System.setErr(originalErr);
                runLogStream.close();
            }
        }

        // Build report after cleanup to avoid racing with final sample
        return buildReport(config, phaseResults, resourceSampler, env, status, error, jsonLinesOut);
    }

    // -------------------------------------------------------------------------
    //  Embedded mode — server in same JVM (for integration tests)
    // -------------------------------------------------------------------------

    /**
     * Runs the benchmark with an embedded (same-JVM) cluster. Used by integration tests and when
     * process isolation is not needed.
     */
    public PerfReport runEmbedded(ScenarioConfig config, @Nullable PrintStream jsonLinesOut)
            throws Exception {
        EnvironmentSnapshot env = EnvironmentSnapshot.capture();
        List<PhaseResult> phaseResults = new ArrayList<>();
        String status = "complete";
        String error = null;

        int numTabletServers =
                config.cluster() != null && config.cluster().tabletServers() != null
                        ? config.cluster().tabletServers()
                        : 1;
        Configuration clusterConf = new Configuration();
        clusterConf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, numTabletServers);
        if (config.cluster() != null && config.cluster().configOverrides() != null) {
            config.cluster().configOverrides().forEach(clusterConf::setString);
        }

        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(numTabletServers)
                        .setClusterConf(clusterConf)
                        .build();

        ResourceSampler resourceSampler = null;
        Connection conn = null;

        try {
            cluster.start();

            Configuration clientConf = cluster.getClientConfig();
            if (config.client() != null && config.client().properties() != null) {
                config.client().properties().forEach(clientConf::setString);
            }
            clientConf.set(ConfigOptions.CLIENT_WRITER_BUFFER_WAIT_TIMEOUT, Duration.ofSeconds(10));
            conn = ConnectionFactory.createConnection(clientConf);

            long tableId = createTable(conn, config);
            cluster.waitUntilTableReady(tableId);
            LOG.info("Table created and ready (id={})", tableId);

            SamplingConfig samplingConfig =
                    config.sampling() != null ? config.sampling() : new SamplingConfig();
            resourceSampler = new ResourceSampler(samplingConfig);
            resourceSampler.startSelfOnly();

            executePhases(conn, config, phaseResults, jsonLinesOut);

        } catch (Throwable e) {
            LOG.error("Engine run failed", e);
            status = "partial";
            error = e.getMessage();
        } finally {
            if (resourceSampler != null) {
                resourceSampler.stop();
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close connection", e);
                }
            }
            try {
                cluster.close();
            } catch (Exception e) {
                LOG.warn("Failed to close cluster", e);
            }
        }

        // Build report outside finally to avoid swallowing exceptions
        return buildReport(config, phaseResults, resourceSampler, env, status, error, jsonLinesOut);
    }

    // -------------------------------------------------------------------------
    //  External connection mode (for testing)
    // -------------------------------------------------------------------------

    /**
     * Runs the benchmark using an externally provided connection (skips cluster management and
     * table creation). Useful for testing where the cluster and tables are managed externally.
     */
    public PerfReport runWithConnection(
            Connection conn, ScenarioConfig config, @Nullable PrintStream jsonLinesOut) {
        EnvironmentSnapshot env = EnvironmentSnapshot.capture();
        List<PhaseResult> phaseResults = new ArrayList<>();
        String status = "complete";
        String error = null;

        try {
            executePhases(conn, config, phaseResults, jsonLinesOut);
        } catch (Throwable e) {
            LOG.error("Engine run failed", e);
            status = "partial";
            error = e.getMessage();
        }

        return buildReport(config, phaseResults, null, env, status, error, jsonLinesOut);
    }

    // -------------------------------------------------------------------------
    //  Shared internals
    // -------------------------------------------------------------------------

    /** Builds the final report, falling back to a partial report if building fails. */
    private PerfReport buildReport(
            ScenarioConfig config,
            List<PhaseResult> phaseResults,
            @Nullable ResourceSampler resourceSampler,
            EnvironmentSnapshot env,
            String status,
            @Nullable String error,
            @Nullable PrintStream jsonLinesOut) {
        String rawYaml = config.rawYaml() != null ? config.rawYaml() : "";
        try {
            PerfReport report =
                    PerfReport.build(
                            config,
                            rawYaml,
                            phaseResults,
                            resourceSampler != null
                                    ? resourceSampler.snapshots()
                                    : Collections.emptyList(),
                            env,
                            status,
                            error);
            emitEvent(jsonLinesOut, "summary", null, null);
            return report;
        } catch (Exception e) {
            LOG.warn("Failed to build report", e);
            return PerfReport.build(
                    config,
                    rawYaml,
                    phaseResults,
                    Collections.emptyList(),
                    env,
                    "partial",
                    "Report build failed: " + e.getMessage());
        }
    }

    private void executePhases(
            Connection conn,
            ScenarioConfig config,
            List<PhaseResult> phaseResults,
            @Nullable PrintStream jsonLinesOut)
            throws Exception {
        List<WorkloadPhaseConfig> phases = config.workload();
        if (phases == null) {
            return;
        }
        for (WorkloadPhaseConfig phase : phases) {
            String phaseName = phase.phase();
            emitEvent(jsonLinesOut, "phase_start", phaseName, null);
            try {
                PhaseResult result = executePhaseWithRetry(conn, config, phase, jsonLinesOut);
                phaseResults.add(result);
                emitEvent(jsonLinesOut, "phase_end", phaseName, null);
            } catch (Exception e) {
                LOG.error("Phase '{}' failed", phaseName, e);
                emitEvent(jsonLinesOut, "phase_error", phaseName, e.getMessage());
                throw e;
            }
        }
    }

    /**
     * Creates the table and returns its ID. Does NOT wait for tablet readiness — that is
     * mode-specific (embedded uses waitUntilTableReady, forked uses waitForTableReady).
     */
    private long createTable(Connection conn, ScenarioConfig config) throws Exception {
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

            return admin.getTableInfo(tablePath).get().getTableId();
        }
    }

    /**
     * Waits for table to be ready by polling Admin API. This replaces the fixed sleep in forked
     * mode with active verification.
     */
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
            Connection conn,
            ScenarioConfig config,
            WorkloadPhaseConfig phase,
            @Nullable PrintStream jsonLinesOut)
            throws Exception {
        int maxRetries = phase.maxRetries() != null ? phase.maxRetries() : 0;
        for (int attempt = 1; ; attempt++) {
            try {
                return executePhase(conn, config, phase, jsonLinesOut);
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
            Connection conn,
            ScenarioConfig config,
            WorkloadPhaseConfig phase,
            @Nullable PrintStream jsonLinesOut)
            throws Exception {

        int threads = phase.threads() != null ? phase.threads() : 1;
        long totalRecords = phase.records() != null ? phase.records() : Long.MAX_VALUE;

        LatencyRecorder latencyRecorder = new LatencyRecorder();
        ThroughputCounter throughputCounter = new ThroughputCounter();

        PhaseExecutor executor = createExecutor(phase.phase());

        // Start periodic progress reporter (1 event/sec via json-lines)
        ScheduledExecutorService progressReporter = null;
        if (jsonLinesOut != null) {
            progressReporter =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "progress-reporter");
                                t.setDaemon(true);
                                return t;
                            });
            final long phaseStartNanos = System.nanoTime();
            progressReporter.scheduleAtFixedRate(
                    () ->
                            emitProgress(
                                    jsonLinesOut,
                                    phase.phase(),
                                    latencyRecorder,
                                    throughputCounter,
                                    phaseStartNanos),
                    1,
                    1,
                    TimeUnit.SECONDS);
        }

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

        // Stop progress reporter and wait for it to finish
        if (progressReporter != null) {
            progressReporter.shutdownNow();
            try {
                if (!progressReporter.awaitTermination(2, TimeUnit.SECONDS)) {
                    LOG.warn("Progress reporter did not terminate within 2s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
            case PREFIX_LOOKUP:
                return new PrefixLookupExecutor();
            case SCAN:
                return new ScanExecutor();
            case MIXED:
                return new MixedExecutor();
            default:
                throw new IllegalArgumentException("Unknown phase type: " + phaseType);
        }
    }

    private void emitProgress(
            PrintStream out,
            String phase,
            LatencyRecorder latency,
            ThroughputCounter throughput,
            long phaseStartNanos) {
        try {
            long elapsedNanos = System.nanoTime() - phaseStartNanos;
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("event", "progress");
            map.put("timestamp", System.currentTimeMillis());
            map.put("phase", phase);
            map.put("elapsedSec", String.format("%.1f", elapsedNanos / 1e9));
            map.put("totalOps", throughput.totalOps());
            map.put("totalBytes", throughput.totalBytes());
            map.put("opsPerSec", String.format("%.1f", throughput.opsPerSecond(elapsedNanos)));
            map.put("bytesPerSec", String.format("%.1f", throughput.bytesPerSecond(elapsedNanos)));
            map.put("p50Ms", String.format("%.2f", latency.p50Nanos() / 1e6));
            map.put("p99Ms", String.format("%.2f", latency.p99Nanos() / 1e6));
            map.put("maxMs", String.format("%.2f", latency.maxNanos() / 1e6));
            out.println(MAPPER.writeValueAsString(map));
            out.flush();
        } catch (Exception e) {
            LOG.debug("Failed to emit progress event", e);
        }
    }

    private void emitEvent(
            @Nullable PrintStream out,
            String event,
            @Nullable String phase,
            @Nullable String message) {
        if (out == null) {
            return;
        }
        try {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("event", event);
            map.put("timestamp", System.currentTimeMillis());
            if (phase != null) {
                map.put("phase", phase);
            }
            if (message != null) {
                map.put("message", message);
            }
            out.println(MAPPER.writeValueAsString(map));
            out.flush();
        } catch (Exception e) {
            LOG.warn("Failed to emit json-lines event", e);
        }
    }
}
