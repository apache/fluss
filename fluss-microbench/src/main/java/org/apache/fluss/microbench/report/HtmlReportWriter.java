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

import org.apache.fluss.microbench.sampling.ResourceSnapshot;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.microbench.report.ReportFormatUtils.escapeHtml;
import static org.apache.fluss.microbench.report.ReportFormatUtils.escapeJavaScript;
import static org.apache.fluss.microbench.report.ReportFormatUtils.formatBytes;
import static org.apache.fluss.microbench.report.ReportFormatUtils.formatMicros;
import static org.apache.fluss.microbench.report.ReportFormatUtils.formatMs;
import static org.apache.fluss.microbench.report.ReportFormatUtils.formatNumber;
import static org.apache.fluss.microbench.report.ReportFormatUtils.formatOneDecimal;
import static org.apache.fluss.microbench.report.ReportFormatUtils.toMB;

/** Generates the HTML report from a {@link PerfReport}. */
class HtmlReportWriter {

    private static final String TEMPLATE_PATH = "report-template.html";

    private HtmlReportWriter() {}

    static void write(PerfReport report, Path outputDir) throws IOException {
        String template = loadTemplate();

        template = template.replace("{{SCENARIO_NAME}}", escapeHtml(report.scenarioName()));
        template = template.replace("{{STATUS}}", report.status().toUpperCase());
        template = template.replace("{{STATUS_CLASS}}", report.status());
        template = template.replace("{{META_LINE}}", buildMetaLine(report));
        template = template.replace("{{KPI_ITEMS}}", buildKpiItems(report));
        template = template.replace("{{CONFIG_ITEMS}}", buildConfigItems(report));
        template = template.replace("{{ENV_ITEMS}}", buildEnvItems(report.environment()));
        template = template.replace("{{WORKLOAD_ROWS}}", buildWorkloadRows(report));
        template = template.replace("{{PHASE_ROWS}}", buildPhaseRows(report.phaseResults()));
        template = template.replace("{{PHASE_CARDS}}", buildPhaseCards(report.phaseResults()));

        // Build chart containers (one chart-box per metric)
        template = template.replace("{{OVERVIEW_CHARTS}}", buildOverviewChartContainers(report));
        template = template.replace("{{CLIENT_CHARTS}}", buildClientChartContainers());
        template = template.replace("{{SERVER_CHARTS}}", buildServerChartContainers());

        // Compute server metric groups once for both HTML containers and chart scripts
        Map<String, String[]> serverMetricGroupMeta = new LinkedHashMap<>();
        Map<String, List<String>> serverMetricGroups =
                report.snapshots().isEmpty()
                        ? Collections.emptyMap()
                        : MetricClassifier.collectMetricGroups(
                                report.snapshots(), serverMetricGroupMeta);

        template =
                template.replace(
                        "{{CHART_SCRIPTS}}",
                        buildChartScripts(report, serverMetricGroups, serverMetricGroupMeta));
        template =
                template.replace(
                        "{{SERVER_METRIC_CHARTS}}",
                        buildServerMetricCharts(
                                report.snapshots(), serverMetricGroups, serverMetricGroupMeta));
        template =
                template.replace(
                        "{{ERROR_SECTION}}",
                        report.error() != null
                                ? "<div class=\"err\"><pre>"
                                        + escapeHtml(report.error())
                                        + "</pre></div>"
                                : "");
        template = template.replace("{{CAVEATS_SECTION}}", buildCaveatsSection(report));

        Files.writeString(outputDir.resolve("report.html"), template);
    }

    private static String loadTemplate() throws IOException {
        try (InputStream is =
                HtmlReportWriter.class.getClassLoader().getResourceAsStream(TEMPLATE_PATH)) {
            if (is == null) {
                throw new IOException("HTML template not found on classpath: " + TEMPLATE_PATH);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String buildMetaLine(PerfReport report) {
        StringBuilder sb = new StringBuilder();
        sb.append("Generated: ")
                .append(
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                                .withZone(ZoneId.systemDefault())
                                .format(Instant.now()));
        return sb.toString();
    }

    private static String buildKpiItems(PerfReport report) {
        ReportSummary s = report.summary();
        StringBuilder sb = new StringBuilder();
        if (s != null) {
            kpi(sb, "Peak Ops/s", formatNumber((long) s.bestOpsPerSecond()), true);
            kpi(sb, "Peak MB/s", formatOneDecimal(s.bestBytesPerSecond() / (1024 * 1024)), true);
            kpi(sb, "Total Ops", formatNumber(s.totalOps()), false);
            kpi(sb, "Total Bytes", formatBytes(s.totalBytes()), false);
            kpi(sb, "Client Peak RSS", toMB(s.peakClientRssBytes()) + " MB", false);
            kpi(sb, "Client Peak Heap", toMB(s.peakClientHeapBytes()) + " MB", false);
            kpi(sb, "Server Peak RSS", toMB(s.peakServerRssBytes()) + " MB", false);
        }
        return sb.toString();
    }

    private static void kpi(StringBuilder sb, String label, String value, boolean accent) {
        sb.append("<div class=\"kpi")
                .append(accent ? " kpi-hi" : "")
                .append("\"><div class=\"kpi-val\">")
                .append(escapeHtml(value))
                .append("</div><div class=\"kpi-lbl\">")
                .append(escapeHtml(label))
                .append("</div></div>\n");
    }

    private static String buildConfigItems(PerfReport report) {
        StringBuilder sb = new StringBuilder();
        envItem(sb, "Scenario", report.scenarioName());
        if (report.mergeEngine() != null) {
            envItem(sb, "Merge Engine", report.mergeEngine());
        }
        if (report.buckets() != null) {
            envItem(sb, "Buckets", String.valueOf(report.buckets()));
        }
        if (report.primaryKey() != null) {
            envItem(sb, "Primary Key", report.primaryKey());
        }
        if (report.logFormat() != null) {
            envItem(sb, "Log Format", report.logFormat());
        }
        if (report.kvFormat() != null) {
            envItem(sb, "KV Format", report.kvFormat());
        }
        if (report.tabletServers() != null) {
            envItem(sb, "Tablet Servers", String.valueOf(report.tabletServers()));
        }
        if (report.dataSeed() != null) {
            envItem(sb, "Data Seed", String.valueOf(report.dataSeed()));
        }
        if (report.description() != null) {
            envItem(sb, "Description", report.description());
        }
        return sb.toString();
    }

    private static String buildWorkloadRows(PerfReport report) {
        List<PerfReport.WorkloadPhaseSummary> phases = report.workloadPhases();
        if (phases == null || phases.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (PerfReport.WorkloadPhaseSummary w : phases) {
            sb.append("<tr>");
            sb.append("<td>").append(escapeHtml(w.phase)).append("</td>");
            sb.append("<td>").append(w.threads != null ? w.threads : "-").append("</td>");
            sb.append("<td>").append(w.records != null ? w.records : "-").append("</td>");
            sb.append("<td>")
                    .append(escapeHtml(w.duration != null ? w.duration : "-"))
                    .append("</td>");
            sb.append("<td>").append(escapeHtml(w.warmup != null ? w.warmup : "-")).append("</td>");
            sb.append("<td>").append(w.rateLimit != null ? w.rateLimit : "-").append("</td>");
            sb.append("<td>")
                    .append(escapeHtml(w.keyRange != null ? w.keyRange : "-"))
                    .append("</td>");
            sb.append("</tr>\n");
        }
        return sb.toString();
    }

    private static String buildEnvItems(EnvironmentSnapshot env) {
        if (env == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        envItem(sb, "Fluss Version", env.flussVersion());
        envItem(sb, "JVM", env.jvmVersion());
        envItem(sb, "OS", env.osName() + " " + env.osVersion());
        envItem(sb, "CPU Cores", String.valueOf(env.cpuCores()));
        envItem(sb, "Total Memory", toMB(env.totalMemoryBytes()) + " MB");
        return sb.toString();
    }

    private static void envItem(StringBuilder sb, String key, String value) {
        sb.append("<div class=\"env-r\"><span class=\"env-k\">")
                .append(escapeHtml(key))
                .append("</span><span class=\"env-v\">")
                .append(escapeHtml(value != null ? value : "unknown"))
                .append("</span></div>\n");
    }

    private static String buildPhaseRows(List<PhaseResult> results) {
        StringBuilder sb = new StringBuilder();
        for (PhaseResult p : results) {
            sb.append("<tr>");
            sb.append("<td>").append(escapeHtml(p.phaseName())).append("</td>");
            sb.append("<td>").append(formatNumber(p.totalOps())).append("</td>");
            sb.append("<td>").append(formatBytes(p.totalBytes())).append("</td>");
            sb.append("<td>").append(formatNumber((long) p.opsPerSecond())).append("</td>");
            sb.append("<td>")
                    .append(formatOneDecimal(p.bytesPerSecond() / (1024 * 1024)))
                    .append("</td>");
            sb.append("<td>").append(formatMs(p.p50Nanos())).append("</td>");
            sb.append("<td>").append(formatMs(p.p95Nanos())).append("</td>");
            sb.append("<td>").append(formatMs(p.p99Nanos())).append("</td>");
            sb.append("<td>").append(formatMs(p.p999Nanos())).append("</td>");
            sb.append("<td>").append(formatMs(p.p9999Nanos())).append("</td>");
            sb.append("<td>").append(formatMs(p.maxNanos())).append("</td>");
            sb.append("<td>")
                    .append(String.format("%.1f s", p.elapsedNanos() / 1e9))
                    .append("</td>");
            sb.append("</tr>\n");
        }
        return sb.toString();
    }

    private static String buildPhaseCards(List<PhaseResult> results) {
        if (results.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"phase-cards\">\n");
        for (PhaseResult p : results) {
            sb.append("<div class=\"phase-card\">\n");
            sb.append("<div class=\"phase-card-name\">")
                    .append(escapeHtml(p.phaseName()))
                    .append("</div>\n");
            sb.append("<div class=\"phase-card-stats\">\n");
            phaseCardStat(sb, "Ops/s", formatNumber((long) p.opsPerSecond()), true);
            phaseCardStat(sb, "MB/s", formatOneDecimal(p.bytesPerSecond() / (1024 * 1024)), true);
            phaseCardStat(sb, "Total Ops", formatNumber(p.totalOps()), false);
            phaseCardStat(sb, "p50", formatMs(p.p50Nanos()), false);
            phaseCardStat(sb, "p99", formatMs(p.p99Nanos()), false);
            phaseCardStat(sb, "p99.9", formatMs(p.p999Nanos()), false);
            sb.append("</div>\n</div>\n");
        }
        sb.append("</div>\n");
        return sb.toString();
    }

    private static void phaseCardStat(
            StringBuilder sb, String label, String value, boolean accent) {
        sb.append("<div class=\"phase-card-stat")
                .append(accent ? " accent" : "")
                .append("\"><div class=\"stat-val\">")
                .append(escapeHtml(value))
                .append("</div><div class=\"stat-lbl\">")
                .append(escapeHtml(label))
                .append("</div></div>\n");
    }

    // ==================== Chart Container Generation ====================

    private static String chartBox(String id, String title) {
        return "<div class=\"chart-box\"><div class=\"chart-head\">"
                + escapeHtml(title)
                + "</div><div class=\"chart-body\"><canvas id=\""
                + id
                + "\"></canvas></div></div>\n";
    }

    private static String buildOverviewChartContainers(PerfReport report) {
        StringBuilder sb = new StringBuilder();
        sb.append(chartBox("opsPerSecChart", "Ops/s"));
        sb.append(chartBox("mbPerSecChart", "MB/s"));
        for (int i = 0; i < report.phaseResults().size(); i++) {
            PhaseResult p = report.phaseResults().get(i);
            if (p.latencyHistogram() != null && p.latencyHistogram().length > 0) {
                sb.append(chartBox("latHist" + i, p.phaseName() + " latency distribution"));
            }
        }
        sb.append(chartBox("cumBytesChart", "Cumulative bytes sent"));
        sb.append(chartBox("cumRecordsChart", "Cumulative records sent"));
        return sb.toString();
    }

    private static final String[][] CLIENT_CHARTS = {
        {"clientRssChart", "Client RSS"},
        {"clientHeapChart", "Client heap"},
        {"clientNonHeapChart", "Client non-heap"},
        {"clientDirectChart", "Client direct memory"},
        {"clientCpuChart", "Client CPU"},
        {"serverCpuChart", "Server CPU"},
        {"clientGcCountChart", "Client GC count"},
        {"clientGcTimeChart", "Client GC time"},
        {"clientDiskReadChart", "Client disk read"},
        {"clientDiskWriteChart", "Client disk write"},
        {"serverDiskReadChart", "Server disk read"},
        {"serverDiskWriteChart", "Server disk write"},
        {"rpcAvgLatencyChart", "RPC avg latency"},
        {"rpcMaxLatencyChart", "RPC max latency"},
        {"rpcSendLatencyChart", "RPC send latency"},
        {"rpcQueueTimeChart", "Batch queue time"},
        {"rpcRequestRateChart", "RPC requests/s"},
        {"rpcResponseRateChart", "RPC responses/s"},
        {"rpcInflightChart", "RPC inflight"},
    };

    private static String buildClientChartContainers() {
        StringBuilder sb = new StringBuilder();
        for (String[] c : CLIENT_CHARTS) {
            sb.append(chartBox(c[0], c[1]));
        }
        return sb.toString();
    }

    private static String buildServerChartContainers() {
        StringBuilder sb = new StringBuilder();
        sb.append(chartBox("serverRssChart", "Server RSS"));
        sb.append(chartBox("serverNettyDirectChart", "Server Netty direct"));
        sb.append(chartBox("serverRocksMemtableChart", "RocksDB memtable"));
        sb.append(chartBox("serverRocksBlockCacheChart", "RocksDB block cache"));
        sb.append(chartBox("serverRocksTableReadersChart", "RocksDB table readers"));
        sb.append(chartBox("serverHeapUsedChart", "Server heap used"));
        sb.append(chartBox("serverHeapCommittedChart", "Server heap committed"));
        sb.append(chartBox("serverNonHeapChart", "Server non-heap used"));
        sb.append(chartBox("serverMetaspaceChart", "Server metaspace"));
        sb.append(chartBox("serverDirectUsedChart", "Server direct memory"));
        sb.append(chartBox("serverGcCountChart", "Server GC count"));
        sb.append(chartBox("serverGcTimeChart", "Server GC time"));
        sb.append(chartBox("serverThreadCountChart", "Server thread count"));
        sb.append(chartBox("serverClassesLoadedChart", "Server classes loaded"));
        return sb.toString();
    }

    // ==================== Chart Definition (data-driven) ====================

    private static final ChartDef[] CLIENT_MEM_CHARTS = {
        ChartDef.ofLong(
                "clientRssChart", "RSS", "MB", s -> toMB(s.client != null ? s.client.rssBytes : 0)),
        ChartDef.ofLong("clientHeapChart", "Heap", "MB", s -> toMB(s.clientHeapUsed)),
        ChartDef.ofLong("clientNonHeapChart", "Non-heap", "MB", s -> toMB(s.clientNonHeapUsed)),
        ChartDef.ofLong("clientDirectChart", "Direct", "MB", s -> toMB(s.clientDirectMemoryUsed)),
    };

    private static final ChartDef[] SERVER_MEM_CHARTS = {
        ChartDef.ofLong(
                "serverRssChart", "RSS", "MB", s -> toMB(s.server != null ? s.server.rssBytes : 0)),
        ChartDef.ofLong(
                "serverNettyDirectChart",
                "Netty direct",
                "MB",
                s -> toMB(s.serverNettyDirectMemory)),
        ChartDef.ofLong(
                "serverRocksMemtableChart", "Memtable", "MB", s -> toMB(s.serverRocksdbMemtable)),
        ChartDef.ofLong(
                "serverRocksBlockCacheChart",
                "Block cache",
                "MB",
                s -> toMB(s.serverRocksdbBlockCache)),
        ChartDef.ofLong(
                "serverRocksTableReadersChart",
                "Table readers",
                "MB",
                s -> toMB(s.serverRocksdbTableReaders)),
    };

    private static final ChartDef[] CPU_CHARTS = {
        ChartDef.ofDouble("clientCpuChart", "Client CPU", "%", s -> s.clientJvmCpuLoad),
        ChartDef.ofDouble(
                "serverCpuChart",
                "Server CPU",
                "%",
                s -> s.server != null ? s.server.cpuPercent : 0),
    };

    private static final ChartDef[] GC_CHARTS = {
        ChartDef.ofDelta("clientGcCountChart", "GC count", "count/interval", s -> s.clientGcCount),
        ChartDef.ofDelta("clientGcTimeChart", "GC time", "ms/interval", s -> s.clientGcTimeMs),
    };

    private static final ChartDef[] DISK_CHARTS = {
        ChartDef.ofDelta(
                "clientDiskReadChart",
                "Read",
                "MB/interval",
                s -> toMB(s.client != null ? s.client.diskReadBytes : 0)),
        ChartDef.ofDelta(
                "clientDiskWriteChart",
                "Write",
                "MB/interval",
                s -> toMB(s.client != null ? s.client.diskWriteBytes : 0)),
        ChartDef.ofDelta(
                "serverDiskReadChart",
                "Read",
                "MB/interval",
                s -> toMB(s.server != null ? s.server.diskReadBytes : 0)),
        ChartDef.ofDelta(
                "serverDiskWriteChart",
                "Write",
                "MB/interval",
                s -> toMB(s.server != null ? s.server.diskWriteBytes : 0)),
    };

    private static final ChartDef[] RPC_LATENCY_CHARTS = {
        ChartDef.ofDouble(
                "rpcAvgLatencyChart", "Avg latency", "ms", s -> s.clientRpcRequestLatencyAvg),
        ChartDef.ofDouble(
                "rpcMaxLatencyChart", "Max latency", "ms", s -> s.clientRpcRequestLatencyMax),
        ChartDef.ofDouble("rpcSendLatencyChart", "Send latency", "ms", s -> s.clientSendLatencyMs),
        ChartDef.ofDouble("rpcQueueTimeChart", "Queue time", "ms", s -> s.clientBatchQueueTimeMs),
    };

    private static final ChartDef[] RPC_THROUGHPUT_CHARTS = {
        ChartDef.ofDouble(
                "rpcRequestRateChart", "Requests/s", "count", s -> s.clientRpcRequestRate),
        ChartDef.ofDouble(
                "rpcResponseRateChart", "Responses/s", "count", s -> s.clientRpcResponseRate),
        ChartDef.ofDouble("rpcInflightChart", "Inflight", "count", s -> s.clientRpcInflight),
    };

    private static final ChartDef[] CUMULATIVE_CHARTS = {
        ChartDef.ofLong("cumBytesChart", "Bytes sent", "MB", s -> toMB(s.clientBytesSendTotal)),
        ChartDef.ofLong(
                "cumRecordsChart", "Records sent", "records", s -> s.clientRecordsSendTotal),
    };

    private static final ChartDef[] SERVER_JVM_MEM_CHARTS = {
        ChartDef.ofServerMetric(
                "serverHeapUsedChart", "Heap used", "MB", "JVM.memory.heap.used", 1024 * 1024),
        ChartDef.ofServerMetric(
                "serverHeapCommittedChart",
                "Heap committed",
                "MB",
                "JVM.memory.heap.committed",
                1024 * 1024),
        ChartDef.ofServerMetric(
                "serverNonHeapChart",
                "Non-heap used",
                "MB",
                "JVM.memory.nonHeap.used",
                1024 * 1024),
        ChartDef.ofServerMetric(
                "serverMetaspaceChart",
                "Metaspace",
                "MB",
                "JVM.memory.metaspace.used",
                1024 * 1024),
        ChartDef.ofServerMetric(
                "serverDirectUsedChart",
                "Direct memory",
                "MB",
                "JVM.memory.direct.memoryUsed",
                1024 * 1024),
    };

    private static final ChartDef[] SERVER_GC_CHARTS = {
        ChartDef.ofServerDelta(
                "serverGcCountChart", "GC count", "count/interval", "JVM.GC.all.count", 1),
        ChartDef.ofServerDelta("serverGcTimeChart", "GC time", "ms/interval", "JVM.GC.all.time", 1),
    };

    private static final ChartDef[] SERVER_THREADS_CHARTS = {
        ChartDef.ofServerMetric(
                "serverThreadCountChart", "Thread count", "count", "JVM.threads.count", 1),
        ChartDef.ofServerMetric(
                "serverClassesLoadedChart",
                "Classes loaded",
                "count",
                "classLoader.classesLoaded",
                1),
    };

    // ==================== Chart.js Script Generation ====================

    private static String buildChartScripts(
            PerfReport report,
            Map<String, List<String>> serverMetricGroups,
            Map<String, String[]> serverMetricGroupMeta) {
        StringBuilder js = new StringBuilder();
        List<ResourceSnapshot> snapshots = report.snapshots();

        if (!snapshots.isEmpty()) {
            long t0 = snapshots.get(0).timestamp;
            // Build time labels
            js.append("var labels = [");
            for (int i = 0; i < snapshots.size(); i++) {
                if (i > 0) {
                    js.append(',');
                }
                js.append("'").append((snapshots.get(i).timestamp - t0) / 1000).append("s'");
            }
            js.append("];\n\n");

            buildThroughputCharts(js, snapshots);
            renderCharts(js, snapshots, CLIENT_MEM_CHARTS);
            renderCharts(js, snapshots, SERVER_MEM_CHARTS);
            renderCharts(js, snapshots, CPU_CHARTS);
            renderCharts(js, snapshots, GC_CHARTS);
            renderCharts(js, snapshots, DISK_CHARTS);
            renderCharts(js, snapshots, RPC_LATENCY_CHARTS);
            renderCharts(js, snapshots, RPC_THROUGHPUT_CHARTS);
            renderCharts(js, snapshots, CUMULATIVE_CHARTS);
            renderCharts(js, snapshots, SERVER_JVM_MEM_CHARTS);
            renderCharts(js, snapshots, SERVER_GC_CHARTS);
            renderCharts(js, snapshots, SERVER_THREADS_CHARTS);
            buildDynamicServerMetricCharts(
                    js, snapshots, serverMetricGroups, serverMetricGroupMeta);
        }

        // Histograms
        buildHistogramScripts(js, report.phaseResults());

        return js.toString();
    }

    private static void buildThroughputCharts(StringBuilder js, List<ResourceSnapshot> snaps) {
        // Ops/s
        js.append("lineChart('opsPerSecChart', labels, [\n");
        js.append("  {label:'Ops/s', data:[");
        for (int i = 0; i < snaps.size(); i++) {
            if (i > 0) {
                js.append(',');
            }
            if (i == 0) {
                js.append(0);
            } else {
                long dt = snaps.get(i).timestamp - snaps.get(i - 1).timestamp;
                long dr =
                        snaps.get(i).clientRecordsSendTotal
                                - snaps.get(i - 1).clientRecordsSendTotal;
                js.append(dt > 0 ? (long) (dr * 1000.0 / dt) : 0);
            }
        }
        js.append("]}\n], 'ops/s');\n\n");

        // MB/s
        js.append("lineChart('mbPerSecChart', labels, [\n");
        js.append("  {label:'MB/s', data:[");
        for (int i = 0; i < snaps.size(); i++) {
            if (i > 0) {
                js.append(',');
            }
            if (i == 0) {
                js.append(0);
            } else {
                long dt = snaps.get(i).timestamp - snaps.get(i - 1).timestamp;
                long db = snaps.get(i).clientBytesSendTotal - snaps.get(i - 1).clientBytesSendTotal;
                js.append(dt > 0 ? String.format("%.1f", db * 1000.0 / dt / (1024 * 1024)) : "0");
            }
        }
        js.append("]}\n], 'MB/s');\n\n");
    }

    /** Renders all charts defined in the given array using a single generic loop. */
    private static void renderCharts(
            StringBuilder js, List<ResourceSnapshot> snaps, ChartDef[] charts) {
        for (ChartDef c : charts) {
            js.append("lineChart('").append(c.id).append("', labels, [\n");
            js.append("  {label:'").append(c.label).append("', data:[");
            for (int i = 0; i < snaps.size(); i++) {
                if (i > 0) {
                    js.append(',');
                }
                if (c.delta && i == 0) {
                    js.append(0);
                } else if (c.serverMetricKey != null) {
                    long val = MetricClassifier.findServerMetric(snaps.get(i), c.serverMetricKey);
                    if (c.delta) {
                        val -=
                                MetricClassifier.findServerMetric(
                                        snaps.get(i - 1), c.serverMetricKey);
                    }
                    js.append(c.divisor > 1 ? val / c.divisor : val);
                } else if (c.longExtractor != null) {
                    long val = c.longExtractor.extract(snaps.get(i));
                    if (c.delta) {
                        val -= c.longExtractor.extract(snaps.get(i - 1));
                    }
                    js.append(val);
                } else {
                    double val = c.doubleExtractor.extract(snaps.get(i));
                    js.append(formatOneDecimal(val));
                }
            }
            js.append("]}");
            js.append("\n], '").append(c.unit).append("');\n\n");
        }
    }

    /**
     * Groups all server metrics by logical category and generates one chart-box per metric key,
     * wrapped in collapsible grafana-row sections.
     */
    private static String buildServerMetricCharts(
            List<ResourceSnapshot> snapshots,
            Map<String, List<String>> groups,
            Map<String, String[]> groupMeta) {
        if (snapshots.isEmpty()) {
            return "";
        }

        StringBuilder html = new StringBuilder();
        String lastSection = "";
        int chartIdx = 0;

        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            String groupId = entry.getKey();
            List<String> keys = entry.getValue();
            if (MetricClassifier.isAllZero(keys, snapshots)) {
                continue;
            }

            String title = groupMeta.get(groupId)[1];
            String section =
                    title.contains("/") ? title.substring(0, title.indexOf("/")).trim() : title;

            // Section header when category changes — emit grafana-row wrapper
            if (!section.equals(lastSection)) {
                if (!lastSection.isEmpty()) {
                    html.append("</div>\n</div>\n</div>\n</div>\n");
                }
                String sectionId = "srv-" + section.toLowerCase().replaceAll("[^a-z0-9]+", "-");
                html.append("<div class=\"grafana-row\" data-section=\"")
                        .append(escapeHtml(sectionId))
                        .append("\">\n");
                html.append("  <div class=\"grafana-row-header\" onclick=\"toggleRow(this)\">\n")
                        .append(
                                "    <span class=\"chevron\"><svg viewBox=\"0 0 16 16\"><path d=\"M5.7 13.7L4.3 12.3 8.6 8 4.3 3.7 5.7 2.3 11.4 8z\"/></svg></span>\n")
                        .append("    <span class=\"grafana-row-title\">")
                        .append(escapeHtml(section))
                        .append("</span>\n")
                        .append("  </div>\n");
                html.append("  <div class=\"grafana-row-body\">\n");
                html.append("    <div class=\"grafana-row-body-inner\">\n");
                html.append("<div class=\"chart-grid\">\n");
                lastSection = section;
            }

            // One chart-box per metric key (skip redundant cumul counters)
            for (String key : keys) {
                if (MetricClassifier.isRedundantCumul(key)) {
                    chartIdx++;
                    continue;
                }
                String chartId = "srvMetric" + chartIdx++;
                String shortLabel = MetricClassifier.shortLabel(key);
                html.append(chartBox(chartId, shortLabel));
            }
        }
        if (!lastSection.isEmpty()) {
            html.append("</div>\n</div>\n</div>\n</div>\n");
        }
        return html.toString();
    }

    private static void buildDynamicServerMetricCharts(
            StringBuilder js,
            List<ResourceSnapshot> snapshots,
            Map<String, List<String>> groups,
            Map<String, String[]> groupMeta) {

        int chartIdx = 0;
        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            List<String> keys = entry.getValue();
            if (MetricClassifier.isAllZero(keys, snapshots)) {
                continue;
            }

            // One chart per metric key (skip redundant cumul counters)
            for (String key : keys) {
                if (MetricClassifier.isRedundantCumul(key)) {
                    chartIdx++;
                    continue;
                }
                String chartId = "srvMetric" + chartIdx++;
                String unit = MetricClassifier.inferUnit(List.of(key));
                String shortLabel = MetricClassifier.shortLabel(key);
                long divisor = MetricClassifier.unitDivisor(key);
                js.append("lineChart('").append(chartId).append("', labels, [\n");
                js.append("  {label:'").append(escapeJavaScript(shortLabel)).append("', data:[");
                for (int i = 0; i < snapshots.size(); i++) {
                    if (i > 0) {
                        js.append(',');
                    }
                    Object val =
                            snapshots.get(i).serverMetrics != null
                                    ? snapshots.get(i).serverMetrics.get(key)
                                    : null;
                    if (val instanceof Number) {
                        double d = ((Number) val).doubleValue();
                        if (divisor > 1) {
                            d = d / divisor;
                            js.append(String.format("%.2f", d));
                        } else if (d == (long) d) {
                            js.append((long) d);
                        } else {
                            js.append(String.format("%.2f", d));
                        }
                    } else {
                        js.append(0);
                    }
                }
                js.append("]}");
                js.append("\n], '").append(unit).append("');\n\n");
            }
        }
    }

    private static void buildHistogramScripts(StringBuilder js, List<PhaseResult> results) {
        int idx = 0;
        for (PhaseResult p : results) {
            long[][] hist = p.latencyHistogram();
            if (hist == null || hist.length == 0) {
                continue;
            }
            js.append("barChart('latHist").append(idx++).append("', [");
            for (int b = 0; b < hist.length; b++) {
                if (b > 0) {
                    js.append(',');
                }
                js.append("'").append(formatMicros(hist[b][0])).append("'");
            }
            js.append("], [");
            for (int b = 0; b < hist.length; b++) {
                if (b > 0) {
                    js.append(',');
                }
                js.append(hist[b][1]);
            }
            js.append("]);\n");
        }
    }

    @FunctionalInterface
    private interface LongExtractor {
        long extract(ResourceSnapshot s);
    }

    @FunctionalInterface
    private interface DoubleExtractor {
        double extract(ResourceSnapshot s);
    }

    /** Declarative definition of a single line chart. */
    private static class ChartDef {
        final String id;
        final String label;
        final String unit;
        final boolean delta;
        final LongExtractor longExtractor;
        final DoubleExtractor doubleExtractor;
        final String serverMetricKey;
        final long divisor;

        private ChartDef(
                String id,
                String label,
                String unit,
                boolean delta,
                LongExtractor longExtractor,
                DoubleExtractor doubleExtractor,
                String serverMetricKey,
                long divisor) {
            this.id = id;
            this.label = label;
            this.unit = unit;
            this.delta = delta;
            this.longExtractor = longExtractor;
            this.doubleExtractor = doubleExtractor;
            this.serverMetricKey = serverMetricKey;
            this.divisor = divisor;
        }

        static ChartDef ofLong(String id, String label, String unit, LongExtractor ex) {
            return new ChartDef(id, label, unit, false, ex, null, null, 1);
        }

        static ChartDef ofDouble(String id, String label, String unit, DoubleExtractor ex) {
            return new ChartDef(id, label, unit, false, null, ex, null, 1);
        }

        static ChartDef ofDelta(String id, String label, String unit, LongExtractor ex) {
            return new ChartDef(id, label, unit, true, ex, null, null, 1);
        }

        static ChartDef ofServerMetric(
                String id, String label, String unit, String key, long divisor) {
            return new ChartDef(id, label, unit, false, null, null, key, divisor);
        }

        static ChartDef ofServerDelta(
                String id, String label, String unit, String key, long divisor) {
            return new ChartDef(id, label, unit, true, null, null, key, divisor);
        }
    }

    private static String buildCaveatsSection(PerfReport report) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "<div class=\"panel\" style=\"margin-bottom:16px\">"
                        + "<div class=\"panel-head\">Measurement Caveats</div>"
                        + "<div class=\"panel-body\"><ul style=\"font-size:12px;color:var(--text2);"
                        + "margin:0;padding-left:18px;line-height:1.8\">\n");
        sb.append(
                "<li><b>Bytes/s</b>: estimated from schema column types, "
                        + "not actual wire bytes</li>\n");
        sb.append(
                "<li><b>Scan latency</b>: measures poll-to-delivery latency per batch, "
                        + "not per-record latency</li>\n");
        sb.append(
                "<li><b>Server native untracked</b>: estimated as RSS minus NMT tracked; "
                        + "async sampling may introduce drift</li>\n");
        sb.append(
                "<li><b>Lookup latency</b>: synchronous wall-clock time including "
                        + "client queueing + RPC + server processing</li>\n");
        sb.append("</ul></div></div>\n");
        return sb.toString();
    }
}
