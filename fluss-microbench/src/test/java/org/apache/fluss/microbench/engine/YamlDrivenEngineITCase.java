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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.microbench.config.ScenarioValidator;
import org.apache.fluss.microbench.report.PerfReport;
import org.apache.fluss.microbench.report.PhaseResult;
import org.apache.fluss.microbench.report.ReportWriter;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link YamlDrivenEngine} phase execution and report building. */
class YamlDrivenEngineITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConf())
                    .build();

    private static final String DATABASE = "fluss";

    @Test
    void runLogTableWriteAndMultiPhase() throws Exception {
        // Setup: create database and table
        try (Connection setupConn =
                        ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
                Admin admin = setupConn.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();

            Schema logSchema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .column("value", DataTypes.BIGINT())
                            .build();
            TableDescriptor logDesc =
                    TableDescriptor.builder().schema(logSchema).distributedBy(1).build();
            TablePath logPath = TablePath.of(DATABASE, "perf_log_table");
            admin.createTable(logPath, logDesc, true).get();
            long logTableId = admin.getTableInfo(logPath).get().getTableId();
            FLUSS_CLUSTER.waitUntilTableReady(logTableId);
        }

        // Use a single connection for all engine runs
        try (Connection conn =
                ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig())) {

            // --- Scenario 1: single write phase with json-lines ---
            String yaml1 =
                    "meta:\n"
                            + "  name: log-write-test\n"
                            + "cluster:\n"
                            + "  tablet-servers: 1\n"
                            + "table:\n"
                            + "  name: perf_log_table\n"
                            + "  columns:\n"
                            + "    - name: id\n"
                            + "      type: INT\n"
                            + "    - name: name\n"
                            + "      type: STRING\n"
                            + "    - name: value\n"
                            + "      type: BIGINT\n"
                            + "  buckets: 1\n"
                            + "data:\n"
                            + "  seed: 42\n"
                            + "workload:\n"
                            + "  - phase: write\n"
                            + "    records: 500\n";

            ScenarioConfig config1 = ScenarioConfig.parse(yaml1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream jsonOut = new PrintStream(baos, true, StandardCharsets.UTF_8.name());

            YamlDrivenEngine engine = new YamlDrivenEngine();
            PerfReport report1 = engine.runWithConnection(conn, config1, jsonOut);

            assertThat(report1.status())
                    .describedAs("Report error: %s", report1.error())
                    .isEqualTo("complete");
            assertThat(report1.error()).isNull();
            assertThat(report1.scenarioName()).isEqualTo("log-write-test");
            assertThat(report1.phaseResults()).hasSize(1);

            PhaseResult writeResult = report1.phaseResults().get(0);
            assertThat(writeResult.phaseName()).isEqualTo("write");
            assertThat(writeResult.totalOps()).isEqualTo(500);
            assertThat(writeResult.elapsedNanos()).isGreaterThan(0);
            assertThat(writeResult.opsPerSecond()).isGreaterThan(0);

            String jsonLines = baos.toString(StandardCharsets.UTF_8.name());
            assertThat(jsonLines).contains("\"event\":\"phase_start\"");
            assertThat(jsonLines).contains("\"event\":\"phase_end\"");
            assertThat(jsonLines).contains("\"event\":\"summary\"");
            assertThat(jsonLines).contains("\"phase\":\"write\"");

            // --- Scenario 2: multi-phase write ---
            String yaml2 =
                    "meta:\n"
                            + "  name: multi-phase-test\n"
                            + "cluster:\n"
                            + "  tablet-servers: 1\n"
                            + "table:\n"
                            + "  name: perf_log_table\n"
                            + "  columns:\n"
                            + "    - name: id\n"
                            + "      type: INT\n"
                            + "    - name: name\n"
                            + "      type: STRING\n"
                            + "    - name: value\n"
                            + "      type: BIGINT\n"
                            + "  buckets: 1\n"
                            + "data:\n"
                            + "  seed: 99\n"
                            + "workload:\n"
                            + "  - phase: write\n"
                            + "    records: 200\n"
                            + "  - phase: write\n"
                            + "    records: 300\n";

            ScenarioConfig config2 = ScenarioConfig.parse(yaml2);
            PerfReport report2 = engine.runWithConnection(conn, config2, null);

            assertThat(report2.status())
                    .describedAs("Report error: %s", report2.error())
                    .isEqualTo("complete");
            assertThat(report2.error()).isNull();
            assertThat(report2.scenarioName()).isEqualTo("multi-phase-test");
            assertThat(report2.phaseResults()).hasSize(2);
            assertThat(report2.environment()).isNotNull();
            assertThat(report2.environment().cpuCores()).isGreaterThan(0);

            PhaseResult phase1 = report2.phaseResults().get(0);
            assertThat(phase1.totalOps()).isEqualTo(200);

            PhaseResult phase2 = report2.phaseResults().get(1);
            assertThat(phase2.totalOps()).isEqualTo(300);
        }
    }

    @Test
    void fullPipelineWithReportOutput(@TempDir Path tempDir) throws Exception {
        // Setup: create database and table
        try (Connection setupConn =
                        ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
                Admin admin = setupConn.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();

            Schema logSchema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .column("value", DataTypes.BIGINT())
                            .build();
            TableDescriptor logDesc =
                    TableDescriptor.builder().schema(logSchema).distributedBy(1).build();
            TablePath logPath = TablePath.of(DATABASE, "perf_smoke_table");
            admin.createTable(logPath, logDesc, true).get();
            long tableId = admin.getTableInfo(logPath).get().getTableId();
            FLUSS_CLUSTER.waitUntilTableReady(tableId);
        }

        // 1. Define a small scenario YAML (log table, 500 records, 1 thread)
        String yaml =
                "meta:\n"
                        + "  name: smoke-test\n"
                        + "cluster:\n"
                        + "  tablet-servers: 1\n"
                        + "table:\n"
                        + "  name: perf_smoke_table\n"
                        + "  columns:\n"
                        + "    - name: id\n"
                        + "      type: INT\n"
                        + "    - name: name\n"
                        + "      type: STRING\n"
                        + "    - name: value\n"
                        + "      type: BIGINT\n"
                        + "  buckets: 1\n"
                        + "data:\n"
                        + "  seed: 42\n"
                        + "workload:\n"
                        + "  - phase: write\n"
                        + "    records: 500\n";

        // 2. Parse and validate
        ScenarioConfig config = ScenarioConfig.parse(yaml);
        List<String> errors = ScenarioValidator.validate(config);
        assertThat(errors).isEmpty();

        // 3. Run engine
        try (Connection conn =
                ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig())) {
            YamlDrivenEngine engine = new YamlDrivenEngine();
            PerfReport report = engine.runWithConnection(conn, config, null);

            assertThat(report.status())
                    .describedAs("Report error: %s", report.error())
                    .isEqualTo("complete");
            assertThat(report.scenarioName()).isEqualTo("smoke-test");
            assertThat(report.phaseResults()).hasSize(1);
            assertThat(report.phaseResults().get(0).totalOps()).isEqualTo(500);

            // 4. Write report to tempDir
            ReportWriter.write(report, tempDir, Arrays.asList("json", "csv"));

            // 5. Verify output files exist
            assertThat(tempDir.resolve("summary.json")).exists();
            assertThat(tempDir.resolve("timeseries.csv")).exists();

            // 6. Verify JSON is parseable and has expected structure
            String json =
                    new String(
                            Files.readAllBytes(tempDir.resolve("summary.json")),
                            StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);
            assertThat(root.has("scenarioName")).isTrue();
            assertThat(root.get("scenarioName").asText()).isEqualTo("smoke-test");
            assertThat(root.has("status")).isTrue();
            assertThat(root.get("status").asText()).isEqualTo("complete");
            assertThat(root.has("phaseResults")).isTrue();
            assertThat(root.get("phaseResults").isArray()).isTrue();
            assertThat(root.get("phaseResults").size()).isEqualTo(1);
            JsonNode phaseNode = root.get("phaseResults").get(0);
            assertThat(phaseNode.get("phaseName").asText()).isEqualTo("write");
            assertThat(phaseNode.get("totalOps").asLong()).isEqualTo(500);
            assertThat(phaseNode.get("opsPerSecond").asDouble()).isGreaterThan(0);
            assertThat(root.has("environment")).isTrue();

            // 7. Verify CSV has header + data rows
            List<String> csvLines =
                    Files.readAllLines(tempDir.resolve("timeseries.csv"), StandardCharsets.UTF_8);
            assertThat(csvLines.size()).isGreaterThanOrEqualTo(1); // at least header
            assertThat(csvLines.get(0)).startsWith("timestamp,");
        }
    }

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }
}
