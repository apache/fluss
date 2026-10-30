/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.IllegalConfigurationException;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link EmbeddedFlussLakeTiering}. */
class EmbeddedFlussLakeTieringTest {

    @TempDir Path tempDir;

    @Test
    void testMergeParamsPrefersCliParamsOverConfigFile() throws IOException {
        File configFile =
                writeConfigFile(
                        "# comment line",
                        "fluss.bootstrap.servers: localhost:9123",
                        "parallelism.default: 4",
                        "");
        Map<String, String> cliParams =
                params(
                        "fluss.bootstrap.servers", "fluss:9123",
                        "datalake.format", "paimon");

        Map<String, String> merged =
                EmbeddedFlussLakeTiering.mergeParams(cliParams, configFile.getAbsolutePath());

        assertThat(merged)
                .containsEntry("fluss.bootstrap.servers", "fluss:9123")
                .containsEntry("datalake.format", "paimon")
                .containsEntry("parallelism.default", "4")
                .hasSize(3);
    }

    @Test
    void testMergeParamsWithoutConfigFile() {
        Map<String, String> merged =
                EmbeddedFlussLakeTiering.mergeParams(
                        params("fluss.bootstrap.servers", "localhost:9123"), null);

        assertThat(merged).containsEntry("fluss.bootstrap.servers", "localhost:9123");
    }

    @Test
    void testLoadConfigFileWithMissingFile() {
        String missingFile = tempDir.resolve("missing.yaml").toAbsolutePath().toString();

        assertThatThrownBy(() -> EmbeddedFlussLakeTiering.loadConfigFile(missingFile))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(missingFile);
    }

    @Test
    void testMergeParamsRejectsConfigFileInsideConfigFile() throws IOException {
        File configFile =
                writeConfigFile(
                        "fluss.bootstrap.servers: localhost:9123",
                        "config.file: /opt/fluss/conf/tiering.yaml",
                        "");

        assertThatThrownBy(
                        () ->
                                EmbeddedFlussLakeTiering.mergeParams(
                                        new HashMap<>(), configFile.getAbsolutePath()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("config.file");
    }

    @Test
    void testExtractParamsSplitsTieringAndFlinkConfig() {
        Map<String, String> params =
                params(
                        "fluss.bootstrap.servers", "localhost:9123",
                        "fluss.client.security.protocol", "SASL",
                        "datalake.format", "paimon",
                        "datalake.paimon.warehouse", "/tmp/paimon",
                        "lake.tiering.example.option", "5 s",
                        "parallelism.default", "4",
                        "taskmanager.numberOfTaskSlots", "4",
                        "pipeline.name", "my-tiering-job");

        Map<String, String> tieringParams = EmbeddedFlussLakeTiering.extractTieringParams(params);
        Map<String, String> flinkParams =
                EmbeddedFlussLakeTiering.extractFlinkConfig(params).toMap();

        assertThat(tieringParams)
                .containsEntry("fluss.bootstrap.servers", "localhost:9123")
                .containsEntry("fluss.client.security.protocol", "SASL")
                .containsEntry("datalake.format", "paimon")
                .containsEntry("datalake.paimon.warehouse", "/tmp/paimon")
                .containsEntry("lake.tiering.example.option", "5 s")
                .hasSize(5);
        assertThat(flinkParams)
                .containsEntry("parallelism.default", "4")
                .containsEntry("taskmanager.numberOfTaskSlots", "4")
                .containsEntry("pipeline.name", "my-tiering-job")
                .hasSize(3);
    }

    @Test
    void testEmbeddedTieringBuildsConfigsFromMergedParams() throws IOException {
        File configFile =
                writeConfigFile(
                        "fluss.bootstrap.servers: localhost:9123",
                        "datalake.format: paimon",
                        "datalake.paimon.metastore: filesystem",
                        "datalake.paimon.warehouse: /tmp/paimon",
                        "lake.tiering.example.option: 5 s",
                        "parallelism.default: 4",
                        "taskmanager.numberOfTaskSlots: 4");
        Map<String, String> params =
                EmbeddedFlussLakeTiering.mergeParams(new HashMap<>(), configFile.getAbsolutePath());

        EmbeddedFlussLakeTiering tiering =
                new EmbeddedFlussLakeTiering(
                        EmbeddedFlussLakeTiering.extractTieringParams(params),
                        EmbeddedFlussLakeTiering.extractFlinkConfig(params));

        assertThat(tiering.flussConfig.getRawValue(ConfigOptions.BOOTSTRAP_SERVERS.key()))
                .isPresent()
                .contains("localhost:9123");
        assertThat(tiering.dataLake).isEqualTo("paimon");
        assertThat(tiering.lakeConfig.toMap())
                .containsEntry("metastore", "filesystem")
                .containsEntry("warehouse", "/tmp/paimon");
        assertThat(tiering.lakeTieringConfig.toMap())
                .containsEntry("lake.tiering.example.option", "5 s");

        // the embedded Flink runtime honors the unprefixed config file entries
        assertThat(tiering.execEnv.getParallelism()).isEqualTo(4);
        // and the tiering job keeps the failover and restart strategy of FlussLakeTiering
        assertThat(
                        tiering.execEnv
                                .getConfiguration()
                                .get(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY))
                .isEqualTo("full");
        assertThat(tiering.execEnv.getConfiguration().get(RESTART_STRATEGY))
                .isEqualTo("exponential-delay");
    }

    @Test
    void testEmbeddedRuntimeExecutesJobWithConfiguredParallelism() throws Exception {
        Map<String, String> tieringParams =
                params(
                        "fluss.bootstrap.servers", "localhost:9123",
                        "datalake.format", "paimon");
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params(
                                "parallelism.default", "2",
                                "taskmanager.numberOfTaskSlots", "2"));
        EmbeddedFlussLakeTiering tiering = new EmbeddedFlussLakeTiering(tieringParams, flinkConfig);

        // the embedded runtime executes a bounded job on the local Flink
        // cluster with the parallelism from the unprefixed config entries
        tiering.execEnv.fromElements(1, 2, 3).sinkTo(new DiscardingSink<>()).name("end");
        JobExecutionResult result = tiering.execEnv.execute("embedded-runtime-smoke");

        assertThat(result).isNotNull();
    }

    @Test
    void testEmbeddedTieringWithoutBootstrapServersFails() {
        Map<String, String> tieringParams = params("datalake.format", "paimon");

        assertThatThrownBy(
                        () ->
                                new EmbeddedFlussLakeTiering(
                                        tieringParams,
                                        EmbeddedFlussLakeTiering.extractFlinkConfig(tieringParams)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fluss.bootstrap.servers");
    }

    @Test
    void testEmbeddedTieringWithoutDataLakeFormatFails() {
        Map<String, String> tieringParams = params("fluss.bootstrap.servers", "localhost:9123");

        assertThatThrownBy(
                        () ->
                                new EmbeddedFlussLakeTiering(
                                        tieringParams,
                                        EmbeddedFlussLakeTiering.extractFlinkConfig(tieringParams)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("datalake.format");
    }

    @Test
    void testParallelismExceedingSlotsFailsFast() {
        Map<String, String> tieringParams =
                params("fluss.bootstrap.servers", "localhost:9123", "datalake.format", "paimon");
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params("parallelism.default", "4", "taskmanager.numberOfTaskSlots", "2"));

        assertThatThrownBy(() -> new EmbeddedFlussLakeTiering(tieringParams, flinkConfig))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("parallelism.default")
                .hasMessageContaining("4")
                .hasMessageContaining("taskmanager.numberOfTaskSlots")
                .hasMessageContaining("2");
    }

    @Test
    void testAutoParallelismSkipsSlotCheck() {
        Map<String, String> tieringParams =
                params("fluss.bootstrap.servers", "localhost:9123", "datalake.format", "paimon");
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params("parallelism.default", "-1", "taskmanager.numberOfTaskSlots", "1"));

        assertThatCode(() -> new EmbeddedFlussLakeTiering(tieringParams, flinkConfig))
                .doesNotThrowAnyException();
    }

    @Test
    void testEqualParallelismAndSlotsPasses() {
        Map<String, String> tieringParams =
                params("fluss.bootstrap.servers", "localhost:9123", "datalake.format", "paimon");
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params("parallelism.default", "2", "taskmanager.numberOfTaskSlots", "2"));

        assertThatCode(() -> new EmbeddedFlussLakeTiering(tieringParams, flinkConfig))
                .doesNotThrowAnyException();
    }

    @Test
    void testConfigProvidersKeysRouteToTieringParams() {
        Map<String, String> params =
                params(
                        "config.providers", "env",
                        "config.providers.env.param.allowlist.pattern", "FLUSS_.*",
                        "fluss.bootstrap.servers", "localhost:9123",
                        "datalake.format", "paimon",
                        "parallelism.default", "4");

        assertThat(EmbeddedFlussLakeTiering.extractTieringParams(params))
                .containsKeys("config.providers", "config.providers.env.param.allowlist.pattern");
        assertThat(EmbeddedFlussLakeTiering.extractFlinkConfig(params).toMap())
                .doesNotContainKeys(
                        "config.providers", "config.providers.env.param.allowlist.pattern");
    }

    /**
     * The {@code config.providers.*} keys must survive the tiering-parameter filter, otherwise
     * provider markers in {@code fluss.*} values would reach the Fluss client unresolved. This test
     * resolves a marker through the real 'directory' provider; the 'env' and 'file' providers
     * resolve through the same {@code ConfigProviders.resolve} path.
     */
    @Test
    void testConfigProviderMarkersInTieringParamsAreResolved() throws IOException {
        Path secretDir = Files.createDirectories(tempDir.resolve("secrets"));
        Path bootstrapFile = secretDir.resolve("bootstrap");
        Files.write(bootstrapFile, "localhost:9123".getBytes(StandardCharsets.UTF_8));

        Map<String, String> params =
                params(
                        "config.providers",
                        "directory",
                        "config.providers.directory.param.allowed.paths",
                        secretDir.toAbsolutePath().toString(),
                        "fluss.bootstrap.servers",
                        "${directory:" + secretDir.toAbsolutePath() + ":bootstrap}",
                        "datalake.format",
                        "paimon");

        EmbeddedFlussLakeTiering tiering =
                new EmbeddedFlussLakeTiering(
                        EmbeddedFlussLakeTiering.extractTieringParams(params),
                        EmbeddedFlussLakeTiering.extractFlinkConfig(params));

        assertThat(tiering.flussConfig.getRawValue(ConfigOptions.BOOTSTRAP_SERVERS.key()))
                .isPresent()
                .contains("localhost:9123");
        // the provider keys configure the tiering service, not the embedded Flink runtime
        assertThat(tiering.execEnv.getConfiguration().toMap())
                .doesNotContainKey("config.providers");
    }

    private File writeConfigFile(String... lines) throws IOException {
        Path configFile = tempDir.resolve("tiering.yaml");
        Files.write(configFile, Arrays.asList(lines));
        return configFile.toFile();
    }

    private static Map<String, String> params(String... keyValuePairs) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            params.put(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return params;
    }
}
