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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.server.utils.TestProcessBuilder;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.NumberSequenceIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

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

    @ParameterizedTest
    @CsvSource({"2, 1, 2", "2, 2, 1", "3, 2, "})
    void testEmbeddedRuntimeExecutesJobWithConfiguredParallelism(
            int parallelism, int taskManagers, Integer slotsPerTaskManager) throws Exception {
        Map<String, String> tieringParams =
                params(
                        "fluss.bootstrap.servers", "localhost:9123",
                        "datalake.format", "paimon");
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        flinkConfig.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, taskManagers);
        if (slotsPerTaskManager != null) {
            flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
        }
        EmbeddedFlussLakeTiering tiering = new EmbeddedFlussLakeTiering(tieringParams, flinkConfig);

        assertThat(tiering.execEnv.getParallelism()).isEqualTo(parallelism);
        tiering.execEnv.fromSequence(1, 3).sinkTo(new DiscardingSink<>()).name("end");
        JobClient jobClient = tiering.execEnv.executeAsync("embedded-runtime-smoke");
        CompletableFuture<JobExecutionResult> result = jobClient.getJobExecutionResult();
        try {
            assertThat(result.get(60, TimeUnit.SECONDS)).isNotNull();
        } finally {
            if (!result.isDone()) {
                jobClient.cancel().get(60, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A terminal failure of the tiering job propagates out of {@link
     * EmbeddedFlussLakeTiering#runEmbedded()}, so the {@code main()} method exits non-zero and the
     * container supervisor restarts the service.
     */
    @Test
    void testRunEmbeddedPropagatesTerminalJobFailure() {
        Map<String, String> tieringParams =
                params(
                        "fluss.bootstrap.servers", "localhost:9123",
                        "datalake.format", "paimon");
        EmbeddedFlussLakeTiering tiering =
                new EmbeddedFlussLakeTiering(tieringParams, new Configuration()) {

                    @Override
                    protected JobClient run() throws Exception {
                        // a bounded job that fails on its first element; with no
                        // restarts the failure is terminal
                        execEnv.setRestartStrategy(RestartStrategies.noRestart());
                        execEnv.fromElements(1, 2, 3)
                                .map(
                                        value -> {
                                            throw new IllegalStateException("expected job failure");
                                        })
                                .sinkTo(new DiscardingSink<>());
                        return execEnv.executeAsync("bounded-failing-job");
                    }
                };

        assertThatThrownBy(tiering::runEmbedded)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("expected job failure");
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void testRunEmbeddedAwaitsTerminationAndRemovesHook(boolean failJob, boolean failClose)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        CompletableFuture<Void> clusterTerminated = new CompletableFuture<>();
        CompletableFuture<Void> releaseClose = new CompletableFuture<>();
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(
                        configuration,
                        clusterConfiguration ->
                                new MiniCluster(clusterConfiguration) {
                                    @Override
                                    public CompletableFuture<Void> closeAsync() {
                                        return super.closeAsync()
                                                .thenCompose(
                                                        ignored -> {
                                                            clusterTerminated.complete(null);
                                                            return releaseClose;
                                                        });
                                    }
                                });
        EmbeddedFlussLakeTiering tiering =
                new EmbeddedFlussLakeTiering(
                        params(
                                "fluss.bootstrap.servers",
                                "localhost:9123",
                                "datalake.format",
                                "paimon"),
                        configuration,
                        runtime) {
                    @Override
                    protected JobClient run() throws Exception {
                        execEnv.setRestartStrategy(RestartStrategies.noRestart());
                        execEnv.fromElements(1)
                                .map(
                                        value -> {
                                            if (failJob) {
                                                throw new IllegalStateException(
                                                        "expected job failure");
                                            }
                                            return value;
                                        })
                                .sinkTo(new DiscardingSink<>());
                        return execEnv.executeAsync("termination-test");
                    }
                };
        CompletableFuture<Throwable> outcome = runInBackground(tiering);
        try {
            clusterTerminated.get(60, TimeUnit.SECONDS);
            assertThatThrownBy(() -> outcome.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            if (failClose) {
                releaseClose.completeExceptionally(new IOException("expected close failure"));
            } else {
                releaseClose.complete(null);
            }
            Throwable failure = outcome.get(60, TimeUnit.SECONDS);
            if (failJob) {
                assertThat(failure)
                        .hasRootCauseInstanceOf(IllegalStateException.class)
                        .hasRootCauseMessage("expected job failure");
                if (failClose) {
                    assertThat(failure.getSuppressed())
                            .singleElement()
                            .satisfies(
                                    suppressed ->
                                            assertThat(suppressed)
                                                    .hasRootCauseMessage("expected close failure"));
                }
            } else if (failClose) {
                assertThat(failure).hasRootCauseMessage("expected close failure");
            } else {
                assertThat(failure).isNull();
                runtime.close();
            }
            assertHookRemoved(tiering);
        } finally {
            releaseClose.complete(null);
            outcome.get(60, TimeUnit.SECONDS);
        }
    }

    @Test
    void testRunEmbeddedRemovesHookWhenJobConstructionFails() {
        EmbeddedFlussLakeTiering tiering =
                new EmbeddedFlussLakeTiering(
                        params(
                                "fluss.bootstrap.servers",
                                "localhost:9123",
                                "datalake.format",
                                "paimon"),
                        new Configuration()) {
                    @Override
                    protected JobClient run() {
                        throw new IllegalStateException("expected construction failure");
                    }
                };

        assertThatThrownBy(tiering::runEmbedded).hasMessage("expected construction failure");
        assertHookRemoved(tiering);
    }

    @Test
    void testShutdownBeforeSubmissionPreventsClusterStartup() throws Exception {
        Configuration configuration = new Configuration();
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(
                        configuration,
                        ignored -> {
                            throw new AssertionError("Cluster must not be created after shutdown");
                        });
        EmbeddedFlussLakeTiering tiering = boundedTiering(configuration, runtime);
        runtime.close();
        runtime.close();

        assertThatThrownBy(tiering::runEmbedded)
                .hasMessageContaining("Embedded runtime is shutting down");
        assertHookRemoved(tiering);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectsNonPositiveShutdownTimeout(int timeoutMillis) {
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT, Duration.ofMillis(timeoutMillis));
        assertThatThrownBy(
                        () ->
                                new EmbeddedFlussLakeTiering.EmbeddedRuntime(
                                        configuration, MiniCluster::new))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.services.shutdown-timeout");
    }

    @ParameterizedTest
    @CsvSource({"30000, 200", "200, 30000"})
    void testShutdownUsesDecoratorTimeout(int initialMillis, int decoratedMillis) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        configuration.set(
                ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT, Duration.ofMillis(initialMillis));
        Duration decoratedTimeout = Duration.ofMillis(decoratedMillis);
        BlockingMiniCluster cluster =
                new BlockingMiniCluster(
                        new MiniClusterConfiguration.Builder().build(), ShutdownBlock.TERMINATION);
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(
                        configuration,
                        clusterConfiguration -> {
                            assertThat(
                                            clusterConfiguration
                                                    .getConfiguration()
                                                    .get(
                                                            ClusterOptions
                                                                    .CLUSTER_SERVICES_SHUTDOWN_TIMEOUT))
                                    .isEqualTo(decoratedTimeout);
                            return cluster;
                        });
        EmbeddedFlussLakeTiering tiering = boundedTiering(configuration, runtime);
        applyShutdownTimeoutDecorator(tiering, decoratedTimeout);
        CompletableFuture<Throwable> outcome = runInBackground(tiering);
        try {
            cluster.closeCalled.get(60, TimeUnit.SECONDS);
            if (decoratedMillis < initialMillis) {
                assertThat(outcome.get(5, TimeUnit.SECONDS).getSuppressed())
                        .singleElement()
                        .satisfies(
                                failure ->
                                        assertThat(failure)
                                                .isInstanceOf(TimeoutException.class)
                                                .hasMessage(
                                                        "Embedded Flink runtime did not terminate within "
                                                                + decoratedTimeout
                                                                + '.'));
            } else {
                assertThatThrownBy(() -> outcome.get(1, TimeUnit.SECONDS))
                        .isInstanceOf(TimeoutException.class);
                cluster.release.complete(null);
                assertThat(outcome.get(60, TimeUnit.SECONDS).getSuppressed()).isEmpty();
            }
        } finally {
            cluster.release.complete(null);
            runtime.close();
            assertThat(outcome.get(60, TimeUnit.SECONDS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("expected startup failure");
        }
        assertHookRemoved(tiering);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectsNonPositiveDecoratorShutdownTimeout(int timeoutMillis) {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(
                        configuration,
                        ignored -> {
                            throw new AssertionError(
                                    "Cluster must not start with an invalid timeout");
                        });
        EmbeddedFlussLakeTiering tiering = boundedTiering(configuration, runtime);
        applyShutdownTimeoutDecorator(tiering, Duration.ofMillis(timeoutMillis));

        assertThatThrownBy(tiering::runEmbedded)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.services.shutdown-timeout");
        assertHookRemoved(tiering);
    }

    @Test
    void testRejectedSubmissionDoesNotChangeShutdownTimeout() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        configuration.set(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT, Duration.ofMillis(200));
        BlockingMiniCluster cluster =
                new BlockingMiniCluster(
                        new MiniClusterConfiguration.Builder().build(), ShutdownBlock.TERMINATION);
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(configuration, ignored -> cluster);
        EmbeddedFlussLakeTiering tiering = boundedTiering(configuration, runtime);
        CompletableFuture<Throwable> outcome = runInBackground(tiering);
        try {
            cluster.closeCalled.get(60, TimeUnit.SECONDS);
            applyShutdownTimeoutDecorator(tiering, Duration.ofSeconds(30));
            assertThatThrownBy(tiering::run)
                    .hasMessageContaining("Embedded runtime is shutting down");
            assertThatThrownBy(runtime::close)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessage("Embedded Flink runtime did not terminate within PT0.2S.");
        } finally {
            cluster.release.complete(null);
            runtime.close();
            assertThat(outcome.get(60, TimeUnit.SECONDS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("expected startup failure");
        }
        assertHookRemoved(tiering);
    }

    @ParameterizedTest
    @EnumSource(ShutdownBlock.class)
    void testShutdownTimeoutCoversStartupCloseCallAndTermination(ShutdownBlock block)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        configuration.set(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT, Duration.ofMillis(200));
        BlockingMiniCluster cluster =
                new BlockingMiniCluster(new MiniClusterConfiguration.Builder().build(), block);
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(configuration, ignored -> cluster);
        EmbeddedFlussLakeTiering tiering = boundedTiering(configuration, runtime);
        CompletableFuture<Throwable> outcome = runInBackground(tiering);
        try {
            cluster.blocked.get(60, TimeUnit.SECONDS);
            long start = System.nanoTime();
            assertThatThrownBy(runtime::close)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("Embedded Flink runtime did not terminate within");
            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .isLessThan(Duration.ofSeconds(5));
            if (block == ShutdownBlock.STARTUP) {
                assertThat(cluster.closeCalled).isNotDone();
            }
        } finally {
            cluster.release.complete(null);
            cluster.closeCalled.get(60, TimeUnit.SECONDS);
            runtime.close();
            assertThat(outcome.get(60, TimeUnit.SECONDS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("expected startup failure");
        }
        assertHookRemoved(tiering);
    }

    @Test
    void testInterruptedShutdownPreservesInterruptAndContinuesCleanup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        BlockingMiniCluster cluster =
                new BlockingMiniCluster(
                        new MiniClusterConfiguration.Builder().build(), ShutdownBlock.TERMINATION);
        EmbeddedFlussLakeTiering.EmbeddedRuntime runtime =
                new EmbeddedFlussLakeTiering.EmbeddedRuntime(configuration, ignored -> cluster);
        EmbeddedFlussLakeTiering tiering = boundedTiering(configuration, runtime);
        CompletableFuture<Throwable> outcome = runInBackground(tiering);
        try {
            cluster.closeCalled.get(60, TimeUnit.SECONDS);
            Thread.currentThread().interrupt();
            assertThatThrownBy(runtime::close).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            cluster.release.complete(null);
            runtime.close();
            assertThat(outcome.get(60, TimeUnit.SECONDS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("expected startup failure");
        }
        assertHookRemoved(tiering);
    }

    private enum ShutdownBlock {
        STARTUP,
        CLOSE_CALL,
        TERMINATION
    }

    /** Models startup failure and independently controlled blocking shutdown stages. */
    private static class BlockingMiniCluster extends MiniCluster {
        private final ShutdownBlock block;
        private final CompletableFuture<Void> blocked = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CompletableFuture<Void> closeCalled = new CompletableFuture<>();

        private BlockingMiniCluster(MiniClusterConfiguration configuration, ShutdownBlock block) {
            super(configuration);
            this.block = block;
        }

        @Override
        public void start() throws Exception {
            if (block == ShutdownBlock.STARTUP) {
                blocked.complete(null);
                release.get(60, TimeUnit.SECONDS);
            }
            throw new IllegalStateException("expected startup failure");
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            closeCalled.complete(null);
            blocked.complete(null);
            if (block == ShutdownBlock.CLOSE_CALL) {
                try {
                    release.get(60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return release;
        }
    }

    private static void applyShutdownTimeoutDecorator(
            EmbeddedFlussLakeTiering tiering, Duration timeout) {
        LakeTieringDecorator decorator =
                (env, flussConfig, lakeConfig, tieringConfig, dataLake) -> {
                    Configuration override = new Configuration();
                    override.set(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT, timeout);
                    env.configure(override, EmbeddedFlussLakeTieringTest.class.getClassLoader());
                };
        decorator.decorate(
                tiering.execEnv,
                tiering.flussConfig,
                tiering.lakeConfig,
                tiering.lakeTieringConfig,
                tiering.dataLake);
    }

    private static EmbeddedFlussLakeTiering boundedTiering(
            Configuration configuration, EmbeddedFlussLakeTiering.EmbeddedRuntime runtime) {
        return new EmbeddedFlussLakeTiering(
                params("fluss.bootstrap.servers", "localhost:9123", "datalake.format", "paimon"),
                configuration,
                runtime) {
            @Override
            protected JobClient run() throws Exception {
                execEnv.fromElements(1).sinkTo(new DiscardingSink<>());
                return execEnv.executeAsync("bounded-test-job");
            }
        };
    }

    private static CompletableFuture<Throwable> runInBackground(EmbeddedFlussLakeTiering tiering) {
        // A dedicated thread keeps ForkJoin-based test waits from executing the blocking job
        // inline.
        CompletableFuture<Throwable> outcome = new CompletableFuture<>();
        Thread runner =
                new Thread(
                        () -> {
                            try {
                                tiering.runEmbedded();
                                outcome.complete(null);
                            } catch (Throwable failure) {
                                outcome.complete(failure);
                            }
                        },
                        "embedded-tiering-test");
        runner.setDaemon(true);
        runner.start();
        return outcome;
    }

    private static void assertHookRemoved(EmbeddedFlussLakeTiering tiering) {
        assertThat(tiering.getShutdownHook()).isNotNull();
        assertThat(Runtime.getRuntime().removeShutdownHook(tiering.getShutdownHook())).isFalse();
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

    @ParameterizedTest
    @CsvSource({
        "2, 1, 2",
        "3, 2, 2",
        "2147483647, 1073741824, 2",
        "2147483647, 2147483647, 2147483647"
    })
    void testParallelismWithinAggregateSlotsPasses(
            int parallelism, int slotsPerTaskManager, int taskManagers) {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
        flinkConfig.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, taskManagers);

        assertThatCode(() -> EmbeddedFlussLakeTiering.validateParallelismAgainstSlots(flinkConfig))
                .doesNotThrowAnyException();
    }

    @Test
    void testParallelismExceedingAggregateSlotsFailsFast() {
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params(
                                "parallelism.default", "3",
                                "taskmanager.numberOfTaskSlots", "1",
                                "minicluster.number-of-taskmanagers", "2"));

        assertThatThrownBy(
                        () -> EmbeddedFlussLakeTiering.validateParallelismAgainstSlots(flinkConfig))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("parallelism.default is 3")
                .hasMessageContaining("only offers 2 task slot(s)")
                .hasMessageContaining("taskmanager.numberOfTaskSlots")
                .hasMessageContaining("minicluster.number-of-taskmanagers");
    }

    @Test
    void testUnsetParallelismWithinAggregateSlotsPasses() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
        flinkConfig.set(
                TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS,
                Runtime.getRuntime().availableProcessors());

        assertThatCode(() -> EmbeddedFlussLakeTiering.validateParallelismAgainstSlots(flinkConfig))
                .doesNotThrowAnyException();
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
    void testUnsetParallelismWithPinnedSlotsFailsFast() {
        // an unset parallelism.default makes the embedded runtime run the job
        // at the CPU core count, which exceeds one slot on any machine with
        // more than one CPU
        assumeThat(Runtime.getRuntime().availableProcessors()).isGreaterThan(1);
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params("taskmanager.numberOfTaskSlots", "1"));

        assertThatThrownBy(
                        () -> EmbeddedFlussLakeTiering.validateParallelismAgainstSlots(flinkConfig))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("parallelism.default is unset");
    }

    @Test
    void testUnsetParallelismAndSlotsSkipSlotCheck() {
        // with both keys unset, the embedded runtime sizes its slots to the
        // job's demand, so there is no slot budget to validate
        Configuration flinkConfig = new Configuration();

        assertThatCode(() -> EmbeddedFlussLakeTiering.validateParallelismAgainstSlots(flinkConfig))
                .doesNotThrowAnyException();
    }

    @Test
    void testParallelismWithUnsetSlotsSkipsSlotCheck() {
        // with only the parallelism pinned, the embedded runtime sizes its
        // slots to the job's demand, so there is no slot budget to validate
        Configuration flinkConfig =
                EmbeddedFlussLakeTiering.extractFlinkConfig(
                        params(
                                "parallelism.default", "4",
                                "minicluster.number-of-taskmanagers", "2"));

        assertThatCode(() -> EmbeddedFlussLakeTiering.validateParallelismAgainstSlots(flinkConfig))
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

    @Test
    void testSigtermClosesEmbeddedSourceAndEnumerator() throws Exception {
        assumeThat(File.separatorChar).isEqualTo('/');
        TestProcessBuilder.TestProcess child =
                new TestProcessBuilder(ShutdownTestProgram.class.getName())
                        .setJvmMemory(MemorySize.ofMebiBytes(256))
                        .addMainClassArg(tempDir.toString())
                        .start();
        Process process = child.getProcess();
        try {
            waitUntil(
                    () -> {
                        assertThat(process.isAlive())
                                .withFailMessage(child.getErrorOutput().toString())
                                .isTrue();
                        return Files.exists(tempDir.resolve("reader-started"))
                                && child.getProcessOutput()
                                        .toString()
                                        .contains("Running the lake tiering job");
                    },
                    Duration.ofSeconds(60),
                    "Embedded source did not start");
            process.destroy();
            waitUntil(
                    () -> Files.exists(tempDir.resolve("enumerator-closing")) || !process.isAlive(),
                    Duration.ofSeconds(10),
                    "Enumerator close did not start");
            assertThat(tempDir.resolve("enumerator-closing")).exists();
            assertThat(process.waitFor(200, TimeUnit.MILLISECONDS)).isFalse();
            Files.createFile(tempDir.resolve("allow-enumerator-close"));
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            assertThat(tempDir.resolve("reader-closed")).exists();
            assertThat(tempDir.resolve("enumerator-closed")).exists();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            TestProcessBuilder.TestProcess.printProcessLog("embedded-shutdown", child);
        }
    }

    @Test
    void testSigtermTimeoutDoesNotWaitIndefinitelyForEnumerator() throws Exception {
        assumeThat(File.separatorChar).isEqualTo('/');
        TestProcessBuilder.TestProcess child =
                new TestProcessBuilder(ShutdownTestProgram.class.getName())
                        .setJvmMemory(MemorySize.ofMebiBytes(256))
                        .addMainClassArg(tempDir.toString())
                        .addMainClassArg("1 s")
                        .start();
        Process process = child.getProcess();
        try {
            waitUntil(
                    () -> {
                        assertThat(process.isAlive())
                                .withFailMessage(child.getErrorOutput().toString())
                                .isTrue();
                        return Files.exists(tempDir.resolve("reader-started"))
                                && child.getProcessOutput()
                                        .toString()
                                        .contains("Running the lake tiering job");
                    },
                    Duration.ofSeconds(60),
                    "Embedded source did not start");
            process.destroy();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(tempDir.resolve("enumerator-closed")).doesNotExist();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            TestProcessBuilder.TestProcess.printProcessLog("embedded-shutdown-timeout", child);
        }
    }

    @Test
    void testSigtermBeforeJobSubmission() throws Exception {
        assumeThat(File.separatorChar).isEqualTo('/');
        TestProcessBuilder.TestProcess child =
                new TestProcessBuilder(ShutdownTestProgram.class.getName())
                        .setJvmMemory(MemorySize.ofMebiBytes(256))
                        .addMainClassArg(tempDir.toString())
                        .addMainClassArg("30 s")
                        .addMainClassArg("block-submission")
                        .start();
        Process process = child.getProcess();
        try {
            waitUntil(
                    () -> {
                        assertThat(process.isAlive())
                                .withFailMessage(child.getErrorOutput().toString())
                                .isTrue();
                        return Files.exists(tempDir.resolve("before-submission"));
                    },
                    Duration.ofSeconds(60),
                    "Embedded entrypoint did not start");
            process.destroy();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(tempDir.resolve("reader-started")).doesNotExist();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            TestProcessBuilder.TestProcess.printProcessLog(
                    "embedded-shutdown-before-submit", child);
        }
    }

    /** Runs a local Flink job in a process that the test can terminate independently. */
    public static class ShutdownTestProgram {
        public static void main(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
            if (args.length > 1) {
                configuration.setString("cluster.services.shutdown-timeout", args[1]);
            }
            EmbeddedFlussLakeTiering tiering =
                    new EmbeddedFlussLakeTiering(
                            params(
                                    "fluss.bootstrap.servers",
                                    "localhost:9123",
                                    "datalake.format",
                                    "paimon"),
                            configuration) {
                        @Override
                        protected JobClient run() throws Exception {
                            if (args.length > 2) {
                                Files.createFile(Paths.get(args[0], "before-submission"));
                                new CompletableFuture<Void>().get(60, TimeUnit.SECONDS);
                            }
                            execEnv.fromSource(
                                            new ClosingSequenceSource(args[0]),
                                            WatermarkStrategy.noWatermarks(),
                                            "shutdown-source")
                                    .sinkTo(new DiscardingSink<>());
                            return execEnv.executeAsync("embedded-shutdown");
                        }
                    };
            tiering.runEmbedded();
        }
    }

    private static class ClosingSequenceSource extends NumberSequenceSource {
        private static final long serialVersionUID = 1L;
        private final String markerDirectory;

        private ClosingSequenceSource(String markerDirectory) {
            super(0, Long.MAX_VALUE);
            this.markerDirectory = markerDirectory;
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(SourceReaderContext context) {
            return new IteratorSourceReader<Long, NumberSequenceIterator, NumberSequenceSplit>(
                    context) {
                @Override
                public void start() {
                    super.start();
                    mark("reader-started");
                }

                @Override
                public void close() throws Exception {
                    super.close();
                    mark("reader-closed");
                }
            };
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                createEnumerator(SplitEnumeratorContext<NumberSequenceSplit> context) {
            return new IteratorSourceEnumerator<NumberSequenceSplit>(
                    context, splitNumberRange(0, Long.MAX_VALUE, context.currentParallelism())) {
                @Override
                public void close() {
                    super.close();
                    mark("enumerator-closing");
                    waitUntil(
                            () ->
                                    Files.exists(
                                            Paths.get(markerDirectory, "allow-enumerator-close")),
                            Duration.ofSeconds(20),
                            "Enumerator close was not released");
                    mark("enumerator-closed");
                }
            };
        }

        private void mark(String name) {
            try {
                Files.createFile(Paths.get(markerDirectory, name));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
