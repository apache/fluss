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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.config.provider.ConfigProviders;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.client.deployment.executors.LocalExecutorFactory;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ShutdownHookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.EXPONENTIAL_DELAY;
import static org.apache.flink.runtime.executiongraph.failover.FailoverStrategyFactoryLoader.FULL_RESTART_STRATEGY_NAME;
import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.DATA_LAKE_CONFIG_PREFIX;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;
import static org.apache.fluss.utils.PropertiesUtils.extractPrefix;

/**
 * Runs the Fluss lake tiering job on an embedded, in-process Flink runtime.
 *
 * <p>Unlike {@link FlussLakeTieringEntrypoint}, which submits the job to a Flink cluster, this
 * entrypoint runs it on Flink's local runtime inside the same JVM. This class is self-contained: it
 * owns the configuration parsing, the split between tiering service parameters and embedded runtime
 * parameters, and the construction and submission of the tiering job.
 *
 * <p>Parameters carrying a tiering prefix ({@code fluss.}, {@code datalake.}, {@code
 * lake.tiering.}) and the {@code config.providers.*} keys configure the tiering service, while all
 * remaining parameters configure the embedded runtime, for example {@code parallelism.default}, or
 * {@code taskmanager.numberOfTaskSlots} together with {@code minicluster.number-of-taskmanagers}.
 * See {@link #validateParallelismAgainstSlots} for the startup checks and {@link #runEmbedded} for
 * the job lifecycle.
 */
@PublicEvolving
public class EmbeddedFlussLakeTiering {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedFlussLakeTiering.class);

    static final String FLUSS_CONF_PREFIX = "fluss.";
    static final String LAKE_TIERING_CONFIG_PREFIX = "lake.tiering.";

    /** The command line option that points to the YAML config file with the tiering parameters. */
    public static final String CONFIG_FILE_OPTION = "config.file";

    final StreamExecutionEnvironment execEnv;
    final String dataLake;
    final org.apache.fluss.config.Configuration flussConfig;
    final org.apache.fluss.config.Configuration lakeConfig;
    final org.apache.fluss.config.Configuration lakeTieringConfig;

    private final EmbeddedRuntime runtime;
    @Nullable private Thread shutdownHook;

    public static void main(String[] args) {
        try {
            Map<String, String> cliParams =
                    new HashMap<>(MultipleParameterToolAdapter.fromArgs(args).toMap());
            String configFilePath = cliParams.remove(CONFIG_FILE_OPTION);

            Map<String, String> params = mergeParams(cliParams, configFilePath);

            EmbeddedFlussLakeTiering tiering =
                    new EmbeddedFlussLakeTiering(
                            extractTieringParams(params), extractFlinkConfig(params));
            tiering.runEmbedded();
        } catch (Throwable t) {
            LOG.error("The embedded Fluss lake tiering service failed.", t);
            System.exit(1);
        }
    }

    /**
     * Creates an embedded tiering service from the given parameters and Flink configuration.
     *
     * <p>The tiering parameters use the same keys as the command line of {@link
     * FlussLakeTieringEntrypoint}: {@code fluss.*} for the Fluss client configuration, {@code
     * datalake.format} to select the lake format, {@code datalake.<format>.*} for the lake
     * configuration, and {@code lake.tiering.*} for the tiering service configuration. Config
     * provider markers are resolved on the given parameters, so callers pass the raw merged
     * parameters without resolving them.
     *
     * <p>The Flink configuration is applied to the embedded runtime that executes the tiering job,
     * on top of the full-restart failover and the exponential-delay restart strategy configured by
     * this class: the given configuration is modified in place, the failover and restart strategy
     * keys are set on it.
     *
     * @param tieringParams the tiering service parameters
     * @param flinkConfig the configuration for the embedded Flink runtime
     * @throws IllegalArgumentException if {@code fluss.bootstrap.servers} or {@code
     *     datalake.format} is missing from the tiering parameters
     * @throws IllegalConfigurationException if a config provider marker is malformed or references
     *     an undeclared provider, or the job's parallelism exceeds the aggregate slot capacity
     */
    public EmbeddedFlussLakeTiering(Map<String, String> tieringParams, Configuration flinkConfig) {
        this(tieringParams, flinkConfig, new EmbeddedRuntime(flinkConfig, MiniCluster::new));
    }

    /**
     * Creates an embedded tiering service from the given parameters, Flink configuration, and
     * runtime. Package-private so tests can supply a runtime with a controllable cluster factory.
     *
     * <p>The parameter and failure semantics are those of the public constructor.
     *
     * @param tieringParams the tiering service parameters
     * @param flinkConfig the configuration for the embedded Flink runtime
     * @param runtime the embedded runtime owning the MiniCluster lifecycle
     */
    EmbeddedFlussLakeTiering(
            Map<String, String> tieringParams, Configuration flinkConfig, EmbeddedRuntime runtime) {
        checkNotNull(tieringParams, "%s must not be null", "tieringParams");
        checkNotNull(flinkConfig, "%s must not be null", "flinkConfig");

        Map<String, String> resolvedParams = resolveConfigProviders(tieringParams);

        // extract fluss config
        Map<String, String> flussConfigMap =
                extractAndRemovePrefix(resolvedParams, FLUSS_CONF_PREFIX);
        // we need to get bootstrap.servers
        String bootstrapServers = flussConfigMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The bootstrap server to fluss is not configured, please configure %s",
                            FLUSS_CONF_PREFIX + ConfigOptions.BOOTSTRAP_SERVERS.key()));
        }
        this.flussConfig = org.apache.fluss.config.Configuration.fromMap(flussConfigMap);

        this.dataLake = resolvedParams.get(ConfigOptions.DATALAKE_FORMAT.key());
        if (dataLake == null) {
            throw new IllegalArgumentException(
                    ConfigOptions.DATALAKE_FORMAT.key() + " is not configured");
        }

        // extract lake config
        Map<String, String> lakeConfigMap =
                extractAndRemovePrefix(
                        resolvedParams, String.format("%s%s.", DATA_LAKE_CONFIG_PREFIX, dataLake));
        this.lakeConfig = org.apache.fluss.config.Configuration.fromMap(lakeConfigMap);

        // extract tiering service config
        Map<String, String> lakeTieringConfigMap =
                extractPrefix(resolvedParams, LAKE_TIERING_CONFIG_PREFIX);
        this.lakeTieringConfig =
                org.apache.fluss.config.Configuration.fromMap(lakeTieringConfigMap);

        // now, we must use full restart strategy if any task is failed,
        // since committer is stateless, if tiering committer is failover, committer
        // will lost the collected committable, and will never collect all committable to do commit
        // todo: support region failover
        flinkConfig.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, FULL_RESTART_STRATEGY_NAME);
        // Configure restart strategy: the tiering job is completely stateless (offsets are
        // persisted in Fluss Server's LakeSnapshot, not in Flink checkpoint state), so it is
        // safe to restart indefinitely on failure. Without this, Flink defaults to "no-restart"
        // when checkpoint is not enabled, causing the job to fail permanently on any exception.
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY, EXPONENTIAL_DELAY.getMainValue());

        this.execEnv = runtime.createEnvironment(flinkConfig);

        validateParallelismAgainstSlots(flinkConfig);
        this.runtime = runtime;
    }

    /**
     * Builds and submits the tiering job on the embedded Flink runtime and blocks until the job
     * reaches a terminal state.
     *
     * <p>The tiering job is unbounded, so this method only returns when the job finishes or fails
     * terminally, is cancelled, or the JVM shuts down. Both normal completion and JVM shutdown
     * await MiniCluster termination, including source and enumerator cleanup, for at most {@code
     * cluster.services.shutdown-timeout} (default 30 seconds). A timeout fails the call; during JVM
     * shutdown it is logged and the process exits without waiting further.
     *
     * @throws Exception if the tiering job fails terminally or cannot be started
     */
    public void runEmbedded() throws Exception {
        shutdownHook = ShutdownHookUtil.addShutdownHook(runtime, "embedded lake tiering", LOG);
        try (EmbeddedRuntime ignored = runtime) {
            checkState(shutdownHook != null, "Cannot register embedded tiering shutdown hook.");
            JobClient jobClient = run();
            LOG.info(
                    "Running the lake tiering job {} on the embedded Flink runtime. "
                            + "The process keeps running until the job terminates.",
                    jobClient.getJobID());
            jobClient.getJobExecutionResult().get();
            LOG.info("The lake tiering job {} finished.", jobClient.getJobID());
        } finally {
            ShutdownHookUtil.removeShutdownHook(shutdownHook, "embedded lake tiering", LOG);
        }
    }

    @VisibleForTesting
    @Nullable
    Thread getShutdownHook() {
        return shutdownHook;
    }

    /**
     * Owns the local executor's cluster so shutdown awaits runtime cleanup, not just job results.
     */
    static final class EmbeddedRuntime implements PipelineExecutorServiceLoader, AutoCloseable {
        private final Object executionLock = new Object();
        private final Object shutdownLock = new Object();
        private final Function<MiniClusterConfiguration, MiniCluster> clusterFactory;
        private final CompletableFuture<Void> termination = new CompletableFuture<>();

        @GuardedBy("shutdownLock")
        private Duration shutdownTimeout;

        @GuardedBy("shutdownLock")
        private boolean closing;

        @GuardedBy("executionLock")
        @Nullable
        private MiniCluster miniCluster;

        EmbeddedRuntime(
                Configuration configuration,
                Function<MiniClusterConfiguration, MiniCluster> clusterFactory) {
            this.clusterFactory = clusterFactory;
            this.shutdownTimeout = getShutdownTimeout(configuration);
        }

        private static Duration getShutdownTimeout(Configuration configuration) {
            Duration timeout = configuration.get(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);
            checkArgument(
                    timeout.toMillis() > 0,
                    "cluster.services.shutdown-timeout must be at least 1 ms.");
            return timeout;
        }

        StreamExecutionEnvironment createEnvironment(Configuration configuration) {
            Configuration localConfiguration = new Configuration(configuration);
            localConfiguration.set(DeploymentOptions.TARGET, LocalExecutor.NAME);
            localConfiguration.set(DeploymentOptions.ATTACHED, true);
            if (!localConfiguration.contains(CoreOptions.DEFAULT_PARALLELISM)) {
                localConfiguration.set(
                        CoreOptions.DEFAULT_PARALLELISM,
                        StreamExecutionEnvironment.getDefaultLocalParallelism());
            }
            return new StreamExecutionEnvironment(this, localConfiguration, null);
        }

        @Override
        public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
            return new LocalExecutorFactory() {
                @Override
                public PipelineExecutor getExecutor(Configuration executorConfiguration) {
                    checkArgument(
                            isCompatibleWith(executorConfiguration), "Expected local execution.");
                    return (pipeline, config, classLoader) -> {
                        synchronized (executionLock) {
                            synchronized (shutdownLock) {
                                checkState(!closing, "Embedded runtime is shutting down.");
                                checkState(
                                        miniCluster == null, "Embedded runtime already has a job.");
                                Configuration effectiveConfig =
                                        new Configuration(executorConfiguration);
                                effectiveConfig.addAll(config);
                                shutdownTimeout = getShutdownTimeout(effectiveConfig);
                            }
                            return LocalExecutor.createWithFactory(
                                            executorConfiguration,
                                            clusterConfiguration -> {
                                                miniCluster =
                                                        clusterFactory.apply(clusterConfiguration);
                                                return miniCluster;
                                            })
                                    .execute(pipeline, config, classLoader);
                        }
                    };
                }
            };
        }

        @Override
        public Stream<String> getExecutorNames() {
            return Stream.of(LocalExecutor.NAME);
        }

        @Override
        public void close() throws Exception {
            boolean startShutdown;
            Duration timeout;
            // Freeze the effective timeout without waiting for the cluster's startup lock.
            synchronized (shutdownLock) {
                startShutdown = !closing;
                closing = true;
                timeout = shutdownTimeout;
            }
            if (startShutdown) {
                // MiniCluster.closeAsync can block on its startup lock. Keep that lock acquisition
                // inside the bounded wait, and prevent shutdown before start from leaking a
                // cluster.
                Thread shutdown =
                        new Thread(
                                () -> {
                                    try {
                                        CompletableFuture<Void> clusterTermination;
                                        synchronized (executionLock) {
                                            clusterTermination =
                                                    miniCluster == null
                                                            ? CompletableFuture.completedFuture(
                                                                    null)
                                                            : miniCluster.closeAsync();
                                        }
                                        clusterTermination.whenComplete(
                                                (ignored, failure) -> {
                                                    if (failure == null) {
                                                        termination.complete(null);
                                                    } else {
                                                        termination.completeExceptionally(failure);
                                                    }
                                                });
                                    } catch (Throwable failure) {
                                        termination.completeExceptionally(failure);
                                    }
                                },
                                "embedded-tiering-shutdown");
                shutdown.setDaemon(true);
                shutdown.start();
            }
            try {
                termination.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (TimeoutException e) {
                throw new TimeoutException(
                        "Embedded Flink runtime did not terminate within " + timeout + '.');
            }
        }
    }

    /**
     * Loads and applies all available decorator plugins, builds the tiering job, and submits it to
     * the execution environment without waiting for its completion.
     *
     * @return the {@link JobClient} of the submitted tiering job
     */
    protected JobClient run() throws Exception {
        // Load and apply all available decorator plugins
        loadAndApplyDecoratorPlugins();

        // build and run lake tiering job
        JobClient jobClient =
                LakeTieringJobBuilder.newBuilder(
                                execEnv, flussConfig, lakeConfig, lakeTieringConfig, dataLake)
                        .build();

        System.out.printf(
                "Starting data tiering service from Fluss to %s, jobId is %s.....%n",
                dataLake, jobClient.getJobID());
        return jobClient;
    }

    /**
     * Loads all available {@link LakeTieringDecoratorPlugin} implementations and applies their
     * decorators in sequence.
     *
     * <p>All available plugins will be loaded and their decorators will be called in the order they
     * are discovered by the ServiceLoader. This allows multiple decorators to be applied
     * sequentially, where each decorator can further customize the Flink execution environment and
     * configurations.
     */
    protected void loadAndApplyDecoratorPlugins() {
        ServiceLoader<LakeTieringDecoratorPlugin> serviceLoader =
                ServiceLoader.load(
                        LakeTieringDecoratorPlugin.class,
                        LakeTieringDecoratorPlugin.class.getClassLoader());
        for (LakeTieringDecoratorPlugin plugin : serviceLoader) {
            String identifier = plugin.identifier();
            System.out.printf(
                    "Applying LakeTieringDecoratorPlugin with identifier: %s%n", identifier);
            LakeTieringDecorator decorator = plugin.createLakeTieringDecorator();
            decorator.decorate(execEnv, flussConfig, lakeConfig, lakeTieringConfig, dataLake);
        }
    }

    /** Resolves {@code ${provider:...}} markers in all parameters before prefix extraction. */
    private static Map<String, String> resolveConfigProviders(Map<String, String> paramsMap) {
        org.apache.fluss.config.Configuration params =
                org.apache.fluss.config.Configuration.fromMap(paramsMap);
        ConfigProviders.resolve(params);
        return new HashMap<>(params.toMap());
    }

    /**
     * Merges the parameters from the given YAML config file and the given command line parameters,
     * with the command line parameters taking precedence.
     *
     * @param cliParams the parsed command line parameters without {@link #CONFIG_FILE_OPTION}
     * @param configFilePath the path of the YAML config file, or null for no config file
     * @return the merged raw parameters
     * @throws IllegalConfigurationException if {@link #CONFIG_FILE_OPTION} is present in the merged
     *     parameters, i.e. it was set anywhere but the command line
     */
    static Map<String, String> mergeParams(
            Map<String, String> cliParams, @Nullable String configFilePath) {
        Map<String, String> params = new HashMap<>();
        if (configFilePath != null) {
            params.putAll(loadConfigFile(configFilePath));
        }
        params.putAll(cliParams);
        if (params.containsKey(CONFIG_FILE_OPTION)) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The config file must be passed via --%s on the command line; setting "
                                    + "%s in the tiering configuration is not supported.",
                            CONFIG_FILE_OPTION, CONFIG_FILE_OPTION));
        }
        return params;
    }

    /**
     * Loads the given YAML config file, using the same key-value syntax as Fluss's {@code
     * server.yaml}.
     *
     * <p>Config provider markers in the file are not resolved here. After the merge, markers in the
     * tiering service parameters (keys carrying the {@code fluss.}, {@code datalake.}, or {@code
     * lake.tiering.} prefixes, plus the {@code config.providers.*} keys declaring the providers)
     * are resolved in the constructor; markers in the remaining parameters, which configure the
     * embedded Flink runtime, are kept verbatim.
     *
     * @param configFilePath the path of the YAML config file
     * @return the parameters parsed from the config file
     */
    static Map<String, String> loadConfigFile(String configFilePath) {
        File configFile = new File(configFilePath);
        if (!configFile.isFile()) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The tiering service config file %s (%s) does not exist.",
                            configFilePath, configFile.getAbsolutePath()));
        }
        return GlobalConfiguration.loadYAMLResource(configFile).toMap();
    }

    /**
     * Extracts the tiering service parameters, i.e. all parameters carrying one of the tiering
     * prefixes ({@code fluss.}, {@code datalake.}, or {@code lake.tiering.}), plus the {@code
     * config.providers.*} keys that declare the config providers resolving the markers in those
     * parameters. Without the {@code config.providers.*} keys, the markers would reach the Fluss
     * client as literals instead of being resolved.
     *
     * @param params the merged raw parameters
     * @return the tiering service parameters
     */
    static Map<String, String> extractTieringParams(Map<String, String> params) {
        Map<String, String> tieringParams = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (isTieringParam(entry.getKey())) {
                tieringParams.put(entry.getKey(), entry.getValue());
            }
        }
        return tieringParams;
    }

    /**
     * Extracts the configuration for the embedded Flink runtime, i.e. all merged parameters that
     * are not tiering service parameters.
     *
     * @param params the merged raw parameters
     * @return the configuration for the embedded Flink runtime
     */
    static Configuration extractFlinkConfig(Map<String, String> params) {
        Configuration flinkConfig = new Configuration();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!isTieringParam(entry.getKey())) {
                flinkConfig.setString(entry.getKey(), entry.getValue());
            }
        }
        return flinkConfig;
    }

    private static boolean isTieringParam(String key) {
        String configProvidersKey = ConfigOptions.CONFIG_PROVIDERS.key();
        return key.startsWith(FLUSS_CONF_PREFIX)
                || key.startsWith(DATA_LAKE_CONFIG_PREFIX)
                || key.startsWith(LAKE_TIERING_CONFIG_PREFIX)
                || key.equals(configProvidersKey)
                || key.startsWith(configProvidersKey + ".");
    }

    /**
     * Fails fast when the parallelism the job runs at exceeds the aggregate slot capacity, i.e.
     * {@code taskmanager.numberOfTaskSlots} multiplied by {@code
     * minicluster.number-of-taskmanagers}. The capacity only binds when {@code
     * taskmanager.numberOfTaskSlots} is pinned, since Flink otherwise sizes the slots to the job's
     * demand; an unset {@code parallelism.default} is validated at the machine's CPU core count,
     * which is the parallelism Flink runs the source at.
     *
     * @param flinkConfig the configuration for the embedded Flink runtime
     * @throws IllegalConfigurationException if the parallelism the job runs at exceeds the
     *     aggregate slot capacity
     */
    static void validateParallelismAgainstSlots(Configuration flinkConfig) {
        // The embedded runtime sizes its task slots to the job's maximum parallelism whenever
        // taskmanager.numberOfTaskSlots is unset, so only a pinned slot budget can be exceeded.
        if (!flinkConfig.getOptional(TaskManagerOptions.NUM_TASK_SLOTS).isPresent()) {
            return;
        }
        // An unset parallelism.default makes the embedded runtime run the job at the machine's
        // CPU core count (Flink's default local parallelism), so that count is the effective
        // parallelism to validate.
        Optional<Integer> configuredParallelism =
                flinkConfig.getOptional(CoreOptions.DEFAULT_PARALLELISM);
        int parallelism = configuredParallelism.orElse(Runtime.getRuntime().availableProcessors());
        int slotsPerTaskManager = flinkConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);
        int taskManagers = flinkConfig.get(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS);
        long totalSlots = (long) slotsPerTaskManager * taskManagers;
        if (parallelism <= 0 || parallelism <= totalSlots) {
            return;
        }
        if (!configuredParallelism.isPresent()) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for the embedded Flink runtime: parallelism.default "
                                    + "is unset, so the embedded runtime runs the tiering job at the "
                                    + "machine's CPU core count (%d), but the runtime only offers %d task "
                                    + "slot(s) (taskmanager.numberOfTaskSlots=%d * "
                                    + "minicluster.number-of-taskmanagers=%d). Set parallelism.default "
                                    + "explicitly (at most %d), or increase taskmanager.numberOfTaskSlots "
                                    + "or minicluster.number-of-taskmanagers.",
                            parallelism,
                            totalSlots,
                            slotsPerTaskManager,
                            taskManagers,
                            totalSlots));
        }
        throw new IllegalConfigurationException(
                String.format(
                        "Invalid configuration for the embedded Flink runtime: parallelism.default "
                                + "is %d, but the runtime only offers %d task slot(s) "
                                + "(taskmanager.numberOfTaskSlots=%d * minicluster.number-of-taskmanagers=%d). "
                                + "The tiering job runs its source at the default parallelism, so "
                                + "parallelism.default must not exceed the aggregate slot capacity. "
                                + "Lower parallelism.default or increase taskmanager.numberOfTaskSlots "
                                + "or minicluster.number-of-taskmanagers.",
                        parallelism, totalSlots, slotsPerTaskManager, taskManagers));
    }
}
