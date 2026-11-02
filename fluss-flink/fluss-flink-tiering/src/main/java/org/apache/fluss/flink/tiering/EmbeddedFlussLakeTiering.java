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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.flink.tiering.FlussLakeTiering.FLUSS_CONF_PREFIX;
import static org.apache.fluss.flink.tiering.FlussLakeTiering.LAKE_TIERING_CONFIG_PREFIX;
import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.DATA_LAKE_CONFIG_PREFIX;

/**
 * Runs the Fluss lake tiering job on an embedded, in-process Flink runtime.
 *
 * <p>Unlike {@link FlussLakeTieringEntrypoint}, which submits the job to a Flink cluster, this
 * entrypoint runs it on Flink's local runtime inside the same JVM. The tiering job itself is
 * unchanged; only the runtime hosting it differs.
 *
 * <p>Parameters carrying a tiering prefix ({@code fluss.}, {@code datalake.}, {@code
 * lake.tiering.}) and the {@code config.providers.*} keys configure the tiering service, while all
 * remaining parameters configure the embedded runtime, for example {@code parallelism.default}, or
 * {@code taskmanager.numberOfTaskSlots} together with {@code minicluster.number-of-taskmanagers}.
 * See {@link #validateParallelismAgainstSlots} for the startup checks and {@link #runEmbedded} for
 * the job lifecycle.
 */
@PublicEvolving
public class EmbeddedFlussLakeTiering extends FlussLakeTiering {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedFlussLakeTiering.class);

    /** The command line option that points to the YAML config file with the tiering parameters. */
    public static final String CONFIG_FILE_OPTION = "config.file";

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
     * FlussLakeTieringEntrypoint}; see {@link FlussLakeTiering} for the expected shapes. The Flink
     * configuration tunes the embedded Flink runtime that executes the tiering job.
     *
     * @param tieringParams the tiering service parameters
     * @param flinkConfig the configuration for the embedded Flink runtime
     * @throws IllegalArgumentException if {@code fluss.bootstrap.servers} or {@code
     *     datalake.format} is missing from the tiering parameters
     * @throws IllegalConfigurationException if a config provider marker is malformed or references
     *     an undeclared provider, or the job's parallelism exceeds the aggregate slot capacity
     */
    public EmbeddedFlussLakeTiering(Map<String, String> tieringParams, Configuration flinkConfig) {
        super(tieringParams, flinkConfig);
        validateParallelismAgainstSlots(flinkConfig);
    }

    /**
     * Builds and submits the tiering job on the embedded Flink runtime and blocks until the job
     * reaches a terminal state.
     *
     * <p>The tiering job is unbounded, so this method only returns when the job finishes or fails
     * terminally, is cancelled, or the JVM shuts down.
     *
     * @throws Exception if the tiering job fails terminally or cannot be started
     */
    public void runEmbedded() throws Exception {
        JobClient jobClient = run();
        LOG.info(
                "Running the lake tiering job {} on the embedded Flink runtime. "
                        + "The process keeps running until the job terminates.",
                jobClient.getJobID());
        jobClient.getJobExecutionResult().get();
        LOG.info("The lake tiering job {} finished.", jobClient.getJobID());
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
     * are resolved by {@link FlussLakeTiering}; markers in the remaining parameters, which
     * configure the embedded Flink runtime, are kept verbatim.
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
