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

import static org.apache.fluss.flink.tiering.FlussLakeTiering.FLUSS_CONF_PREFIX;
import static org.apache.fluss.flink.tiering.FlussLakeTiering.LAKE_TIERING_CONFIG_PREFIX;
import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.DATA_LAKE_CONFIG_PREFIX;

/**
 * The entrypoint that runs the Fluss lake tiering job on an embedded, in-process Flink runtime.
 *
 * <p>Unlike {@link FlussLakeTieringEntrypoint}, which submits the tiering job to a Flink cluster
 * via {@code flink run}, this entrypoint executes the job on Flink's local runtime inside the same
 * JVM process, so no separate Flink cluster needs to be provisioned. The tiering job itself is
 * unchanged: it polls Fluss for tables with pending lake snapshots and tiers them to the configured
 * lake format, scaling with the configured parallelism.
 *
 * <p>Configuration is read from a YAML config file, using the same key-value syntax as Fluss's
 * {@code server.yaml}, and from the command line. The file is passed via {@link
 * #CONFIG_FILE_OPTION} and command line parameters take precedence over file entries. Parameters
 * carrying one of the tiering prefixes ({@code fluss.}, {@code datalake.}, or {@code
 * lake.tiering.}), together with the {@code config.providers.*} keys that declare config providers
 * for them, configure the tiering service exactly like the parameters of {@link
 * FlussLakeTieringEntrypoint}; all remaining parameters configure the embedded Flink runtime, for
 * example {@code parallelism.default} for the tiering parallelism, {@code
 * taskmanager.numberOfTaskSlots} for the number of slots the embedded runtime offers, or {@code
 * pipeline.name} for the job name.
 *
 * <p>Startup fails fast on configurations the embedded runtime cannot honor: {@code config.file} is
 * only accepted on the command line, and a positive {@code parallelism.default} must stay within
 * {@code taskmanager.numberOfTaskSlots} because the tiering job's source runs at the default
 * parallelism. If {@code parallelism.default} is unset, the embedded runtime runs the job at the
 * machine's CPU core count and the slot check cannot catch it, so set {@code parallelism.default}
 * and {@code taskmanager.numberOfTaskSlots} explicitly.
 *
 * <p>The tiering job is unbounded: this entrypoint blocks until the job reaches a terminal state.
 * The tiering job is stateless (progress is persisted through lake snapshots tracked by Fluss), so
 * on a terminal failure the process exits with a non-zero code and the surrounding container or
 * process supervisor simply starts it again.
 */
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
     * configuration tunes the embedded Flink runtime that executes the tiering job. Startup fails
     * fast when the configured default parallelism exceeds the number of task slots.
     *
     * @param tieringParams the tiering service parameters
     * @param flinkConfig the configuration for the embedded Flink runtime
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
     * Fails fast when the configured default parallelism exceeds the number of task slots of the
     * embedded Flink runtime.
     *
     * <p>The tiering job's source runs at the environment default parallelism ({@code
     * parallelism.default}) while only the committer and the sink are pinned to parallelism 1, so
     * the default parallelism must not exceed the slots ({@code taskmanager.numberOfTaskSlots}) the
     * embedded runtime offers. An unset or negative ({@code -1}, auto) parallelism skips the check.
     *
     * @param flinkConfig the configuration for the embedded Flink runtime
     * @throws IllegalConfigurationException if the default parallelism exceeds the task slots
     */
    private static void validateParallelismAgainstSlots(Configuration flinkConfig) {
        int parallelism = flinkConfig.get(CoreOptions.DEFAULT_PARALLELISM);
        int slots = flinkConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);
        if (parallelism <= 0 || parallelism <= slots) {
            return;
        }
        throw new IllegalConfigurationException(
                String.format(
                        "Invalid configuration for the embedded Flink runtime: parallelism.default "
                                + "is %d, but the runtime only offers %d task slot(s) "
                                + "(taskmanager.numberOfTaskSlots). The tiering job runs its source at "
                                + "the default parallelism, so parallelism.default must not exceed "
                                + "taskmanager.numberOfTaskSlots. Lower parallelism.default or set "
                                + "taskmanager.numberOfTaskSlots to at least %d.",
                        parallelism, slots, parallelism));
    }
}
