/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.cli;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.GlobalConfiguration;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.PipelineOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/** Cli for Fluss lakehouse integrating. */
public class FlussLakehouseCli {

    private static final Duration DEFAULT_CHECKPOINT_INTERVAL = Duration.ofMinutes(3);
    private static final String DEFAULT_PIPELINE_NAME = "fluss-paimon-tiering-service";

    private static final int INITIAL_RET_CODE = 31;
    private static final String FLINK_CONFIG_PREFIX = "flink.";

    public static void main(final String[] args) {
        int retCode = INITIAL_RET_CODE;
        try {
            retCode = mainInternal(args);
        } finally {
            System.exit(retCode);
        }
    }

    public static int mainInternal(String[] args) {
        if (args.length < 1) {
            System.out.println("Please specify an action to execute.");
            return INITIAL_RET_CODE;
        }
        if (args.length < 2) {
            System.out.println("Please specify an jarFile to execute.");
        }
        String runAction = args[0];
        String jarFile = args[1];
        int retCode = INITIAL_RET_CODE;
        try {
            // get the config dir and dynamic config
            Tuple2<String, Configuration> configDirAndDynamicConfig =
                    ConfigurationParserUtils.getConfigDirAndDynamicConfig(args);
            // combine config dir and dynamic config to one configuration
            Configuration globalConfiguration =
                    GlobalConfiguration.loadConfiguration(
                            configDirAndDynamicConfig.f0, configDirAndDynamicConfig.f1);

            // validate datalake format
            DataLakeFormat datalakeFormat = globalConfiguration.get(ConfigOptions.DATALAKE_FORMAT);
            if (datalakeFormat == null) {
                throw new IllegalArgumentException(
                        "The datalake format is not set,"
                                + " please set the configuration "
                                + ConfigOptions.DATALAKE_FORMAT.key());
            }

            // get config for fluss source, now only bootstrap server is required
            String flussBootstrapServer = getFlussBootStrapServers(globalConfiguration);

            // add dynamic config to flink
            Map<String, String> dynamicConfigs = configDirAndDynamicConfig.f1.toMap();
            Map<String, String> flinkConfigs = removeFlinkConfig(dynamicConfigs);
            org.apache.flink.configuration.Configuration flinkConfig =
                    org.apache.flink.configuration.Configuration.fromMap(flinkConfigs);

            if (!flinkConfig.contains(CHECKPOINTING_INTERVAL)) {
                flinkConfig.set(CHECKPOINTING_INTERVAL, DEFAULT_CHECKPOINT_INTERVAL);
            }
            // set pipeline name
            flinkConfig.set(PipelineOptions.NAME, DEFAULT_PIPELINE_NAME);

            // now, start execute
            retCode = run(runAction, jarFile, flussBootstrapServer, flinkConfig, dynamicConfigs);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
        return retCode;
    }

    private static Map<String, String> removeFlinkConfig(Map<String, String> config) {
        Map<String, String> flinkConfig = new HashMap<>();
        Iterator<Map.Entry<String, String>> configEntryIterator = config.entrySet().iterator();
        while (configEntryIterator.hasNext()) {
            Map.Entry<String, String> entry = configEntryIterator.next();
            if (entry.getKey().startsWith(FLINK_CONFIG_PREFIX)) {
                flinkConfig.put(
                        entry.getKey().substring(FLINK_CONFIG_PREFIX.length()), entry.getValue());
                configEntryIterator.remove();
            }
        }
        return flinkConfig;
    }

    private static int run(
            String runAction,
            String jarFile,
            String flussBootstrapServer,
            org.apache.flink.configuration.Configuration flinkConfig,
            Map<String, String> dynamicConfigs) {
        List<CustomCommandLine> customCommandLines = new ArrayList<>();
        customCommandLines.add(new DefaultCLI());

        CliFrontend cliFrontend = new FlussCliFrontend(flinkConfig, customCommandLines);
        // let's combine fluss config and lake config as arguments
        List<String> arguments =
                new ArrayList<>(
                        Arrays.asList(
                                runAction,
                                jarFile,
                                "--" + ConfigOptions.BOOTSTRAP_SERVERS.key(),
                                flussBootstrapServer));
        for (Map.Entry<String, String> entry : dynamicConfigs.entrySet()) {
            arguments.add("--" + entry.getKey());
            arguments.add(entry.getValue());
        }
        String[] newArgs = arguments.toArray(new String[0]);
        return cliFrontend.parseAndRun(newArgs);
    }

    private static String getFlussBootStrapServers(Configuration configuration) {
        Map<String, String> configMap = configuration.toMap();
        String bootstrapServers = configMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers != null) {
            return bootstrapServers;
        }
        // let's try to use coordinator server as bootstrap server
        String coordinatorHost = configuration.get(ConfigOptions.COORDINATOR_HOST);
        if (coordinatorHost == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The host for coordinator is not set, "
                                    + "can't get bootstrap server for Fluss. Please "
                                    + "specify bootstrap server %s or configuration %s for the host for coordinator.",
                            ConfigOptions.BOOTSTRAP_SERVERS.key(),
                            ConfigOptions.COORDINATOR_HOST.key()));
        }
        String coordinatorPort = configuration.get(ConfigOptions.COORDINATOR_PORT);
        return coordinatorHost + ":" + coordinatorPort;
    }
}
