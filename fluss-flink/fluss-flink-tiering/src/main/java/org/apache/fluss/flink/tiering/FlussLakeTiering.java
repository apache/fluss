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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.ServiceLoader;

import static org.apache.flink.runtime.executiongraph.failover.FailoverStrategyFactoryLoader.FULL_RESTART_STRATEGY_NAME;
import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.DATA_LAKE_CONFIG_PREFIX;
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;
import static org.apache.fluss.utils.PropertiesUtils.extractPrefix;

/**
 * The entrypoint logic for building and launching a Fluss-to-Lake (e.g., Paimon) data tiering job.
 *
 * <p>This class is responsible for parsing configuration parameters, initializing the Flink
 * execution environment, and coordinating the construction of the tiering pipeline.
 *
 * <p>Extensibility: Customization of Flink execution environment and configurations is supported
 * through the {@link LakeTieringDecoratorPlugin} SPI mechanism. Different environments (e.g.,
 * internal vs. public cloud) can provide their own decorator implementations.
 */
public class FlussLakeTiering {

    private static final String FLUSS_CONF_PREFIX = "fluss.";
    private static final String LAKE_TIERING_CONFIG_PREFIX = "lake.tiering.";

    protected final StreamExecutionEnvironment execEnv;
    protected final String dataLake;
    protected final Configuration flussConfig;
    protected final Configuration lakeConfig;
    protected final Configuration lakeTieringConfig;

    public FlussLakeTiering(String[] args) {
        // parse params
        final MultipleParameterToolAdapter params = MultipleParameterToolAdapter.fromArgs(args);
        Map<String, String> paramsMap = params.toMap();

        // extract fluss config
        Map<String, String> flussConfigMap = extractAndRemovePrefix(paramsMap, FLUSS_CONF_PREFIX);
        // we need to get bootstrap.servers
        String bootstrapServers = flussConfigMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The bootstrap server to fluss is not configured, please configure %s",
                            FLUSS_CONF_PREFIX + ConfigOptions.BOOTSTRAP_SERVERS.key()));
        }
        this.flussConfig = Configuration.fromMap(flussConfigMap);

        dataLake = paramsMap.get(ConfigOptions.DATALAKE_FORMAT.key());
        if (dataLake == null) {
            throw new IllegalArgumentException(
                    ConfigOptions.DATALAKE_FORMAT.key() + " is not configured");
        }

        // extract lake config
        Map<String, String> lakeConfigMap =
                extractAndRemovePrefix(
                        paramsMap, String.format("%s%s.", DATA_LAKE_CONFIG_PREFIX, dataLake));
        this.lakeConfig = Configuration.fromMap(lakeConfigMap);

        // extract tiering service config
        Map<String, String> lakeTieringConfigMap =
                extractPrefix(paramsMap, LAKE_TIERING_CONFIG_PREFIX);
        this.lakeTieringConfig = Configuration.fromMap(lakeTieringConfigMap);

        // now, we must use full restart strategy if any task is failed,
        // since committer is stateless, if tiering committer is failover, committer
        // will lost the collected committable, and will never collect all committable to do commit
        // todo: support region failover
        org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        flinkConfig.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, FULL_RESTART_STRATEGY_NAME);

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
    }

    protected void run() throws Exception {
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
}
