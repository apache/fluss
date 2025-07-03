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

package com.alibaba.fluss.flink.tiering;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

import static com.alibaba.fluss.flink.tiering.source.TieringSourceOptions.DATA_LAKE_CONFIG_PREFIX;
import static com.alibaba.fluss.utils.PropertiesUtils.extractAndRemovePrefix;
import static org.apache.flink.runtime.executiongraph.failover.FailoverStrategyFactoryLoader.FULL_RESTART_STRATEGY_NAME;

/** The entrypoint for Flink to tier fluss data to lake format like paimon. */
public class FlussLakeTieringEntrypoint {

    private static final String FLUSS_CONF_PREFIX = "fluss.";

    public static void main(String[] args) throws Exception {

        // parse params
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
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
        flussConfigMap.put(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        String dataLake = paramsMap.get(ConfigOptions.DATALAKE_FORMAT.key());
        if (dataLake == null) {
            throw new IllegalArgumentException(
                    ConfigOptions.DATALAKE_FORMAT.key() + " is not configured");
        }

        // extract lake config
        Map<String, String> lakeConfigMap =
                extractAndRemovePrefix(
                        paramsMap, String.format("%s%s.", DATA_LAKE_CONFIG_PREFIX, dataLake));

        // now, we must use full restart strategy if any task is failed,
        // since committer is stateless, if tiering committer is failover, committer
        // will lost the collected committable, and will never collect all committable to do commit
        // todo: support region failover
        org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        flinkConfig.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, FULL_RESTART_STRATEGY_NAME);

        // build tiering source
        final StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        // build lake tiering job
        JobClient jobClient =
                LakeTieringJobBuilder.newBuilder(
                                execEnv,
                                Configuration.fromMap(flussConfigMap),
                                Configuration.fromMap(lakeConfigMap),
                                dataLake)
                        .build();

        System.out.printf(
                "Starting data tiering service from Fluss to %s, jobId is %s.....%n",
                dataLake, jobClient.getJobID());
    }
}
