/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

import static com.alibaba.fluss.flink.tiering.source.TieringSourceOptions.DATA_LAKE_CONFIG_PREFIX;
import static com.alibaba.fluss.utils.PropertiesUtils.extractAndRemovePrefix;

/** The entrypoint for Flink to tiering fluss data to Paimon. */
public class FlussLakeTieringEntrypoint {

    private static final String FLUSS_CONF_PREFIX = "fluss.";

    public static void main(String[] args) throws Exception {

        // parse params
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        Map<String, String> paramsMap = params.toMap();

        // extract fluss config
        Map<String, String> flussConfigMap = extractAndRemovePrefix(paramsMap, FLUSS_CONF_PREFIX);
        // we need to get bootstrap.servers
        String bootstrapServers = paramsMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrap.servers is not configured");
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

        // build tiering source
        final StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // build lake tiering job
        LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        Configuration.fromMap(flussConfigMap),
                        Configuration.fromMap(lakeConfigMap),
                        dataLake)
                .build();

        JobClient jobClient = execEnv.executeAsync();

        System.out.printf(
                "Starting data tiering service from Fluss to %s, jobId is %s.....%n",
                dataLake, jobClient.getJobID());
    }
}
