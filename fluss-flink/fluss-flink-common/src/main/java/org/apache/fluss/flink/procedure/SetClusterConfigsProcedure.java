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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Procedure to set or delete cluster configuration dynamically.
 *
 * <p>This procedure allows modifying dynamic cluster configurations. The changes are:
 *
 * <ul>
 *   <li>Validated by the CoordinatorServer before persistence
 *   <li>Persisted in ZooKeeper for durability
 *   <li>Applied to all relevant servers (Coordinator and TabletServers)
 *   <li>Survive server restarts
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Set a configuration
 * CALL sys.set_cluster_configs('kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB');
 * CALL sys.set_cluster_configs('datalake.format', 'paimon');
 *
 * -- Set multiple configurations at one time
 * CALL sys.set_cluster_configs('kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB','datalake.format', 'paimon');
 * </pre>
 *
 * <p><b>Note:</b> Not all configurations support dynamic changes. The server will validate the
 * change and reject it if the configuration cannot be modified dynamically or if the new value is
 * invalid.
 */
public class SetClusterConfigsProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {@ArgumentHint(name = "config_pairs", type = @DataTypeHint("STRING"))},
            isVarArgs = true)
    public String[] call(ProcedureContext context, String... configPairs) throws Exception {
        try {
            // Validate config key
            if (configPairs.length == 0) {
                throw new IllegalArgumentException(
                        "config_pairs cannot be null or empty. "
                                + "Please specify a valid configuration pairs.");
            }

            if (configPairs.length % 2 != 0) {
                throw new IllegalArgumentException(
                        "config_pairs must be set in pairs. "
                                + "Please specify a valid configuration pairs.");
            }
            List<AlterConfig> configList = new ArrayList<>();
            List<String> resultMessage = new ArrayList<>();

            for (int i = 0; i < configPairs.length; i += 2) {
                String configKey = configPairs[i].trim();
                if (configKey.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Config key cannot be null or empty. "
                                    + "Please specify a valid configuration key.");
                }
                String configValue = configPairs[i + 1];

                String operationDesc = String.format("set to '%s'", configValue);

                // Construct configuration modification operation.
                AlterConfig alterConfig =
                        new AlterConfig(configKey, configValue, AlterConfigOpType.SET);
                configList.add(alterConfig);
                resultMessage.add(
                        String.format(
                                "Successfully %s configuration '%s'. ", operationDesc, configKey));
            }

            // Call Admin API to modify cluster configuration
            // This will trigger validation on CoordinatorServer before persistence
            admin.alterClusterConfigs(configList).get();

            return resultMessage.toArray(new String[0]);
        } catch (IllegalArgumentException e) {
            // Re-throw validation errors with original message
            throw e;
        } catch (Exception e) {
            // Wrap other exceptions with more context
            throw new RuntimeException(
                    String.format("Failed to set cluster config: %s", e.getMessage()), e);
        }
    }
}
