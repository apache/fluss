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
 * Procedure to reset cluster configurations to their default values.
 *
 * <p>This procedure reverts the configurations to their initial system defaults. The changes are:
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
 * -- reset a configuration
 * CALL sys.reset_cluster_configs('kv.rocksdb.shared-rate-limiter.bytes-per-sec');
 *
 * -- reset multiple configurations at one time
 * CALL sys.reset_cluster_configs('kv.rocksdb.shared-rate-limiter.bytes-per-sec', 'datalake.format');
 *
 * </pre>
 *
 * <p><b>Note:</b> In theory, an operation like <b>Reset to default value</b> should always succeed,
 * as the default value should be a valid one
 */
public class ResetClusterConfigsProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {@ArgumentHint(name = "config_keys", type = @DataTypeHint("STRING"))},
            isVarArgs = true)
    public String[] call(ProcedureContext context, String... configKeys) throws Exception {
        try {
            // Validate config key
            if (configKeys.length == 0) {
                throw new IllegalArgumentException(
                        "config_keys cannot be null or empty. "
                                + "Please specify valid configuration keys.");
            }

            List<AlterConfig> configList = new ArrayList<>();
            List<String> resultMessage = new ArrayList<>();

            for (String key : configKeys) {
                String configKey = key.trim();
                if (configKey.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Config key cannot be null or empty. "
                                    + "Please specify a valid configuration key.");
                }

                String operationDesc = "deleted (reset to default)";

                AlterConfig alterConfig =
                        new AlterConfig(configKey, null, AlterConfigOpType.DELETE);
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
                    String.format("Failed to reset cluster config: %s", e.getMessage()), e);
        }
    }
}
