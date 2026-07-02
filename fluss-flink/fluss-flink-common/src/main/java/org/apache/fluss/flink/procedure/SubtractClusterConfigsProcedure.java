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
 * Procedure to subtract (remove) values from list-type cluster configurations dynamically.
 *
 * <p>This procedure removes specific values from existing list-type configurations. The SUBTRACT
 * operation only works on configurations defined as list types (e.g., {@code
 * security.sasl.plain.users}). If the list becomes empty after subtraction, the configuration key
 * is removed entirely. The changes are:
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
 * -- Remove a user from the SASL user list
 * CALL sys.subtract_cluster_configs('security.sasl.plain.users', 'bob:bob-secret');
 *
 * -- Remove multiple key-value pairs at one time
 * CALL sys.subtract_cluster_configs('security.sasl.plain.users', 'bob:bob-secret', 'security.sasl.plain.users', 'alice:alice-secret');
 * </pre>
 *
 * <p><b>Note:</b> SUBTRACT operations are only supported for list-type configuration keys. The
 * server will reject the change if the configuration key is not a list type. Subtracting a value
 * that does not exist in the list is a no-op.
 */
public class SubtractClusterConfigsProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {@ArgumentHint(name = "config_pairs", type = @DataTypeHint("STRING"))},
            isVarArgs = true)
    public String[] call(ProcedureContext context, String... configPairs) throws Exception {
        try {
            if (configPairs.length == 0) {
                throw new IllegalArgumentException(
                        "config_pairs cannot be null or empty. "
                                + "Please specify valid configuration pairs.");
            }

            if (configPairs.length % 2 != 0) {
                throw new IllegalArgumentException(
                        "config_pairs must be set in pairs. "
                                + "Please specify valid configuration pairs.");
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

                AlterConfig alterConfig =
                        new AlterConfig(configKey, configValue, AlterConfigOpType.SUBTRACT);
                configList.add(alterConfig);
                resultMessage.add(
                        String.format(
                                "Successfully subtracted '%s' from configuration '%s'. ",
                                configValue, configKey));
            }

            admin.alterClusterConfigs(configList).get();

            return resultMessage.toArray(new String[0]);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to subtract cluster config: %s", e.getMessage()), e);
        }
    }
}
