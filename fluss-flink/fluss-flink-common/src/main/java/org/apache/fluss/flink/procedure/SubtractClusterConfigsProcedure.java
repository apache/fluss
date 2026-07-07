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

import org.apache.fluss.config.cluster.AlterConfigOpType;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to subtract (remove) values from collection-type cluster configurations dynamically.
 *
 * <p>This procedure removes specific values from existing list-type or map-type configurations. The
 * SUBTRACT operation only works on collection configurations (e.g., {@code
 * security.sasl.plain.credentials}). If the collection becomes empty after subtraction, the
 * configuration key is removed entirely. The changes are:
 *
 * <ul>
 *   <li>Validated by the CoordinatorServer before persistence
 *   <li>Persisted in ZooKeeper for durability
 *   <li>Applied to all relevant servers (Coordinator and TabletServers)
 *   <li>Survives server restarts
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Remove a user from the SASL credentials map
 * CALL sys.subtract_cluster_configs('security.sasl.plain.credentials', 'bob:bob-secret');
 *
 * -- Remove multiple key-value pairs at one time
 * CALL sys.subtract_cluster_configs(
 *     'security.sasl.plain.credentials',
 *     'bob:bob-secret',
 *     'security.sasl.plain.credentials',
 *     'alice:alice-secret');
 * </pre>
 *
 * <p><b>Note:</b> SUBTRACT operations are only supported for list-type or map-type configuration
 * keys. The server will reject the change if the configuration key is not a collection type.
 * Subtracting a value that does not exist in the collection is a no-op.
 */
public class SubtractClusterConfigsProcedure extends CollectionClusterConfigsProcedureBase {

    @ProcedureHint(
            argument = {@ArgumentHint(name = "config_pairs", type = @DataTypeHint("STRING"))},
            isVarArgs = true)
    public String[] call(ProcedureContext context, String... configPairs) throws Exception {
        return alterCollectionClusterConfigs(
                configPairs, AlterConfigOpType.SUBTRACT, "subtracted", "from", "subtract");
    }
}
