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

import java.util.ArrayList;
import java.util.List;

/** Base procedure for modifying collection-type cluster configurations. */
abstract class CollectionClusterConfigsProcedureBase extends ProcedureBase {

    protected String[] alterCollectionClusterConfigs(
            String[] configPairs,
            AlterConfigOpType opType,
            String successVerb,
            String successPreposition,
            String failureVerb)
            throws Exception {
        try {
            if (configPairs == null || configPairs.length == 0) {
                throw new IllegalArgumentException(
                        "config_pairs cannot be null or empty. "
                                + "Please specify valid configuration pairs.");
            }

            if (configPairs.length % 2 != 0) {
                throw new IllegalArgumentException(
                        "config_pairs must be set in pairs. "
                                + "Please specify valid configuration pairs.");
            }

            List<AlterConfig> alterConfigs = new ArrayList<>();
            List<String> resultMessages = new ArrayList<>();

            for (int i = 0; i < configPairs.length; i += 2) {
                String configKey = configPairs[i].trim();
                if (configKey.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Config key cannot be null or empty. "
                                    + "Please specify a valid configuration key.");
                }
                String configValue = configPairs[i + 1];

                alterConfigs.add(new AlterConfig(configKey, configValue, opType));
                resultMessages.add(
                        String.format(
                                "Successfully %s '%s' %s configuration '%s'. ",
                                successVerb, configValue, successPreposition, configKey));
            }

            admin.alterClusterConfigs(alterConfigs).get();

            return resultMessages.toArray(new String[0]);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to %s cluster config: %s", failureVerb, e.getMessage()),
                    e);
        }
    }
}
