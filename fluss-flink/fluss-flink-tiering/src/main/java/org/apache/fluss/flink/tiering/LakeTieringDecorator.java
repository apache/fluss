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

package org.apache.fluss.flink.tiering;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Interface for customizing Flink execution environment and configurations for lake tiering jobs.
 *
 * <p>Implementations of this interface can customize the Flink execution environment and
 * configurations as needed for specific deployment environments (e.g., injecting internal security
 * tokens, setting environment-specific configurations).
 *
 * @since 0.9
 */
@PublicEvolving
public interface LakeTieringDecorator {

    /**
     * Customizes the Flink execution environment and configurations for the lake tiering job.
     *
     * <p>This method is called before building the tiering job, allowing implementations to modify
     * the Flink execution environment or any of the provided configurations as needed.
     *
     * @param env the Flink StreamExecutionEnvironment to customize
     * @param flussConfig the Fluss configuration (may be modified)
     * @param dataLakeConfig the data lake configuration (may be modified)
     * @param lakeTieringConfig the lake tiering configuration (may be modified)
     * @param dataLakeFormat the data lake format identifier (e.g., "paimon", "iceberg")
     */
    void decorate(
            StreamExecutionEnvironment env,
            Configuration flussConfig,
            Configuration dataLakeConfig,
            Configuration lakeTieringConfig,
            String dataLakeFormat);
}
