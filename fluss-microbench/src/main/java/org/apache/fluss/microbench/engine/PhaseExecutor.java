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

package org.apache.fluss.microbench.engine;

import org.apache.fluss.client.Connection;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;

/**
 * Executes a single workload phase (write, lookup, prefix-lookup, scan, or mixed) against a Fluss
 * table.
 */
public interface PhaseExecutor {

    /**
     * Executes the workload phase.
     *
     * @param conn the Fluss connection
     * @param tableConfig table schema and storage configuration
     * @param dataConfig data generation configuration
     * @param phaseConfig workload phase parameters (records, duration, rate-limit, etc.)
     * @param latencyRecorder recorder for per-operation latency
     * @param throughputCounter counter for throughput tracking
     * @param startIndex start index for data generation (inclusive)
     * @param endIndex end index for data generation (exclusive)
     */
    void execute(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phaseConfig,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex)
            throws Exception;
}
