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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;

import java.util.List;

/**
 * Executor for prefix-lookup workload phases. Creates a prefix lookuper where {@code
 * key-prefix-length} determines how many PK columns to use as the lookup prefix. Uses async
 * pipelined lookups via {@link LookupExecutor#asyncLookupLoop}.
 */
public class PrefixLookupExecutor implements PhaseExecutor {

    @Override
    public void execute(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phaseConfig,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex)
            throws Exception {

        List<ColumnConfig> columns = tableConfig.columns();
        List<String> pkColumns = tableConfig.primaryKey();
        if (!tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Prefix lookup requires a primary key table");
        }

        int prefixLength =
                phaseConfig.keyPrefixLength() != null
                        ? phaseConfig.keyPrefixLength()
                        : pkColumns.size();
        if (prefixLength > pkColumns.size()) {
            throw new IllegalArgumentException(
                    "key-prefix-length ("
                            + prefixLength
                            + ") exceeds primary key column count ("
                            + pkColumns.size()
                            + ")");
        }

        List<String> prefixColumns = pkColumns.subList(0, prefixLength);
        int[] prefixIndexes = ExecutorUtils.resolvePkIndexes(prefixColumns, columns);
        LookupExecutor.LookupContext ctx =
                ExecutorUtils.buildLookupContext(
                        dataConfig,
                        phaseConfig,
                        columns,
                        prefixIndexes,
                        latencyRecorder,
                        throughputCounter,
                        startIndex,
                        endIndex);

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().lookupBy(prefixColumns).createLookuper();
            LookupExecutor.asyncLookupLoop(lookuper, ctx);
        }
    }
}
