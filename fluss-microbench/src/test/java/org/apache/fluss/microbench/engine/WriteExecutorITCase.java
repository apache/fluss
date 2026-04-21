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
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link WriteExecutor}. */
class WriteExecutorITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initClusterConf())
                    .build();

    private static final String DATABASE = "fluss";
    private static final String TABLE_NAME = "perf_write_test";
    private static final TablePath TABLE_PATH = TablePath.of(DATABASE, TABLE_NAME);
    private static final String PK_TABLE_NAME = "perf_write_pk_test";
    private static final TablePath PK_TABLE_PATH = TablePath.of(DATABASE, PK_TABLE_NAME);

    private static Connection conn;

    @BeforeAll
    static void beforeAll() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);

        // Create a log table (no PK) to test AppendWriter path
        Admin admin = conn.getAdmin();
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();

        admin.createTable(TABLE_PATH, tableDescriptor, true).get();

        long tableId = admin.getTableInfo(TABLE_PATH).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        // Create a PK table to test UpsertWriter path
        Schema pkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor pkTableDescriptor =
                TableDescriptor.builder().schema(pkSchema).distributedBy(1).build();

        admin.createTable(PK_TABLE_PATH, pkTableDescriptor, true).get();

        long pkTableId = admin.getTableInfo(PK_TABLE_PATH).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(pkTableId);

        admin.close();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void writeExecutorWithLogTable() throws Exception {
        // Build perf config objects
        TableConfig tableConfig = buildTableConfig(TABLE_NAME, null);
        DataConfig dataConfig = buildDataConfig();

        WorkloadPhaseConfig phaseConfig = new WorkloadPhaseConfig();
        phaseConfig.setPhase("write");
        phaseConfig.setRecords(1000L);

        LatencyRecorder latencyRecorder = new LatencyRecorder();
        ThroughputCounter throughputCounter = new ThroughputCounter();

        WriteExecutor executor = new WriteExecutor();
        executor.execute(
                conn,
                tableConfig,
                dataConfig,
                phaseConfig,
                latencyRecorder,
                throughputCounter,
                0,
                1000);

        assertThat(latencyRecorder.count()).isEqualTo(1000);
        assertThat(throughputCounter.totalOps()).isEqualTo(1000);
    }

    @Test
    void writeExecutorWithPkTable() throws Exception {
        TableConfig tableConfig = buildTableConfig(PK_TABLE_NAME, Collections.singletonList("id"));
        DataConfig dataConfig = buildDataConfig();

        WorkloadPhaseConfig phaseConfig = new WorkloadPhaseConfig();
        phaseConfig.setPhase("write");
        phaseConfig.setRecords(1000L);

        LatencyRecorder latencyRecorder = new LatencyRecorder();
        ThroughputCounter throughputCounter = new ThroughputCounter();

        WriteExecutor executor = new WriteExecutor();
        executor.execute(
                conn,
                tableConfig,
                dataConfig,
                phaseConfig,
                latencyRecorder,
                throughputCounter,
                0,
                1000);

        assertThat(latencyRecorder.count()).isEqualTo(1000);
        assertThat(throughputCounter.totalOps()).isEqualTo(1000);
    }

    private static TableConfig buildTableConfig(String tableName, List<String> primaryKey) {
        TableConfig tableConfig = new TableConfig();
        tableConfig.setName(tableName);

        ColumnConfig idCol = new ColumnConfig();
        idCol.setName("id");
        idCol.setType("INT");

        ColumnConfig nameCol = new ColumnConfig();
        nameCol.setName("name");
        nameCol.setType("STRING");

        ColumnConfig valueCol = new ColumnConfig();
        valueCol.setName("value");
        valueCol.setType("BIGINT");

        tableConfig.setColumns(Arrays.asList(idCol, nameCol, valueCol));
        tableConfig.setBuckets(1);
        if (primaryKey != null) {
            tableConfig.setPrimaryKey(primaryKey);
        }
        return tableConfig;
    }

    private static DataConfig buildDataConfig() {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setSeed(42L);
        Map<String, Map<String, Object>> generators = new HashMap<>();

        Map<String, Object> idGen = new HashMap<>();
        idGen.put("type", "random-int");
        generators.put("id", idGen);

        Map<String, Object> nameGen = new HashMap<>();
        nameGen.put("type", "random-string");
        generators.put("name", nameGen);

        Map<String, Object> valueGen = new HashMap<>();
        valueGen.put("type", "random-long");
        generators.put("value", valueGen);

        dataConfig.setGenerators(generators);
        return dataConfig;
    }

    private static Configuration initClusterConf() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }
}
