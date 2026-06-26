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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.values.TestingPaimonLakeStoragePlugin;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.utils.FlinkTestBase.waitUntilPartitions;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for hybrid Fluss lookup with lake fallback. */
abstract class HybridLakeLookupITCase extends AbstractTestBase {

    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());
    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = "defaultdb";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE)
                                    .set(ConfigOptions.DATALAKE_ENABLED, true)
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON))
                    .setNumOfTabletServers(3)
                    .setClock(CLOCK)
                    .build();

    private static Connection conn;
    private static Admin admin;
    private static Configuration clientConf;

    private StreamExecutionEnvironment execEnv;
    private StreamTableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void before() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
        TestingPaimonLakeStoragePlugin.clear();
    }

    @AfterEach
    void after() {
        TestingPaimonLakeStoragePlugin.clear();
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    void testFlinkSqlLookupFallbackToLakeForColdMiss() throws Exception {
        String tableName = "hybrid_lake_lookup_cold";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String coldPartition =
                ZonedDateTime.now(ZoneId.systemDefault())
                        .minus(Duration.ofDays(2))
                        .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHH"));

        createHybridLookupTable(tableName);
        TestingPaimonLakeStoragePlugin.setRows(
                tablePath, Collections.singletonList(row(1, coldPartition, "lake-name")));
        commitReadableLakeSnapshot(tablePath);
        createLookupSource(Collections.singletonList(Row.of(1, coldPartition)));

        CloseableIterator<Row> collected = tEnv.executeSql(lookupJoinSql(tableName)).collect();

        assertResultsIgnoreOrder(
                collected,
                Collections.singletonList("+I[1, " + coldPartition + ", lake-name]"),
                true);
        assertThat(TestingPaimonLakeStoragePlugin.plannedLookups(tablePath)).isEqualTo(1);
    }

    @Test
    void testFlinkSqlLookupDoesNotFallbackForHotMiss() throws Exception {
        String tableName = "hybrid_lake_lookup_hot";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String hotPartition = createHybridLookupTable(tableName);
        TestingPaimonLakeStoragePlugin.setRows(
                tablePath, Collections.singletonList(row(2, hotPartition, "lake-name")));
        commitReadableLakeSnapshot(tablePath);
        createLookupSource(Collections.singletonList(Row.of(2, hotPartition)));

        CloseableIterator<Row> collected = tEnv.executeSql(lookupJoinSql(tableName)).collect();

        assertResultsIgnoreOrder(
                collected, Collections.singletonList("+I[2, " + hotPartition + ", null]"), true);
        assertThat(TestingPaimonLakeStoragePlugin.plannedLookups(tablePath)).isZero();
    }

    private String createHybridLookupTable(String tableName) throws Exception {
        tEnv.executeSql(
                        String.format(
                                "create table %s ("
                                        + " id int not null,"
                                        + " pt varchar not null,"
                                        + " name varchar,"
                                        + " primary key (id, pt) NOT ENFORCED"
                                        + ") partitioned by (pt) with ("
                                        + " 'bucket.num' = '1',"
                                        + " 'bucket.key' = 'id',"
                                        + " 'lookup.async' = 'true',"
                                        + " 'lookup.lake-fallback.enabled' = 'true',"
                                        + " 'lookup.hot-window' = '12 h',"
                                        + " 'table.datalake.enabled' = 'true',"
                                        + " 'table.datalake.format' = 'paimon',"
                                        + " 'table.auto-partition.enabled' = 'true',"
                                        + " 'table.auto-partition.time-unit' = 'hour'"
                                        + ")",
                                tableName))
                .await();
        Map<Long, String> partitionNamesById =
                waitUntilPartitions(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                        TablePath.of(DEFAULT_DB, tableName),
                        1);
        long tableId = admin.getTableInfo(TablePath.of(DEFAULT_DB, tableName)).get().getTableId();
        for (Long partitionId : partitionNamesById.keySet()) {
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                    new TableBucket(tableId, partitionId, 0));
        }
        return partitionNamesById.values().iterator().next();
    }

    private void createLookupSource(Collection<Row> rows) {
        Schema sourceSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.STRING())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        RowTypeInfo sourceTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING}, new String[] {"id", "pt"});
        DataStream<Row> source = execEnv.fromCollection(rows).returns(sourceTypeInfo);
        tEnv.createTemporaryView("lookup_src", tEnv.fromDataStream(source, sourceSchema));
    }

    private String lookupJoinSql(String tableName) {
        return String.format(
                "SELECT src.id, src.pt, dim.name "
                        + "FROM lookup_src AS src "
                        + "LEFT JOIN %s FOR SYSTEM_TIME AS OF src.proc AS dim "
                        + "ON src.id = dim.id AND src.pt = dim.pt",
                tableName);
    }

    private void commitReadableLakeSnapshot(TablePath tablePath) throws Exception {
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        long maxTimestamp = System.currentTimeMillis();
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo requestForTable = request.addTablesReq();
        requestForTable.setTableId(tableId);
        requestForTable.setSnapshotId(1L);
        for (PartitionInfo partitionInfo : admin.listPartitionInfos(tablePath).get()) {
            PbLakeTableOffsetForBucket lakeTableOffsetForBucket = requestForTable.addBucketsReq();
            lakeTableOffsetForBucket.setPartitionId(partitionInfo.getPartitionId());
            lakeTableOffsetForBucket.setBucketId(0);
            lakeTableOffsetForBucket.setLogEndOffset(0L);
            lakeTableOffsetForBucket.setMaxTimestamp(maxTimestamp);
        }
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorGateway.commitLakeTableSnapshot(request).get();
    }
}
