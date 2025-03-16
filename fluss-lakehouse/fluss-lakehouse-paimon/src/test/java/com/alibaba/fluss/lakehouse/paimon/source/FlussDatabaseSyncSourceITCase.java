/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.source;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.testutils.TestingDatabaseSyncSink;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for {@link FlussDatabaseSyncSource}. */
class FlussDatabaseSyncSourceITCase extends FlinkTestBase {

    private static final String CATALOG_NAME = "testcatalog";
    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;
    private static StreamTableEnvironment tEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);

        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // create table environment
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
    }

    @Test
    void testDatabaseSync() throws Exception {
        // first, write some records to a table
        TablePath t1 = TablePath.of(DEFAULT_DB, "sync_pktable");
        TableDescriptor table1Descriptor = createPkTableAndWriteRows(t1);

        // try to sink to another database
        String sinkDataBase = "fluss_sink";
        createDatabase(sinkDataBase);
        Filter<String> databaseFilter = (databaseName) -> databaseName.equals(DEFAULT_DB);
        createTable(TablePath.of(sinkDataBase, "sync_pktable"), table1Descriptor);
        FlussDatabaseSyncSource syncDatabaseFlussSource =
                FlussDatabaseSyncSource.newBuilder(FLUSS_CLUSTER_EXTENSION.getClientConfig())
                        .withDatabaseFilter(databaseFilter)
                        .build();

        DataStreamSource<MultiplexCdcRecord> input =
                execEnv.fromSource(
                        syncDatabaseFlussSource,
                        WatermarkStrategy.noWatermarks(),
                        "flinkSyncDatabaseSource");
        input.sinkTo(new TestingDatabaseSyncSink(sinkDataBase, clientConf));

        JobClient jobClient = execEnv.executeAsync();
        // check the records are synced to target database
        verifyRecordsSynced(
                sinkDataBase, "sync_pktable", Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]"));

        // now, create another table
        TablePath t2 = TablePath.of(DEFAULT_DB, "logtable");
        TableDescriptor table2Descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .build();
        // create the table in sink database
        createTable(TablePath.of(sinkDataBase, "logtable"), table2Descriptor);
        // create in source database
        createTable(t2, table2Descriptor);
        List<InternalRow> rows = Arrays.asList(row(11, "v1"), row(12, "v2"), row(13, "v3"));
        // write records
        writeRows(t2, rows, true);
        // check the records are synced to target database
        verifyRecordsSynced(
                sinkDataBase, "logtable", Arrays.asList("+I[11, v1]", "+I[12, v2]", "+I[13, v3]"));

        // test sync with partitioned tables
        // first create the table in sink database
        TablePath sinkPartitionedTable = TablePath.of(sinkDataBase, "sync_partitioned_pktable");
        createPartitionedTable(sinkPartitionedTable);

        // then create in source database
        TablePath sourcePartitionedTablePath = TablePath.of(DEFAULT_DB, "sync_partitioned_pktable");
        Map<Long, String> partitionNameByIds = createPartitionedTable(sourcePartitionedTablePath);
        List<String> expected =
                writeRowsIntoPartitionedTable(sourcePartitionedTablePath, partitionNameByIds);

        // check the records are synced to target database
        verifyRecordsSyncedIgnoreOrder(
                sinkDataBase, sourcePartitionedTablePath.getTableName(), expected);
        jobClient.cancel().get();
    }

    private Map<Long, String> createPartitionedTable(TablePath tablePath) throws Exception {
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        createTable(tablePath, DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR);
        return waitUntilPartitions(zooKeeperClient, tablePath);
    }

    private List<String> writeRowsIntoPartitionedTable(
            TablePath tablePath, Map<Long, String> partitionNameByIds) throws Exception {
        List<String> expected = new ArrayList<>();
        List<InternalRow> rows = new ArrayList<>();
        for (String partitionName : partitionNameByIds.values()) {
            rows.addAll(
                    Arrays.asList(
                            row(11, "v1", partitionName),
                            row(12, "v2", partitionName),
                            row(13, "v3", partitionName)));
            expected.addAll(
                    Arrays.asList(
                            "+I[11, v1, " + partitionName + "]",
                            "+I[12, v2, " + partitionName + "]",
                            "+I[13, v3, " + partitionName + "]"));
        }

        writeRows(tablePath, rows, false);
        return expected;
    }

    @Test
    void testDatabaseSyncWithFilter() throws Exception {
        // create a table in default db
        TablePath t1 = TablePath.of(DEFAULT_DB, "pktable");
        TableDescriptor tableDescriptor = createPkTableAndWriteRows(t1);

        // then create two tables in another database
        String sourceSyncDb = "fluss_sync_source";
        createDatabase(sourceSyncDb);

        // create pktable1
        TablePath t2 = TablePath.of(sourceSyncDb, "pktable1");
        createPkTableAndWriteRows(t2);

        // create pktable2
        String tableToSync = "pktable2";
        TablePath t3 = TablePath.of(sourceSyncDb, tableToSync);
        createPkTableAndWriteRows(t3);

        Filter<String> databaseFilter =
                (databaseName) -> Objects.equals(databaseName, sourceSyncDb);

        Filter<TableInfo> tableFilter =
                (tableInfo) -> tableInfo.getTablePath().getTableName().equals(tableToSync);

        // only create pktable2 in sink db
        String sinkDataBase = "fluss_sink";
        createDatabase(sinkDataBase);
        createTable(TablePath.of(sinkDataBase, tableToSync), tableDescriptor);

        FlussDatabaseSyncSource syncDatabaseFlussSource =
                FlussDatabaseSyncSource.newBuilder(FLUSS_CLUSTER_EXTENSION.getClientConfig())
                        .withDatabaseFilter(databaseFilter)
                        .withTableFilter(tableFilter)
                        .build();

        DataStreamSource<MultiplexCdcRecord> input =
                execEnv.fromSource(
                        syncDatabaseFlussSource,
                        WatermarkStrategy.noWatermarks(),
                        "flinkSyncDatabaseSource");
        input.sinkTo(new TestingDatabaseSyncSink(sinkDataBase, clientConf));

        // execute the sync job
        JobClient jobClient = execEnv.executeAsync();
        // check the records are synced to target database,
        // only pktable2 is synced, other tables should be ignore. If not, the test will throw
        // table not exists exception since we only create pktable2 in sink db
        verifyRecordsSynced(
                sinkDataBase, tableToSync, Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]"));

        jobClient.cancel().get();
    }

    private TableDescriptor createPkTableAndWriteRows(TablePath tablePath) throws Exception {
        TableDescriptor table1Descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .primaryKey("a")
                                        .build())
                        .build();
        createTable(tablePath, table1Descriptor);
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        // write records
        writeRows(tablePath, rows, false);
        return table1Descriptor;
    }

    private void verifyRecordsSynced(String sinkDatabase, String tableName, List<String> expected)
            throws Exception {
        verifyRecordsSynced(sinkDatabase, tableName, expected, false);
    }

    private void verifyRecordsSyncedIgnoreOrder(
            String sinkDatabase, String tableName, List<String> expected) throws Exception {
        verifyRecordsSynced(sinkDatabase, tableName, expected, true);
    }

    private void verifyRecordsSynced(
            String sinkDatabase, String tableName, List<String> expected, boolean ignoreOrder)
            throws Exception {
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(String.format("select * from `%s`.`%s`", sinkDatabase, tableName))
                        .collect()) {
            int expectRecords = expected.size();
            List<String> actual = new ArrayList<>(expectRecords);
            for (int i = 0; i < expectRecords; i++) {
                String row = rowIter.next().toString();
                actual.add(row);
            }
            if (ignoreOrder) {
                assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
            } else {
                assertThat(actual).containsExactlyElementsOf(expected);
            }
        }
    }

    protected void createDatabase(String database) throws Exception {
        admin.createDatabase(database, DatabaseDescriptor.EMPTY, true).get();
    }
}
