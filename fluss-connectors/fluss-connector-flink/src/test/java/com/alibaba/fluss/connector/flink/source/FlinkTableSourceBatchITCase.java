/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for using flink sql to read fluss table. */
class FlinkTableSourceBatchITCase extends FlinkTestBase {

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = "defaultdb";
    static StreamExecutionEnvironment execEnv;
    static StreamTableEnvironment tEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkTestBase.beforeAll();

        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // create table environment
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inBatchMode());
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);

        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    }

    @BeforeEach
    void before() {
        // create database
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    void testScanSingleRowFilter() throws Exception {
        String tableName = prepareSourceTable(new String[] {"name", "id"}, null);
        String query = String.format("SELECT * FROM %s WHERE id = 1 AND name = 'name1'", tableName);

        assertThat(tEnv.explainSql(query))
                .contains(
                        String.format(
                                "TableSourceScan(table=[[testcatalog, defaultdb, %s, "
                                        + "filter=[and(=(id, 1), =(name, _UTF-16LE'name1':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))], "
                                        + "project=[address]]], fields=[address])",
                                tableName));
        CloseableIterator<Row> collected = tEnv.executeSql(query).collect();
        List<String> expected = Collections.singletonList("+I[1, address1, name1]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testScanSingleRowFilter2() throws Exception {
        String tableName = prepareSourceTable(new String[] {"id", "name"}, null);
        String query = String.format("SELECT * FROM %s WHERE id = 1 AND name = 'name1'", tableName);

        assertThat(tEnv.explainSql(query))
                .contains(
                        String.format(
                                "TableSourceScan(table=[[testcatalog, defaultdb, %s, "
                                        + "filter=[and(=(id, 1), =(name, _UTF-16LE'name1':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))], "
                                        + "project=[address]]], fields=[address])",
                                tableName));
        CloseableIterator<Row> collected = tEnv.executeSql(query).collect();
        List<String> expected = Collections.singletonList("+I[1, address1, name1]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testScanSingleRowFilter3() throws Exception {
        String tableName = prepareSourceTable(new String[] {"id"}, null);
        String query = String.format("SELECT id,name FROM %s WHERE id = 1", tableName);

        assertThat(tEnv.explainSql(query))
                .contains(
                        String.format(
                                "TableSourceScan(table=[[testcatalog, defaultdb, %s, "
                                        + "filter=[=(id, 1)], "
                                        + "project=[name]]], fields=[name])",
                                tableName));
        CloseableIterator<Row> collected = tEnv.executeSql(query).collect();
        List<String> expected = Collections.singletonList("+I[1, name1]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testScanSingleRowFilterOnPartitionedTable() throws Exception {
        String tableName = prepareSourceTable(new String[] {"id", "dt"}, "dt");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<Long, String> partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
        Iterator<String> partitionIterator =
                partitionNameById.values().stream().sorted().iterator();
        String partition1 = partitionIterator.next();
        String query =
                String.format("SELECT * FROM %s WHERE id = 1 AND dt='%s'", tableName, partition1);

        assertThat(tEnv.explainSql(query))
                .contains(
                        String.format(
                                "TableSourceScan(table=[[testcatalog, defaultdb, %s, "
                                        + "filter=[and(=(id, 1), =(dt, _UTF-16LE'%s':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))], "
                                        + "project=[address, name]]], fields=[address, name])\n",
                                tableName, partition1));

        CloseableIterator<Row> collected = tEnv.executeSql(query).collect();
        List<String> expected =
                Collections.singletonList(String.format("+I[1, address1, name1, %s]", partition1));
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testScanSingleRowFilterException() throws Exception {
        String tableName = prepareSourceTable(new String[] {"id", "name"}, null);
        String query = String.format("SELECT * FROM %s WHERE id = 1", tableName);

        // doesn't have all condition for primary key, doesn't support to execute
        assertThatThrownBy(() -> tEnv.explainSql(query))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Currently, Fluss only support queries on table with datalake enabled"
                                + " or point queries on primary key when it's in batch execution mode.");
    }

    @Test
    void testLakeTableQueryOnLakeDisabledTable() throws Exception {
        String tableName = prepareSourceTable(new String[] {"id", "name"}, null);
        assertThatThrownBy(() -> tEnv.executeSql(String.format("SELECT * FROM %s$lake", tableName)))
                .cause()
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        String.format(
                                "Table %s.%s is not datalake enabled.", DEFAULT_DB, tableName));
    }

    @Test
    void testLimitPrimaryTableScan() throws Exception {
        String tableName = prepareSourceTable(new String[] {"id"}, null);
        // normal scan
        String query = String.format("SELECT * FROM %s limit 2", tableName);
        CloseableIterator<Row> iterRows = tEnv.executeSql(query).collect();
        List<String> collected = assertAndCollectRecords(iterRows, 2);
        List<String> expected =
                Arrays.asList(
                        "+I[1, address1, name1]",
                        "+I[2, address2, name2]",
                        "+I[3, address3, name3]",
                        "+I[4, address4, name4]",
                        "+I[5, address5, name5]");
        assertThat(collected).isSubsetOf(expected);
        assertThat(collected).hasSize(2);

        // limit which is larger than all the data.
        query = String.format("SELECT * FROM %s limit 10", tableName);
        iterRows = tEnv.executeSql(query).collect();
        collected = assertAndCollectRecords(iterRows, 5);
        assertThat(collected).isSubsetOf(expected);
        assertThat(collected).hasSize(5);

        // projection scan
        query = String.format("SELECT id, name FROM %s limit 3", tableName);
        iterRows = tEnv.executeSql(query).collect();
        collected = assertAndCollectRecords(iterRows, 3);
        expected =
                Arrays.asList(
                        "+I[1, name1]",
                        "+I[2, name2]",
                        "+I[3, name3]",
                        "+I[4, name4]",
                        "+I[5, name5]");
        assertThat(collected).isSubsetOf(expected);
        assertThat(collected).hasSize(3);

        // limit out of bounds
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format(
                                                "SELECT id, name FROM %s limit 10000", tableName)))
                .hasMessageContaining("LIMIT statement doesn't support greater than 2048");
    }

    @Test
    void testLimitLogTableScan() throws Exception {
        String tableName = prepareLogTable();

        // normal scan
        String query = String.format("SELECT * FROM %s limit 2", tableName);
        CloseableIterator<Row> iterRows = tEnv.executeSql(query).collect();
        List<String> collected = assertAndCollectRecords(iterRows, 2);
        List<String> expected =
                Arrays.asList(
                        "+I[1, address1, name1]",
                        "+I[2, address2, name2]",
                        "+I[3, address3, name3]",
                        "+I[4, address4, name4]",
                        "+I[5, address5, name5]");
        assertThat(collected).isSubsetOf(expected);
        assertThat(collected).hasSize(2);

        // projection scan
        query = String.format("SELECT id, name FROM %s limit 3", tableName);
        iterRows = tEnv.executeSql(query).collect();
        collected = assertAndCollectRecords(iterRows, 3);
        expected =
                Arrays.asList(
                        "+I[1, name1]",
                        "+I[2, name2]",
                        "+I[3, name3]",
                        "+I[4, name4]",
                        "+I[5, name5]");
        assertThat(collected).isSubsetOf(expected);
        assertThat(collected).hasSize(3);

        // test partition table.
        String partitionTable = preparePartitionedLogTable();
        query = String.format("SELECT id, name FROM %s limit 3", partitionTable);
        iterRows = tEnv.executeSql(query).collect();
        collected = assertAndCollectRecords(iterRows, 3);
        assertThat(collected).isSubsetOf(expected);
        assertThat(collected).hasSize(3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCountPushDown(boolean partitionTable) throws Exception {
        String tableName = partitionTable ? preparePartitionedLogTable() : prepareLogTable();
        int expectedRows = partitionTable ? 20 : 5;
        // normal scan
        String query = String.format("SELECT COUNT(*) FROM %s", tableName);
        assertThat(tEnv.explainSql(query))
                .contains(
                        String.format(
                                "TableSourceScan(table=[[testcatalog, defaultdb, %s, project=[id], "
                                        + "aggregates=[grouping=[], aggFunctions=[Count1AggFunction()]]]], "
                                        + "fields=[count1$0])",
                                tableName));
        CloseableIterator<Row> iterRows = tEnv.executeSql(query).collect();
        List<String> collected = assertAndCollectRecords(iterRows, 1);
        List<String> expected = Collections.singletonList(String.format("+I[%s]", expectedRows));
        assertThat(collected).isEqualTo(expected);

        // test not push down grouping count.
        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                                String.format(
                                                        "SELECT COUNT(*) FROM %s group by id",
                                                        tableName))
                                        .wait())
                .hasMessageContaining(
                        "Currently, Fluss only support queries on table with datalake enabled or point queries on primary key when it's in batch execution mode.");

        // test not support primary key now
        String primaryTableName = prepareSourceTable(new String[] {"id"}, null);
        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                                String.format(
                                                        "SELECT COUNT(*) FROM %s ",
                                                        primaryTableName))
                                        .wait())
                .hasMessageContaining(
                        "Currently, Fluss only support queries on table with datalake enabled or point queries on primary key when it's in batch execution mode.");
    }

    private String prepareSourceTable(String[] keys, String partitionedKey) throws Exception {
        String tableName =
                String.format("test_%s_%s", String.join("_", keys), RandomUtils.nextInt());
        if (partitionedKey == null) {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "  id int not null,"
                                    + "  address varchar,"
                                    + "  name varchar,"
                                    + "  primary key (%s) NOT ENFORCED)"
                                    + " with ('bucket.num' = '4')",
                            tableName, String.join(",", keys)));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "  id int not null,"
                                    + "  address varchar,"
                                    + "  name varchar,"
                                    + "  dt varchar,"
                                    + "  primary key (%s) NOT ENFORCED) partitioned by (%s)"
                                    + " with ("
                                    + "  'bucket.num' = '4', "
                                    + "  'table.auto-partition.enabled' = 'true',"
                                    + "  'table.auto-partition.time-unit' = 'year')",
                            tableName, String.join(",", keys), partitionedKey));
        }

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String partition1 = null;
        if (partitionedKey != null) {
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
            // just pick first partition to insert data
            Iterator<String> partitionIterator =
                    partitionNameById.values().stream().sorted().iterator();
            partition1 = partitionIterator.next();
        }

        // prepare table data
        try (Table dimTable = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = dimTable.newUpsert().createWriter();
            for (int i = 1; i <= 5; i++) {
                Object[] values =
                        partition1 == null
                                ? new Object[] {i, "address" + i, "name" + i}
                                : new Object[] {i, "address" + i, "name" + i, partition1};
                upsertWriter.upsert(row(values));
            }
            upsertWriter.flush();
        }

        return tableName;
    }

    private String prepareLogTable() throws Exception {
        String tableName = String.format("test_log_table_%s", RandomUtils.nextInt());
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  name varchar)"
                                + " with ("
                                + "  'bucket.num' = '4', "
                                + "  'table.auto-partition.enabled' = 'false' "
                                + ")",
                        tableName));

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        // prepare table data
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 1; i <= 5; i++) {
                Object[] values = new Object[] {i, "address" + i, "name" + i};
                appendWriter.append(row(values));
                // make sure every bucket has records
                appendWriter.flush();
            }
        }

        return tableName;
    }

    private String preparePartitionedLogTable() throws Exception {
        String tableName = String.format("test_partitioned_log_table_%s", RandomUtils.nextInt());
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  name varchar,"
                                + "  dt varchar)"
                                + "  partitioned by (dt)"
                                + " with ("
                                + "  'bucket.num' = '4', "
                                + "  'table.auto-partition.enabled' = 'true',"
                                + "  'table.auto-partition.time-unit' = 'year')",
                        tableName));

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<Long, String> partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
        Collection<String> partitions = partitionNameById.values();

        // prepare table data
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 1; i <= 5; i++) {
                for (String partition : partitions) {
                    Object[] values = new Object[] {i, "address" + i, "name" + i, partition};
                    appendWriter.append(row(values));
                    // make sure every bucket has records
                    appendWriter.flush();
                }
            }
        }

        return tableName;
    }
}
