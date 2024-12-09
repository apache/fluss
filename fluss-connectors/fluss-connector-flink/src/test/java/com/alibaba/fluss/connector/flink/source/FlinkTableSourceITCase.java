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

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.snapshot.BucketsSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.KvSnapshotInfo;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.RowType;

import org.apache.commons.lang3.RandomUtils;
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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for using flink sql to read fluss table. */
class FlinkTableSourceITCase extends FlinkTestBase {

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
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
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
    public void testCreateTableLike() throws Exception {
        tEnv.executeSql(
                        "CREATE TEMPORARY TABLE Orders (\n"
                                + "a int not null primary key not enforced, "
                                + "b varchar"
                                + ") WITH ( \n"
                                + "    'connector' = 'datagen',\n"
                                + "    'rows-per-second' = '10'"
                                + ");")
                .await();
        tEnv.executeSql("create table like_test LIKE Orders (EXCLUDING OPTIONS)").await();
        TablePath tablePath = TablePath.of(DEFAULT_DB, "like_test");

        List<InternalRow> rows =
                Arrays.asList(
                        compactedRow(DATA1_ROW_TYPE, new Object[] {1, "v1"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {2, "v2"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {3, "v3"}));

        // write records
        writeRows(tablePath, rows, false);

        waitUtilAllBucketFinishSnapshot(admin, tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        assertResultsIgnoreOrder(
                tEnv.executeSql("select * from like_test").collect(), expectedRows, true);
    }

    @Test
    void testPkTableReadOnlySnapshot() throws Exception {
        tEnv.executeSql(
                "create table read_snapshot_test (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "read_snapshot_test");

        List<InternalRow> rows =
                Arrays.asList(
                        compactedRow(DATA1_ROW_TYPE, new Object[] {1, "v1"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {2, "v2"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {3, "v3"}));

        // write records
        writeRows(tablePath, rows, false);

        waitUtilAllBucketFinishSnapshot(admin, tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        assertResultsIgnoreOrder(
                tEnv.executeSql("select * from read_snapshot_test").collect(), expectedRows, true);
    }

    @Test
    void testNonPkTableRead() throws Exception {
        tEnv.executeSql("create table non_pk_table_test (a int, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "non_pk_table_test");

        List<InternalRow> rows =
                Arrays.asList(
                        row(DATA1_ROW_TYPE, new Object[] {1, "v1"}),
                        row(DATA1_ROW_TYPE, new Object[] {2, "v2"}),
                        row(DATA1_ROW_TYPE, new Object[] {3, "v3"}));

        // write records
        writeRows(tablePath, rows, true);

        List<String> expected = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from non_pk_table_test").collect()) {
            int expectRecords = expected.size();
            List<String> actual = new ArrayList<>(expectRecords);
            for (int i = 0; i < expectRecords; i++) {
                String row = rowIter.next().toString();
                actual.add(row);
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ARROW", "INDEXED"})
    void testAppendTableProjectPushDown(String logFormat) throws Exception {
        String tableName = "append_table_project_push_down_" + logFormat;
        tEnv.executeSql(
                String.format(
                        "create table %s (a int, b varchar, c bigint, d int, e int, f bigint) with"
                                + " ('connector' = 'fluss', 'table.log.format' = '%s')",
                        tableName, logFormat));
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Table table = conn.getTable(tablePath);
        RowType dataType = table.getDescriptor().getSchema().toRowType();
        List<InternalRow> rows =
                Arrays.asList(
                        genRow(false, dataType, new Object[] {1, "v1", 100L, 1000, 100, 1000L}),
                        genRow(false, dataType, new Object[] {2, "v2", 200L, 2000, 200, 2000L}),
                        genRow(false, dataType, new Object[] {3, "v3", 300L, 3000, 300, 3000L}),
                        genRow(false, dataType, new Object[] {4, "v4", 400L, 4000, 400, 4000L}),
                        genRow(false, dataType, new Object[] {5, "v5", 500L, 5000, 500, 5000L}),
                        genRow(false, dataType, new Object[] {6, "v6", 600L, 6000, 600, 6000L}),
                        genRow(false, dataType, new Object[] {7, "v7", 700L, 7000, 700, 7000L}),
                        genRow(false, dataType, new Object[] {8, "v8", 800L, 8000, 800, 8000L}),
                        genRow(false, dataType, new Object[] {9, "v9", 900L, 9000, 900, 9000L}),
                        genRow(
                                false,
                                dataType,
                                new Object[] {10, "v10", 1000L, 10000, 1000, 10000L}));
        writeRows(tablePath, rows, true);

        // projection + reorder.
        String query = "select b, d, c from " + tableName;
        // make sure the plan has pushed down the projection into source
        assertThat(tEnv.explainSql(query))
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, "
                                + tableName
                                + ", project=[b, d, c]]], fields=[b, d, c])");

        List<String> expected =
                Arrays.asList(
                        "+I[v1, 1000, 100]",
                        "+I[v2, 2000, 200]",
                        "+I[v3, 3000, 300]",
                        "+I[v4, 4000, 400]",
                        "+I[v5, 5000, 500]",
                        "+I[v6, 6000, 600]",
                        "+I[v7, 7000, 700]",
                        "+I[v8, 8000, 800]",
                        "+I[v9, 9000, 900]",
                        "+I[v10, 10000, 1000]");
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(query).collect()) {
            int expectRecords = expected.size();
            List<String> actual = new ArrayList<>(expectRecords);
            for (int i = 0; i < expectRecords; i++) {
                Row r = rowIter.next();
                String row = r.toString();
                actual.add(row);
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"PK_SNAPSHOT", "PK_LOG", "LOG"})
    void testTableProjectPushDown(String mode) throws Exception {
        boolean isPkTable = mode.startsWith("PK");
        boolean testPkLog = mode.equals("PK_LOG");
        String tableName = "table_" + mode;
        String pkDDL = isPkTable ? ", primary key (a) not enforced" : "";
        tEnv.executeSql(
                String.format(
                        "create table %s (a int, b varchar, c bigint, d int %s) with ('connector' = 'fluss')",
                        tableName, pkDDL));
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Table table = conn.getTable(tablePath);

        RowType dataType = table.getDescriptor().getSchema().toRowType();
        List<InternalRow> rows =
                Arrays.asList(
                        genRow(isPkTable, dataType, new Object[] {1, "v1", 100L, 1000}),
                        genRow(isPkTable, dataType, new Object[] {2, "v2", 200L, 2000}),
                        genRow(isPkTable, dataType, new Object[] {3, "v3", 300L, 3000}),
                        genRow(isPkTable, dataType, new Object[] {4, "v4", 400L, 4000}),
                        genRow(isPkTable, dataType, new Object[] {5, "v5", 500L, 5000}),
                        genRow(isPkTable, dataType, new Object[] {6, "v6", 600L, 6000}),
                        genRow(isPkTable, dataType, new Object[] {7, "v7", 700L, 7000}),
                        genRow(isPkTable, dataType, new Object[] {8, "v8", 800L, 8000}),
                        genRow(isPkTable, dataType, new Object[] {9, "v9", 900L, 9000}),
                        genRow(isPkTable, dataType, new Object[] {10, "v10", 1000L, 10000}));

        if (isPkTable) {
            if (!testPkLog) {
                // write records and wait snapshot before collect job start,
                // to make sure reading from kv snapshot
                writeRows(tablePath, rows, false);
                waitUtilAllBucketFinishSnapshot(admin, TablePath.of(DEFAULT_DB, tableName));
            }
        } else {
            writeRows(tablePath, rows, true);
        }

        String query = "select b, a, c from " + tableName;
        // make sure the plan has pushed down the projection into source
        assertThat(tEnv.explainSql(query))
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, "
                                + tableName
                                + ", project=[b, a, c]]], fields=[b, a, c])");

        List<String> expected =
                Arrays.asList(
                        "+I[v1, 1, 100]",
                        "+I[v2, 2, 200]",
                        "+I[v3, 3, 300]",
                        "+I[v4, 4, 400]",
                        "+I[v5, 5, 500]",
                        "+I[v6, 6, 600]",
                        "+I[v7, 7, 700]",
                        "+I[v8, 8, 800]",
                        "+I[v9, 9, 900]",
                        "+I[v10, 10, 1000]");
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(query).collect()) {
            int expectRecords = expected.size();
            List<String> actual = new ArrayList<>(expectRecords);
            if (testPkLog) {
                // delay the write after collect job start,
                // to make sure reading from log instead of snapshot
                writeRows(tablePath, rows, false);
            }
            for (int i = 0; i < expectRecords; i++) {
                Row r = rowIter.next();
                String row = r.toString();
                actual.add(row);
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    @Test
    void testPkTableReadMixSnapshotAndLog() throws Exception {
        tEnv.executeSql(
                "create table mix_snapshot_log_test (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "mix_snapshot_log_test");

        List<InternalRow> rows =
                Arrays.asList(
                        compactedRow(DATA1_ROW_TYPE, new Object[] {1, "v1"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {2, "v2"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {3, "v3"}));

        // write records
        writeRows(tablePath, rows, false);

        waitUtilAllBucketFinishSnapshot(admin, tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from mix_snapshot_log_test").collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // now, we put rows to the table again, should read the log
        expectedRows =
                Arrays.asList(
                        "-U[1, v1]",
                        "+U[1, v1]",
                        "-U[2, v2]",
                        "+U[2, v2]",
                        "-U[3, v3]",
                        "+U[3, v3]");
        writeRows(tablePath, rows, false);
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    // -------------------------------------------------------------------------------------
    // Fluss scan start mode tests
    // -------------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadLogTableWithDifferentScanStartupMode(boolean isPartitioned) throws Exception {
        String tableName = "tab1_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String partitionName = null;
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        if (!isPartitioned) {
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int, b varchar, c bigint, d int ) "
                                    + "with ('connector' = 'fluss')",
                            tableName));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "a int, b varchar, c bigint, d int, p varchar"
                                    + ") partitioned by (p) "
                                    + "with ("
                                    + "'connector' = 'fluss',"
                                    + "'table.auto-partition.enabled' = 'true',"
                                    + "'table.auto-partition.time-unit' = 'year',"
                                    + "'table.auto-partition.num-precreate' = '1')",
                            tableName));
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);
            // just pick one partition
            partitionName = partitionNameById.values().iterator().next();
        }
        Table table = conn.getTable(tablePath);

        RowType dataType = table.getDescriptor().getSchema().toRowType();
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(dataType, rowValues(new Object[] {1, "v1", 100L, 1000}, partitionName)),
                        row(dataType, rowValues(new Object[] {2, "v2", 200L, 2000}, partitionName)),
                        row(dataType, rowValues(new Object[] {3, "v3", 300L, 3000}, partitionName)),
                        row(dataType, rowValues(new Object[] {4, "v4", 400L, 4000}, partitionName)),
                        row(
                                dataType,
                                rowValues(new Object[] {5, "v5", 500L, 5000}, partitionName)));

        writeRows(tablePath, rows1, true);

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(dataType, rowValues(new Object[] {6, "v6", 600L, 6000}, partitionName)),
                        row(dataType, rowValues(new Object[] {7, "v7", 700L, 7000}, partitionName)),
                        row(dataType, rowValues(new Object[] {8, "v8", 800L, 8000}, partitionName)),
                        row(dataType, rowValues(new Object[] {9, "v9", 900L, 9000}, partitionName)),
                        row(
                                dataType,
                                rowValues(new Object[] {10, "v10", 1000L, 10000}, partitionName)));
        // for second batch, we don't wait snapshot finish.
        writeRows(tablePath, rows2, true);

        // 1. read log table with scan.startup.mode='initial'
        String options = " /*+ OPTIONS('scan.startup.mode' = 'initial') */";
        String query = "select a, b, c, d from " + tableName + options;
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1, 100, 1000]",
                        "+I[2, v2, 200, 2000]",
                        "+I[3, v3, 300, 3000]",
                        "+I[4, v4, 400, 4000]",
                        "+I[5, v5, 500, 5000]",
                        "+I[6, v6, 600, 6000]",
                        "+I[7, v7, 700, 7000]",
                        "+I[8, v8, 800, 8000]",
                        "+I[9, v9, 900, 9000]",
                        "+I[10, v10, 1000, 10000]");
        assertQueryResult(query, expected);

        // 2. read kv table with scan.startup.mode='earliest'
        options = " /*+ OPTIONS('scan.startup.mode' = 'earliest') */";
        query = "select a, b, c, d from " + tableName + options;
        assertQueryResult(query, expected);

        // 3. read log table with scan.startup.mode='timestamp'
        options =
                String.format(
                        " /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' ='%d') */",
                        1000);
        query = "select a, b, c, d from " + tableName + options;
        assertQueryResult(query, expected);
    }

    @Test
    void testReadKvTableWithScanStartupModeEqualsInitial() throws Exception {
        tEnv.executeSql(
                "create table read_initial_test (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "read_initial_test");

        List<InternalRow> rows1 =
                Arrays.asList(
                        compactedRow(DATA1_ROW_TYPE, new Object[] {1, "v1"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {2, "v2"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {3, "v3"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {3, "v33"}));

        // write records and wait generate snapshot.
        writeRows(tablePath, rows1, false);
        waitUtilAllBucketFinishSnapshot(admin, tablePath);

        List<InternalRow> rows2 =
                Arrays.asList(
                        compactedRow(DATA1_ROW_TYPE, new Object[] {1, "v11"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {2, "v22"}),
                        compactedRow(DATA1_ROW_TYPE, new Object[] {4, "v4"}));

        String options = " /*+ OPTIONS('scan.startup.mode' = 'initial') */";
        String query = "select a, b from read_initial_test " + options;
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1]",
                        "+I[2, v2]",
                        "+I[3, v33]",
                        "-U[1, v1]",
                        "+U[1, v11]",
                        "-U[2, v2]",
                        "+U[2, v22]",
                        "+I[4, v4]");
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(query).collect()) {
            int expectRecords = 8;
            List<String> actual = new ArrayList<>(expectRecords);
            // delay to write after collect job start, to make sure reading from log instead of
            // snapshot
            writeRows(tablePath, rows2, false);
            for (int i = 0; i < expectRecords; i++) {
                Row r = rowIter.next();
                String row = r.toString();
                actual.add(row);
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    private static Stream<Arguments> readKvTableScanStartupModeArgs() {
        return Stream.of(
                Arguments.of("earliest", true),
                Arguments.of("earliest", false),
                Arguments.of("timestamp", true),
                Arguments.of("timestamp", false));
    }

    @ParameterizedTest
    @MethodSource("readKvTableScanStartupModeArgs")
    void testReadKvTableWithEarliestAndTimestampScanStartupMode(String mode, boolean isPartitioned)
            throws Exception {
        String tableName = mode + "_test_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String partitionName = null;
        if (!isPartitioned) {
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int not null primary key not enforced, b varchar)",
                            tableName));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int not null, b varchar, c varchar, primary key (a, c) NOT ENFORCED) partitioned by (c) "
                                    + "with ("
                                    + " 'table.auto-partition.enabled' = 'true',"
                                    + " 'table.auto-partition.time-unit' = 'year',"
                                    + " 'table.auto-partition.num-precreate' = '1')",
                            tableName));
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);
            // just pick one partition
            partitionName = partitionNameById.values().iterator().next();
        }

        RowType dataType = conn.getTable(tablePath).getDescriptor().getSchema().toRowType();

        List<InternalRow> rows1 =
                Arrays.asList(
                        compactedRow(dataType, rowValues(new Object[] {1, "v1"}, partitionName)),
                        compactedRow(dataType, rowValues(new Object[] {2, "v2"}, partitionName)),
                        compactedRow(dataType, rowValues(new Object[] {3, "v3"}, partitionName)),
                        compactedRow(dataType, rowValues(new Object[] {3, "v33"}, partitionName)));

        // write records and wait generate snapshot.
        writeRows(tablePath, rows1, false);
        if (partitionName == null) {
            waitUtilAllBucketFinishSnapshot(admin, tablePath);
        } else {
            waitUtilAllBucketFinishSnapshot(admin, tablePath, Collections.singleton(partitionName));
        }

        List<InternalRow> rows2 =
                Arrays.asList(
                        compactedRow(dataType, rowValues(new Object[] {1, "v11"}, partitionName)),
                        compactedRow(dataType, rowValues(new Object[] {2, "v22"}, partitionName)),
                        compactedRow(dataType, rowValues(new Object[] {4, "v4"}, partitionName)));
        writeRows(tablePath, rows2, false);

        String options =
                String.format(
                        " /*+ OPTIONS('scan.startup.mode' = '%s', 'scan.startup.timestamp' = '1000') */",
                        mode);
        String query = "select a, b from " + tableName + options;
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1]",
                        "+I[2, v2]",
                        "+I[3, v3]",
                        "-U[3, v3]",
                        "+U[3, v33]",
                        "-U[1, v1]",
                        "+U[1, v11]",
                        "-U[2, v2]",
                        "+U[2, v22]",
                        "+I[4, v4]");
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(query).collect()) {
            int expectRecords = 10;
            List<String> actual = new ArrayList<>(expectRecords);
            for (int i = 0; i < expectRecords; i++) {
                Row r = rowIter.next();
                String row = r.toString();
                actual.add(row);
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    @Test
    void testReadPrimaryKeyPartitionedTable() throws Exception {
        RowType rowType =
                com.alibaba.fluss.types.DataTypes.ROW(
                        new DataField("a", com.alibaba.fluss.types.DataTypes.INT()),
                        new DataField("b", com.alibaba.fluss.types.DataTypes.STRING()),
                        new DataField("c", com.alibaba.fluss.types.DataTypes.STRING()));
        tEnv.executeSql(
                "create table partitioned_table"
                        + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) "
                        + "with ('table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'year')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_table");

        // write data into partitions and wait snapshot is done
        Map<Long, String> partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
        List<String> expectedRowValues =
                writeRowsToPartition(tablePath, rowType, partitionNameById.values());
        waitUtilAllBucketFinishSnapshot(admin, tablePath, partitionNameById.values());

        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from partitioned_table").collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, false);

        // then create some new partitions, and write rows to the new partitions
        List<String> newPartitions = Arrays.asList("2000", "2001");
        FlinkTestBase.createPartitions(
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, newPartitions);
        // write data to the new partitions
        expectedRowValues = writeRowsToPartition(tablePath, rowType, newPartitions);
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    // -------------------------------------------------------------------------------------
    // Fluss look source tests
    // -------------------------------------------------------------------------------------

    private static Stream<Arguments> lookupArgs() {
        return Stream.of(
                Arguments.of(Caching.ENABLE_CACHE, false),
                Arguments.of(Caching.DISABLE_CACHE, false),
                Arguments.of(Caching.ENABLE_CACHE, true),
                Arguments.of(Caching.DISABLE_CACHE, true));
    }

    /** lookup table with one pk, one join condition. */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup1PkTable(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, c, h.name FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList("+I[1, 11, name1]", "+I[2, 2, name2]", "+I[3, 33, name3]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with one pk, two join condition and one of the join condition is constant value.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup1PkTableWith2Conditions(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.name FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id AND h.name = 'name3'",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Collections.singletonList("+I[3, name33, name3]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with one pk, 3 join condition on dim fields, 1st for variable non-pk, 2nd for
     * pk, 3rd for constant value.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup1PkTableWith3Conditions(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, c, h.address FROM src LEFT JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.a = h.id AND h.address= 'address2'",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, name1, 11, null]",
                        "+I[2, name2, 2, address2]",
                        "+I[3, name33, 33, null]",
                        "+I[10, name4, 44, null]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /** lookup table with two pk, join condition contains all the pks. */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTable(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(caching, async, new String[] {"id", "name"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.a = h.id",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with two pk, but the defined key are in reserved order. The result should
     * exactly the same with {@link #testLookup2PkTable(Caching, boolean)}.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTableWithUnorderedKey(Caching caching, boolean async) throws Exception {
        // the primary key is (name, id) but the schema order is (id, name)
        String dim =
                prepareDimTableAndSourceTable(caching, async, new String[] {"name", "id"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.a = h.id",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with two pk, only one key is in the join condition. The result should throw
     * exception.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTableWith1KeyInCondition(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(caching, async, new String[] {"id", "name"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id",
                        dim);
        assertThatThrownBy(() -> tEnv.executeSql(dimJoinQuery))
                .hasStackTraceContaining(
                        "Fluss lookup function only supports lookup table with "
                                + "lookup keys contain all primary keys. Can't find primary "
                                + "key 'name' in lookup keys [id]");
    }

    /**
     * lookup table with two pk, 3 join condition on dim fields, 1st for variable non-pk, 2nd for
     * pk, 3rd for constant value.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTableWith3Conditions(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(caching, async, new String[] {"id", "name"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, h.name, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON 'name2' = h.name AND src.a = h.id AND h.address= 'address' || CAST(src.c AS STRING)",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Collections.singletonList("+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookupPartitionedTable(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, "p_date");

        String dimJoinQuery =
                String.format(
                        "SELECT a, h.name, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id AND src.p_date = h.p_date",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testLookupFullCacheThrowException() {
        tEnv.executeSql(
                "create table lookup_join_throw_table"
                        + " (a int not null primary key not enforced, b varchar)"
                        + " with ('lookup.cache' = 'FULL')");
        // should throw exception
        assertThatThrownBy(() -> tEnv.executeSql("select * from lookup_join_throw_table"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Full lookup caching is not supported yet.");
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    private InternalRow genRow(boolean isPkTable, RowType rowType, Object[] objects) {
        if (isPkTable) {
            return compactedRow(rowType, objects);
        } else {
            return row(rowType, objects);
        }
    }

    /**
     * Creates dim table in Fluss and source table in Flink, and generates data for them.
     *
     * @return the table name of the dim table
     */
    private String prepareDimTableAndSourceTable(
            Caching caching, boolean async, String[] keys, @Nullable String partitionedKey)
            throws Exception {
        String options = async ? "'lookup.async' = 'true'" : "'lookup.async' = 'false'";
        if (caching == Caching.ENABLE_CACHE) {
            options +=
                    ",'lookup.cache' = 'PARTIAL'"
                            + ",'lookup.partial-cache.max-rows' = '1000'"
                            + ",'lookup.partial-cache.expire-after-write' = '10min'";
        }

        // create dim table
        String tableName =
                String.format(
                        "lookup_test_%s_%s_pk_%s_%s",
                        caching.name().toLowerCase(),
                        async ? "async" : "sync",
                        String.join("_", keys),
                        RandomUtils.nextInt());
        if (partitionedKey == null) {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "  id int not null,"
                                    + "  address varchar,"
                                    + "  name varchar,"
                                    + "  primary key (%s) NOT ENFORCED) with (%s)",
                            tableName, String.join(",", keys), options));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "  id int not null,"
                                    + "  address varchar,"
                                    + "  name varchar,"
                                    + "  %s varchar , "
                                    + "  primary key (%s, %s) NOT ENFORCED) partitioned by (%s) with (%s , "
                                    + "'table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'year')",
                            tableName,
                            partitionedKey,
                            String.join(",", keys),
                            partitionedKey,
                            partitionedKey,
                            options));
        }

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String partition1 = null;
        String partition2 = null;
        if (partitionedKey != null) {
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
            // just pick one partition to insert data
            Iterator<String> partitionIterator = partitionNameById.values().iterator();
            partition1 = partitionIterator.next();
            partition2 = partitionIterator.next();
        }

        // prepare dim table data
        try (Table dimTable = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = dimTable.getUpsertWriter();
            RowType dimTableRowType = dimTable.getDescriptor().getSchema().toRowType();
            for (int i = 1; i <= 5; i++) {
                Object[] values =
                        partition1 == null
                                ? new Object[] {i, "address" + i, "name" + i}
                                : new Object[] {i, "address" + i, "name" + i, partition1};
                upsertWriter.upsert(compactedRow(dimTableRowType, values));
            }
            upsertWriter.flush();
        }

        // prepare a source table
        List<Row> testData =
                partition1 == null
                        ? Arrays.asList(
                                Row.of(1, "name1", 11),
                                Row.of(2, "name2", 2),
                                Row.of(3, "name33", 33),
                                Row.of(10, "name4", 44))
                        : Arrays.asList(
                                Row.of(1, "name1", 11, partition1),
                                Row.of(2, "name2", 2, partition1),
                                Row.of(3, "name33", 33, partition2),
                                Row.of(10, "name4", 44, partition2));
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .columnByExpression("proc", "PROCTIME()");
        if (partitionedKey != null) {
            builder.column(partitionedKey, DataTypes.STRING());
        }
        Schema srcSchema = builder.build();
        RowTypeInfo srcTestTypeInfo =
                partitionedKey == null
                        ? new RowTypeInfo(
                                new TypeInformation[] {Types.INT, Types.STRING, Types.INT},
                                new String[] {"a", "b", "c"})
                        : new RowTypeInfo(
                                new TypeInformation[] {
                                    Types.INT, Types.STRING, Types.INT, Types.STRING
                                },
                                new String[] {"a", "b", "c", partitionedKey});
        DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(srcTestTypeInfo);
        tEnv.dropTemporaryView("src");
        tEnv.createTemporaryView("src", tEnv.fromDataStream(srcDs, srcSchema));

        return tableName;
    }

    private void waitUtilAllBucketFinishSnapshot(Admin admin, TablePath tablePath) {
        waitUtil(
                () -> {
                    KvSnapshotInfo kvSnapshotInfo = admin.getKvSnapshot(tablePath).get();
                    BucketsSnapshotInfo bucketsSnapshotInfo = kvSnapshotInfo.getBucketsSnapshots();
                    for (int bucketId : bucketsSnapshotInfo.getBucketIds()) {
                        if (!bucketsSnapshotInfo.getBucketSnapshotInfo(bucketId).isPresent()) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(1),
                "Fail to wait util all bucket finish snapshot");
    }

    private void waitUtilAllBucketFinishSnapshot(
            Admin admin, TablePath tablePath, Collection<String> partitions) {
        waitUtil(
                () -> {
                    for (String partition : partitions) {
                        BucketsSnapshotInfo bucketsSnapshotInfo =
                                admin.getPartitionSnapshot(tablePath, partition)
                                        .get()
                                        .getBucketsSnapshotInfo();
                        for (int bucketId : bucketsSnapshotInfo.getBucketIds()) {
                            if (!bucketsSnapshotInfo.getBucketSnapshotInfo(bucketId).isPresent()) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(1),
                "Fail to wait util all bucket finish snapshot");
    }

    private void assertQueryResult(String query, List<String> expected) throws Exception {
        try (org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(query).collect()) {
            int expectRecords = expected.size();
            List<String> actual = new ArrayList<>(expectRecords);
            for (int i = 0; i < expectRecords; i++) {
                Row r = rowIter.next();
                String row = r.toString();
                actual.add(row);
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    private Object[] rowValues(Object[] values, @Nullable String partition) {
        if (partition == null) {
            return values;
        } else {
            Object[] newValues = new Object[values.length + 1];
            System.arraycopy(values, 0, newValues, 0, values.length);
            newValues[values.length] = partition;
            return newValues;
        }
    }
}
