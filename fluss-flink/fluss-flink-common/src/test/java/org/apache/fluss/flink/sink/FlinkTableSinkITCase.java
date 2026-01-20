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

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertQueryResultExactOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.waitUntilPartitions;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkTableSink} inherit AbstractTestBase for resource cleanup. */
abstract class FlinkTableSinkITCase extends AbstractTestBase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "defaultdb";

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected TableEnvironment tBatchEnv;

    static Stream<Arguments> writePartitionedTableParams() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(true, true));
    }

    @BeforeEach
    void before() throws Exception {
        // open a catalog so that we can get table from the catalog
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        // create table environment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        tEnv = StreamTableEnvironment.create(env);
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        // create batch table environment
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        // create database
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
        tBatchEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() throws Exception {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAppendLog(boolean compressed) throws Exception {
        String compressedProperties =
                compressed
                        ? ",'table.log.format' = 'arrow', 'table.log.arrow.compression.type' = 'zstd'"
                        : "";
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3'"
                        + compressedProperties
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO sink_test(a, b, c) "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco'), "
                                + "(4, 3504, 'jerry'), "
                                + "(5, 3505, 'piggy'), "
                                + "(6, 3506, 'stave')")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                        "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                        "+I[5, 3505, piggy]", "+I[6, 3506, stave]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAppendLogDuringAddColumn(boolean compressed) throws Exception {
        String compressedProperties =
                compressed
                        ? ",'table.log.format' = 'arrow', 'table.log.arrow.compression.type' = 'zstd'"
                        : "";
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3'"
                        + compressedProperties
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO sink_test "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco') ")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        // add new column
        tEnv.executeSql("alter table sink_test add add_column int").await();
        tEnv.executeSql(
                        "INSERT INTO sink_test "
                                + "VALUES (4, 3504, 'jerry', 4), "
                                + "(5, 3505, 'piggy', 5), "
                                + "(6, 3506, 'stave', 6)")
                .await();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                        "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                        "+I[5, 3505, piggy]", "+I[6, 3506, stave]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);

        // read with new schema.
        rowIter = tEnv.executeSql("select * from sink_test").collect();
        expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim, null]", "+I[2, 3502, Fabian, null]",
                        "+I[3, 3503, coco, null]", "+I[4, 3504, jerry, 4]",
                        "+I[5, 3505, piggy, 5]", "+I[6, 3506, stave, 6]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @ParameterizedTest
    @EnumSource(value = DistributionMode.class)
    void testAppendLogWithBucketKey(DistributionMode distributionMode) throws Exception {
        tEnv.executeSql(
                String.format(
                        "create table sink_test (a int not null, b bigint, c string) "
                                + "with ('bucket.num' = '3', 'bucket.key' = 'c', 'sink.distribution-mode'= '%s')",
                        distributionMode));
        String insertSql =
                "INSERT INTO sink_test(a, b, c) "
                        + "VALUES (1, 3501, 'Tim'), "
                        + "(2, 3502, 'Fabian'), "
                        + "(3, 3503, 'Tim'), "
                        + "(4, 3504, 'jerry'), "
                        + "(5, 3505, 'piggy'), "
                        + "(7, 3507, 'Fabian'), "
                        + "(8, 3508, 'stave'), "
                        + "(9, 3509, 'Tim'), "
                        + "(10, 3510, 'coco'), "
                        + "(11, 3511, 'stave'), "
                        + "(12, 3512, 'Tim')";

        if (distributionMode == DistributionMode.PARTITION_DYNAMIC) {
            assertThatThrownBy(() -> tEnv.executeSql(insertSql))
                    .hasMessageContaining(
                            "PARTITION_DYNAMIC is only supported for partitioned tables");
            return;
        }
        String insertPlan = tEnv.explainSql(insertSql, ExplainDetail.JSON_EXECUTION_PLAN);
        if (distributionMode == DistributionMode.BUCKET) {
            assertThat(insertPlan).contains("\"ship_strategy\" : \"BUCKET\"");
        } else {
            assertThat(insertPlan).contains("\"ship_strategy\" : \"FORWARD\"");
        }
        // there shouldn't have REBALANCE shuffle strategy, this asserts operator parallelism
        assertThat(insertPlan).doesNotContain("\"ship_strategy\" : \"REBALANCE\"");
        tEnv.executeSql(insertSql).await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        //noinspection ArraysAsListWithZeroOrOneArgument
        List<List<String>> expectedGroups =
                Arrays.asList(
                        Arrays.asList(
                                "+I[1, 3501, Tim]",
                                "+I[3, 3503, Tim]",
                                "+I[9, 3509, Tim]",
                                "+I[12, 3512, Tim]"),
                        Arrays.asList("+I[2, 3502, Fabian]", "+I[7, 3507, Fabian]"),
                        Arrays.asList("+I[4, 3504, jerry]"),
                        Arrays.asList("+I[5, 3505, piggy]"),
                        Arrays.asList("+I[8, 3508, stave]", "+I[11, 3511, stave]"),
                        Arrays.asList("+I[10, 3510, coco]"));

        List<String> expectedRows =
                expectedGroups.stream().flatMap(List::stream).collect(Collectors.toList());

        List<String> actual = collectRowsWithTimeout(rowIter, expectedRows.size());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);

        // check data with the same bucket key should be read in sequence.
        if (distributionMode == DistributionMode.BUCKET) {
            for (List<String> expected : expectedGroups) {
                if (expected.size() <= 1) {
                    continue;
                }
                int prevIndex = actual.indexOf(expected.get(0));
                for (int i = 1; i < expected.size(); i++) {
                    int index = actual.indexOf(expected.get(i));
                    assertThat(index).isGreaterThan(prevIndex);
                    prevIndex = index;
                }
            }
        }
    }

    @Test
    void testAppendLogWithRoundRobin() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3', 'client.writer.bucket.no-key-assigner' = 'round_robin')");
        tEnv.executeSql(
                        "INSERT INTO sink_test(a, b, c) "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco'), "
                                + "(4, 3504, 'jerry'), "
                                + "(5, 3505, 'piggy'), "
                                + "(6, 3506, 'stave')")
                .await();

        Map<Integer, List<String>> rows = new HashMap<>();
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Table table = conn.getTable(TablePath.of(DEFAULT_DB, "sink_test"));
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.subscribeFromBeginning(0);
            logScanner.subscribeFromBeginning(1);
            logScanner.subscribeFromBeginning(2);
            long scanned = 0;
            while (scanned < 6) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : scanRecords.buckets()) {
                    List<String> rowsBucket =
                            rows.computeIfAbsent(bucket.getBucket(), k -> new ArrayList<>());
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        InternalRow row = record.getRow();
                        rowsBucket.add(
                                Row.of(row.getInt(0), row.getLong(1), row.getString(2).toString())
                                        .toString());
                    }
                }
                scanned += scanRecords.count();
            }
        }
        List<String> expectedRows0 = Arrays.asList("+I[1, 3501, Tim]", "+I[4, 3504, jerry]");
        List<String> expectedRows1 = Arrays.asList("+I[2, 3502, Fabian]", "+I[5, 3505, piggy]");
        List<String> expectedRows2 = Arrays.asList("+I[3, 3503, coco]", "+I[6, 3506, stave]");
        assertThat(rows.values())
                .containsExactlyInAnyOrder(expectedRows0, expectedRows1, expectedRows2);
    }

    @Test
    void testAppendLogWithMultiBatch() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3')");
        int batchSize = 3;
        for (int i = 0; i < batchSize; i++) {
            tEnv.executeSql(
                            "INSERT INTO sink_test(a, b, c) "
                                    + "VALUES (1, 3501, 'Tim'), "
                                    + "(2, 3502, 'Fabian'), "
                                    + "(3, 3503, 'coco'), "
                                    + "(4, 3504, 'jerry'), "
                                    + "(5, 3505, 'piggy'), "
                                    + "(6, 3506, 'stave')")
                    .await();
        }

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        List<String> expectedRows = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            expectedRows.addAll(
                    Arrays.asList(
                            "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                            "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                            "+I[5, 3505, piggy]", "+I[6, 3506, stave]"));
        }
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @ParameterizedTest
    @EnumSource(value = DistributionMode.class)
    void testAppendLogPartitionTable(DistributionMode distributionMode) throws Exception {
        tEnv.executeSql(
                String.format(
                        "create table sink_test (a int not null, b bigint, c string) "
                                + " partitioned by (c) "
                                + "with ('bucket.num' = '3', 'sink.distribution-mode'= '%s')",
                        distributionMode));
        String insertSql =
                "INSERT INTO sink_test(a, b, c) "
                        + "VALUES (1, 3501, 'Tim'), "
                        + "(2, 3502, 'Fabian'), "
                        + "(3, 3503, 'Tim'), "
                        + "(4, 3504, 'jerry'), "
                        + "(5, 3505, 'piggy'), "
                        + "(7, 3507, 'Fabian'), "
                        + "(8, 3508, 'stave'), "
                        + "(9, 3509, 'Tim'), "
                        + "(10, 3510, 'coco'), "
                        + "(11, 3511, 'stave'), "
                        + "(12, 3512, 'Tim')";

        if (distributionMode == DistributionMode.BUCKET) {
            assertThatThrownBy(() -> tEnv.explainSql(insertSql, ExplainDetail.JSON_EXECUTION_PLAN))
                    .hasMessageContaining(
                            "BUCKET mode is only supported for log tables with bucket keys");
            return;
        }

        String insertPlan = tEnv.explainSql(insertSql, ExplainDetail.JSON_EXECUTION_PLAN);
        if (distributionMode == DistributionMode.PARTITION_DYNAMIC) {
            assertThat(insertPlan)
                    .contains(String.format("\"ship_strategy\" : \"%s\"", distributionMode.name()));
        } else {
            assertThat(insertPlan).contains("\"ship_strategy\" : \"FORWARD\"");
        }
        assertThat(insertPlan).doesNotContain("\"ship_strategy\" : \"REBALANCE\"");

        tEnv.executeSql(insertSql).await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        //noinspection ArraysAsListWithZeroOrOneArgument
        List<List<String>> expectedGroups =
                Arrays.asList(
                        Arrays.asList(
                                "+I[1, 3501, Tim]",
                                "+I[3, 3503, Tim]",
                                "+I[9, 3509, Tim]",
                                "+I[12, 3512, Tim]"),
                        Arrays.asList("+I[2, 3502, Fabian]", "+I[7, 3507, Fabian]"),
                        Arrays.asList("+I[4, 3504, jerry]"),
                        Arrays.asList("+I[5, 3505, piggy]"),
                        Arrays.asList("+I[8, 3508, stave]", "+I[11, 3511, stave]"),
                        Arrays.asList("+I[10, 3510, coco]"));

        List<String> expectedRows =
                expectedGroups.stream().flatMap(List::stream).collect(Collectors.toList());

        List<String> actual = collectRowsWithTimeout(rowIter, expectedRows.size());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    @ParameterizedTest
    @EnumSource(value = DistributionMode.class)
    void testPut(DistributionMode distributionMode) throws Exception {
        tEnv.executeSql(
                String.format(
                        "create table sink_test (a int not null primary key not enforced, b bigint, c string)"
                                + " with('bucket.num' = '3', 'sink.distribution-mode'= '%s')",
                        distributionMode));

        String insertSql =
                "INSERT INTO sink_test(a, b, c) "
                        + "VALUES (1, 3501, 'Tim'), "
                        + "(2, 3502, 'Fabian'), "
                        + "(3, 3503, 'coco'), "
                        + "(4, 3504, 'jerry'), "
                        + "(5, 3505, 'piggy'), "
                        + "(6, 3506, 'stave')";
        if (distributionMode == DistributionMode.PARTITION_DYNAMIC) {
            assertThatThrownBy(() -> tEnv.explainSql(insertSql, ExplainDetail.JSON_EXECUTION_PLAN))
                    .hasMessageContaining(
                            "Unsupported distribution mode: PARTITION_DYNAMIC for primary key table");
            return;
        }

        String insertPlan = tEnv.explainSql(insertSql, ExplainDetail.JSON_EXECUTION_PLAN);
        if (distributionMode == DistributionMode.BUCKET) {
            assertThat(insertPlan).contains("\"ship_strategy\" : \"BUCKET\"");
        } else if (distributionMode == DistributionMode.AUTO
                || distributionMode == DistributionMode.NONE) {
            assertThat(insertPlan).contains("\"ship_strategy\" : \"FORWARD\"");
        }
        assertThat(insertPlan).doesNotContain("\"ship_strategy\" : \"REBALANCE\"");

        tEnv.executeSql(insertSql).await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                        "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                        "+I[5, 3505, piggy]", "+I[6, 3506, stave]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        tEnv.executeSql(
                        "INSERT INTO sink_test(c, b, a) "
                                + "VALUES "
                                + "('Timmy', 501, 11), "
                                + "('Fab', 502, 12), "
                                + "('cony', 503, 13), "
                                + "('jemmy', 504, 14), "
                                + "('pig', 505, 15), "
                                + "('stephen', 506, 16)")
                .await();
        expectedRows =
                Arrays.asList(
                        "+I[11, 501, Timmy]", "+I[12, 502, Fab]",
                        "+I[13, 503, cony]", "+I[14, 504, jemmy]",
                        "+I[15, 505, pig]", "+I[16, 506, stephen]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testPutDuringAddColumn() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null primary key not enforced, b bigint, c string)");
        tEnv.executeSql(
                        "INSERT INTO sink_test "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco') ")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        // add new column
        tEnv.executeSql("alter table sink_test add add_column int").await();
        tEnv.executeSql(
                        "INSERT INTO sink_test "
                                + "VALUES (4, 3504, 'jerry', 4), "
                                + "(5, 3505, 'piggy', 5), "
                                + "(6, 3506, 'stave', 6)")
                .await();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                        "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                        "+I[5, 3505, piggy]", "+I[6, 3506, stave]");
        // read with old schema
        assertResultsIgnoreOrder(rowIter, expectedRows, true);

        // read with new schema.
        rowIter = tEnv.executeSql("select * from sink_test").collect();
        expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim, null]", "+I[2, 3502, Fabian, null]",
                        "+I[3, 3503, coco, null]", "+I[4, 3504, jerry, 4]",
                        "+I[5, 3505, piggy, 5]", "+I[6, 3506, stave, 6]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testPartialUpsert() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null primary key not enforced, b bigint, c string) with('bucket.num' = '3')");

        // partial insert
        tEnv.executeSql("INSERT INTO sink_test(a, b) VALUES (1, 111), (2, 222)").await();
        tEnv.executeSql("INSERT INTO sink_test(c, a) VALUES ('c1', 1), ('c2', 2)").await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();

        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 111, null]",
                        "+I[2, 222, null]",
                        "-U[1, 111, null]",
                        "+U[1, 111, c1]",
                        "-U[2, 222, null]",
                        "+U[2, 222, c2]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // partial delete
        org.apache.flink.table.api.Table changeLogTable =
                tEnv.fromChangelogStream(
                        env.fromElements(
                                        Row.ofKind(
                                                org.apache.flink.types.RowKind.INSERT,
                                                1,
                                                333L,
                                                "c11"),
                                        Row.ofKind(
                                                org.apache.flink.types.RowKind.DELETE,
                                                1,
                                                333L,
                                                "c11"))
                                .returns(Types.ROW(Types.INT, Types.LONG, Types.STRING)));
        tEnv.createTemporaryView("changeLog", changeLogTable);

        // check the target fields in row 1 is set to null
        tEnv.executeSql("INSERT INTO sink_test(a, b) SELECT f0, f1 FROM changeLog").await();
        expectedRows =
                Arrays.asList(
                        "-U[1, 111, c1]", "+U[1, 333, c1]", "-U[1, 333, c1]", "+U[1, null, c1]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // check the row 1 will be deleted finally since all the fields in the row are set to null
        tEnv.executeSql("INSERT INTO sink_test(a, c) SELECT f0, f2 FROM changeLog").await();
        expectedRows = Arrays.asList("-U[1, null, c1]", "+U[1, null, c11]", "-D[1, null, c11]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testPartialUpsertDuringAddColumn() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null primary key not enforced, b bigint, c string) with('bucket.num' = '3')");

        // partial insert
        tEnv.executeSql("INSERT INTO sink_test(a, b) VALUES (1, 111), (2, 222)").await();
        tEnv.executeSql("INSERT INTO sink_test(c, a) VALUES ('c1', 1), ('c2', 2)").await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        // add new column
        tEnv.executeSql("alter table sink_test add add_column string").await();
        tEnv.executeSql(
                        "INSERT INTO sink_test(add_column, a ) VALUES ('new_value', 1), ('new_value', 2)")
                .await();

        // read with old schema
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 111, null]",
                        "+I[2, 222, null]",
                        "-U[1, 111, null]",
                        "+U[1, 111, c1]",
                        "-U[2, 222, null]",
                        "+U[2, 222, c2]",
                        "-U[1, 111, c1]",
                        "+U[1, 111, c1]",
                        "-U[2, 222, c2]",
                        "+U[2, 222, c2]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);

        // read with new schema
        CloseableIterator<Row> newSchemaRowIter =
                tEnv.executeSql(
                                "select * from sink_test /*+ OPTIONS('scan.startup.mode' = 'earliest') */ ")
                        .collect();
        expectedRows =
                Arrays.asList(
                        "+I[1, 111, null, null]",
                        "+I[2, 222, null, null]",
                        "-U[1, 111, null, null]",
                        "+U[1, 111, c1, null]",
                        "-U[2, 222, null, null]",
                        "+U[2, 222, c2, null]",
                        "-U[1, 111, c1, null]",
                        "+U[1, 111, c1, new_value]",
                        "-U[2, 222, c2, null]",
                        "+U[2, 222, c2, new_value]");
        assertResultsIgnoreOrder(newSchemaRowIter, expectedRows, true);
    }

    @Test
    void testFirstRowMergeEngine() throws Exception {
        tEnv.executeSql(
                "create table first_row_source (a int not null primary key not enforced,"
                        + " b string) with('table.merge-engine' = 'first_row')");
        tEnv.executeSql("create table log_sink (a int, b string)");

        // insert the primary table with first_row merge engine into the log table to verify that
        // the first_row merge engine only generates append-only stream
        JobClient insertJobClient =
                tEnv.executeSql("insert into log_sink select * from first_row_source")
                        .getJobClient()
                        .get();

        // insert once
        tEnv.executeSql(
                        "insert into first_row_source(a, b) VALUES (1, 'v1'), (2, 'v2'), (1, 'v11'), (3, 'v3')")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from log_sink").collect();

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // insert again
        tEnv.executeSql("insert into first_row_source(a, b) VALUES (3, 'v33'), (4, 'v44')").await();
        expectedRows = Collections.singletonList("+I[4, v44]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // insert with all keys already exists.
        tEnv.executeSql("insert into first_row_source(a, b) VALUES (3, 'v333'), (4, 'v444')")
                .await();

        tEnv.executeSql("insert into first_row_source(a, b) VALUES (5, 'v5')").await();
        expectedRows = Collections.singletonList("+I[5, v5]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);

        insertJobClient.cancel().get();
    }

    @Test
    void testInsertWithoutSpecifiedCols() {
        tEnv.executeSql("create table sink_insert_all (a int, b bigint, c string)");
        tEnv.executeSql("create table source_insert_all (a int, b bigint, c string)");
        // we just use explain to reduce test time
        String expectPlan =
                "== Abstract Syntax Tree ==\n"
                        + "LogicalSink(table=[testcatalog.defaultdb.sink_insert_all], fields=[a, b, c])\n"
                        + "+- LogicalProject(a=[$0], b=[$1], c=[$2])\n"
                        + "   +- LogicalTableScan(table=[[testcatalog, defaultdb, source_insert_all]])\n"
                        + "\n"
                        + "== Optimized Physical Plan ==\n"
                        + "Sink(table=[testcatalog.defaultdb.sink_insert_all], fields=[a, b, c])\n"
                        + "+- TableSourceScan(table=[[testcatalog, defaultdb, source_insert_all]], fields=[a, b, c])\n"
                        + "\n"
                        + "== Optimized Execution Plan ==\n"
                        + "Sink(table=[testcatalog.defaultdb.sink_insert_all], fields=[a, b, c])\n"
                        + "+- TableSourceScan(table=[[testcatalog, defaultdb, source_insert_all]], fields=[a, b, c])\n";
        assertThat(tEnv.explainSql("insert into sink_insert_all select * from source_insert_all"))
                .isEqualTo(expectPlan);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIgnoreDelete(boolean isPrimaryKeyTable) throws Exception {
        String sinkName =
                isPrimaryKeyTable
                        ? "ignore_delete_primary_key_table_sink"
                        : "ignore_delete_log_table_sink";
        String sourceName = isPrimaryKeyTable ? "source_primary_key_table" : "source_log_table";
        org.apache.flink.table.api.Table cdcSourceData =
                tEnv.fromChangelogStream(
                        env.fromCollection(
                                Arrays.asList(
                                        Row.ofKind(RowKind.INSERT, 1, 3501L, "Tim"),
                                        Row.ofKind(RowKind.DELETE, 1, 3501L, "Tim"),
                                        Row.ofKind(RowKind.INSERT, 2, 3502L, "Fabian"),
                                        Row.ofKind(RowKind.UPDATE_BEFORE, 2, 3502L, "Fabian"),
                                        Row.ofKind(RowKind.UPDATE_AFTER, 3, 3503L, "coco"))));
        tEnv.createTemporaryView(String.format("%s", sourceName), cdcSourceData);

        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "a int not null, "
                                + "b bigint, "
                                + "c string "
                                + (isPrimaryKeyTable ? ", primary key (a) NOT ENFORCED" : "")
                                + ") with('bucket.num' = '3',"
                                + " 'sink.ignore-delete'='true')",
                        sinkName));
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM %s", sinkName, sourceName))
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql(String.format("select * from %s", sinkName)).collect();
        List<String> expectedRows =
                Arrays.asList("+I[1, 3501, Tim]", "+I[2, 3502, Fabian]", "+I[3, 3503, coco]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @ParameterizedTest
    @MethodSource("writePartitionedTableParams")
    void testWritePartitionedTable(boolean isPrimaryKeyTable, boolean isAutoPartition)
            throws Exception {
        String tableName =
                String.format(
                        "%s_partitioned_%s_table_sink",
                        isPrimaryKeyTable ? "primary_key" : "log", isAutoPartition ? "auto" : "");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        Collection<String> partitions;
        if (isAutoPartition) {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "a int not null,"
                                    + " b bigint, "
                                    + "c string"
                                    + (isPrimaryKeyTable ? ", primary key (a,c) NOT ENFORCED" : "")
                                    + ")"
                                    + " partitioned by (c) "
                                    + "with ('table.auto-partition.enabled' = 'true',"
                                    + " 'table.auto-partition.time-unit' = 'year')",
                            tableName));
            partitions =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath)
                            .values();
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "a int not null,"
                                    + " b bigint, "
                                    + "c string"
                                    + (isPrimaryKeyTable ? ", primary key (a,c) NOT ENFORCED" : "")
                                    + ")"
                                    + " partitioned by (c) ",
                            tableName));
            int currentYear = LocalDate.now().getYear();
            tEnv.executeSql(
                    String.format(
                            "alter table %s add partition (c = '%s')", tableName, currentYear));
            partitions =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1)
                            .values();
        }

        InsertAndExpectValues insertAndExpectValues = rowsToInsertInto(partitions);

        List<String> insertValues = insertAndExpectValues.insertValues;
        List<String> expectedRows = insertAndExpectValues.expectedRows;

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s(a, b, c) " + "VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();

        // This test requires dynamically discovering newly created partitions, so
        // 'scan.partition.discovery.interval' needs to be set to 2s (default is 1 minute),
        // otherwise the test may hang for 1 minute.
        CloseableIterator<Row> rowIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from %s /*+ OPTIONS('scan.partition.discovery.interval' = '2s') */",
                                        tableName))
                        .collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // create two partitions, write data to the new partitions
        List<String> newPartitions = Arrays.asList("2030", "2031");
        tEnv.executeSql(
                String.format("alter table %s add partition (c = '%s')", tableName, "2030"));
        tEnv.executeSql(
                String.format("alter table %s add partition (c = '%s')", tableName, "2031"));

        // insert into the new partition again, check we can read the data
        // in new partitions
        insertAndExpectValues = rowsToInsertInto(newPartitions);
        insertValues = insertAndExpectValues.insertValues;
        expectedRows = insertAndExpectValues.expectedRows;
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s(a, b, c) " + "VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // test insert new added partitions
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s PARTITION (c = 2030) values (22, 2222), (33, 3333)",
                                tableName))
                .await();
        assertResultsIgnoreOrder(
                rowIter, Arrays.asList("+I[22, 2222, 2030]", "+I[33, 3333, 2030]"), true);
    }

    @Test
    void testDeleteAndUpdateStmtOnPkTable() throws Exception {
        String tableName = "pk_table_delete_test";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint, "
                                + " c string,"
                                + " primary key (a) not enforced"
                                + ")",
                        tableName));
        // test delete data with non-exists key.
        tBatchEnv.executeSql("DELETE FROM " + tableName + " WHERE a = 5").await();

        List<String> insertValues =
                Arrays.asList(
                        "(1, 3501, 'Beijing')",
                        "(2, 3502, 'Shanghai')",
                        "(3, 3503, 'Berlin')",
                        "(4, 3504, 'Seattle')",
                        "(5, 3505, 'Boston')",
                        "(6, 3506, 'London')");
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s(a,b,c) VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();

        // test delete row5
        tBatchEnv.executeSql("DELETE FROM " + tableName + " WHERE a = 5").await();
        CloseableIterator<Row> rowIter =
                tBatchEnv
                        .executeSql(String.format("select * from %s WHERE a = 5", tableName))
                        .collect();
        assertThat(rowIter.hasNext()).isFalse();

        // test delete data with non-exists key.
        tBatchEnv.executeSql("DELETE FROM " + tableName + " WHERE a = 15").await();

        // test update row4
        tBatchEnv.executeSql("UPDATE " + tableName + " SET c = 'New York' WHERE a = 4").await();
        CloseableIterator<Row> row4 =
                tBatchEnv
                        .executeSql(String.format("select * from %s WHERE a = 4", tableName))
                        .collect();
        List<String> expected = Collections.singletonList("+I[4, 3504, New York]");
        assertResultsIgnoreOrder(row4, expected, true);

        // use stream env to assert changelogs
        CloseableIterator<Row> changelogIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */",
                                        tableName))
                        .collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Beijing]",
                        "+I[2, 3502, Shanghai]",
                        "+I[3, 3503, Berlin]",
                        "+I[4, 3504, Seattle]",
                        "+I[5, 3505, Boston]",
                        "+I[6, 3506, London]",
                        "-D[5, 3505, Boston]",
                        "-U[4, 3504, Seattle]",
                        "+U[4, 3504, New York]");
        assertResultsIgnoreOrder(changelogIter, expectedRows, true);

        // test schema evolution.
        tBatchEnv
                .executeSql(String.format("alter table %s add new_added_column int", tableName))
                .await();
        tBatchEnv
                .executeSql("UPDATE " + tableName + " SET new_added_column = 2 WHERE a = 4")
                .await();
        CloseableIterator<Row> row5 =
                tBatchEnv
                        .executeSql(String.format("select * from %s WHERE a = 4", tableName))
                        .collect();
        expected = Collections.singletonList("+I[4, 3504, New York, 2]");
        assertResultsIgnoreOrder(row5, expected, true);
    }

    @Test
    void testDeleteAndUpdateStmtOnPartitionedPkTable() throws Exception {
        String tableName = "partitioned_pk_table_delete_test";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint, "
                                + " c string,"
                                + " primary key (a, c) not enforced"
                                + ") partitioned by (c)"
                                + " with ('table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        tableName));
        Collection<String> partitions =
                waitUntilPartitions(
                                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                                TablePath.of(DEFAULT_DB, tableName))
                        .values();
        String partition = partitions.iterator().next();
        List<String> insertValues =
                Arrays.asList(
                        "(1, 3501, '" + partition + "')",
                        "(2, 3502, '" + partition + "')",
                        "(3, 3503, '" + partition + "')",
                        "(4, 3504, '" + partition + "')",
                        "(5, 3505, '" + partition + "')",
                        "(6, 3506, '" + partition + "')");
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s(a,b,c) VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();

        // test delete row5
        tBatchEnv
                .executeSql("DELETE FROM " + tableName + " WHERE a = 5 AND c = '" + partition + "'")
                .await();
        CloseableIterator<Row> rowIter =
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "select * from %s WHERE a = 5 AND c = '%s'",
                                        tableName, partition))
                        .collect();
        assertThat(rowIter.hasNext()).isFalse();

        // test update row4
        tBatchEnv
                .executeSql(
                        "UPDATE "
                                + tableName
                                + " SET b = 4004 WHERE a = 4 AND c = '"
                                + partition
                                + "'")
                .await();
        CloseableIterator<Row> row4 =
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "select * from %s WHERE a = 4 AND c = '%s'",
                                        tableName, partition))
                        .collect();
        List<String> expected = Collections.singletonList("+I[4, 4004, " + partition + "]");
        assertResultsIgnoreOrder(row4, expected, true);

        // use stream env to assert changelogs
        CloseableIterator<Row> changelogIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */",
                                        tableName))
                        .collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, " + partition + "]",
                        "+I[2, 3502, " + partition + "]",
                        "+I[3, 3503, " + partition + "]",
                        "+I[4, 3504, " + partition + "]",
                        "+I[5, 3505, " + partition + "]",
                        "+I[6, 3506, " + partition + "]",
                        "-D[5, 3505, " + partition + "]",
                        "-U[4, 3504, " + partition + "]",
                        "+U[4, 4004, " + partition + "]");
        assertResultsIgnoreOrder(changelogIter, expectedRows, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUnsupportedDeleteAndUpdateStmtOnLogTable(boolean isPartitionedTable) {
        String tableName =
                isPartitionedTable ? "partitioned_log_table_delete_test" : "log_table_delete_test";
        String partitionedTableStmt =
                " partitioned by (c) with ('table.auto-partition.enabled' = 'true','table.auto-partition.time-unit' = 'year')";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint, "
                                + " c string"
                                + ")"
                                + (isPartitionedTable ? partitionedTableStmt : ""),
                        tableName));
        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("DELETE FROM " + tableName + " WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Log Table doesn't support DELETE and UPDATE statements.");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql(
                                                "UPDATE "
                                                        + tableName
                                                        + " SET c = 'New York' WHERE a = 4")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Log Table doesn't support DELETE and UPDATE statements.");
    }

    @Test
    void testUnsupportedDeleteAndUpdateStmtOnPartialPK() {
        // test primary-key table
        String t1 = "t1";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint not null, "
                                + " c string,"
                                + " primary key (a, b) not enforced"
                                + ")",
                        t1));
        assertThatThrownBy(() -> tBatchEnv.executeSql("DELETE FROM " + t1 + " WHERE a = 1").await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports DELETE statement with conditions on primary key.");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t1 + " SET b = 4004 WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Updates to primary keys are not supported, primaryKeys ([a, b]), updatedColumns ([b])");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql(
                                                "UPDATE " + t1 + " SET c = 'New York' WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports UPDATE statement with conditions on primary key.");

        // test partitioned primary-key table
        String t2 = "t2";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint not null, "
                                + " c string,"
                                + " primary key (a, c) not enforced"
                                + ") partitioned by (c)"
                                + " with ('table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        t2));
        assertThatThrownBy(() -> tBatchEnv.executeSql("DELETE FROM " + t2 + " WHERE a = 1").await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports DELETE statement with conditions on primary key.");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t2 + " SET c = '2028' WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Updates to primary keys are not supported, primaryKeys ([a, c]), updatedColumns ([c])");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t2 + " SET b = 4004 WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports UPDATE statement with conditions on primary key.");
    }

    @Test
    void testUnsupportedStmtOnFirstRowMergeEngine() {
        String t1 = "firstRowMergeEngineTable";
        TablePath tablePath = TablePath.of(DEFAULT_DB, t1);
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint null, "
                                + " c string null, "
                                + " primary key (a) not enforced"
                                + ") with ('table.merge-engine' = 'first_row')",
                        t1));
        assertThatThrownBy(() -> tBatchEnv.executeSql("DELETE FROM " + t1 + " WHERE a = 1").await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Table %s uses the 'FIRST_ROW' merge engine which does not support DELETE or UPDATE statements.",
                        tablePath);

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t1 + " SET b = 4004 WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Table %s uses the 'FIRST_ROW' merge engine which does not support DELETE or UPDATE statements.",
                        tablePath);

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("INSERT INTO " + t1 + "(a, c) VALUES(1, 'c1')")
                                        .await())
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Table %s uses the 'FIRST_ROW' merge engine which does not support partial updates."
                                + " Please make sure the number of specified columns in INSERT INTO matches columns of the Fluss table.",
                        tablePath);
    }

    @Test
    void testUnsupportedStmtOnVersionMergeEngine() {
        String t1 = "versionMergeEngineTable";
        TablePath tablePath = TablePath.of(DEFAULT_DB, t1);
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint null, "
                                + " c string null, "
                                + " primary key (a) not enforced"
                                + ") with ('table.merge-engine' = 'versioned', "
                                + "'table.merge-engine.versioned.ver-column' = 'b')",
                        t1));
        assertThatThrownBy(() -> tBatchEnv.executeSql("DELETE FROM " + t1 + " WHERE a = 1").await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Table %s uses the 'VERSIONED' merge engine which does not support DELETE or UPDATE statements.",
                        tablePath);

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t1 + " SET b = 4004 WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Table %s uses the 'VERSIONED' merge engine which does not support DELETE or UPDATE statements.",
                        tablePath);

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("INSERT INTO " + t1 + "(a, c) VALUES(1, 'c1')")
                                        .await())
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Table %s uses the 'VERSIONED' merge engine which does not support partial updates."
                                + " Please make sure the number of specified columns in INSERT INTO matches columns of the Fluss table.",
                        tablePath);
    }

    @Test
    void testVersionMergeEngineWithTypeBigint() throws Exception {
        tEnv.executeSql(
                "create table merge_engine_with_version (a int not null primary key not enforced,"
                        + " b string, ts bigint) with('table.merge-engine' = 'versioned',"
                        + "'table.merge-engine.versioned.ver-column' = 'ts')");

        // insert once
        tEnv.executeSql(
                        "insert into merge_engine_with_version (a, b, ts) VALUES "
                                + "(1, 'v1', 1000), (2, 'v2', 1000), (1, 'v11', 999), (3, 'v3', 1000)")
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from merge_engine_with_version").collect();

        // id=1 not update
        List<String> expectedRows =
                Arrays.asList("+I[1, v1, 1000]", "+I[2, v2, 1000]", "+I[3, v3, 1000]");

        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // insert again, update id=3 and id=2 (>= old version), insert id=4, ignore id=1
        tEnv.executeSql(
                        "insert into merge_engine_with_version (a, b, ts) VALUES "
                                + "(3, 'v33', 1001), (4, 'v44', 1000), (1, 'v11', 999), (2, 'v22', 1000)")
                .await();
        expectedRows =
                Arrays.asList(
                        "-U[3, v3, 1000]",
                        "+U[3, v33, 1001]",
                        "+I[4, v44, 1000]",
                        "-U[2, v2, 1000]",
                        "+U[2, v22, 1000]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testVersionMergeEngineWithTypeTimestamp() throws Exception {
        tEnv.executeSql(
                        "create table merge_engine_with_version (a int not null primary key not enforced,"
                                + " b string, ts TIMESTAMP(3)) with('table.merge-engine' = 'versioned',"
                                + "'table.merge-engine.versioned.ver-column' = 'ts')")
                .await();
        // insert once
        tEnv.executeSql(
                        "INSERT INTO merge_engine_with_version (a, b, ts) VALUES "
                                + "(1, 'v1', TIMESTAMP '2024-12-27 12:00:00.123'), "
                                + "(2, 'v2', TIMESTAMP '2024-12-27 12:00:00.123'), "
                                + "(3, 'v3', TIMESTAMP '2024-12-27 12:00:00.123');")
                .await();
        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from merge_engine_with_version").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, v1, 2024-12-27T12:00:00.123]",
                        "+I[2, v2, 2024-12-27T12:00:00.123]",
                        "+I[3, v3, 2024-12-27T12:00:00.123]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // insert again. id=1 not update, but id=3 updated
        tEnv.executeSql(
                "INSERT INTO merge_engine_with_version (a, b, ts) VALUES "
                        + "(1, 'v11', TIMESTAMP '2024-12-27 11:59:59.123'), "
                        + "(3, 'v33', TIMESTAMP '2024-12-27 12:00:00.123');");
        expectedRows =
                Arrays.asList(
                        "-U[3, v3, 2024-12-27T12:00:00.123]",
                        "+U[3, v33, 2024-12-27T12:00:00.123]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testVersionMergeEngineWithTypeTimestampLTZ9() throws Exception {

        tEnv.getConfig().set("table.local-time-zone", "UTC");
        tEnv.executeSql(
                        "create table merge_engine_with_version (a int not null primary key not enforced,"
                                + " b string, ts TIMESTAMP(9) WITH LOCAL TIME ZONE ) with("
                                + "'table.merge-engine' = 'versioned',"
                                + "'table.merge-engine.versioned.ver-column' = 'ts')")
                .await();

        // insert once
        tEnv.executeSql(
                        "INSERT INTO merge_engine_with_version (a, b, ts) VALUES "
                                + "(1, 'v1', CAST(TIMESTAMP '2024-12-27 12:00:00.123456789' AS TIMESTAMP(9) WITH LOCAL TIME ZONE)), "
                                + "(2, 'v2', CAST(TIMESTAMP '2024-12-27 12:00:00.123456789' AS TIMESTAMP(9) WITH LOCAL TIME ZONE)), "
                                + "(3, 'v3', CAST(TIMESTAMP '2024-12-27 12:00:00.123456789' AS TIMESTAMP(9) WITH LOCAL TIME ZONE));")
                .await();
        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from merge_engine_with_version").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, v1, 2024-12-27T12:00:00.123456789Z]",
                        "+I[2, v2, 2024-12-27T12:00:00.123456789Z]",
                        "+I[3, v3, 2024-12-27T12:00:00.123456789Z]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // insert again. id=1 not update, but id=3 updated
        tEnv.executeSql(
                        "INSERT INTO merge_engine_with_version (a, b, ts) VALUES "
                                + "(1, 'v11', CAST(TIMESTAMP '2024-12-27 12:00:00.123456788' AS TIMESTAMP(9) WITH LOCAL TIME ZONE)), "
                                + "(3, 'v33', CAST(TIMESTAMP '2024-12-27 12:00:00.123456789' AS TIMESTAMP(9) WITH LOCAL TIME ZONE));")
                .await();
        expectedRows =
                Arrays.asList(
                        "-U[3, v3, 2024-12-27T12:00:00.123456789Z]",
                        "+U[3, v33, 2024-12-27T12:00:00.123456789Z]");

        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testComprehensiveAggregationFunctions() throws Exception {
        // Test all 11 aggregate functions (each function tested once with representative data type)
        tEnv.executeSql(
                "create table comprehensive_agg ("
                        + "id int not null primary key not enforced, "
                        // Numeric aggregations
                        + "sum_int int, "
                        + "sum_double double, "
                        + "product_int int, "
                        // Max/Min aggregations (representative types: int, double, string,
                        // timestamp)
                        + "max_int int, "
                        + "max_timestamp timestamp(3), "
                        + "min_double double, "
                        + "min_string string, "
                        // Value selection aggregations (test with/without nulls)
                        + "first_val int, "
                        + "first_val_non_null int, "
                        + "last_val int, "
                        + "last_val_non_null int, "
                        // Boolean aggregations
                        + "bool_and_val boolean, "
                        + "bool_or_val boolean, "
                        // String aggregation with custom delimiter
                        + "listagg_val string"
                        + ") with ("
                        + "'table.merge-engine' = 'aggregation', "
                        + "'fields.sum_int.agg' = 'sum', "
                        + "'fields.sum_double.agg' = 'sum', "
                        + "'fields.product_int.agg' = 'product', "
                        + "'fields.max_int.agg' = 'max', "
                        + "'fields.max_timestamp.agg' = 'max', "
                        + "'fields.min_double.agg' = 'min', "
                        + "'fields.min_string.agg' = 'min', "
                        + "'fields.first_val.agg' = 'first_value', "
                        + "'fields.first_val_non_null.agg' = 'first_value_ignore_nulls', "
                        + "'fields.last_val.agg' = 'last_value', "
                        + "'fields.last_val_non_null.agg' = 'last_value_ignore_nulls', "
                        + "'fields.bool_and_val.agg' = 'bool_and', "
                        + "'fields.bool_or_val.agg' = 'bool_or', "
                        + "'fields.listagg_val.agg' = 'listagg', "
                        + "'fields.listagg_val.listagg.delimiter' = '|')");

        // Insert first batch - initial values
        tEnv.executeSql(
                        "INSERT INTO comprehensive_agg VALUES ("
                                + "1, " // id
                                + "1000, 10.5, " // sum_int, sum_double
                                + "2, " // product_int
                                + "100, TIMESTAMP '2024-01-15 15:00:00', " // max_int, max_timestamp
                                + "100.0, 'beta', " // min_double, min_string
                                + "100, 100, 100, 100, " // first_value, first_value_ignore_nulls,
                                // last_value, last_value_ignore_nulls
                                + "true, false, " // bool_and_val, bool_or_val
                                + "'alpha'" // listagg_val
                                + ")")
                .await();

        // Insert second batch - trigger aggregation
        tEnv.executeSql(
                        "INSERT INTO comprehensive_agg VALUES ("
                                + "1, " // id
                                + "2000, 20.5, " // sum: 1000+2000=3000, 10.5+20.5=31.0
                                + "3, " // product: 2*3=6
                                + "200, TIMESTAMP '2024-02-01 18:00:00', " // max: 200, 2024-02-01
                                // 18:00
                                + "50.0, 'alpha', " // min: 50.0, alpha
                                + "200, 200, 200, 200, " // first: keep 100, first_ignore_nulls:
                                // 200, last: 200, last_ignore_nulls: 200
                                + "true, true, " // bool_and: true AND true=true, bool_or: false OR
                                // true=true
                                + "'beta'" // listagg: alpha|beta
                                + ")")
                .await();

        // Insert third batch - further aggregation with null handling test
        tEnv.executeSql(
                        "INSERT INTO comprehensive_agg VALUES ("
                                + "1, " // id
                                + "3000, 30.5, " // sum: 3000+3000=6000, 31.0+30.5=61.5
                                + "5, " // product: 6*5=30
                                + "150, TIMESTAMP '2024-01-20 14:00:00', " // max: keep 200, keep
                                // 2024-02-01 18:00
                                + "80.0, 'charlie', " // min: keep 50.0, keep alpha
                                + "300, CAST(NULL AS INT), 300, 300, " // first: keep 100, ignore
                                // null keep 200, last: 300,
                                // last_ignore_nulls: 300
                                + "false, true, " // bool_and: true AND false=false, bool_or: true
                                // OR true=true
                                + "'gamma'" // listagg: alpha|beta|gamma
                                + ")")
                .await();

        // Query and verify aggregated results
        CloseableIterator<Row> rowIter =
                tEnv.executeSql("SELECT * FROM comprehensive_agg").collect();

        // Expected results: changelog with 5 records (+I, -U, +U, -U, +U)
        List<String> expectedRows =
                Arrays.asList(
                        // First insert: initial values
                        "+I[1, 1000, 10.5, 2, 100, 2024-01-15T15:00, 100.0, beta, 100, 100, 100, 100, true, false, alpha]",
                        // Second insert: retraction
                        "-U[1, 1000, 10.5, 2, 100, 2024-01-15T15:00, 100.0, beta, 100, 100, 100, 100, true, false, alpha]",
                        // Second insert: aggregated result
                        // sum: 1000+2000=3000, 10.5+20.5=31.0
                        // product: 2*3=6
                        // max: 200, 2024-02-01T18:00
                        // min: 50.0, alpha
                        // first: 100, first_non_null: 200, last: 200, last_non_null: 200
                        // bool_and: true, bool_or: true
                        // listagg: alpha|beta
                        "+U[1, 3000, 31.0, 6, 200, 2024-02-01T18:00, 50.0, alpha, 100, 100, 200, 200, true, true, alpha|beta]",
                        // Third insert: retraction
                        "-U[1, 3000, 31.0, 6, 200, 2024-02-01T18:00, 50.0, alpha, 100, 100, 200, 200, true, true, alpha|beta]",
                        // Third insert: final aggregated result
                        // sum: 3000+3000=6000, 31.0+30.5=61.5
                        // product: 6*5=30
                        // max: 200 (unchanged), 2024-02-01T18:00 (unchanged)
                        // min: 50.0 (unchanged), alpha (unchanged)
                        // first: 100, first_ignore_nulls: 200 (null ignored), last: 300,
                        // last_ignore_nulls: 300
                        // bool_and: false, bool_or: true
                        // listagg: alpha|beta|gamma
                        "+U[1, 6000, 61.5, 30, 200, 2024-02-01T18:00, 50.0, alpha, 100, 100, 300, 300, false, true, alpha|beta|gamma]");

        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    private InsertAndExpectValues rowsToInsertInto(Collection<String> partitions) {
        List<String> insertValues = new ArrayList<>();
        List<String> expectedValues = new ArrayList<>();
        for (String partition : partitions) {
            insertValues.addAll(
                    Arrays.asList(
                            "(1, 3501, '" + partition + "')",
                            "(2, 3502, '" + partition + "')",
                            "(3, 3503, '" + partition + "')",
                            "(4, 3504, '" + partition + "')",
                            "(5, 3505, '" + partition + "')",
                            "(6, 3506, '" + partition + "')"));
            expectedValues.addAll(
                    Arrays.asList(
                            "+I[1, 3501, " + partition + "]",
                            "+I[2, 3502, " + partition + "]",
                            "+I[3, 3503, " + partition + "]",
                            "+I[4, 3504, " + partition + "]",
                            "+I[5, 3505, " + partition + "]",
                            "+I[6, 3506, " + partition + "]"));
        }
        return new InsertAndExpectValues(insertValues, expectedValues);
    }

    private static class InsertAndExpectValues {
        private final List<String> insertValues;
        private final List<String> expectedRows;

        public InsertAndExpectValues(List<String> insertValues, List<String> expectedRows) {
            this.insertValues = insertValues;
            this.expectedRows = expectedRows;
        }
    }

    @Test
    void testDeleteBehaviorDisabledForDeleteStmt() {
        String tableName = "delete_behavior_disable_table";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint null, "
                                + " c string null, "
                                + " primary key (a) not enforced"
                                + ") with ('table.delete.behavior' = 'disable')",
                        tableName));

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("DELETE FROM " + tableName + " WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        String.format(
                                "Table %s has delete behavior set to 'disable' which does not support DELETE statements.",
                                tablePath));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ignore", "disable", "allow"})
    void testDeleteBehaviorForInsertStmt(String deleteBehavior) throws Exception {
        String tableName = "delete_behavior_ignore_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null primary key not enforced,"
                                + " b string"
                                + ") with ('table.delete.behavior' = '%s')",
                        tableName, deleteBehavior));

        // 1. Verify the changelog mode of the table
        String changelogModePlan =
                tEnv.explainSql("SELECT * FROM " + tableName, ExplainDetail.CHANGELOG_MODE);
        if (deleteBehavior.equals("allow")) {
            assertThat(changelogModePlan)
                    .contains(
                            "TableSourceScan(table=[[testcatalog, defaultdb, delete_behavior_ignore_table]], fields=[a, b], "
                                    + "changelogMode=[I,UB,UA,D])");
        } else {
            // For 'ignore' and 'disable', delete operations are not emitted in the changelog
            assertThat(changelogModePlan)
                    .contains(
                            "TableSourceScan(table=[[testcatalog, defaultdb, delete_behavior_ignore_table]], fields=[a, b], "
                                    + "changelogMode=[I,UB,UA])");
        }

        // 2. Write data including delete operations and verify the final table state

        // Insert some data
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')",
                                tableName))
                .await();

        // Create a changelog stream with deletes that should be ignored
        org.apache.flink.table.api.Table changelogData =
                tEnv.fromChangelogStream(
                        env.fromCollection(
                                Arrays.asList(
                                        Row.ofKind(RowKind.INSERT, 4, "test4"),
                                        Row.ofKind(RowKind.DELETE, 1, "test1"), // Should be ignored
                                        Row.ofKind(RowKind.UPDATE_AFTER, 2, "updated_test2"))));
        tEnv.createTemporaryView("changelog_source", changelogData);

        // Disable upsert materialization to avoid generate SinkMaterializer operator,
        // because we want to see the original delete messages in sink
        tEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");
        String plan =
                tEnv.explainSql(
                        String.format("INSERT INTO %s SELECT * FROM changelog_source", tableName));
        assertThat(plan).doesNotContain("upsertMaterialize=[true]");

        // Insert changelog data
        TableResult tableResult =
                tEnv.executeSql(
                        String.format("INSERT INTO %s SELECT * FROM changelog_source", tableName));

        // 3. Verify the final table state based on delete behavior
        if (deleteBehavior.equals("disable")) {
            // For 'disable', the delete operation is not supported, so we expect an exception
            assertThatThrownBy(tableResult::await)
                    .hasStackTraceContaining(
                            "DeletionDisabledException: Delete operations are disabled for this table."
                                    + " The table.delete.behavior is set to 'disable'.");
        } else {
            // For 'ignore', the delete operation is ignored, so we just wait for the insert and
            // update to be applied
            tableResult.await();
            CloseableIterator<Row> rowIter =
                    tEnv.executeSql(String.format("select * from %s", tableName)).collect();

            final List<String> expectedRows;
            if (deleteBehavior.equals("ignore")) {
                // Row with a=1 should still exist (delete was ignored)
                expectedRows =
                        Arrays.asList(
                                "+I[1, test1]", // Delete was ignored
                                "+I[2, test2]",
                                "-U[2, test2]",
                                "+U[2, updated_test2]",
                                "+I[3, test3]",
                                "+I[4, test4]");
            } else {
                // For 'allow', the delete operation should be reflected in the final state
                expectedRows =
                        Arrays.asList(
                                "+I[1, test1]",
                                "-D[1, test1]", // a=1 was deleted
                                "+I[2, test2]",
                                "-U[2, test2]",
                                "+U[2, updated_test2]",
                                "+I[3, test3]",
                                "+I[4, test4]");
            }
            assertResultsIgnoreOrder(rowIter, expectedRows, true);
        }
    }

    @Test
    void testWalModeWithDefaultMergeEngineAndAggregation() throws Exception {
        // use single parallelism to make result ordering stable
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        String tableName = "wal_mode_pk_table";
        // Create a table with WAL mode and default merge engine
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " id int not null,"
                                + " category string,"
                                + " amount bigint,"
                                + " primary key (id) not enforced"
                                + ") with ('table.changelog.image' = 'wal')",
                        tableName));

        // Insert initial data
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s VALUES "
                                        + "(1, 'A', 100), "
                                        + "(2, 'B', 200), "
                                        + "(3, 'A', 150), "
                                        + "(4, 'B', 250)",
                                tableName))
                .await();

        // Use batch mode to update and delete records
        tBatchEnv.executeSql("UPDATE " + tableName + " SET amount = 120 WHERE id = 1").await();
        tBatchEnv.executeSql("UPDATE " + tableName + " SET amount = 180 WHERE id = 3").await();
        tBatchEnv.executeSql("DELETE FROM " + tableName + " WHERE id = 4").await();

        // Do aggregation on the table and verify ChangelogNormalize node is generated
        String aggQuery =
                String.format(
                        "SELECT category, SUM(amount) as total_amount FROM %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */ GROUP BY category",
                        tableName);

        // Explain the aggregation query to check for ChangelogNormalize
        String aggPlan = tEnv.explainSql(aggQuery);
        // ChangelogNormalize should be present to normalize the changelog for aggregation
        // In Flink, when the source produces changelog with primary key semantics (I, UA, D),
        // a ChangelogNormalize operator is inserted before aggregation
        assertThat(aggPlan).contains("ChangelogNormalize");

        // Expected aggregation results:
        // Category A: 120 (id=1) + 180 (id=3) = 300
        // Category B: 200 (id=2) = 200 (id=4 was deleted)
        List<String> expectedAggResults =
                Arrays.asList(
                        "+I[A, 100]",
                        "+I[B, 200]",
                        "-U[A, 100]",
                        "+U[A, 250]",
                        "-U[B, 200]",
                        "+U[B, 450]",
                        "-U[A, 250]",
                        "+U[A, 150]",
                        "-U[A, 150]",
                        "+U[A, 270]",
                        "-U[A, 270]",
                        "+U[A, 120]",
                        "-U[A, 120]",
                        "+U[A, 300]",
                        "-U[B, 450]",
                        "+U[B, 200]");

        // Collect results with timeout
        assertQueryResultExactOrder(tEnv, aggQuery, expectedAggResults);
    }

    @Test
    void testWalModeWithAutoIncrement() throws Exception {
        // use single parallelism to make result ordering stable
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        String tableName = "wal_mode_pk_table";
        // Create a table with WAL mode and auto increment column
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " id int not null,"
                                + " auto_increment_id bigint,"
                                + " amount bigint,"
                                + " primary key (id) not enforced"
                                + ") with ('table.changelog.image' = 'wal', 'auto-increment.fields'='auto_increment_id')",
                        tableName));

        // Insert initial data
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s (id, amount) VALUES "
                                        + "(1, 100), "
                                        + "(2, 200), "
                                        + "(3, 150), "
                                        + "(4, 250)",
                                tableName))
                .await();

        // Use batch mode to update and delete records

        // Upsert data, not support update/delete rows in table with auto-inc column for now.
        // TODO: Support Batch Update
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s (id, amount) VALUES " + "(1, 120), " + "(3, 180)",
                                tableName))
                .await();

        List<String> expectedResults =
                Arrays.asList(
                        "+I[1, 1, 100]",
                        "+I[2, 2, 200]",
                        "+I[3, 3, 150]",
                        "+I[4, 4, 250]",
                        "-U[1, 1, 100]",
                        "+U[1, 1, 120]",
                        "-U[3, 3, 150]",
                        "+U[3, 3, 180]");

        // Collect results with timeout
        assertQueryResultExactOrder(
                tEnv,
                String.format(
                        "SELECT id, auto_increment_id, amount FROM %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */",
                        tableName),
                expectedResults);
    }
}
