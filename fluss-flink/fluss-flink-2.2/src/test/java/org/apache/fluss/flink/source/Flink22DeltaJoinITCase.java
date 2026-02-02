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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for Delta Join optimization in Flink 2.2. */
public class Flink22DeltaJoinITCase extends FlinkTestBase {

    private static final String CATALOG_NAME = "test_catalog";

    private StreamTableEnvironment tEnv;

    @BeforeEach
    public void beforeEach() {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.useCatalog(CATALOG_NAME);
        tEnv.executeSql(String.format("create database if not exists `%s`", DEFAULT_DB));
        tEnv.useDatabase(DEFAULT_DB);

        // start two jobs for this test: one for DML involving the delta join, and the other for DQL
        // to query the results of the sink table
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        // Set FORCE strategy for delta join
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database `%s` cascade", DEFAULT_DB));
    }

    /**
     * Creates a source table with specified schema and options.
     *
     * @param tableName the name of the table
     * @param columns column definitions (e.g., "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint")
     * @param primaryKey primary key columns (e.g., "c1, d1")
     * @param bucketKey bucket key column (e.g., "c1")
     * @param extraOptions additional WITH options (e.g., "lookup.cache" = "partial"), or null
     */
    private void createSource(
            String tableName,
            String columns,
            String primaryKey,
            String bucketKey,
            @Nullable Map<String, String> extraOptions) {
        Map<String, String> withOptions = new HashMap<>();
        if (extraOptions != null) {
            withOptions.putAll(extraOptions);
        }
        withOptions.put("connector", "fluss");
        withOptions.put("bucket.key", bucketKey);

        tEnv.executeSql(
                String.format(
                        "create table %s ( %s, primary key (%s) NOT ENFORCED ) with ( %s )",
                        tableName,
                        columns,
                        primaryKey,
                        withOptions.entrySet().stream()
                                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(", "))));
    }

    /**
     * Creates a sink table with specified columns.
     *
     * @param tableName the name of the table
     * @param columns the column definitions (e.g., "a1 int, c1 bigint, a2 int")
     * @param primaryKey the primary key columns (e.g., "c1, d1, c2, d2")
     */
    private void createSink(String tableName, String columns, String primaryKey) {
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " %s, "
                                + " primary key (%s) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss'"
                                + ")",
                        tableName, columns, primaryKey));
    }

    @Test
    void testDeltaJoin() throws Exception {
        // disable cache to get stable results with updating
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, false);
        String leftTableName = "left_table";
        String rightTableName = "right_table";
        String sinkTableName = "sink_table";

        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));
        createSink(
                sinkTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint, a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c1, d1, c2, d2");

        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 100L, 2, 20000L),
                        row(3, "v3", 300L, 3, 30000L),
                        row(4, "v4", 400L, 4, 40000L),
                        // update
                        row(5, "v5", 100L, 1, 50000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);
        // wait for the first snapshot to finish to get the stable result
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(leftTablePath);

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v3", 330L, 4, 30000L),
                        row(4, "v4", 500L, 4, 50000L),
                        // update
                        row(6, "v6", 100L, 1, 60000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);
        // wait for the first snapshot to finish to get the stable result
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(rightTablePath);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[(c1 = c2)]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[5, v5, 100, 1, 50000, 6, v6, 100, 1, 60000]",
                        "-U[5, v5, 100, 1, 50000, 6, v6, 100, 1, 60000]",
                        "+U[5, v5, 100, 1, 50000, 6, v6, 100, 1, 60000]",
                        "+I[2, v2, 100, 2, 20000, 6, v6, 100, 1, 60000]",
                        "-U[2, v2, 100, 2, 20000, 6, v6, 100, 1, 60000]",
                        "+U[2, v2, 100, 2, 20000, 6, v6, 100, 1, 60000]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinOnPrimaryKey() throws Exception {
        // disable cache to get stable results with updating
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, false);
        String leftTableName = "left_table";
        String rightTableName = "right_table";
        String sinkTableName = "sink_table";

        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));
        createSink(
                sinkTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint, a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c1, d1, c2, d2");

        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v3", 300L, 3, 30000L),
                        row(4, "v4", 400L, 4, 40000L),
                        // update
                        row(5, "v5", 100L, 1, 50000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);
        // wait for the first snapshot to finish to get the stable result
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(leftTablePath);

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v3", 300L, 4, 30000L),
                        row(4, "v4", 500L, 4, 50000L),
                        // update
                        row(6, "v6", 100L, 1, 60000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);
        // wait for the first snapshot to finish to get the stable result
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(rightTablePath);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[5, v5, 100, 1, 50000, 6, v6, 100, 1, 60000]",
                        "-U[5, v5, 100, 1, 50000, 6, v6, 100, 1, 60000]",
                        "+U[5, v5, 100, 1, 50000, 6, v6, 100, 1, 60000]",
                        "+I[2, v2, 200, 2, 20000, 2, v2, 200, 2, 20000]",
                        "-U[2, v2, 200, 2, 20000, 2, v2, 200, 2, 20000]",
                        "+U[2, v2, 200, 2, 20000, 2, v2, 200, 2, 20000]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithCalc() throws Exception {
        String leftTableName = "left_table_proj";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v1", 300L, 3, 30000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_proj";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v3", 300L, 4, 30000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_proj";
        createSink(sinkTableName, "a1 int, c1 bigint, d1 int, a2 int", "c1, d1");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, d1, a2 FROM ("
                                + " SELECT * FROM %s WHERE d1 > 1"
                                + ") INNER JOIN ("
                                + " SELECT * FROM %s WHERE c2 < 300"
                                + ") ON c1 = c2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[(c1 = c2)]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList("+I[2, 200, 2, 2]", "-U[2, 200, 2, 2]", "+U[2, 200, 2, 2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithAppendOnlySourceAndCalc() throws Exception {
        String leftTableName = "left_table_proj";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.merge-engine", "first_row"));

        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v1", 300L, 3, 30000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_proj";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.merge-engine", "first_row"));

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v3", 200L, 2, 20000L),
                        row(3, "v4", 400L, 4, 30000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_proj";
        createSink(sinkTableName, "a1 int, c1 bigint, a2 int", "c1");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2 FROM %s INNER JOIN %s ON c1 = c2 WHERE a1 > 1",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[(c1 = c2)]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected = Arrays.asList("+I[2, 200, 2]", "-U[2, 200, 2]", "+U[2, 200, 2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinFailsWhenFilterOnNonUpsertKeys() {
        String leftTableName = "left_table_force_fail";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String rightTableName = "right_table_force_fail";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String sinkTableName = "sink_table_force_fail";
        createSink(
                sinkTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint, a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c1, d1, c2, d2");

        // Filter on e1 > e2, where e1 and e2 are NOT part of the upsert key
        // TODO we can add a UpsertFilterOperator that can convert the un-match-filter UPSERT record
        //  into DELETE record.
        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 WHERE e1 > e2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");

        // Non-equiv-cond on e1 > e2, where e1 and e2 are NOT part of the upsert key
        String sql2 =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 AND e1 > e2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql2))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinWithNonEquiConditionOnUpsertKeys() throws Exception {
        String leftTableName = "left_table_nonequi_upsert";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1, e1",
                "c1, d1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20001L),
                        row(3, "v3", 300L, 3, 30000L),
                        // Add row with same PK (100, 1) to generate UPDATE in CDC mode
                        // This row is filtered out by (e2 / 100) <> c2, so doesn't affect join
                        // result
                        row(4, "v1_updated", 100L, 1, 10000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_nonequi_upsert";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v4", 200L, 2, 20000L),
                        row(3, "v5", 300L, 4, 40000L),
                        // Add row with same PK (100, 1) to generate UPDATE in CDC mode
                        // This row is filtered out by (e2 / 100) <> c2, so doesn't affect join
                        // result
                        row(5, "v1_updated", 100L, 1, 10000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_nonequi_upsert";
        createSink(sinkTableName, "a1 int, c1 bigint, d1 int, e1 bigint, a2 int", "c1, d1, e1");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, d1, e1, a2 FROM %s INNER JOIN %s "
                                + "ON c1 = c2 AND d1 = d2 AND e1 <> (c2 * 100)",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains(
                        "DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2) AND (e1 <> (c2 * 100)))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[2, 200, 2, 20001, 2]",
                        "-U[2, 200, 2, 20001, 2]",
                        "+U[2, 200, 2, 20001, 2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithAppendOnlySourceAndNonEquiCondition() throws Exception {
        String leftTableName = "left_table_nonequi_insert";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.merge-engine", "first_row"));

        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v3", 300L, 3, 5000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_nonequi_insert";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.merge-engine", "first_row"));

        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 8000L),
                        row(2, "v4", 200L, 2, 15000L),
                        row(3, "v5", 300L, 3, 3000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_nonequi_insert";
        createSink(
                sinkTableName, "a1 int, c1 bigint, d1 int, e1 bigint, a2 int, e2 bigint", "c1, d1");

        // INSERT_ONLY sources with non-equi condition on non-upsert key fields (e1, e2)
        // This should succeed because INSERT_ONLY mode allows any non-equi conditions
        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, d1, e1, a2, e2 FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 AND e1 > e2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains(
                        "DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2) AND (e1 > e2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        // Rows where e1 > e2:
        // Row 1: e1=10000 > e2=8000 ✓
        // Row 2: e1=20000 > e2=15000 ✓
        // Row 3: e1=5000 > e2=3000 ✓
        List<String> expected =
                Arrays.asList(
                        "+I[1, 100, 1, 10000, 1, 8000]",
                        "-U[1, 100, 1, 10000, 1, 8000]",
                        "+U[1, 100, 1, 10000, 1, 8000]",
                        "+I[2, 200, 2, 20000, 2, 15000]",
                        "-U[2, 200, 2, 20000, 2, 15000]",
                        "+U[2, 200, 2, 20000, 2, 15000]",
                        "+I[3, 300, 3, 5000, 3, 3000]",
                        "-U[3, 300, 3, 5000, 3, 3000]",
                        "+U[3, 300, 3, 5000, 3, 3000]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithLookupCache() throws Exception {
        String leftTableName = "left_table_cache";
        createSource(
                leftTableName,
                "a1 int, c1 bigint, d1 int",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));
        List<InternalRow> rows1 = Collections.singletonList(row(1, 100L, 1));
        writeRows(conn, TablePath.of(DEFAULT_DB, leftTableName), rows1, false);

        String rightTableName = "right_table_cache";
        createSource(
                rightTableName,
                "a2 int, c2 bigint, d2 int",
                "c2",
                "c2",
                ImmutableMap.of(
                        "table.delete.behavior",
                        "IGNORE",
                        "lookup.cache",
                        "partial",
                        "lookup.partial-cache.max-rows",
                        "100"));
        List<InternalRow> rows2 = Collections.singletonList(row(1, 100L, 1));
        writeRows(conn, TablePath.of(DEFAULT_DB, rightTableName), rows2, false);

        String sinkTableName = "sink_table_cache";
        createSink(sinkTableName, "a1 int, c1 bigint, d1 int, a2 int", "c1, d1");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, d1, a2 FROM %s AS T1 INNER JOIN %s AS T2 ON T1.c1 = T2.c2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[(c1 = c2)]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList("+I[1, 100, 1, 1]", "-U[1, 100, 1, 1]", "+U[1, 100, 1, 1]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinFailsWhenSourceHasDelete() {
        String leftTableName = "left_table_delete_force";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                null);

        String rightTableName = "right_table_delete_force";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                null);

        String sinkTableName = "sink_table_delete_force";
        createSink(
                sinkTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint, a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c1, d1, c2, d2");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinWithJoinKeyExceedsPrimaryKey() {
        String leftTableName = "left_table_exceed_pk";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String rightTableName = "right_table_exceed_pk";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String sinkTableName = "sink_table_exceed_pk";
        createSink(
                sinkTableName, "a1 int, c1 bigint, d1 int, e1 bigint, a2 int, e2 bigint", "c1, d1");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, d1, e1, a2, e2 FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 AND e1 = e2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinWithAppendOnlySourceAndJoinKeyExceedsPrimaryKey() throws Exception {
        String leftTableName = "left_table_exceed_pk";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.merge-engine", "first_row"));

        List<InternalRow> rows1 =
                Arrays.asList(row(1, "v1", 100L, 1, 10000L), row(2, "v2", 200L, 2, 20000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_exceed_pk";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.merge-engine", "first_row"));

        List<InternalRow> rows2 =
                Arrays.asList(row(1, "v1", 100L, 1, 10000L), row(2, "v3", 200L, 2, 99999L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_exceed_pk";
        createSink(
                sinkTableName, "a1 int, c1 bigint, d1 int, e1 bigint, a2 int, e2 bigint", "c1, d1");

        // Join on PK (c1, d1) + additional non-PK field (e1)
        // This should succeed because join keys {c1, d1, e1} contain complete PK {c1, d1}
        // The e1 = e2 condition is applied as a post-lookup equi-condition filter
        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, d1, e1, a2, e2 FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 AND e1 = e2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains(
                        "DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2) AND (e1 = e2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        // Only first row should match (e1 = e2 = 10000)
        // Second row filtered out (e1 = 20000 AND != e2 = 99999)
        List<String> expected =
                Arrays.asList(
                        "+I[1, 100, 1, 10000, 1, 10000]",
                        "-U[1, 100, 1, 10000, 1, 10000]",
                        "+U[1, 100, 1, 10000, 1, 10000]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinFailsWhenJoinKeyNotContainIndex() {
        String leftTableName = "left_table_no_idx_force";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String rightTableName = "right_table_no_idx_force";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String sinkTableName = "sink_table_no_idx_force";
        createSink(
                sinkTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint, a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "a1, a2");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON a1 = a2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithOuterJoin() {
        String leftTableName = "left_table_outer_fail";
        createSource(
                leftTableName,
                "a1 int, c1 bigint, d1 int",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String rightTableName = "right_table_outer_fail";
        createSource(
                rightTableName,
                "a2 int, c2 bigint, d2 int",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String sinkTableName = "sink_table_outer_fail";
        createSink(sinkTableName, "a1 int, c1 bigint, a2 int", "c1");

        // Test LEFT JOIN
        String leftJoinSql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2 FROM %s LEFT JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(leftJoinSql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");

        // Test RIGHT JOIN
        String rightJoinSql =
                String.format(
                        "INSERT INTO %s SELECT a1, c2, a2 FROM %s RIGHT JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(rightJoinSql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");

        // Test FULL OUTER JOIN
        String fullOuterJoinSql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2 FROM %s FULL OUTER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(fullOuterJoinSql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithCascadeJoin() {
        String table1 = "cascade_table1";
        createSource(
                table1,
                "a1 int, c1 bigint, d1 int",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String table2 = "cascade_table2";
        createSource(
                table2,
                "a2 int, c2 bigint, d2 int",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String table3 = "cascade_table3";
        createSource(
                table3,
                "a3 int, c3 bigint, d3 int",
                "c3, d3",
                "c3",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String sinkTableName = "cascade_sink";
        createSink(
                sinkTableName,
                "a1 int, c1 bigint, a2 int, c2 bigint, a3 int, c3 bigint",
                "c1, c2, c3");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2, c2, a3, c3 "
                                + "FROM %s "
                                + "INNER JOIN %s ON c1 = c2 AND d1 = d2 "
                                + "INNER JOIN %s ON c1 = c3 AND d1 = d3",
                        sinkTableName, table1, table2, table3);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithSinkMaterializer() {
        // With CDC sources, when sink PK doesn't match upstream update key,
        // Flink would insert SinkUpsertMaterializer which prevents delta join
        String leftTableName = "left_table_materializer";
        createSource(
                leftTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String rightTableName = "right_table_materializer";
        createSource(
                rightTableName,
                "a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        // Sink PK (a1, a2) doesn't match upstream update key (c1, d1, c2, d2)
        // TODO: this depends on Fluss supports MVCC/point-in-time lookup to support change upsert
        //  keys
        String sinkTableName = "sink_table_materializer";
        createSink(
                sinkTableName,
                "a1 int, b1 varchar, c1 bigint, d1 int, e1 bigint, a2 int, b2 varchar, c2 bigint, d2 int, e2 bigint",
                "a1, a2");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithNonDeterministicFunctions() {
        String leftTableName = "left_table_nondeterministic";
        createSource(
                leftTableName,
                "a1 int, c1 bigint, d1 int",
                "c1, d1",
                "c1",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String rightTableName = "right_table_nondeterministic";
        createSource(
                rightTableName,
                "a2 int, c2 bigint, d2 int",
                "c2, d2",
                "c2",
                ImmutableMap.of("table.delete.behavior", "IGNORE"));

        String sinkTableName = "sink_table_nondeterministic";
        // TODO this should be supported in Flink in future for non-deterministic functions before
        //  sinking
        createSink(sinkTableName, "c1 bigint, d1 bigint, rand_val double", "c1");

        String sql =
                String.format(
                        "INSERT INTO %s SELECT c1, d1, RAND() FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }
}
