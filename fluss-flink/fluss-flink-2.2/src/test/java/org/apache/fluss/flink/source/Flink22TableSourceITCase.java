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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link FlinkTableSource} in Flink 2.2. */
public class Flink22TableSourceITCase extends FlinkTableSourceITCase {

    @Test
    void testDeltaJoin() throws Exception {
        // start two jobs for this test: one for DML involving the delta join, and the other for DQL
        // to query the results of the sink table
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                // currently, delta join only support append-only source
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v1", 300L, 3, 30000L),
                        row(4, "v4", 400L, 4, 40000L));
        // write records
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                // currently, delta join only support append-only source
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v3", 200L, 2, 20000L),
                        row(3, "v4", 300L, 4, 30000L),
                        row(4, "v4", 500L, 4, 50000L));
        // write records
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c1, d1, c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

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
                        "+I[1, v1, 100, 1, 10000, 1, v1, 100, 1, 10000]",
                        "-U[1, v1, 100, 1, 10000, 1, v1, 100, 1, 10000]",
                        "+U[1, v1, 100, 1, 10000, 1, v1, 100, 1, 10000]",
                        "+I[2, v2, 200, 2, 20000, 2, v3, 200, 2, 20000]",
                        "-U[2, v2, 200, 2, 20000, 2, v3, 200, 2, 20000]",
                        "+U[2, v2, 200, 2, 20000, 2, v3, 200, 2, 20000]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithProjectionAndFilter() throws Exception {
        // start two jobs for this test: one for DML involving the delta join, and the other for DQL
        // to query the results of the sink table
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_proj";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v1", 300L, 3, 30000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_proj";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v3", 200L, 2, 20000L),
                        row(3, "v4", 300L, 4, 30000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_proj";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " a2 int, "
                                + " primary key (c1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Test with projection and filter
        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2 FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 WHERE a1 > 1",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected = Arrays.asList("+I[2, 200, 2]", "-U[2, 200, 2]", "+U[2, 200, 2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinFailsWhenFilterOnNonUpsertKeys() throws Exception {
        // When filtering on non-upsert-key columns with CDC sources,
        // the optimizer can't use DeltaJoin
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_force_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.delete.behavior' = 'IGNORE' "
                                + ")",
                        leftTableName));

        String rightTableName = "right_table_force_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.delete.behavior' = 'IGNORE' "
                                + ")",
                        rightTableName));

        String sinkTableName = "sink_table_force_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c1, d1, c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        // Use FORCE strategy - should throw exception
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Filter on e1 > e2, where e1 and e2 are NOT part of the upsert key
        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 WHERE e1 > e2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinWithLookupCache() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_cache";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 = Arrays.asList(row(1, 100L, 1));
        writeRows(conn, TablePath.of(DEFAULT_DB, leftTableName), rows1, false);

        String rightTableName = "right_table_cache";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row', "
                                + " 'lookup.cache' = 'partial', "
                                + " 'lookup.partial-cache.max-rows' = '100' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 = Arrays.asList(row(1, 100L, 1));
        writeRows(conn, TablePath.of(DEFAULT_DB, rightTableName), rows2, false);

        String sinkTableName = "sink_table_cache";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " a2 int, "
                                + " primary key (a1) NOT ENFORCED" // Dummy PK
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT T1.a1, T2.a2 FROM %s AS T1 INNER JOIN %s AS T2 ON T1.c1 = T2.c2 AND T1.d1 = T2.d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected = Arrays.asList("+I[1, 1]", "-U[1, 1]", "+U[1, 1]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithPrimaryKeyTableNoDeletes() throws Exception {
        // Test delta join with normal primary key tables (not first_row) using
        // table.delete.behavior=IGNORE
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_normal_pk";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.delete.behavior' = 'IGNORE' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v3", 300L, 3, 30000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_normal_pk";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.delete.behavior' = 'IGNORE' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v4", 200L, 2, 20000L),
                        row(3, "v5", 400L, 4, 40000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_normal_pk";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, b1, c1, d1, a2, b2 FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1, 100, 1, 1, v1]",
                        "-U[1, v1, 100, 1, 1, v1]",
                        "+U[1, v1, 100, 1, 1, v1]",
                        "+I[2, v2, 200, 2, 2, v4]",
                        "-U[2, v2, 200, 2, 2, v4]",
                        "+U[2, v2, 200, 2, 2, v4]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinOnBucketKey() throws Exception {
        // Test delta join on bucket key only (not full primary key)
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_bucket_key";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 100L, 2, 20000L),
                        row(3, "v3", 200L, 1, 30000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_bucket_key";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 =
                Arrays.asList(row(10, "r1", 100L, 5, 50000L), row(20, "r2", 200L, 6, 60000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_bucket_key";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " primary key (a1, a2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Join on bucket key only (c1 = c2), not full primary key
        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, b1, c1, a2, b2 FROM %s INNER JOIN %s ON c1 = c2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[=(c1, c2)]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        // Each left row with c1=100 joins with the right row with c2=100
        // Each left row with c1=200 joins with the right row with c2=200
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1, 100, 10, r1]",
                        "-U[1, v1, 100, 10, r1]",
                        "+U[1, v1, 100, 10, r1]",
                        "+I[2, v2, 100, 10, r1]",
                        "-U[2, v2, 100, 10, r1]",
                        "+U[2, v2, 100, 10, r1]",
                        "+I[3, v3, 200, 20, r2]",
                        "-U[3, v3, 200, 20, r2]",
                        "+U[3, v3, 200, 20, r2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinFailsWhenSourceHasDelete() throws Exception {
        // When source can produce DELETE records, DeltaJoin is not applicable
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        // No merge-engine or delete.behavior - regular PK tables with full CDC
        String leftTableName = "left_table_delete_force";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1' "
                                + ")",
                        leftTableName));

        String rightTableName = "right_table_delete_force";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2' "
                                + ")",
                        rightTableName));

        String sinkTableName = "sink_table_delete_force";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c1, d1, c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWhenJoinKeyNotContainIndex() throws Exception {
        // When join key doesn't include at least one complete index, DeltaJoin isn't applicable
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_no_idx_force";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));

        String rightTableName = "right_table_no_idx_force";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));

        String sinkTableName = "sink_table_no_idx_force";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (a1, a2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Join on a1 = a2, but index is on c1/c2 (bucket.key), not a1/a2
        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON a1 = a2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithLeftJoin() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_left_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));

        String rightTableName = "right_table_left_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));

        String sinkTableName = "sink_table_left_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " a2 int, "
                                + " primary key (c1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2 FROM %s LEFT JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithRightJoin() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_right_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));

        String rightTableName = "right_table_right_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));

        String sinkTableName = "sink_table_right_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c2 bigint, "
                                + " a2 int, "
                                + " primary key (c2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c2, a2 FROM %s RIGHT JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithFullOuterJoin() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_full_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));

        String rightTableName = "right_table_full_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));

        String sinkTableName = "sink_table_full_fail";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " c2 bigint, "
                                + " a2 int, "
                                + " primary key (c1, c2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, c2, a2 FROM %s FULL OUTER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    @Test
    void testDeltaJoinFailsWithCascadeJoin() throws Exception {
        // DeltaJoin requires that all inputs must come directly from supported upstream nodes
        // (TableSourceScan, Exchange, DropUpdateBefore or Calc)
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String table1 = "cascade_table1";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        table1));

        String table2 = "cascade_table2";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        table2));

        String table3 = "cascade_table3";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a3 int, "
                                + " c3 bigint, "
                                + " d3 int, "
                                + " primary key (c3, d3) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c3', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        table3));

        String sinkTableName = "cascade_sink";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " a2 int, "
                                + " c2 bigint, "
                                + " a3 int, "
                                + " c3 bigint, "
                                + " primary key (c1, c2, c3) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Cascade join: (table1 JOIN table2) JOIN table3
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
}
