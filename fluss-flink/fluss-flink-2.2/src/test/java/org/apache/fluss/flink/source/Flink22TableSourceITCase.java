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
}
