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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsExactOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for Flink union data in lake and fluss for log table. */
class FlinkUnionReadLogTableITCase extends FlinkUnionReadTestBase {

    @TempDir public static File savepointDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadLogTableFullType(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new ArrayList<>();
        long tableId = prepareLogTable(t1, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // now, start to read the log table, which will read paimon
        // may read fluss or not, depends on the log offset of paimon snapshot
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());

        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);

        // can database sync job
        jobClient.cancel().get();

        // write some log data again
        writtenRows.addAll(writeRows(t1, 3, isPartitioned));

        // query the log table again and check the data
        // it should read both paimon snapshot and fluss log
        actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);

        // test project push down
        actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select f_byte from " + tableName).collect());
        List<Row> expected =
                writtenRows.stream()
                        .map(row -> Row.of(row.getField(1)))
                        .collect(Collectors.toList());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        if (isPartitioned) {
            // get first partition
            String partition = waitUntilPartitions(t1).values().iterator().next();
            String sqlWithPartitionFilter =
                    "select * FROM " + tableName + " WHERE p = '" + partition + "'";

            String plan = batchTEnv.explainSql(sqlWithPartitionFilter);

            // check if the plan contains partition filter
            assertThat(plan)
                    .contains("TableSourceScan(")
                    .contains("filter=[=(p, _UTF-16LE'" + partition + "'");

            List<Row> expectedFiltered =
                    writtenRows.stream()
                            .filter(r -> partition.equals(r.getField(15)))
                            .collect(Collectors.toList());

            List<Row> actualFiltered =
                    CollectionUtil.iteratorToList(
                            batchTEnv.executeSql(sqlWithPartitionFilter).collect());

            assertThat(actualFiltered).containsExactlyInAnyOrderElementsOf(expectedFiltered);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadLogTableInStreamMode(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "stream_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new LinkedList<>();
        long tableId = prepareLogTable(t1, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // now, start to read the log table, which will read paimon
        // may read fluss or not, depends on the log offset of paimon snapshot
        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(
                actual, writtenRows.stream().map(Row::toString).collect(Collectors.toList()), true);

        // can database sync job
        jobClient.cancel().get();

        // write some log data again
        writtenRows.addAll(writeRows(t1, 3, isPartitioned));

        // query the log table again and check the data
        // it should read both paimon snapshot and fluss log
        actual =
                streamTEnv
                        .executeSql(
                                "select * from "
                                        + tableName
                                        + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                        .collect();
        if (isPartitioned) {
            // we write to a new partition to verify partition discovery
            writtenRows.addAll(writeFullTypeRows(t1, 10, "3027"));
        }
        assertResultsIgnoreOrder(
                actual, writtenRows.stream().map(Row::toString).collect(Collectors.toList()), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTableFailover(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName1 =
                "restore_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String resultTableName =
                "result_table" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath table1 = TablePath.of(DEFAULT_DB, tableName1);
        TablePath resultTable = TablePath.of(DEFAULT_DB, resultTableName);
        List<Row> writtenRows = new LinkedList<>();
        long tableId = prepareLogTable(table1, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(table1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        StreamTableEnvironment streamTEnv = buildSteamTEnv(null);
        // now, start to read the log table to write to a fluss result table
        // may read fluss or not, depends on the log offset of paimon snapshot
        createFullTypeLogTable(resultTable, DEFAULT_BUCKET_NUM, isPartitioned, false);
        TableResult insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName1);

        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + resultTableName).collect();
        assertResultsExactOrder(actual, writtenRows, false);

        // now, stop the job with save point
        String savepointPath =
                insertResult
                        .getJobClient()
                        .get()
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();

        // re buildSteamTEnv
        streamTEnv = buildSteamTEnv(savepointPath);
        insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName1);

        // write some log data again
        List<Row> rows = writeRows(table1, 3, isPartitioned);

        assertResultsExactOrder(actual, rows, true);

        // cancel jobs
        insertResult.getJobClient().get().cancel().get();
        jobClient.cancel().get();
    }

    private long prepareLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, List<Row> flinkRows)
            throws Exception {
        // createFullTypeLogTable creates a datalake-enabled table with a partition column.
        long t1Id = createFullTypeLogTable(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition :
                    partitionNameById.values().stream().sorted().collect(Collectors.toList())) {
                for (int i = 0; i < 3; i++) {
                    flinkRows.addAll(writeFullTypeRows(tablePath, 10, partition));
                }
            }
        } else {
            for (int i = 0; i < 3; i++) {
                flinkRows.addAll(writeFullTypeRows(tablePath, 10, null));
            }
        }
        return t1Id;
    }

    private List<Row> writeFullTypeRows(
            TablePath tablePath, int rowCount, @Nullable String partition) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            if (partition == null) {
                rows.add(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8}));

                flinkRows.add(
                        Row.of(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                new java.math.BigDecimal("9.00"),
                                new java.math.BigDecimal("1000"),
                                Instant.ofEpochMilli(1698235273400L),
                                Instant.ofEpochMilli(1698235273400L).plusNanos(7000),
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC")),
                                LocalDateTime.ofInstant(
                                                Instant.ofEpochMilli(1698235273501L),
                                                ZoneId.of("UTC"))
                                        .plusNanos(8000),
                                new byte[] {5, 6, 7, 8}));
            } else {
                rows.add(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                partition));

                flinkRows.add(
                        Row.of(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                new java.math.BigDecimal("9.00"),
                                new java.math.BigDecimal("1000"),
                                Instant.ofEpochMilli(1698235273400L),
                                Instant.ofEpochMilli(1698235273400L).plusNanos(7000),
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC")),
                                LocalDateTime.ofInstant(
                                                Instant.ofEpochMilli(1698235273501L),
                                                ZoneId.of("UTC"))
                                        .plusNanos(8000),
                                new byte[] {5, 6, 7, 8},
                                partition));
            }
        }
        writeRows(tablePath, rows, true);
        return flinkRows;
    }

    private List<Row> writeRows(TablePath tablePath, int rowCount, boolean isPartitioned)
            throws Exception {
        if (isPartitioned) {
            List<Row> rows = new ArrayList<>();
            for (String partition : waitUntilPartitions(tablePath).values()) {
                rows.addAll(writeFullTypeRows(tablePath, rowCount, partition));
            }
            return rows;
        } else {
            return writeFullTypeRows(tablePath, rowCount, null);
        }
    }
}
