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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsExactOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertRowResultsIgnoreOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for Flink union data in lake and fluss for primary key table. */
class FlinkUnionReadPrimaryKeyTableITCase extends FlinkUnionReadTestBase {

    @TempDir public static File savepointDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadFullType(Boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "pk_table_full" + (isPartitioned ? "_partitioned" : "_non_partitioned");
        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        // create table & write initial data
        long tableId =
                preparePKTableFullType(t1, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // wait unit records have been synced
        waitUntilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // check the status of replica after synced
        assertReplicaStatus(bucketLogEndOffset);

        List<String> partitions = new ArrayList<>();
        if (isPartitioned) {
            partitions.addAll(waitUntilPartitions(t1).values());
            Collections.sort(partitions);
        }

        // will read paimon snapshot, won't merge log since it's empty
        List<String> resultEmptyLog =
                toSortedRows(batchTEnv.executeSql("select * from " + tableName));
        String expetedResultFromPaimon = buildExpectedResult(isPartitioned, partitions, 0, 1);
        assertThat(resultEmptyLog.toString().replace("+U", "+I"))
                .isEqualTo(expetedResultFromPaimon);

        // read paimon directly using $lake
        TableResult tableResult =
                batchTEnv.executeSql(String.format("select * from %s$lake", tableName));
        List<String> paimonSnapshotRows =
                CollectionUtil.iteratorToList(tableResult.collect()).stream()
                        .map(
                                row -> {
                                    int userColumnCount = row.getArity() - 3;
                                    Object[] fields = new Object[userColumnCount];
                                    for (int i = 0; i < userColumnCount; i++) {
                                        fields[i] = row.getField(i);
                                    }
                                    return Row.of(fields);
                                })
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        // paimon's source will emit +U[0, v0, xx] instead of +I[0, v0, xx], so
        // replace +U with +I to make it equal
        assertThat(paimonSnapshotRows.toString().replace("+U", "+I"))
                .isEqualTo(expetedResultFromPaimon);

        // test point query with fluss
        String queryFilterStr = "c4 = 30";
        String partitionName =
                isPartitioned ? waitUntilPartitions(t1).values().iterator().next() : null;
        if (partitionName != null) {
            queryFilterStr = queryFilterStr + " and c19= '" + partitionName + "'";
        }

        Map<String, Integer> flinkMap1 = new HashMap<>();
        flinkMap1.put("key1", 1);
        flinkMap1.put("key2", 2);

        Map<String, Integer> flinkMap2 = new HashMap<>();
        flinkMap2.put("key3", 3);
        flinkMap2.put("key4", 4);

        List<Row> expectedRows = new ArrayList<>();
        if (isPartitioned) {
            for (String partition : waitUntilPartitions(t1).values()) {
                expectedRows.add(
                        Row.of(
                                false,
                                (byte) 1,
                                (short) 2,
                                3,
                                4L,
                                5.1f,
                                6.0d,
                                "string",
                                Decimal.fromUnscaledLong(9, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273182L, 0),
                                TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                TimestampNtz.fromMillis(1698235273183L, 0),
                                TimestampNtz.fromMillis(1698235273183L, 6000),
                                new byte[] {1, 2, 3, 4},
                                new float[] {1.1f, 1.2f, 1.3f},
                                Row.of(100, "nested_value_1", 3.14),
                                flinkMap1,
                                partition));

                expectedRows.add(
                        Row.of(
                                true,
                                (byte) 10,
                                (short) 20,
                                30,
                                40L,
                                50.1f,
                                60.0d,
                                "another_string",
                                Decimal.fromUnscaledLong(90, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273200L),
                                TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                                TimestampNtz.fromMillis(1698235273201L),
                                TimestampNtz.fromMillis(1698235273201L, 6000),
                                new byte[] {1, 2, 3, 4},
                                new float[] {1.1f, 1.2f, 1.3f},
                                Row.of(200, "nested_value_2", 6.28),
                                flinkMap2,
                                partition));
            }
        } else {
            expectedRows =
                    Arrays.asList(
                            Row.of(
                                    false,
                                    (byte) 1,
                                    (short) 2,
                                    3,
                                    4L,
                                    5.1f,
                                    6.0d,
                                    "string",
                                    Decimal.fromUnscaledLong(9, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273182L, 0),
                                    TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                    TimestampNtz.fromMillis(1698235273183L, 0),
                                    TimestampNtz.fromMillis(1698235273183L, 6000),
                                    new byte[] {1, 2, 3, 4},
                                    new float[] {1.1f, 1.2f, 1.3f},
                                    Row.of(100, "nested_value_1", 3.14),
                                    null),
                            Row.of(
                                    true,
                                    (byte) 10,
                                    (short) 20,
                                    30,
                                    40L,
                                    50.1f,
                                    60.0d,
                                    "another_string",
                                    Decimal.fromUnscaledLong(90, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273200L),
                                    TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                                    TimestampNtz.fromMillis(1698235273201L),
                                    TimestampNtz.fromMillis(1698235273201L, 6000),
                                    new byte[] {1, 2, 3, 4},
                                    new float[] {1.1f, 1.2f, 1.3f},
                                    Row.of(200, "nested_value_2", 6.28),
                                    flinkMap2,
                                    null));
        }
        tableResult =
                batchTEnv.executeSql(
                        String.format("select * from %s where %s", tableName, queryFilterStr));

        List<String> flussPointQueryRows = toSortedRows(tableResult);
        List<String> expectedPointQueryRows =
                expectedRows.stream()
                        .filter(
                                row -> {
                                    boolean isMatch = row.getField(3).equals(30);
                                    if (partitionName != null) {
                                        isMatch = isMatch && row.getField(18).equals(partitionName);
                                    }
                                    return isMatch;
                                })
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(flussPointQueryRows).isEqualTo(expectedPointQueryRows);

        // test point query with paimon
        List<String> paimonPointQueryRows =
                CollectionUtil.iteratorToList(
                                batchTEnv
                                        .executeSql(
                                                String.format(
                                                        "select * from %s$lake where %s",
                                                        tableName, queryFilterStr))
                                        .collect())
                        .stream()
                        .map(
                                row -> {
                                    int columnCount = row.getArity() - 3;
                                    Object[] fields = new Object[columnCount];
                                    for (int i = 0; i < columnCount; i++) {
                                        fields[i] = row.getField(i);
                                    }
                                    return Row.of(fields);
                                })
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(paimonPointQueryRows).isEqualTo(expectedPointQueryRows);

        // read paimon system table
        List<String> paimonOptionsRows =
                toSortedRows(
                        batchTEnv.executeSql(
                                String.format("select * from %s$lake$options", tableName)));
        assertThat(paimonOptionsRows.toString()).contains("+I[bucket, 1], +I[bucket-key, c4]");

        // stop lake tiering service
        jobClient.cancel().get();

        // write a row again
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(t1);
            for (String partition : partitionNameById.values()) {
                writeFullTypeRow(t1, partition);
            }
        } else {
            writeFullTypeRow(t1, null);
        }

        Map<String, Integer> flinkMap3 = new HashMap<>();
        flinkMap3.put("key5", 5);
        flinkMap3.put("key6", 6);

        expectedRows = new ArrayList<>();
        if (isPartitioned) {
            for (String partition : waitUntilPartitions(t1).values()) {
                expectedRows.add(
                        Row.of(
                                false,
                                (byte) 1,
                                (short) 2,
                                3,
                                4L,
                                5.1f,
                                6.0d,
                                "string",
                                Decimal.fromUnscaledLong(9, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273182L),
                                TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                TimestampNtz.fromMillis(1698235273183L),
                                TimestampNtz.fromMillis(1698235273183L, 6000),
                                new byte[] {1, 2, 3, 4},
                                new float[] {1.1f, 1.2f, 1.3f},
                                Row.of(100, "nested_value_1", 3.14),
                                flinkMap1,
                                partition));

                expectedRows.add(
                        Row.of(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_2",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                new float[] {2.1f, 2.2f, 2.3f},
                                Row.of(300, "nested_value_3", 9.99),
                                flinkMap3,
                                partition));
            }
        } else {
            expectedRows =
                    Arrays.asList(
                            Row.of(
                                    false,
                                    (byte) 1,
                                    (short) 2,
                                    3,
                                    4L,
                                    5.1f,
                                    6.0d,
                                    "string",
                                    Decimal.fromUnscaledLong(9, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273182L),
                                    TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                    TimestampNtz.fromMillis(1698235273183L),
                                    TimestampNtz.fromMillis(1698235273183L, 6000),
                                    new byte[] {1, 2, 3, 4},
                                    new float[] {1.1f, 1.2f, 1.3f},
                                    Row.of(100, "nested_value_1", 3.14),
                                    null),
                            Row.of(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    30,
                                    400L,
                                    500.1f,
                                    600.0d,
                                    "another_string_2",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    new float[] {2.1f, 2.2f, 2.3f},
                                    Row.of(300, "nested_value_3", 9.99),
                                    flinkMap3,
                                    null));
        }

        // now, query the result, it must be the union result of lake snapshot and log
        List<String> result = toSortedRows(batchTEnv.executeSql("select * from " + tableName));
        String expectedResult = buildExpectedResult(isPartitioned, partitions, 0, 2);
        assertThat(result.toString().replace("+U", "+I")).isEqualTo(expectedResult);

        // query with project push down
        List<String> projectRows =
                toSortedRows(batchTEnv.executeSql("select c3, c4 from " + tableName));
        List<Row> expectedProjectRows =
                expectedRows.stream()
                        .map(
                                row ->
                                        Row.of(
                                                row.getField(2), // c3
                                                row.getField(3))) // c4
                        .collect(Collectors.toList());
        assertThat(projectRows.toString()).isEqualTo(sortedRows(expectedProjectRows).toString());
        // query with project push down
        List<String> projectRows2 =
                toSortedRows(batchTEnv.executeSql("select c3 from " + tableName));
        List<Row> expectedProjectRows2 =
                expectedRows.stream()
                        .map(row -> Row.of(row.getField(2))) // c3
                        .collect(Collectors.toList());
        assertThat(projectRows2.toString()).isEqualTo(sortedRows(expectedProjectRows2).toString());
    }

    @Test
    void testUnionReadWhenSomeBucketNotTiered() throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "pk_table_union_read_some_bucket_not_tiered";
        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        int bucketNum = 3;
        // create table & write initial data
        long tableId = createSimplePkTable(t1, bucketNum, false, true);

        writeRows(
                t1,
                Arrays.asList(
                        GenericRow.of(
                                1, BinaryString.fromString("v11"), BinaryString.fromString("v12")),
                        GenericRow.of(
                                2, BinaryString.fromString("v21"), BinaryString.fromString("v22"))),
                false);

        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 1L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 1L);

        // wait unit records have been synced
        waitUntilBucketsSynced(bucketLogEndOffset.keySet());

        // check the status of replica after synced
        assertReplicaStatus(bucketLogEndOffset);

        jobClient.cancel().get();
        writeRows(
                t1,
                Arrays.asList(
                        GenericRow.of(
                                0, BinaryString.fromString("v01"), BinaryString.fromString("v02")),
                        GenericRow.of(
                                3, BinaryString.fromString("v31"), BinaryString.fromString("v32"))),
                false);

        List<String> result = toSortedRows(batchTEnv.executeSql("select * from " + tableName));
        assertThat(result.toString())
                .isEqualTo("[+I[0, v01, v02], +I[1, v11, v12], +I[2, v21, v22], +I[3, v31, v32]]");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadInStreamMode(Boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName =
                "stream_pk_table_full" + (isPartitioned ? "_partitioned" : "_non_partitioned");
        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        // create table & write initial data
        long tableId =
                preparePKTableFullType(t1, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // wait unit records have been synced
        waitUntilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // check the status of replica after synced
        assertReplicaStatus(bucketLogEndOffset);

        Map<String, Integer> streamMap1 = new HashMap<>();
        streamMap1.put("key1", 1);
        streamMap1.put("key2", 2);

        Map<String, Integer> streamMap2 = new HashMap<>();
        streamMap2.put("key3", 3);
        streamMap2.put("key4", 4);

        // will read paimon snapshot, should only +I since no change log
        List<Row> expectedRows = new ArrayList<>();
        if (isPartitioned) {
            for (String partition : waitUntilPartitions(t1).values()) {
                expectedRows.add(
                        Row.of(
                                false,
                                (byte) 1,
                                (short) 2,
                                3,
                                4L,
                                5.1f,
                                6.0d,
                                "string",
                                Decimal.fromUnscaledLong(9, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273182L),
                                TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                TimestampNtz.fromMillis(1698235273183L),
                                TimestampNtz.fromMillis(1698235273183L, 6000),
                                new byte[] {1, 2, 3, 4},
                                new float[] {1.1f, 1.2f, 1.3f},
                                Row.of(100, "nested_value_1", 3.14),
                                streamMap1,
                                partition));
                expectedRows.add(
                        Row.of(
                                true,
                                (byte) 10,
                                (short) 20,
                                30,
                                40L,
                                50.1f,
                                60.0d,
                                "another_string",
                                Decimal.fromUnscaledLong(90, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273200L),
                                TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                                TimestampNtz.fromMillis(1698235273201L),
                                TimestampNtz.fromMillis(1698235273201L, 6000),
                                new byte[] {1, 2, 3, 4},
                                new float[] {1.1f, 1.2f, 1.3f},
                                Row.of(200, "nested_value_2", 6.28),
                                streamMap2,
                                partition));
            }
        } else {
            expectedRows =
                    Arrays.asList(
                            Row.of(
                                    false,
                                    (byte) 1,
                                    (short) 2,
                                    3,
                                    4L,
                                    5.1f,
                                    6.0d,
                                    "string",
                                    Decimal.fromUnscaledLong(9, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273182L),
                                    TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                    TimestampNtz.fromMillis(1698235273183L),
                                    TimestampNtz.fromMillis(1698235273183L, 6000),
                                    new byte[] {1, 2, 3, 4},
                                    new float[] {1.1f, 1.2f, 1.3f},
                                    Row.of(100, "nested_value_1", 3.14),
                                    streamMap1,
                                    null),
                            Row.of(
                                    true,
                                    (byte) 10,
                                    (short) 20,
                                    30,
                                    40L,
                                    50.1f,
                                    60.0d,
                                    "another_string",
                                    Decimal.fromUnscaledLong(90, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273200L),
                                    TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                                    TimestampNtz.fromMillis(1698235273201L),
                                    TimestampNtz.fromMillis(1698235273201L, 6000),
                                    new byte[] {1, 2, 3, 4},
                                    new float[] {1.1f, 1.2f, 1.3f},
                                    Row.of(200, "nested_value_2", 6.28),
                                    streamMap2,
                                    null));
        }

        String query = "select * from " + tableName;
        CloseableIterator<Row> actual = streamTEnv.executeSql(query).collect();
        assertRowResultsIgnoreOrder(actual, expectedRows, false);

        // stop lake tiering service
        jobClient.cancel().get();

        // write a row again
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(t1);
            for (String partition : partitionNameById.values()) {
                writeFullTypeRow(t1, partition);
            }
        } else {
            writeFullTypeRow(t1, null);
        }

        Map<String, Integer> streamMap3 = new HashMap<>();
        streamMap3.put("key5", 5);
        streamMap3.put("key6", 6);

        // should generate -U & +U
        List<Row> expectedRows2 = new ArrayList<>();
        if (isPartitioned) {
            for (String partition : waitUntilPartitions(t1).values()) {
                expectedRows2.add(
                        Row.ofKind(
                                RowKind.UPDATE_BEFORE,
                                true,
                                (byte) 10,
                                (short) 20,
                                30,
                                40L,
                                50.1f,
                                60.0d,
                                "another_string",
                                Decimal.fromUnscaledLong(90, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273200L),
                                TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                                TimestampNtz.fromMillis(1698235273201L),
                                TimestampNtz.fromMillis(1698235273201L, 6000),
                                new byte[] {1, 2, 3, 4},
                                new float[] {1.1f, 1.2f, 1.3f},
                                Row.of(200, "nested_value_2", 6.28),
                                streamMap2,
                                partition));
                expectedRows2.add(
                        Row.ofKind(
                                RowKind.UPDATE_AFTER,
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_2",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                new float[] {2.1f, 2.2f, 2.3f},
                                Row.of(300, "nested_value_3", 9.99),
                                streamMap3,
                                partition));
            }
        } else {
            expectedRows2.add(
                    Row.ofKind(
                            RowKind.UPDATE_BEFORE,
                            true,
                            (byte) 10,
                            (short) 20,
                            30,
                            40L,
                            50.1f,
                            60.0d,
                            "another_string",
                            Decimal.fromUnscaledLong(90, 5, 2),
                            Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                            TimestampLtz.fromEpochMillis(1698235273200L),
                            TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                            TimestampNtz.fromMillis(1698235273201L),
                            TimestampNtz.fromMillis(1698235273201L, 6000),
                            new byte[] {1, 2, 3, 4},
                            new float[] {1.1f, 1.2f, 1.3f},
                            Row.of(200, "nested_value_2", 6.28),
                            streamMap2,
                            null));
            expectedRows2.add(
                    Row.ofKind(
                            RowKind.UPDATE_AFTER,
                            true,
                            (byte) 100,
                            (short) 200,
                            30,
                            400L,
                            500.1f,
                            600.0d,
                            "another_string_2",
                            Decimal.fromUnscaledLong(900, 5, 2),
                            Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                            TimestampLtz.fromEpochMillis(1698235273400L),
                            TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                            TimestampNtz.fromMillis(1698235273501L),
                            TimestampNtz.fromMillis(1698235273501L, 8000),
                            new byte[] {5, 6, 7, 8},
                            new float[] {2.1f, 2.2f, 2.3f},
                            Row.of(300, "nested_value_3", 9.99),
                            streamMap3,
                            null));
        }

        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, expectedRows2, true);
        } else {
            assertResultsExactOrder(actual, expectedRows2, true);
        }

        // query again
        actual = streamTEnv.executeSql(query).collect();
        List<Row> totalExpectedRows = new ArrayList<>(expectedRows);
        totalExpectedRows.addAll(expectedRows2);

        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, totalExpectedRows, true);
        } else {
            assertResultsExactOrder(actual, totalExpectedRows, true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadPrimaryKeyTableFailover(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName1 =
                "restore_pk_table_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String resultTableName =
                "result_pk_table_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath table1 = TablePath.of(DEFAULT_DB, tableName1);
        TablePath resultTable = TablePath.of(DEFAULT_DB, resultTableName);

        // create table and write data
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        Function<String, List<InternalRow>> rowGenerator =
                (partition) ->
                        Arrays.asList(
                                row(3, "string", partition), row(30, "another_string", partition));
        long tableId =
                prepareSimplePKTable(
                        table1,
                        DEFAULT_BUCKET_NUM,
                        isPartitioned,
                        rowGenerator,
                        bucketLogEndOffset);

        // check the status of replica after synced
        assertReplicaStatus(bucketLogEndOffset);

        // create result table
        createSimplePkTable(resultTable, DEFAULT_BUCKET_NUM, isPartitioned, false);
        // union read lake data
        StreamTableEnvironment streamTEnv = buildStreamTEnv(null);
        TableResult insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName1);

        // will read paimon snapshot, should only +I since no change log
        List<Row> expectedRows = new ArrayList<>();
        if (isPartitioned) {
            for (String partition : waitUntilPartitions(table1).values()) {
                expectedRows.add(Row.of(3, "string", partition));
                expectedRows.add(Row.of(30, "another_string", partition));
            }
        } else {
            expectedRows =
                    Arrays.asList(Row.of(3, "string", null), Row.of(30, "another_string", null));
        }

        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + resultTableName).collect();

        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, expectedRows, false);
        } else {
            assertResultsExactOrder(actual, expectedRows, false);
        }

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

        // re buildStreamTEnv
        streamTEnv = buildStreamTEnv(savepointPath);
        insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName1);

        // write some log data again
        // write a row again
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(table1);
            for (String partition : partitionNameById.values()) {
                writeRows(
                        table1,
                        Collections.singletonList(row(30, "another_string_2", partition)),
                        false);
            }
        } else {
            writeRows(table1, Collections.singletonList(row(30, "another_string_2", null)), false);
        }

        // should generate -U & +U
        List<Row> expectedRows2 = new ArrayList<>();
        if (isPartitioned) {
            for (String partition : waitUntilPartitions(table1).values()) {
                expectedRows2.add(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 30, "another_string", partition));
                expectedRows2.add(
                        Row.ofKind(RowKind.UPDATE_AFTER, 30, "another_string_2", partition));
            }
        } else {
            expectedRows2.add(Row.ofKind(RowKind.UPDATE_BEFORE, 30, "another_string", null));
            expectedRows2.add(Row.ofKind(RowKind.UPDATE_AFTER, 30, "another_string_2", null));
        }

        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, expectedRows2, true);
        } else {
            assertResultsExactOrder(actual, expectedRows2, true);
        }

        // cancel jobs
        insertResult.getJobClient().get().cancel().get();
        jobClient.cancel().get();
    }

    @Test
    void testUnionReadWithAddColumn() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "unionReadAddColumnPKTable");

        // 1. Create PK Table (Lake Enabled)
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();

        long tableId = createTable(tablePath, tableDescriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        // 2. Write initial data
        List<InternalRow> initialRows = Arrays.asList(row(1, "v1"), row(2, "v2"));
        writeRows(tablePath, initialRows, false);

        // 3. Start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // 4. Wait for data to snapshot to Paimon
            assertReplicaStatus(tableBucket, 2);

            // 5. Add Column "c3"
            List<TableChange> addColumnChanges =
                    Collections.singletonList(
                            TableChange.addColumn(
                                    "c3",
                                    DataTypes.INT(),
                                    "new column",
                                    TableChange.ColumnPosition.last()));
            admin.alterTable(tablePath, addColumnChanges, false).get();

            // 6. Write new data (Update Key 2, Insert Key 3)
            // Updating key 2 validates that union read correctly merges
            // the new schema data from log with old schema data from Paimon
            List<InternalRow> newRows = Arrays.asList(row(2, "v2_updated", 20), row(3, "v3", 30));
            writeRows(tablePath, newRows, false);

            // 7. Query via Flink SQL
            CloseableIterator<Row> iterator =
                    batchTEnv.executeSql("SELECT * FROM " + tablePath.getTableName()).collect();

            // 8. Verify union read correctly handles schema evolution with PK updates:
            // - Key 1: from Paimon snapshot (old schema, c3 should be null)
            // - Key 2: from Fluss log (updated value, new schema)
            // - Key 3: from Fluss log (new insert, new schema)
            List<String> actualRows = collectRowsWithTimeout(iterator, 3, true);

            assertThat(actualRows)
                    .containsExactlyInAnyOrder(
                            "+I[1, v1, null]", "+I[2, v2_updated, 20]", "+I[3, v3, 30]");

            // 9. Add Column "c4" (Schema V3)
            // Verify union read reconciles tiered data (V1) with a fluss log
            // containing multiple schema versions (V2 and V3).
            jobClient.cancel().get();
            addColumnChanges =
                    Collections.singletonList(
                            TableChange.addColumn(
                                    "c4",
                                    DataTypes.INT(),
                                    "another new column",
                                    TableChange.ColumnPosition.last()));
            admin.alterTable(tablePath, addColumnChanges, false).get();

            // 10. Write data for Schema V3 (Update Key 2 and Key 3)
            newRows =
                    Arrays.asList(row(2, "v2_updated_again", 20, 30), row(3, "v3_update", 30, 40));
            writeRows(tablePath, newRows, false);

            // 11. Final Query Verify (Paimon V1 + Log V2 + Log V3)
            // - Key 1: from Paimon snapshot (oldest schema, c3/c4 should be null)
            // - Key 2: from Fluss log (latest update, newest schema)
            // - Key 3: from Fluss log (latest update, newest schema)
            iterator = batchTEnv.executeSql("SELECT * FROM " + tablePath.getTableName()).collect();
            actualRows = collectRowsWithTimeout(iterator, 3, true);

            assertThat(actualRows)
                    .containsExactlyInAnyOrder(
                            "+I[1, v1, null, null]",
                            "+I[2, v2_updated_again, 20, 30]",
                            "+I[3, v3_update, 30, 40]");
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testUnionReadPartitionsExistInPaimonButExpiredInFluss() throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "expired_partition_pk_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        // create table and write data
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        Function<String, List<InternalRow>> rowGenerator =
                (partition) ->
                        Arrays.asList(
                                row(3, "string", partition), row(30, "another_string", partition));
        long tableId =
                prepareSimplePKTable(
                        tablePath, DEFAULT_BUCKET_NUM, true, rowGenerator, bucketLogEndOffset);

        // wait until records has been synced
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, true);

        // Get all partitions
        Map<Long, String> partitionNameByIds = waitUntilPartitions(tablePath);
        assertThat(partitionNameByIds.size()).isGreaterThan(0);

        // Build expected rows for all partitions
        // Simple PK table has 3 columns: c1 (INT), c2 (STRING), c3 (STRING) where c3 is partition
        List<Row> expectedAllRows = new ArrayList<>();
        for (String partition : partitionNameByIds.values()) {
            expectedAllRows.add(Row.of(3, "string", partition));
            expectedAllRows.add(Row.of(30, "another_string", partition));
        }

        // Select one partition to drop (expire in Fluss)
        Long partitionToDropId = partitionNameByIds.keySet().iterator().next();
        String partitionToDropName = partitionNameByIds.get(partitionToDropId);

        // Filter rows that belong to the partition to be dropped
        // c3 is the partition column (index 2)
        List<Row> rowsInExpiredPartition =
                expectedAllRows.stream()
                        .filter(row -> partitionToDropName.equals(row.getField(2)))
                        .collect(Collectors.toList());
        assertThat(rowsInExpiredPartition).isNotEmpty();

        // Now drop the partition in Fluss (make it expired)
        // The partition data still exists in Paimon
        // c3 is the partition column for simple PK table
        admin.dropPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("c3", partitionToDropName)),
                        false)
                .get();

        // Retry until partition dropped
        retry(
                Duration.ofSeconds(60),
                () -> {
                    List<PartitionInfo> remainingPartitions =
                            admin.listPartitionInfos(tablePath).get();
                    assertThat(remainingPartitions.size()).isEqualTo(1);
                });

        // Now query the table - it should read data from both:
        // 1. Remaining partitions from Fluss
        // 2. Expired partition from Paimon (union read)
        CloseableIterator<Row> iterator =
                streamTEnv
                        .executeSql(
                                "select * from "
                                        + tableName
                                        + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                        .collect();
        List<String> actual = collectRowsWithTimeout(iterator, expectedAllRows.size(), true);
        assertThat(actual)
                .containsExactlyInAnyOrderElementsOf(
                        expectedAllRows.stream().map(Row::toString).collect(Collectors.toList()));

        // Test partition filter - query only the expired partition
        // c3 is the partition column for simple PK table
        String sqlWithPartitionFilter =
                "select"
                        + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */"
                        + " * FROM "
                        + tableName
                        + " WHERE c3 = '"
                        + partitionToDropName
                        + "'";
        iterator = streamTEnv.executeSql(sqlWithPartitionFilter).collect();
        List<String> filteredActual =
                collectRowsWithTimeout(iterator, rowsInExpiredPartition.size(), true);

        // Should still be able to read data from expired partition via Paimon
        assertThat(filteredActual)
                .as("Should read expired partition data from Paimon when filtering by partition")
                .containsExactlyInAnyOrderElementsOf(
                        rowsInExpiredPartition.stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()));

        // cancel the tiering job
        jobClient.cancel().get();
    }

    private List<Row> sortedRows(List<Row> rows) {
        rows.sort(Comparator.comparing(Row::toString));
        return rows;
    }

    private List<String> toSortedRows(TableResult tableResult) {
        return CollectionUtil.iteratorToList(tableResult.collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    private long preparePKTableFullType(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            Map<TableBucket, Long> bucketLogEndOffset)
            throws Exception {
        long tableId = createPkTableFullType(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                for (int i = 0; i < 2; i++) {
                    List<InternalRow> rows = generateKvRowsFullType(partition);
                    // write records
                    writeRows(tablePath, rows, false);
                }
            }
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
        } else {
            for (int i = 0; i < 2; i++) {
                List<InternalRow> rows = generateKvRowsFullType(null);
                // write records
                writeRows(tablePath, rows, false);
            }
            bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, null));
        }
        return tableId;
    }

    private long prepareSimplePKTable(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            Function<String, List<InternalRow>> rowGenerator,
            Map<TableBucket, Long> bucketLogEndOffset)
            throws Exception {
        long tableId = createSimplePkTable(tablePath, bucketNum, isPartitioned, true);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                for (int i = 0; i < 2; i++) {
                    List<InternalRow> rows = rowGenerator.apply(partition);
                    // write records
                    writeRows(tablePath, rows, false);
                }
            }
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
        } else {
            for (int i = 0; i < 2; i++) {
                List<InternalRow> rows = rowGenerator.apply(null);
                // write records
                writeRows(tablePath, rows, false);
            }
            bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, null));
        }
        return tableId;
    }

    private Map<TableBucket, Long> getBucketLogEndOffset(
            long tableId, int bucketNum, Long partitionId) {
        Map<TableBucket, Long> bucketLogEndOffsets = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
            Replica replica = getLeaderReplica(tableBucket);
            bucketLogEndOffsets.put(tableBucket, replica.getLocalLogEndOffset());
        }
        return bucketLogEndOffsets;
    }

    private String buildExpectedResult(
            boolean isPartitioned, List<String> partitions, int record1, int record2) {
        List<String> records = new ArrayList<>();
        records.add(
                "+I[false, 1, 2, 3, 4, 5.1, 6.0, string, 0.09, 10, "
                        + "2023-10-25T12:01:13.182Z, "
                        + "2023-10-25T12:01:13.182005Z, "
                        + "2023-10-25T12:01:13.183, "
                        + "2023-10-25T12:01:13.183006, "
                        + "[1, 2, 3, 4], [1.1, 1.2, 1.3], +I[100, nested_value_1, 3.14], {key1=1, key2=2}, %s]");
        records.add(
                "+I[true, 10, 20, 30, 40, 50.1, 60.0, another_string, 0.90, 100, "
                        + "2023-10-25T12:01:13.200Z, "
                        + "2023-10-25T12:01:13.200005Z, "
                        + "2023-10-25T12:01:13.201, "
                        + "2023-10-25T12:01:13.201006, "
                        + "[1, 2, 3, 4], [1.1, 1.2, 1.3], +I[200, nested_value_2, 6.28], {key3=3, key4=4}, %s]");
        records.add(
                "+I[true, 100, 200, 30, 400, 500.1, 600.0, another_string_2, 9.00, 1000, "
                        + "2023-10-25T12:01:13.400Z, "
                        + "2023-10-25T12:01:13.400007Z, "
                        + "2023-10-25T12:01:13.501, "
                        + "2023-10-25T12:01:13.501008, "
                        + "[5, 6, 7, 8], [2.1, 2.2, 2.3], +I[300, nested_value_3, 9.99], {key5=5, key6=6}, %s]");

        if (isPartitioned) {
            return String.format(
                    "[%s, %s, %s, %s]",
                    String.format(records.get(record1), partitions.get(0)),
                    String.format(records.get(record1), partitions.get(1)),
                    String.format(records.get(record2), partitions.get(0)),
                    String.format(records.get(record2), partitions.get(1)));
        } else {
            return String.format(
                    "[%s, %s]",
                    String.format(records.get(record1), "null"),
                    String.format(records.get(record2), "null"));
        }
    }

    protected long createPkTableFullType(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("c1", DataTypes.BOOLEAN())
                        .column("c2", DataTypes.TINYINT())
                        .column("c3", DataTypes.SMALLINT())
                        .column("c4", DataTypes.INT())
                        .column("c5", DataTypes.BIGINT())
                        .column("c6", DataTypes.FLOAT())
                        .column("c7", DataTypes.DOUBLE())
                        .column("c8", DataTypes.STRING())
                        .column("c9", DataTypes.DECIMAL(5, 2))
                        .column("c10", DataTypes.DECIMAL(20, 0))
                        .column("c11", DataTypes.TIMESTAMP_LTZ(3))
                        .column("c12", DataTypes.TIMESTAMP_LTZ(6))
                        .column("c13", DataTypes.TIMESTAMP(3))
                        .column("c14", DataTypes.TIMESTAMP(6))
                        .column("c15", DataTypes.BINARY(4))
                        .column("c16", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .column(
                                "c17",
                                DataTypes.ROW(
                                        DataTypes.FIELD("nested_int", DataTypes.INT()),
                                        DataTypes.FIELD("nested_string", DataTypes.STRING()),
                                        DataTypes.FIELD("nested_double", DataTypes.DOUBLE())))
                        .column("c18", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column("c19", DataTypes.STRING());

        return createPkTable(tablePath, bucketNum, isPartitioned, true, schemaBuilder, "c4", "c19");
    }

    protected long createSimplePkTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, boolean lakeEnabled)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING());

        return createPkTable(
                tablePath, bucketNum, isPartitioned, lakeEnabled, schemaBuilder, "c1", "c3");
    }

    protected long createPkTable(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            boolean lakeEnabled,
            Schema.Builder schemaBuilder,
            String primaryKey,
            String partitionKeys)
            throws Exception {

        TableDescriptor.Builder tableBuilder = TableDescriptor.builder().distributedBy(bucketNum);
        if (lakeEnabled) {
            tableBuilder
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        }

        if (isPartitioned) {
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy(partitionKeys);
            schemaBuilder.primaryKey(partitionKeys, primaryKey);
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        } else {
            schemaBuilder.primaryKey(primaryKey);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private void writeFullTypeRow(TablePath tablePath, String partition) throws Exception {
        Map<Object, Object> map3 = new HashMap<>();
        map3.put(BinaryString.fromString("key5"), 5);
        map3.put(BinaryString.fromString("key6"), 6);

        List<InternalRow> rows =
                Collections.singletonList(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_2",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                new GenericArray(new float[] {2.1f, 2.2f, 2.3f}),
                                GenericRow.of(300, BinaryString.fromString("nested_value_3"), 9.99),
                                new GenericMap(map3),
                                partition));
        writeRows(tablePath, rows, false);
    }

    private static List<InternalRow> generateKvRowsFullType(@Nullable String partition) {
        Map<Object, Object> map1 = new HashMap<>();
        map1.put(BinaryString.fromString("key1"), 1);
        map1.put(BinaryString.fromString("key2"), 2);

        Map<Object, Object> map2 = new HashMap<>();
        map2.put(BinaryString.fromString("key3"), 3);
        map2.put(BinaryString.fromString("key4"), 4);

        return Arrays.asList(
                row(
                        false,
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        5.1f,
                        6.0d,
                        "string",
                        Decimal.fromUnscaledLong(9, 5, 2),
                        Decimal.fromBigDecimal(new java.math.BigDecimal(10), 20, 0),
                        TimestampLtz.fromEpochMillis(1698235273182L),
                        TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                        TimestampNtz.fromMillis(1698235273183L),
                        TimestampNtz.fromMillis(1698235273183L, 6000),
                        new byte[] {1, 2, 3, 4},
                        new GenericArray(new float[] {1.1f, 1.2f, 1.3f}),
                        GenericRow.of(100, BinaryString.fromString("nested_value_1"), 3.14),
                        new GenericMap(map1),
                        partition),
                row(
                        true,
                        (byte) 10,
                        (short) 20,
                        30,
                        40L,
                        50.1f,
                        60.0d,
                        "another_string",
                        Decimal.fromUnscaledLong(90, 5, 2),
                        Decimal.fromBigDecimal(new java.math.BigDecimal(100), 20, 0),
                        TimestampLtz.fromEpochMillis(1698235273200L),
                        TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                        TimestampNtz.fromMillis(1698235273201L),
                        TimestampNtz.fromMillis(1698235273201L, 6000),
                        new byte[] {1, 2, 3, 4},
                        new GenericArray(new float[] {1.1f, 1.2f, 1.3f}),
                        GenericRow.of(200, BinaryString.fromString("nested_value_2"), 6.28),
                        new GenericMap(map2),
                        partition));
    }
}
