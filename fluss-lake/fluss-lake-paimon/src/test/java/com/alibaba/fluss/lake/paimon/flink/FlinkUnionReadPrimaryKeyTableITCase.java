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

package com.alibaba.fluss.lake.paimon.flink;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for Flink union data in lake and fluss for primary key table. */
class FlinkUnionReadPrimaryKeyTableITCase extends FlinkUnionReadTestBase {

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
        waitUtilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // check the status of replica after synced
        assertReplicaStatus(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // will read paimon snapshot, won't merge log since it's empty
        List<String> resultEmptyLog =
                toSortedRows(batchTEnv.executeSql("select * from " + tableName));
        String expetedResultFromPaimon = buildExpectedResult(isPartitioned, 0, 1);
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
            queryFilterStr = queryFilterStr + " and c16= '" + partitionName + "'";
        }

        List<Row> expectedRows = new ArrayList<>();
        if (isPartitioned) {
            List<String> partitions = Arrays.asList("2025", "2026");

            for (String partition : partitions) {
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
                                        isMatch = isMatch && row.getField(15).equals(partitionName);
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

        expectedRows = new ArrayList<>();
        if (isPartitioned) {
            List<String> partitions = Arrays.asList("2025", "2026");

            for (String partition : partitions) {
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
                                    null));
        }

        // now, query the result, it must be the union result of lake snapshot and log
        List<String> result = toSortedRows(batchTEnv.executeSql("select * from " + tableName));
        String expectedResult = buildExpectedResult(isPartitioned, 0, 2);
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

    private String buildExpectedResult(boolean isPartitioned, int record1, int record2) {
        List<String> records = new ArrayList<>();
        records.add(
                "+I[false, 1, 2, 3, 4, 5.1, 6.0, string, 0.09, 10, "
                        + "2023-10-25T12:01:13.182Z, "
                        + "2023-10-25T12:01:13.182005Z, "
                        + "2023-10-25T12:01:13.183, "
                        + "2023-10-25T12:01:13.183006, "
                        + "[1, 2, 3, 4], %s]");
        records.add(
                "+I[true, 10, 20, 30, 40, 50.1, 60.0, another_string, 0.90, 100, "
                        + "2023-10-25T12:01:13.200Z, "
                        + "2023-10-25T12:01:13.200005Z, "
                        + "2023-10-25T12:01:13.201, "
                        + "2023-10-25T12:01:13.201006, "
                        + "[1, 2, 3, 4], %s]");
        records.add(
                "+I[true, 100, 200, 30, 400, 500.1, 600.0, another_string_2, 9.00, 1000, "
                        + "2023-10-25T12:01:13.400Z, "
                        + "2023-10-25T12:01:13.400007Z, "
                        + "2023-10-25T12:01:13.501, "
                        + "2023-10-25T12:01:13.501008, "
                        + "[5, 6, 7, 8], %s]");

        if (isPartitioned) {
            return String.format(
                    "[%s, %s, %s, %s]",
                    String.format(records.get(record1), "2025"),
                    String.format(records.get(record1), "2026"),
                    String.format(records.get(record2), "2025"),
                    String.format(records.get(record2), "2026"));
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
                        .column("c16", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (isPartitioned) {
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c16");
            schemaBuilder.primaryKey("c4", "c16");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        } else {
            schemaBuilder.primaryKey("c4");
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private void writeFullTypeRow(TablePath tablePath, String partition) throws Exception {
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
                                partition));
        writeRows(tablePath, rows, false);
    }

    private List<InternalRow> generateKvRowsFullType(@Nullable String partition) {
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
                        partition));
    }
}
