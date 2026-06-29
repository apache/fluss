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

package org.apache.fluss.lake.hudi.flink;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.Decimal;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test union read primary key table with Hudi. */
class FlinkUnionReadPrimaryKeyTableITCase extends FlinkUnionReadTestBase {

    @TempDir public static File savepointDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false})
    void testUnionReadFullType(boolean isPartitioned) throws Exception {
        JobClient jobClient = buildCheckpointedTieringJob();

        String tableName = "pk_table_full_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        long tableId =
                prepareFullTypePkTable(
                        tablePath, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);
        assertReplicaStatus(bucketLogEndOffset);

        List<String> partitions = getPartitions(tablePath, isPartitioned);
        List<Row> expectedRows = createExpectedInitialFullTypeRows(partitions);
        assertThat(normalizeUpsertKind(toSortedRows("select * from " + tableName)))
                .isEqualTo(toSortedStrings(expectedRows));

        String queryFilter = "c4 = 30";
        String partitionName = isPartitioned ? partitions.get(0) : null;
        if (partitionName != null) {
            queryFilter = queryFilter + " and c16 = '" + partitionName + "'";
        }
        List<String> expectedPointQueryRows =
                expectedRows.stream()
                        .filter(
                                row -> {
                                    boolean matches = row.getField(3).equals(30);
                                    if (partitionName != null) {
                                        matches = matches && row.getField(15).equals(partitionName);
                                    }
                                    return matches;
                                })
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(toSortedRows("select * from " + tableName + " where " + queryFilter))
                .isEqualTo(expectedPointQueryRows);

        jobClient.cancel().get();

        for (String partition : partitions) {
            writeFullTypeUpdateRow(tablePath, partition);
        }

        expectedRows = createExpectedUpdatedFullTypeRows(partitions);
        assertThat(normalizeUpsertKind(toSortedRows("select * from " + tableName)))
                .isEqualTo(toSortedStrings(expectedRows));

        List<Row> expectedProjectRows =
                expectedRows.stream()
                        .map(row -> Row.of(row.getField(2), row.getField(3)))
                        .collect(Collectors.toList());
        assertThat(toSortedRows("select c3, c4 from " + tableName))
                .isEqualTo(toSortedStrings(expectedProjectRows));

        List<Row> expectedProjectRows2 =
                expectedRows.stream()
                        .map(row -> Row.of(row.getField(2)))
                        .collect(Collectors.toList());
        assertThat(toSortedRows("select c3 from " + tableName))
                .isEqualTo(toSortedStrings(expectedProjectRows2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadInStreamMode(boolean isPartitioned) throws Exception {
        JobClient jobClient = buildCheckpointedTieringJob();

        String tableName =
                "stream_pk_table_full_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        long tableId =
                prepareFullTypePkTable(
                        tablePath, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);
        assertReplicaStatus(bucketLogEndOffset);

        List<String> partitions = getPartitions(tablePath, isPartitioned);
        List<Row> expectedRows = createExpectedInitialFullTypeRows(partitions);

        String query = "select * from " + tableName;
        CloseableIterator<Row> actual = streamTEnv.executeSql(query).collect();
        assertRowResultsIgnoreOrder(actual, expectedRows, false);

        jobClient.cancel().get();

        for (String partition : partitions) {
            writeFullTypeUpdateRow(tablePath, partition);
        }

        List<Row> expectedUpdateRows = createExpectedFullTypeUpdateChangelogRows(partitions);
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, expectedUpdateRows, true);
        } else {
            assertResultsExactOrder(actual, expectedUpdateRows, true);
        }

        actual = streamTEnv.executeSql(query).collect();
        List<Row> totalExpectedRows = new ArrayList<>(expectedRows);
        totalExpectedRows.addAll(expectedUpdateRows);
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, totalExpectedRows, true);
        } else {
            assertResultsExactOrder(actual, totalExpectedRows, true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadPrimaryKeyTableFailover(boolean isPartitioned) throws Exception {
        JobClient jobClient = buildCheckpointedTieringJob();

        String tableName =
                "restore_pk_table_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String resultTableName =
                "result_pk_table_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        TablePath resultTablePath = TablePath.of(DEFAULT_DB, resultTableName);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        Function<String, List<InternalRow>> rowGenerator =
                partition ->
                        Arrays.asList(
                                row(3, "string", partition), row(30, "another_string", partition));
        long tableId =
                prepareSimplePkTable(
                        tablePath,
                        DEFAULT_BUCKET_NUM,
                        isPartitioned,
                        rowGenerator,
                        bucketLogEndOffset);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);
        assertReplicaStatus(bucketLogEndOffset);

        createSimplePkTable(resultTablePath, DEFAULT_BUCKET_NUM, isPartitioned, false);
        StreamTableEnvironment streamTEnv = buildStreamTEnv(null);
        TableResult insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName);

        List<String> partitions = getPartitions(tablePath, isPartitioned);
        List<Row> expectedRows = createExpectedSimpleRows(partitions, false);
        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + resultTableName).collect();
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, expectedRows, false);
        } else {
            assertResultsExactOrder(actual, expectedRows, false);
        }

        String savepointPath =
                insertResult
                        .getJobClient()
                        .get()
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();

        streamTEnv = buildStreamTEnv(savepointPath);
        insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName);

        for (String partition : partitions) {
            writeRows(
                    tablePath,
                    Collections.singletonList(row(30, "another_string_2", partition)),
                    false);
        }

        List<Row> expectedUpdateRows = createExpectedSimpleRows(partitions, true);
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, expectedUpdateRows, true);
        } else {
            assertResultsExactOrder(actual, expectedUpdateRows, true);
        }

        insertResult.getJobClient().get().cancel().get();
        jobClient.cancel().get();
    }

    private JobClient buildCheckpointedTieringJob() throws Exception {
        execEnv.enableCheckpointing(1000);
        return buildTieringJob(execEnv);
    }

    private long prepareFullTypePkTable(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            Map<TableBucket, Long> bucketLogEndOffset)
            throws Exception {
        long tableId = createFullTypePkTable(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                writeRows(tablePath, createInternalFullTypeRows(partition), false);
            }
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
        } else {
            writeRows(tablePath, createInternalFullTypeRows(null), false);
            bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, null));
        }
        return tableId;
    }

    private long prepareSimplePkTable(
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
                writeRows(tablePath, rowGenerator.apply(partition), false);
            }
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
        } else {
            writeRows(tablePath, rowGenerator.apply(null), false);
            bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, null));
        }
        return tableId;
    }

    private Map<TableBucket, Long> getBucketLogEndOffset(
            long tableId, int bucketNum, @Nullable Long partitionId) {
        Map<TableBucket, Long> bucketLogEndOffsets = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
            Replica replica = getLeaderReplica(tableBucket);
            bucketLogEndOffsets.put(tableBucket, replica.getLocalLogEndOffset());
        }
        return bucketLogEndOffsets;
    }

    private long createFullTypePkTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("c1", DataTypes.BOOLEAN())
                        .column("c2", DataTypes.INT())
                        .column("c3", DataTypes.INT())
                        .column("c4", DataTypes.INT())
                        .column("c5", DataTypes.BIGINT())
                        .column("c6", DataTypes.FLOAT())
                        .column("c7", DataTypes.DOUBLE())
                        .column("c8", DataTypes.STRING())
                        .column("c9", DataTypes.DECIMAL(5, 2))
                        .column("c10", DataTypes.DECIMAL(20, 0))
                        .column("c11", DataTypes.TIMESTAMP_LTZ(6))
                        .column("c12", DataTypes.TIMESTAMP_LTZ(6))
                        .column("c13", DataTypes.TIMESTAMP(3))
                        .column("c14", DataTypes.TIMESTAMP(6))
                        .column("c15", DataTypes.BINARY(4))
                        .column("c16", DataTypes.STRING());
        return createPkTable(tablePath, bucketNum, isPartitioned, true, schemaBuilder, "c4", "c16");
    }

    private long createSimplePkTable(
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

    private long createPkTable(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            boolean lakeEnabled,
            Schema.Builder schemaBuilder,
            String primaryKey,
            String partitionKey)
            throws Exception {
        TableDescriptor.Builder tableBuilder = TableDescriptor.builder().distributedBy(bucketNum);
        if (lakeEnabled) {
            tableBuilder
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                    .customProperty(HUDI_CONF_PREFIX + "precombine.field", primaryKey);
        }

        if (isPartitioned) {
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy(partitionKey);
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
            schemaBuilder.primaryKey(primaryKey, partitionKey);
        } else {
            schemaBuilder.primaryKey(primaryKey);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private void writeFullTypeUpdateRow(TablePath tablePath, @Nullable String partition)
            throws Exception {
        writeRows(
                tablePath,
                Collections.singletonList(
                        row(
                                true,
                                100,
                                200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_2",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                partition)),
                false);
    }

    private static List<InternalRow> createInternalFullTypeRows(@Nullable String partition) {
        return Arrays.asList(
                row(
                        false,
                        1,
                        2,
                        3,
                        4L,
                        5.1f,
                        6.0d,
                        "string",
                        Decimal.fromUnscaledLong(9, 5, 2),
                        Decimal.fromBigDecimal(new BigDecimal(10), 20, 0),
                        TimestampLtz.fromEpochMillis(1698235273182L),
                        TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                        TimestampNtz.fromMillis(1698235273183L),
                        TimestampNtz.fromMillis(1698235273183L, 6000),
                        new byte[] {1, 2, 3, 4},
                        partition),
                row(
                        true,
                        10,
                        20,
                        30,
                        40L,
                        50.1f,
                        60.0d,
                        "another_string",
                        Decimal.fromUnscaledLong(90, 5, 2),
                        Decimal.fromBigDecimal(new BigDecimal(100), 20, 0),
                        TimestampLtz.fromEpochMillis(1698235273200L),
                        TimestampLtz.fromEpochMillis(1698235273200L, 5000),
                        TimestampNtz.fromMillis(1698235273201L),
                        TimestampNtz.fromMillis(1698235273201L, 6000),
                        new byte[] {1, 2, 3, 4},
                        partition));
    }

    private List<Row> createExpectedInitialFullTypeRows(List<String> partitions) {
        List<Row> rows = new ArrayList<>();
        for (String partition : partitions) {
            rows.add(createInitialFullTypeRow(partition));
            rows.add(createInitialFullTypePk30Row(RowKind.INSERT, partition));
        }
        return rows;
    }

    private List<Row> createExpectedUpdatedFullTypeRows(List<String> partitions) {
        List<Row> rows = new ArrayList<>();
        for (String partition : partitions) {
            rows.add(createInitialFullTypeRow(partition));
            rows.add(createUpdatedFullTypePk30Row(RowKind.INSERT, partition));
        }
        return rows;
    }

    private List<Row> createExpectedFullTypeUpdateChangelogRows(List<String> partitions) {
        List<Row> rows = new ArrayList<>();
        for (String partition : partitions) {
            rows.add(createInitialFullTypePk30Row(RowKind.UPDATE_BEFORE, partition));
            rows.add(createUpdatedFullTypePk30Row(RowKind.UPDATE_AFTER, partition));
        }
        return rows;
    }

    private Row createInitialFullTypeRow(@Nullable String partition) {
        return Row.of(
                false,
                1,
                2,
                3,
                4L,
                5.1f,
                6.0d,
                "string",
                new BigDecimal("0.09"),
                new BigDecimal("10"),
                Instant.ofEpochMilli(1698235273182L),
                Instant.ofEpochMilli(1698235273182L).plusNanos(5000),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273183L), ZoneId.of("UTC")),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273183L), ZoneId.of("UTC"))
                        .plusNanos(6000),
                new byte[] {1, 2, 3, 4},
                partition);
    }

    private Row createInitialFullTypePk30Row(RowKind rowKind, @Nullable String partition) {
        Row row = createInitialFullTypePk30Row(partition);
        row.setKind(rowKind);
        return row;
    }

    private Row createInitialFullTypePk30Row(@Nullable String partition) {
        return Row.of(
                true,
                10,
                20,
                30,
                40L,
                50.1f,
                60.0d,
                "another_string",
                new BigDecimal("0.90"),
                new BigDecimal("100"),
                Instant.ofEpochMilli(1698235273200L),
                Instant.ofEpochMilli(1698235273200L).plusNanos(5000),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273201L), ZoneId.of("UTC")),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273201L), ZoneId.of("UTC"))
                        .plusNanos(6000),
                new byte[] {1, 2, 3, 4},
                partition);
    }

    private Row createUpdatedFullTypePk30Row(RowKind rowKind, @Nullable String partition) {
        Row row = createUpdatedFullTypePk30Row(partition);
        row.setKind(rowKind);
        return row;
    }

    private Row createUpdatedFullTypePk30Row(@Nullable String partition) {
        return Row.of(
                true,
                100,
                200,
                30,
                400L,
                500.1f,
                600.0d,
                "another_string_2",
                new BigDecimal("9.00"),
                new BigDecimal("1000"),
                Instant.ofEpochMilli(1698235273400L),
                Instant.ofEpochMilli(1698235273400L).plusNanos(7000),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC")),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC"))
                        .plusNanos(8000),
                new byte[] {5, 6, 7, 8},
                partition);
    }

    private List<Row> createExpectedSimpleRows(List<String> partitions, boolean updateOnly) {
        List<Row> rows = new ArrayList<>();
        for (String partition : partitions) {
            if (updateOnly) {
                rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, 30, "another_string", partition));
                rows.add(Row.ofKind(RowKind.UPDATE_AFTER, 30, "another_string_2", partition));
            } else {
                rows.add(Row.of(3, "string", partition));
                rows.add(Row.of(30, "another_string", partition));
            }
        }
        return rows;
    }

    private List<String> getPartitions(TablePath tablePath, boolean isPartitioned) {
        if (!isPartitioned) {
            return Collections.singletonList(null);
        }
        Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
        return partitionNameById.values().stream().sorted().collect(Collectors.toList());
    }

    private List<String> toSortedRows(String sql) {
        return CollectionUtil.iteratorToList(batchTEnv.executeSql(sql).collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    private List<String> toSortedStrings(List<Row> rows) {
        return rows.stream()
                .sorted(Comparator.comparing(Row::toString))
                .map(Row::toString)
                .collect(Collectors.toList());
    }

    private List<String> normalizeUpsertKind(List<String> rows) {
        return rows.stream().map(row -> row.replace("+U", "+I")).collect(Collectors.toList());
    }
}
