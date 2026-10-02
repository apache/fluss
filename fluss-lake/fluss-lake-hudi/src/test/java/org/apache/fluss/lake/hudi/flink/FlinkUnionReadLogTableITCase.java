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
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
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
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertRowResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test union read log table with full type. */
class FlinkUnionReadLogTableITCase extends FlinkUnionReadTestBase {

    @TempDir public static File savepointDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadLogTableFullType(boolean isPartitioned) throws Exception {
        JobClient jobClient = buildCheckpointedTieringJob();

        String tableName = "logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new ArrayList<>();
        long tableId = prepareLogTable(tablePath, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        List<Row> actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);

        jobClient.cancel().get();

        writtenRows.addAll(writeRows(tablePath, 3, 3, isPartitioned));

        actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);

        actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select f_int from " + tableName).collect());
        List<Row> expected =
                writtenRows.stream()
                        .map(row -> Row.of(row.getField(1)))
                        .collect(Collectors.toList());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        if (isPartitioned) {
            String partition = waitUntilPartitions(tablePath).values().iterator().next();
            String sqlWithPartitionFilter =
                    "select * FROM " + tableName + " WHERE p = '" + partition + "'";

            String plan = batchTEnv.explainSql(sqlWithPartitionFilter);
            assertThat(plan)
                    .contains("TableSourceScan(")
                    .contains("filter=[=(p, _UTF-16LE'" + partition + "'");

            List<Row> expectedFiltered =
                    writtenRows.stream()
                            .filter(row -> partition.equals(row.getField(13)))
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
        JobClient jobClient = buildCheckpointedTieringJob();

        String tableName = "stream_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new LinkedList<>();
        long tableId = prepareLogTable(tablePath, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(
                actual, writtenRows.stream().map(Row::toString).collect(Collectors.toList()), true);

        jobClient.cancel().get();

        writtenRows.addAll(writeRows(tablePath, 3, 3, isPartitioned));

        actual =
                streamTEnv
                        .executeSql(
                                "select * from "
                                        + tableName
                                        + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                        .collect();
        if (isPartitioned) {
            writtenRows.addAll(writeFullTypeRows(tablePath, 10, 10, "3027"));
        }
        assertResultsIgnoreOrder(
                actual, writtenRows.stream().map(Row::toString).collect(Collectors.toList()), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTableFailover(boolean isPartitioned) throws Exception {
        JobClient jobClient = buildCheckpointedTieringJob();

        String tableName =
                "restore_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String resultTableName =
                "result_table_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        TablePath resultTablePath = TablePath.of(DEFAULT_DB, resultTableName);
        List<Row> writtenRows = new LinkedList<>();
        long tableId = prepareLogTable(tablePath, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        StreamTableEnvironment streamTEnv = buildStreamTEnv(null);
        createFullTypeLogTable(resultTablePath, DEFAULT_BUCKET_NUM, isPartitioned, false);
        TableResult insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName);

        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + resultTableName).collect();
        assertRowResultsIgnoreOrder(actual, writtenRows, false);

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

        List<Row> rows = writeRows(tablePath, 3, 3, isPartitioned);
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, rows, true);
        } else {
            assertResultsExactOrder(actual, rows, true);
        }

        insertResult.getJobClient().get().cancel().get();
        jobClient.cancel().get();
    }

    private JobClient buildCheckpointedTieringJob() throws Exception {
        execEnv.enableCheckpointing(1000);
        return buildTieringJob(execEnv);
    }

    private long prepareLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, List<Row> flinkRows)
            throws Exception {
        long tableId = createFullTypeLogTable(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition :
                    partitionNameById.values().stream().sorted().collect(Collectors.toList())) {
                for (int batchIndex = 0; batchIndex < 3; batchIndex++) {
                    flinkRows.addAll(writeFullTypeRows(tablePath, batchIndex, 10, partition));
                }
            }
        } else {
            for (int batchIndex = 0; batchIndex < 3; batchIndex++) {
                flinkRows.addAll(writeFullTypeRows(tablePath, batchIndex, 10, null));
            }
        }
        return tableId;
    }

    private long createFullTypeLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        return createFullTypeLogTable(tablePath, bucketNum, isPartitioned, true);
    }

    private long createFullTypeLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, boolean lakeEnabled)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("f_boolean", DataTypes.BOOLEAN())
                        .column("f_int", DataTypes.INT())
                        .column("f_long", DataTypes.BIGINT())
                        .column("f_float", DataTypes.FLOAT())
                        .column("f_double", DataTypes.DOUBLE())
                        .column("f_string", DataTypes.STRING())
                        .column("f_decimal1", DataTypes.DECIMAL(5, 2))
                        .column("f_decimal2", DataTypes.DECIMAL(20, 0))
                        .column("f_timestamp_ltz1", DataTypes.TIMESTAMP_LTZ(3))
                        .column("f_timestamp_ltz2", DataTypes.TIMESTAMP_LTZ(6))
                        .column("f_timestamp_ntz1", DataTypes.TIMESTAMP(3))
                        .column("f_timestamp_ntz2", DataTypes.TIMESTAMP(6))
                        .column("f_binary", DataTypes.BINARY(4));

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder().distributedBy(bucketNum, "f_string");
        if (lakeEnabled) {
            tableBuilder
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                    .customProperty(HUDI_CONF_PREFIX + "precombine.field", "f_string");
        }
        tableBuilder.customProperty(
                HUDI_CONF_PREFIX + FlinkOptions.RECORD_KEY_FIELD.key(),
                isPartitioned ? "f_string,p" : "f_string");

        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("p");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private List<Row> writeFullTypeRows(
            TablePath tablePath, int batchIndex, int rowCount, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            long timestamp = 1698235273400L + batchIndex * 1000L + rowIndex;
            Object[] internalRowValues =
                    createInternalFullTypeRowValues(batchIndex, rowIndex, rowCount, timestamp);
            Object[] flinkRowValues =
                    createFlinkFullTypeRowValues(batchIndex, rowIndex, rowCount, timestamp);
            if (partition == null) {
                rows.add(row(internalRowValues));
                flinkRows.add(Row.of(flinkRowValues));
            } else {
                rows.add(row(appendPartition(internalRowValues, partition)));
                flinkRows.add(Row.of(appendPartition(flinkRowValues, partition)));
            }
        }
        writeRows(tablePath, rows, true);
        return flinkRows;
    }

    private Object[] createInternalFullTypeRowValues(
            int batchIndex, int rowIndex, int rowCount, long timestamp) {
        String stringValue = "another_string_" + batchIndex + "_" + rowIndex;
        return new Object[] {
            true,
            30,
            400L,
            rowCount == 3 ? 234.1f : 500.1f,
            600.0d,
            stringValue,
            Decimal.fromUnscaledLong(900, 5, 2),
            Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
            TimestampLtz.fromEpochMillis(timestamp),
            TimestampLtz.fromEpochMillis(1698235273400L, 7000),
            TimestampNtz.fromMillis(1698235273501L),
            TimestampNtz.fromMillis(1698235273501L, 8000),
            new byte[] {5, 6, 7, 8}
        };
    }

    private Object[] createFlinkFullTypeRowValues(
            int batchIndex, int rowIndex, int rowCount, long timestamp) {
        String stringValue = "another_string_" + batchIndex + "_" + rowIndex;
        return new Object[] {
            true,
            30,
            400L,
            rowCount == 3 ? 234.1f : 500.1f,
            600.0d,
            stringValue,
            new java.math.BigDecimal("9.00"),
            new java.math.BigDecimal("1000"),
            Instant.ofEpochMilli(timestamp),
            Instant.ofEpochMilli(1698235273400L).plusNanos(7000),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC")),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC"))
                    .plusNanos(8000),
            new byte[] {5, 6, 7, 8}
        };
    }

    private Object[] appendPartition(Object[] rowValues, String partition) {
        Object[] partitionedRowValues = new Object[rowValues.length + 1];
        System.arraycopy(rowValues, 0, partitionedRowValues, 0, rowValues.length);
        partitionedRowValues[rowValues.length] = partition;
        return partitionedRowValues;
    }

    private List<Row> writeRows(
            TablePath tablePath, int batchIndex, int rowCount, boolean isPartitioned)
            throws Exception {
        if (isPartitioned) {
            List<Row> rows = new ArrayList<>();
            for (String partition : waitUntilPartitions(tablePath).values()) {
                rows.addAll(writeFullTypeRows(tablePath, batchIndex, rowCount, partition));
            }
            return rows;
        } else {
            return writeFullTypeRows(tablePath, batchIndex, rowCount, null);
        }
    }
}
