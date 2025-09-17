/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.flink;

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
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsExactOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertRowResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;

/** Test case for union read primary key table. */
public class FlinkUnionReadPrimaryKeyTableITCase extends FlinkUnionReadTestBase {
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
        assertReplicaStatus(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // will read iceberg snapshot, should only +I since no change log
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
                                    TimestampLtz.fromEpochMillis(1698235273182L),
                                    TimestampLtz.fromEpochMillis(1698235273182L, 5000),
                                    TimestampNtz.fromMillis(1698235273183L),
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
}
