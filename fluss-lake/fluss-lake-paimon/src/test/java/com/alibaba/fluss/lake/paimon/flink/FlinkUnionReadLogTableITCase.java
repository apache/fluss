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

import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for Flink union data in lake and fluss for log table. */
class FlinkUnionReadLogTableITCase extends FlinkUnionReadTestBase {

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
    }

    private long prepareLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, List<Row> flinkRows)
            throws Exception {
        long t1Id = createFullTypeLogTable(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
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
