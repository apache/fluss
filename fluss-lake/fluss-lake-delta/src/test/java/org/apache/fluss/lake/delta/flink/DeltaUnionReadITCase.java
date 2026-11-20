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

package org.apache.fluss.lake.delta.flink;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.delta.testutils.FlinkDeltaTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for the Flink union read of a Fluss log table tiered to Delta Lake. */
class DeltaUnionReadITCase extends FlinkDeltaTieringTestBase {

    private static final String CATALOG_NAME = "fluss_catalog";
    private static final int TIERED_ROWS = 10;
    private static final int LOG_ROWS = 5;

    private static StreamExecutionEnvironment execEnv;
    private TableEnvironment batchTEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkDeltaTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @BeforeEach
    void beforeEach() {
        // a separate environment, so that the batch mode doesn't reach the streaming tiering job
        batchTEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        batchTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME,
                        BOOTSTRAP_SERVERS.key(),
                        String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS))));
        batchTEnv.executeSql("use catalog " + CATALOG_NAME);
        batchTEnv.executeSql("use " + DEFAULT_DB);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTable(boolean isPartitioned) throws Exception {
        String tableName = "union_read_" + (isPartitioned ? "partitioned" : "log");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, 1, isPartitioned);
        Map<String, Long> partitions =
                isPartitioned
                        ? FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath)
                        : Collections.singletonMap(null, null);

        List<Row> expected = new ArrayList<>();
        for (String partition : partitions.keySet()) {
            writeRows(tablePath, genRows(0, TIERED_ROWS, partition, expected));
        }
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            for (Long partitionId : partitions.values()) {
                assertReplicaStatus(new TableBucket(tableId, partitionId, 0), TIERED_ROWS);
            }
        } finally {
            jobClient.cancel().get();
        }
        // stays in the Fluss log, the tiering job is stopped
        for (String partition : partitions.keySet()) {
            writeRows(tablePath, genRows(TIERED_ROWS, LOG_ROWS, partition, expected));
        }

        // the tiered rows come from Delta and the rest from the Fluss log, without duplicates
        assertThat(query("select * from " + tableName))
                .containsExactlyInAnyOrderElementsOf(toStrings(expected));

        // the projection is pushed down to both, including the partition column of the files
        int last = isPartitioned ? 6 : 5;
        assertThat(query("select " + (isPartitioned ? "c" : "ts_ntz") + ", b, a from " + tableName))
                .containsExactlyInAnyOrderElementsOf(
                        expected.stream()
                                .map(
                                        row ->
                                                Row.of(
                                                        row.getField(last),
                                                        row.getField(1),
                                                        row.getField(0)))
                                .map(Row::toString)
                                .collect(Collectors.toList()));
    }

    private List<String> query(String sql) {
        return toStrings(CollectionUtil.iteratorToList(batchTEnv.executeSql(sql).collect()));
    }

    private static List<String> toStrings(List<Row> rows) {
        return rows.stream().map(Row::toString).collect(Collectors.toList());
    }

    private static List<InternalRow> genRows(
            int start, int count, @Nullable String partition, List<Row> expected) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = start; i < start + count; i++) {
            long millis = 1_700_000_000_000L + i;
            long micros = 1_600_000_000_000_000L + i;
            List<Object> fields = new ArrayList<>();
            fields.add(i);
            fields.add(BinaryString.fromString("v" + i));
            fields.add(
                    new GenericArray(
                            new Object[] {
                                BinaryString.fromString("t" + i), BinaryString.fromString("x")
                            }));
            fields.add(GenericRow.of(BinaryString.fromString("city" + i), 10000 + i));
            fields.add(TimestampLtz.fromEpochMillis(millis));
            fields.add(TimestampNtz.fromMicros(micros));

            Row row =
                    Row.of(
                            i,
                            "v" + i,
                            new String[] {"t" + i, "x"},
                            Row.of("city" + i, 10000 + i),
                            Instant.ofEpochMilli(millis),
                            LocalDateTime.ofEpochSecond(
                                    micros / 1_000_000,
                                    (int) (micros % 1_000_000) * 1000,
                                    ZoneOffset.UTC));
            if (partition != null) {
                fields.add(BinaryString.fromString(partition));
                row = Row.join(row, Row.of(partition));
            }
            rows.add(GenericRow.of(fields.toArray()));
            expected.add(row);
        }
        return rows;
    }
}
