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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;

/**
 * Union-read tests for Iceberg primary-key tables, comparing the deletion-vector path against the
 * default equality-delete / sort-merge path. Both must return identical exactly-once results.
 */
class FlinkUnionReadDvTableITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    /** Update rows already tiered to Iceberg; union read must not surface stale versions. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUnionReadWithUpdates(boolean deletionVectorsEnabled) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName =
                    deletionVectorsEnabled ? "dv_update_table" : "sort_merge_update_table";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createPkTable(tablePath, deletionVectorsEnabled);

            // round 1: insert keys 0..4, tier to Iceberg.
            writeRows(tablePath, upsertRows(0, 5, "v"), false);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);

            // round 2: update keys 0..2 (already tiered), tier again.
            writeRows(tablePath, upsertRows(0, 3, "updated"), false);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);

            List<String> expected =
                    Arrays.asList(
                            "+I[0, updated_0]",
                            "+I[1, updated_1]",
                            "+I[2, updated_2]",
                            "+I[3, v_3]",
                            "+I[4, v_4]");
            CloseableIterator<Row> rowIter =
                    batchTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, expected, true);
        } finally {
            jobClient.cancel().get();
        }
    }

    /** Update and delete rows already in Iceberg, then union read; 3 buckets, +/- partitions. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadCrossRound(boolean partitioned) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = partitioned ? "dv_cross_round_part" : "dv_cross_round";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            createCrossRoundTable(tablePath, partitioned);
            List<String> parts =
                    partitioned ? Arrays.asList("p0", "p1") : Collections.singletonList(null);

            // round 1: keys 0..9 tiered to Iceberg
            List<InternalRow> rows = new ArrayList<>();
            for (String p : parts) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, parts.get(0));

            // round 2: update 0..2, delete 3..4
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (String p : parts) {
                    for (int i = 0; i < 3; i++) {
                        writer.upsert(rowOf(i, "updated_" + i, p));
                    }
                    writer.delete(rowOf(3, "v_3", p));
                    writer.delete(rowOf(4, "v_4", p));
                }
                writer.flush();
            }
            // two readable advances: round-2 positions are indexed one commit late
            readable = tickUntilReadableAdvances(tablePath, readable, parts.get(0));
            tickUntilReadableAdvances(tablePath, readable, parts.get(0));

            List<String> expected = new ArrayList<>();
            for (String p : parts) {
                for (int i : new int[] {0, 1, 2, 5, 6, 7, 8, 9}) {
                    expected.add(rowString(i, i < 3 ? "updated_" + i : "v_" + i, p));
                }
            }
            expected.add(rowString(-1, "tick", parts.get(0)));
            CloseableIterator<Row> rowIter =
                    batchTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, expected, true);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void createCrossRoundTable(TablePath tablePath, boolean partitioned) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder().column("c1", DataTypes.INT()).column("c2", DataTypes.STRING());
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true");
        if (partitioned) {
            schema.column("c3", DataTypes.STRING()).primaryKey("c1", "c3");
            builder.partitionedBy("c3");
        } else {
            schema.primaryKey("c1");
        }
        createTable(tablePath, builder.schema(schema.build()).build());
    }

    /** Upserts a tick row (DV tables need fresh commits) until the readable snapshot advances. */
    private long tickUntilReadableAdvances(
            TablePath tablePath, long previous, @Nullable String partition) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                long id = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                if (id != previous) {
                    return id;
                }
            } catch (Exception e) {
                // no readable snapshot yet
            }
            writeRows(tablePath, Collections.singletonList(rowOf(-1, "tick", partition)), false);
            Thread.sleep(1000);
        }
        throw new AssertionError("readable snapshot of " + tablePath + " stuck at " + previous);
    }

    private static InternalRow rowOf(int key, String value, @Nullable String partition) {
        return partition == null ? row(key, value) : row(key, value, partition);
    }

    private static String rowString(int key, String value, @Nullable String partition) {
        return partition == null
                ? String.format("+I[%d, %s]", key, value)
                : String.format("+I[%d, %s, %s]", key, value, partition);
    }

    private long createPkTable(TablePath tablePath, boolean deletionVectorsEnabled)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(DEFAULT_BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (deletionVectorsEnabled) {
            builder.property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true");
        }
        return createTable(tablePath, builder.build());
    }

    private static List<InternalRow> upsertRows(int from, int to, String valuePrefix) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(row(i, valuePrefix + "_" + i));
        }
        return rows;
    }
}
