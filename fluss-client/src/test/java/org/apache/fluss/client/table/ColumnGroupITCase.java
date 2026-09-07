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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendColumnsResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidColumnGroupOffsetException;
import org.apache.fluss.exception.UnknownColumnGroupException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test of FIP-45 column groups on the revised design: a column group is a shadow log
 * sharing the base log's offsets, the server ships base and group bytes zero-copy, the client
 * stitches rows, and reads projecting a group are gated at the group's high watermark.
 */
class ColumnGroupITCase extends ClientToServerITCaseBase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("geo", DataTypes.STRING())
                    .columnGroup("geo_group")
                    .column("score", DataTypes.DOUBLE())
                    .columnGroup("risk_group")
                    .build();

    private static final int BASE_ROWS = 10;

    private TablePath createColumnGroupTable(String name) throws Exception {
        TablePath tablePath = TablePath.of("test_db_cg", name);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA)
                        .distributedBy(1)
                        // WP2 (follower replication of column groups) is not in the POC, so the
                        // committed enrichment watermark only advances with a single replica.
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1")
                        .build();
        createTable(tablePath, descriptor, false);
        return tablePath;
    }

    private static void writeBaseRows(Table table, int count) throws Exception {
        AppendWriter writer = table.newAppend().createWriter();
        for (int i = 0; i < count; i++) {
            // the writer takes the full-width row; enrichment columns are ignored here
            writer.append(row(i, "name" + i, null, null));
        }
        writer.flush();
    }

    private static List<InternalRow> geoRows(int from, int toExclusive) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i < toExclusive; i++) {
            rows.add(row("geo" + i));
        }
        return rows;
    }

    private static List<InternalRow> scoreRows(int from, int toExclusive) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i < toExclusive; i++) {
            rows.add(row(i * 0.5d));
        }
        return rows;
    }

    private static List<ScanRecord> pollUpTo(LogScanner scanner, int expected, Duration timeout) {
        List<ScanRecord> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (records.size() < expected && System.currentTimeMillis() < deadline) {
            ScanRecords scanRecords = scanner.poll(Duration.ofMillis(200));
            for (ScanRecord record : scanRecords) {
                records.add(record);
            }
        }
        return records;
    }

    @Test
    void testBaseOnlyReadsAreNotGatedByColumnGroups() throws Exception {
        TablePath tablePath = createColumnGroupTable("base_only");
        try (Table table = conn.getTable(tablePath)) {
            writeBaseRows(table, BASE_ROWS);

            // a projection touching only base columns reads up to the high watermark as today,
            // even though no enrichment was written yet
            try (LogScanner scanner = createLogScanner(table, new int[] {0, 1})) {
                scanner.subscribe(0, 0L);
                List<ScanRecord> records = pollUpTo(scanner, BASE_ROWS, Duration.ofSeconds(10));
                assertThat(records).hasSize(BASE_ROWS);
                for (int i = 0; i < BASE_ROWS; i++) {
                    ScanRecord record = records.get(i);
                    assertThat(record.logOffset()).isEqualTo(i);
                    assertThat(record.timestamp()).isGreaterThan(0L);
                    assertThat(record.getRow().getInt(0)).isEqualTo(i);
                    assertThat(record.getRow().getString(1).toString()).isEqualTo("name" + i);
                }
            }

            // a projection touching a column group is gated at the group's high watermark (0)
            try (LogScanner scanner = createLogScanner(table, new int[] {0, 2})) {
                scanner.subscribe(0, 0L);
                assertThat(pollUpTo(scanner, 1, Duration.ofSeconds(2))).isEmpty();
            }
        }
    }

    @Test
    void testEnrichmentIsStitchedAndGatedPerGroup() throws Exception {
        TablePath tablePath = createColumnGroupTable("stitch");
        try (Table table = conn.getTable(tablePath)) {
            writeBaseRows(table, BASE_ROWS);
            long tableId = table.getTableInfo().getTableId();
            TableBucket bucket = new TableBucket(tableId, 0);
            AppendWriter writer = table.newAppend().createWriter();

            // remember the base batch timestamps: stitched rows must carry the same ones
            Map<Long, Long> baseTimestamps = new HashMap<>();
            try (LogScanner scanner = createLogScanner(table, new int[] {0})) {
                scanner.subscribe(0, 0L);
                for (ScanRecord record : pollUpTo(scanner, BASE_ROWS, Duration.ofSeconds(10))) {
                    baseTimestamps.put(record.logOffset(), record.timestamp());
                }
            }
            assertThat(baseTimestamps).hasSize(BASE_ROWS);

            // fill the first half of geo_group
            AppendColumnsResult result =
                    writer.appendColumns("geo_group", bucket, 0L, geoRows(0, 5)).get();
            assertThat(result.getLogEndOffset()).isEqualTo(5L);
            assertThat(result.getHighWatermark()).isEqualTo(5L);

            // a read projecting geo (in a different order than the schema) sees exactly the
            // filled prefix, with base values, group values and base timestamps
            try (LogScanner scanner = createLogScanner(table, new int[] {2, 0})) {
                scanner.subscribe(0, 0L);
                List<ScanRecord> records = pollUpTo(scanner, 5, Duration.ofSeconds(10));
                assertThat(records).hasSize(5);
                for (int i = 0; i < 5; i++) {
                    ScanRecord record = records.get(i);
                    assertThat(record.logOffset()).isEqualTo(i);
                    assertThat(record.getRow().getFieldCount()).isEqualTo(2);
                    assertThat(record.getRow().getString(0).toString()).isEqualTo("geo" + i);
                    assertThat(record.getRow().getInt(1)).isEqualTo(i);
                    assertThat(record.timestamp()).isEqualTo(baseTimestamps.get((long) i));
                }
                // the rest is gated until enrichment lands
                assertThat(pollUpTo(scanner, 1, Duration.ofSeconds(1))).isEmpty();

                writer.appendColumns("geo_group", bucket, 5L, geoRows(5, BASE_ROWS)).get();
                records = pollUpTo(scanner, 5, Duration.ofSeconds(10));
                assertThat(records).hasSize(5);
                for (int i = 0; i < 5; i++) {
                    assertThat(records.get(i).logOffset()).isEqualTo(5 + i);
                    assertThat(records.get(i).getRow().getString(0).toString())
                            .isEqualTo("geo" + (5 + i));
                }
            }

            // SELECT * touches risk_group too, which is still empty: nothing is visible yet
            try (LogScanner scanner = createLogScanner(table)) {
                scanner.subscribe(0, 0L);
                assertThat(pollUpTo(scanner, 1, Duration.ofSeconds(1))).isEmpty();

                // fill risk_group in batches that do not line up with the base batch
                writer.appendColumns("risk_group", bucket, 0L, scoreRows(0, 3)).get();
                writer.appendColumns("risk_group", bucket, 3L, scoreRows(3, 6)).get();
                writer.appendColumns("risk_group", bucket, 6L, scoreRows(6, 9)).get();
                AppendColumnsResult last =
                        writer.appendColumns("risk_group", bucket, 9L, scoreRows(9, 10)).get();
                assertThat(last.getLogEndOffset()).isEqualTo(BASE_ROWS);

                List<ScanRecord> records = pollUpTo(scanner, BASE_ROWS, Duration.ofSeconds(10));
                assertThat(records).hasSize(BASE_ROWS);
                for (int i = 0; i < BASE_ROWS; i++) {
                    InternalRow row = records.get(i).getRow();
                    assertThat(records.get(i).logOffset()).isEqualTo(i);
                    assertThat(row.getFieldCount()).isEqualTo(4);
                    assertThat(row.getInt(0)).isEqualTo(i);
                    assertThat(row.getString(1).toString()).isEqualTo("name" + i);
                    assertThat(row.getString(2).toString()).isEqualTo("geo" + i);
                    assertThat(row.getDouble(3)).isEqualTo(i * 0.5d);
                    assertThat(records.get(i).timestamp()).isEqualTo(baseTimestamps.get((long) i));
                }
            }

            // a scan starting inside the base batch and inside a group batch stitches correctly
            try (LogScanner scanner = createLogScanner(table, new int[] {3, 2, 1})) {
                scanner.subscribe(0, 4L);
                List<ScanRecord> records = pollUpTo(scanner, 6, Duration.ofSeconds(10));
                assertThat(records).hasSize(6);
                for (int i = 0; i < 6; i++) {
                    int offset = 4 + i;
                    InternalRow row = records.get(i).getRow();
                    assertThat(records.get(i).logOffset()).isEqualTo(offset);
                    assertThat(row.getDouble(0)).isEqualTo(offset * 0.5d);
                    assertThat(row.getString(1).toString()).isEqualTo("geo" + offset);
                    assertThat(row.getString(2).toString()).isEqualTo("name" + offset);
                }
            }

            // a projection with only group columns still advances (a carrier base column is
            // fetched and dropped)
            try (LogScanner scanner = createLogScanner(table, new int[] {2})) {
                scanner.subscribe(0, 0L);
                List<ScanRecord> records = pollUpTo(scanner, BASE_ROWS, Duration.ofSeconds(10));
                assertThat(records).hasSize(BASE_ROWS);
                assertThat(records.get(7).getRow().getFieldCount()).isEqualTo(1);
                assertThat(records.get(7).getRow().getString(0).toString()).isEqualTo("geo7");
            }
        }
    }

    @Test
    void testColumnGroupSurvivesTabletServerRestart() throws Exception {
        TablePath tablePath = createColumnGroupTable("restart");
        try (Table table = conn.getTable(tablePath)) {
            writeBaseRows(table, BASE_ROWS);
            TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
            AppendWriter writer = table.newAppend().createWriter();
            writer.appendColumns("geo_group", bucket, 0L, geoRows(0, 7)).get();

            // the (only) replica is the leader; restart the tablet server hosting it
            int leaderServer = -1;
            for (TabletServer server : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
                if (server.getReplicaManager().getReplica(bucket)
                        instanceof ReplicaManager.OnlineReplica) {
                    leaderServer = server.getServerId();
                }
            }
            assertThat(leaderServer).isNotNegative();
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(leaderServer);
            FLUSS_CLUSTER_EXTENSION.startTabletServer(leaderServer);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);

            // the group log was recovered from disk and the watermark re-established on
            // leadership, so the filled prefix is visible and appends continue after it
            try (LogScanner scanner = createLogScanner(table, new int[] {0, 2})) {
                scanner.subscribe(0, 0L);
                List<ScanRecord> records = pollUpTo(scanner, 7, Duration.ofSeconds(20));
                assertThat(records).hasSize(7);
                assertThat(records.get(6).getRow().getString(1).toString()).isEqualTo("geo6");
                assertThat(pollUpTo(scanner, 1, Duration.ofSeconds(1))).isEmpty();

                AppendColumnsResult result =
                        writer.appendColumns("geo_group", bucket, 7L, geoRows(7, BASE_ROWS)).get();
                assertThat(result.getLogEndOffset()).isEqualTo(BASE_ROWS);
                assertThat(pollUpTo(scanner, 3, Duration.ofSeconds(10))).hasSize(3);
            }
        }
    }

    @Test
    void testAppendColumnsValidation() throws Exception {
        TablePath tablePath = createColumnGroupTable("validation");
        try (Table table = conn.getTable(tablePath)) {
            writeBaseRows(table, BASE_ROWS);
            TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
            AppendWriter writer = table.newAppend().createWriter();

            // unknown group is rejected on the client
            assertThatThrownBy(
                            () -> writer.appendColumns("no_such_group", bucket, 0L, geoRows(0, 1)))
                    .isInstanceOf(UnknownColumnGroupException.class);

            // a gap: the group expects offset 0
            assertThatThrownBy(
                            () ->
                                    writer.appendColumns("geo_group", bucket, 2L, geoRows(2, 4))
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InvalidColumnGroupOffsetException.class)
                    .satisfies(
                            e ->
                                    assertThat(
                                                    ((InvalidColumnGroupOffsetException)
                                                                    e.getCause())
                                                            .getExpectedSourceOffset())
                                            .isEqualTo(0L));

            // running past the base high watermark
            assertThatThrownBy(
                            () ->
                                    writer.appendColumns(
                                                    "geo_group",
                                                    bucket,
                                                    0L,
                                                    geoRows(0, BASE_ROWS + 1))
                                            .get())
                    .hasCauseInstanceOf(InvalidColumnGroupOffsetException.class)
                    .hasMessageContaining("high watermark");

            // a valid write, then a whole-batch replay is acknowledged without effect
            assertThat(
                            writer.appendColumns("geo_group", bucket, 0L, geoRows(0, 5))
                                    .get()
                                    .getLogEndOffset())
                    .isEqualTo(5L);
            assertThat(
                            writer.appendColumns("geo_group", bucket, 0L, geoRows(0, 5))
                                    .get()
                                    .getLogEndOffset())
                    .isEqualTo(5L);
            assertThat(
                            writer.appendColumns("geo_group", bucket, 1L, geoRows(1, 3))
                                    .get()
                                    .getLogEndOffset())
                    .isEqualTo(5L);

            // a batch straddling the log end offset is rejected with the expected offset so the
            // client can re-slice
            assertThatThrownBy(
                            () ->
                                    writer.appendColumns("geo_group", bucket, 3L, geoRows(3, 8))
                                            .get())
                    .hasCauseInstanceOf(InvalidColumnGroupOffsetException.class)
                    .satisfies(
                            e ->
                                    assertThat(
                                                    ((InvalidColumnGroupOffsetException)
                                                                    e.getCause())
                                                            .getExpectedSourceOffset())
                                            .isEqualTo(5L));

            // wrong arity is rejected on the client
            assertThatThrownBy(
                            () ->
                                    writer.appendColumns(
                                            "geo_group",
                                            bucket,
                                            5L,
                                            Arrays.asList(row(BinaryString.fromString("x"), 1.0d))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
