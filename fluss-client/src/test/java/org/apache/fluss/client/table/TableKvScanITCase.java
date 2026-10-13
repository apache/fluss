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

package org.apache.fluss.client.table;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end IT case for {@link org.apache.fluss.client.table.scanner.batch.KvBatchScanner}. */
class TableKvScanITCase extends ClientToServerITCaseBase {

    private static final String DB = "test-kv-scan-db";

    private static final Schema PK_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final RowType PK_ROW_TYPE = PK_SCHEMA.getRowType();

    private static final int BUCKETS = 3;

    private static final TableDescriptor PK_TABLE =
            TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(BUCKETS, "id").build();

    @Test
    void wholeTableScanReturnsAllRows() throws Exception {
        TablePath tablePath = TablePath.of(DB, "whole-table-scan");
        long tableId = createTable(tablePath, PK_TABLE, true);
        waitAllReplicasReady(tableId, BUCKETS);

        try (Table table = conn.getTable(tablePath)) {
            int rowCount = 50;
            upsertRows(table, rowCount);

            try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                List<InternalRow> rows = BatchScanUtils.collectRows(scanner);
                assertThat(rows).hasSize(rowCount);
                assertThat(idsOf(rows)).containsExactlyInAnyOrderElementsOf(rangeAsList(rowCount));
            }
        }
    }

    @Test
    void singleBucketScanReturnsOnlyThatBucketsRows() throws Exception {
        TablePath tablePath = TablePath.of(DB, "single-bucket-scan");
        long tableId = createTable(tablePath, PK_TABLE, true);
        waitAllReplicasReady(tableId, BUCKETS);

        try (Table table = conn.getTable(tablePath)) {
            int rowCount = 30;
            upsertRows(table, rowCount);

            int bucketCount = table.getTableInfo().getNumBuckets();
            int totalAcrossBuckets = 0;
            for (int b = 0; b < bucketCount; b++) {
                try (BatchScanner scanner =
                        table.newScan().createBatchScanner(new TableBucket(tableId, b))) {
                    totalAcrossBuckets += BatchScanUtils.collectRows(scanner).size();
                }
            }
            assertThat(totalAcrossBuckets).isEqualTo(rowCount);
        }
    }

    @Test
    void emptyTableReturnsNoRows() throws Exception {
        TablePath tablePath = TablePath.of(DB, "empty-table-scan");
        long tableId = createTable(tablePath, PK_TABLE, true);
        waitAllReplicasReady(tableId, BUCKETS);

        try (Table table = conn.getTable(tablePath);
                BatchScanner scanner = table.newScan().createBatchScanner()) {
            assertThat(BatchScanUtils.collectRows(scanner)).isEmpty();
        }
    }

    @Test
    void snapshotIsolationHidesPostOpenWrites() throws Exception {
        // Single bucket so the first poll opens *the* bucket scanner and pins its snapshot;
        // multi-bucket scanners open buckets lazily, which would race the post-open write.
        TablePath tablePath = TablePath.of(DB, "snapshot-isolation-scan");
        TableDescriptor singleBucket =
                TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(1, "id").build();
        long tableId = createTable(tablePath, singleBucket, true);
        waitAllReplicasReady(tableId, 1);

        // Use a very small fetch max bytes to ensure the first poll returns only partial rows,
        // so subsequent collectRows will issue new scanKv RPCs to truly verify snapshot isolation.
        Configuration smallFetchConf = new Configuration(clientConf);
        smallFetchConf.setString(ConfigOptions.CLIENT_SCANNER_KV_FETCH_MAX_BYTES.key(), "128b");

        try (Connection smallFetchConn = ConnectionFactory.createConnection(smallFetchConf);
                Table table = smallFetchConn.getTable(tablePath)) {
            int initialRows = 20;
            upsertRows(table, initialRows);

            try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                // Pull the first batch (snapshot is pinned; small fetch bytes returns partial rows)
                List<InternalRow> firstBatch = drainOnePoll(scanner);
                assertThat(firstBatch.size()).isLessThan(initialRows);

                // Write a new row after the snapshot (id=20, name="post-snapshot")
                upsertRow(table, initialRows, "post-snapshot");

                // Continue pulling remaining data (triggers new scanKv RPCs)
                List<InternalRow> remaining = BatchScanUtils.collectRows(scanner);
                List<InternalRow> all = new ArrayList<>(firstBatch);
                all.addAll(remaining);

                // Total rows should be 20 (excluding the row written after the snapshot)
                assertThat(all).hasSize(initialRows);
                assertThat(idsOf(all)).doesNotContain(initialRows); // id=20 not visible
            }
        }
    }

    @Test
    void projectionAppliedClientSide() throws Exception {
        TablePath tablePath = TablePath.of(DB, "projection-scan");
        long tableId = createTable(tablePath, PK_TABLE, true);
        waitAllReplicasReady(tableId, BUCKETS);

        try (Table table = conn.getTable(tablePath)) {
            int rowCount = 10;
            upsertRows(table, rowCount);

            try (BatchScanner scanner =
                    table.newScan().project(new int[] {1}).createBatchScanner()) {
                List<InternalRow> projected = BatchScanUtils.collectRows(scanner);
                assertThat(projected).hasSize(rowCount);
                Set<String> names = new HashSet<>();
                for (InternalRow row : projected) {
                    assertThat(row.getFieldCount()).isEqualTo(1);
                    names.add(row.getString(0).toString());
                }
                assertThat(names).hasSize(rowCount);
            }
        }
    }

    @Test
    void projectionByColumnNamesAppliedClientSide() throws Exception {
        TablePath tablePath = TablePath.of(DB, "projection-by-name-scan");
        long tableId = createTable(tablePath, PK_TABLE, true);
        waitAllReplicasReady(tableId, BUCKETS);

        try (Table table = conn.getTable(tablePath)) {
            int rowCount = 10;
            upsertRows(table, rowCount);

            try (BatchScanner scanner =
                    table.newScan()
                            .project(Collections.singletonList("name"))
                            .createBatchScanner()) {
                List<InternalRow> projected = BatchScanUtils.collectRows(scanner);
                assertThat(projected).hasSize(rowCount);
                Set<String> names = new HashSet<>();
                for (InternalRow row : projected) {
                    assertThat(row.getFieldCount()).isEqualTo(1);
                    names.add(row.getString(0).toString());
                }
                assertThat(names).hasSize(rowCount);
            }
        }
    }

    @Test
    void logTableWithoutLimitFails() throws Exception {
        TablePath tablePath = TablePath.of(DB, "log-table-needs-limit");
        Schema logSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor logTable =
                TableDescriptor.builder().schema(logSchema).distributedBy(1).build();
        createTable(tablePath, logTable, true);

        try (Table table = conn.getTable(tablePath)) {
            assertThatThrownBy(() -> table.newScan().createBatchScanner())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Log Table");
        }
    }

    @Test
    void partitionedPrimaryKeyTableScanCoversAllPartitions() throws Exception {
        TablePath tablePath = TablePath.of(DB, "partitioned-pk-scan");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("c")
                        .distributedBy(2, "a")
                        .build();
        createTable(tablePath, descriptor, false);

        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("c", "p" + i), false).get();
        }
        List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
        assertThat(partitions).hasSize(3);

        int rowsPerPartition = 4;
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (PartitionInfo partition : partitions) {
                String name = partition.getPartitionName();
                for (int j = 0; j < rowsPerPartition; j++) {
                    writer.upsert(
                            compactedRow(schema.getRowType(), new Object[] {j, "v" + j, name}));
                }
            }
            writer.flush();

            try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                List<InternalRow> rows = BatchScanUtils.collectRows(scanner);
                assertThat(rows).hasSize(partitions.size() * rowsPerPartition);
                Set<String> partitionsSeen = new HashSet<>();
                for (InternalRow row : rows) {
                    partitionsSeen.add(row.getString(2).toString());
                }
                assertThat(partitionsSeen)
                        .containsExactlyInAnyOrderElementsOf(
                                partitions.stream()
                                        .map(PartitionInfo::getPartitionName)
                                        .collect(Collectors.toSet()));
            }
        }
    }

    @Test
    void partitionedTablePerBucketScansCoverAllPartitionBuckets() throws Exception {
        TablePath tablePath = TablePath.of(DB, "partitioned-per-bucket-scan");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "c")
                        .build();
        int bucketsPerPartition = 2;
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("c")
                        .distributedBy(bucketsPerPartition, "a")
                        .build();
        long tableId = createTable(tablePath, descriptor, false);

        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("c", "p" + i), false).get();
        }
        List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
        assertThat(partitions).hasSize(3);

        int rowsPerPartition = 8;
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (PartitionInfo partition : partitions) {
                String name = partition.getPartitionName();
                for (int j = 0; j < rowsPerPartition; j++) {
                    writer.upsert(
                            compactedRow(schema.getRowType(), new Object[] {j, "v" + j, name}));
                }
            }
            writer.flush();

            // Iterate every (partition, bucket) combination and sum per-partition rows.
            for (PartitionInfo partition : partitions) {
                long partitionId = partition.getPartitionId();
                int rowsForThisPartition = 0;
                for (int b = 0; b < bucketsPerPartition; b++) {
                    TableBucket tb = new TableBucket(tableId, partitionId, b);
                    try (BatchScanner scanner = table.newScan().createBatchScanner(tb)) {
                        List<InternalRow> bucketRows = BatchScanUtils.collectRows(scanner);
                        for (InternalRow row : bucketRows) {
                            assertThat(row.getString(2).toString())
                                    .isEqualTo(partition.getPartitionName());
                        }
                        rowsForThisPartition += bucketRows.size();
                    }
                }
                assertThat(rowsForThisPartition).isEqualTo(rowsPerPartition);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void upsertRows(Table table, int count) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        for (int i = 0; i < count; i++) {
            writer.upsert(compactedRow(PK_ROW_TYPE, new Object[] {i, "v" + i}));
        }
        writer.flush();
    }

    private static void upsertRow(Table table, int id, String name) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(compactedRow(PK_ROW_TYPE, new Object[] {id, name}));
        writer.flush();
    }

    /** Polls until at least one non-empty batch is consumed, or the scanner is drained. */
    private static List<InternalRow> drainOnePoll(BatchScanner scanner) throws Exception {
        Duration timeout = Duration.ofSeconds(10);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(200));
            if (it == null) {
                return new ArrayList<>();
            }
            try {
                List<InternalRow> rows = new ArrayList<>();
                while (it.hasNext()) {
                    rows.add(it.next());
                }
                if (!rows.isEmpty()) {
                    return rows;
                }
            } finally {
                it.close();
            }
        }
        throw new AssertionError("scanner produced no batch within " + timeout);
    }

    private static List<Integer> idsOf(List<InternalRow> rows) {
        List<Integer> ids = new ArrayList<>(rows.size());
        for (InternalRow row : rows) {
            ids.add(row.getInt(0));
        }
        return ids;
    }

    private static List<Integer> rangeAsList(int endExclusive) {
        List<Integer> out = new ArrayList<>(endExclusive);
        for (int i = 0; i < endExclusive; i++) {
            out.add(i);
        }
        return out;
    }
}
