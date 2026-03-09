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

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.scanner.KvScan;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link KvScan}. */
class TableKvScanITCase extends ClientToServerITCaseBase {

    private static final int NUM_BUCKETS = 3;

    private static final Schema PK_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema PARTITIONED_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("p", DataTypes.STRING())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id", "p")
                    .build();

    @Test
    void testBasicScan() throws Exception {
        TablePath path = TablePath.of("test_db", "test_basic_scan");
        createPkTable(path);

        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "a"));
            writer.upsert(row(2, "b"));
            writer.upsert(row(3, "c"));
            writer.flush();

            List<InternalRow> result = kvScanAll(table);

            assertThat(result).hasSize(3);
            result.sort(Comparator.comparingInt(r -> r.getInt(0)));
            assertThatRow(result.get(0)).withSchema(PK_SCHEMA.getRowType()).isEqualTo(row(1, "a"));
            assertThatRow(result.get(1)).withSchema(PK_SCHEMA.getRowType()).isEqualTo(row(2, "b"));
            assertThatRow(result.get(2)).withSchema(PK_SCHEMA.getRowType()).isEqualTo(row(3, "c"));
        }
    }

    @Test
    void testEmptyTable() throws Exception {
        TablePath path = TablePath.of("test_db", "test_empty_table");
        createPkTable(path);

        try (Table table = conn.getTable(path)) {
            assertThat(kvScanAll(table)).isEmpty();
        }
    }

    @Test
    void testLargeDataScan() throws Exception {
        TablePath path = TablePath.of("test_db", "test_large_data_scan");
        createPkTable(path);

        int rowCount = 10_000;
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; i < rowCount; i++) {
                writer.upsert(row(i, "val" + i));
            }
            writer.flush();

            List<InternalRow> result = kvScanAll(table);

            assertThat(result).hasSize(rowCount);
            result.sort(Comparator.comparingInt(r -> r.getInt(0)));

            assertThatRow(result.get(0))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(0, "val0"));
            assertThatRow(result.get(rowCount / 2))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(rowCount / 2, "val" + rowCount / 2));
            assertThatRow(result.get(rowCount - 1))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(rowCount - 1, "val" + (rowCount - 1)));
        }
    }

    @Test
    void testPartitionedTableScan() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_partitioned_scan");
        long tableId = createPartitionedTable(tablePath);

        admin.createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("p", "p1")), false)
                .get();
        admin.createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("p", "p2")), false)
                .get();
        waitPartitionedTableReplicasReady(tableId, tablePath);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "p1", "a1"));
            writer.upsert(row(2, "p1", "b1"));
            writer.upsert(row(1, "p2", "a2"));
            writer.flush();

            List<InternalRow> result = kvScanAll(table);
            assertThat(result).hasSize(3);
            result.sort(
                    Comparator.comparingInt((InternalRow r) -> r.getInt(0))
                            .thenComparing(r -> r.getString(1).toString()));
            assertThatRow(result.get(0))
                    .withSchema(PARTITIONED_SCHEMA.getRowType())
                    .isEqualTo(row(1, "p1", "a1"));
            assertThatRow(result.get(1))
                    .withSchema(PARTITIONED_SCHEMA.getRowType())
                    .isEqualTo(row(1, "p2", "a2"));
            assertThatRow(result.get(2))
                    .withSchema(PARTITIONED_SCHEMA.getRowType())
                    .isEqualTo(row(2, "p1", "b1"));
        }
    }

    @Test
    void testPartitionedTableEmptyPartition() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_partitioned_empty");
        long tableId = createPartitionedTable(tablePath);

        admin.createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("p", "p1")), false)
                .get();
        admin.createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("p", "p2")), false)
                .get();
        waitPartitionedTableReplicasReady(tableId, tablePath);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "p1", "a1"));
            writer.upsert(row(2, "p1", "b1"));
            writer.flush();

            List<InternalRow> result = kvScanAll(table);
            // Only p1 rows; p2 is empty and must not contribute any rows.
            assertThat(result).hasSize(2);
        }
    }

    @Test
    void testDeleteVisibility() throws Exception {
        TablePath path = TablePath.of("test_db", "test_delete_visibility");
        createPkTable(path);

        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "a"));
            writer.upsert(row(2, "b"));
            writer.upsert(row(3, "c"));
            writer.flush();

            writer.delete(row(2, "b"));
            writer.flush();

            List<InternalRow> result = kvScanAll(table);

            assertThat(result).hasSize(2);
            result.sort(Comparator.comparingInt(r -> r.getInt(0)));
            assertThatRow(result.get(0)).withSchema(PK_SCHEMA.getRowType()).isEqualTo(row(1, "a"));
            assertThatRow(result.get(1)).withSchema(PK_SCHEMA.getRowType()).isEqualTo(row(3, "c"));
        }
    }

    @Test
    void testUpsertOverwrite() throws Exception {
        TablePath path = TablePath.of("test_db", "test_upsert_overwrite");
        createPkTable(path);

        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "original"));
            writer.flush();

            writer.upsert(row(1, "updated"));
            writer.flush();

            List<InternalRow> result = kvScanAll(table);

            assertThat(result).hasSize(1);
            assertThatRow(result.get(0))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(1, "updated"));
        }
    }

    /**
     * Verifies that a scan started after a mutation sees the updated state, and a scan started
     * before the mutation sees the original state.
     */
    @Test
    void testSequentialScansReflectMutations() throws Exception {
        TablePath path = TablePath.of("test_db", "test_sequential_scans");
        createPkTable(path);

        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "a"));
            writer.upsert(row(2, "b"));
            writer.upsert(row(3, "c"));
            writer.flush();

            // First scan: fully drains before any mutation.
            List<InternalRow> beforeMutation = kvScanAll(table);

            // Mutate: add row 4, delete row 1.
            writer.upsert(row(4, "d"));
            writer.delete(row(1, "a"));
            writer.flush();

            // Second scan: must see {2, 3, 4}.
            List<InternalRow> afterMutation = kvScanAll(table);

            assertThat(beforeMutation).hasSize(3);
            beforeMutation.sort(Comparator.comparingInt(r -> r.getInt(0)));
            assertThatRow(beforeMutation.get(0))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(1, "a"));
            assertThatRow(beforeMutation.get(1))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(2, "b"));
            assertThatRow(beforeMutation.get(2))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(3, "c"));

            assertThat(afterMutation).hasSize(3);
            afterMutation.sort(Comparator.comparingInt(r -> r.getInt(0)));
            assertThatRow(afterMutation.get(0))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(2, "b"));
            assertThatRow(afterMutation.get(1))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(3, "c"));
            assertThatRow(afterMutation.get(2))
                    .withSchema(PK_SCHEMA.getRowType())
                    .isEqualTo(row(4, "d"));
        }
    }

    @Test
    void testNonPrimaryKeyTableThrows() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_log_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(NUM_BUCKETS).build();

        createTable(tablePath, descriptor, false);
        try (Table table = conn.getTable(tablePath)) {
            assertThatThrownBy(() -> table.newKvScan())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not a Primary Key Table");
        }
    }

    private void createPkTable(TablePath path) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(PK_SCHEMA)
                        .distributedBy(NUM_BUCKETS, "id")
                        .build();
        long tableId = createTable(path, descriptor, true);
        waitAllReplicasReady(tableId, NUM_BUCKETS);
    }

    private long createPartitionedTable(TablePath path) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(PARTITIONED_SCHEMA)
                        .partitionedBy("p")
                        .distributedBy(NUM_BUCKETS, "id")
                        .build();
        return createTable(path, descriptor, true);
    }

    private void waitPartitionedTableReplicasReady(long tableId, TablePath tablePath)
            throws Exception {
        List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
        for (PartitionInfo partition : partitions) {
            for (int i = 0; i < NUM_BUCKETS; i++) {
                FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                        new TableBucket(tableId, partition.getPartitionId(), i));
            }
        }
    }

    private List<InternalRow> kvScanAll(Table table) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        try (CloseableIterator<InternalRow> iterator = table.newKvScan().execute()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        }
        return rows;
    }
}
