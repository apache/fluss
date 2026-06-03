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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.MultiTableWriteRecord;
import org.apache.fluss.client.table.writer.MultiTableWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link MultiTableLogScannerImpl}. */
public class MultiTableLogScannerITCase extends ClientToServerITCaseBase {

    private static final Schema LOG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    private static final Schema LOG_SCHEMA_2 =
            Schema.newBuilder()
                    .column("key", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .column("score", DataTypes.BIGINT())
                    .build();

    @Test
    void testSingleTableSubscribeAndPoll() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_single");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, descriptor, false);
        waitAllReplicasReady(tableId, 1);

        int recordCount = 10;
        List<GenericRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow r = row(i, "name-" + i);
                expectedRows.add(r);
                writer.append(r).get();
            }
        }

        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tablePath, 0);

            List<GenericRow> actualRows = new ArrayList<>();
            while (actualRows.size() < recordCount) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord record : records) {
                    assertThat(record.getTablePath()).isEqualTo(tablePath);
                    assertThat(record.getTableId()).isEqualTo(tableId);
                    assertThat(record.getSchemaId()).isGreaterThanOrEqualTo(0);
                    assertThat(record.getSchema().getRowType()).isEqualTo(LOG_SCHEMA.getRowType());
                    assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    InternalRow r = record.getRow();
                    actualRows.add(row(r.getInt(0), r.getString(1)));
                }
            }

            assertThat(actualRows).hasSize(recordCount);
            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }

    @Test
    void testMultipleTableSubscribeAndPoll() throws Exception {
        TablePath tablePath1 = TablePath.of("test_db", "multi_scanner_t1");
        TablePath tablePath2 = TablePath.of("test_db", "multi_scanner_t2");
        TableDescriptor descriptor1 =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        TableDescriptor descriptor2 =
                TableDescriptor.builder().schema(LOG_SCHEMA_2).distributedBy(1).build();

        long tableId1 = createTable(tablePath1, descriptor1, false);
        long tableId2 = createTable(tablePath2, descriptor2, true);
        waitAllReplicasReady(tableId1, 1);
        waitAllReplicasReady(tableId2, 1);

        // Write data to table 1.
        int recordCount1 = 5;
        List<GenericRow> expectedRows1 = new ArrayList<>();
        try (Table table1 = conn.getTable(tablePath1)) {
            AppendWriter writer = table1.newAppend().createWriter();
            for (int i = 0; i < recordCount1; i++) {
                GenericRow r = row(i, "t1-" + i);
                expectedRows1.add(r);
                writer.append(r).get();
            }
        }

        // Write data to table 2.
        int recordCount2 = 5;
        List<GenericRow> expectedRows2 = new ArrayList<>();
        try (Table table2 = conn.getTable(tablePath2)) {
            AppendWriter writer = table2.newAppend().createWriter();
            for (int i = 0; i < recordCount2; i++) {
                GenericRow r = row(100 + i, "t2-" + i, (long) (i * 10));
                expectedRows2.add(r);
                writer.append(r).get();
            }
        }

        int totalRecords = recordCount1 + recordCount2;
        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tablePath1, 0);
            scanner.subscribeFromBeginning(tablePath2, 0);

            Map<TablePath, List<GenericRow>> actualRowsByTable = new HashMap<>();
            int totalPolled = 0;
            while (totalPolled < totalRecords) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TablePath tp : records.tablePaths()) {
                    for (MultiTableRecord record : records.records(tp)) {
                        assertThat(record.getTablePath()).isEqualTo(tp);
                        assertThat(record.getSchemaId()).isGreaterThanOrEqualTo(0);
                        List<GenericRow> rows =
                                actualRowsByTable.computeIfAbsent(tp, k -> new ArrayList<>());
                        InternalRow r = record.getRow();
                        if (tp.equals(tablePath1)) {
                            rows.add(row(r.getInt(0), r.getString(1)));
                        } else {
                            rows.add(row(r.getInt(0), r.getString(1), r.getLong(2)));
                        }
                    }
                }
                totalPolled += records.count();
            }

            assertThat(actualRowsByTable).containsKey(tablePath1);
            assertThat(actualRowsByTable).containsKey(tablePath2);
            assertThat(actualRowsByTable.get(tablePath1))
                    .containsExactlyInAnyOrderElementsOf(expectedRows1);
            assertThat(actualRowsByTable.get(tablePath2))
                    .containsExactlyInAnyOrderElementsOf(expectedRows2);
        }
    }

    @Test
    void testUnsubscribe() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_unsub");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(2).build();
        long tableId = createTable(tablePath, descriptor, false);
        waitAllReplicasReady(tableId, 2);

        // Write data.
        int recordCount = 10;
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                writer.append(row(i, "v-" + i)).get();
            }
        }

        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tablePath, 0);
            scanner.subscribeFromBeginning(tablePath, 1);

            // Poll some data to ensure subscription works.
            int polled = 0;
            long deadline = System.currentTimeMillis() + 10_000;
            while (polled < recordCount && System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                polled += records.count();
            }
            assertThat(polled).isEqualTo(recordCount);

            // Unsubscribe bucket 1 and verify it no longer returns records from bucket 1.
            scanner.unsubscribe(tablePath, 1);

            // Write more data.
            try (Table table = conn.getTable(tablePath)) {
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 0; i < 10; i++) {
                    writer.append(row(100 + i, "new-" + i)).get();
                }
            }

            // Poll again and collect bucket info - should only see bucket 0.
            int extraPolled = 0;
            deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets(tablePath)) {
                    assertThat(bucket.getBucket()).isEqualTo(0);
                    assertThat(bucket.getTableId()).isEqualTo(tableId);
                }
                extraPolled += records.count();
            }
            // We should have polled some records only from bucket 0.
            assertThat(extraPolled).isGreaterThan(0);
        }
    }

    @Test
    void testPollWithoutSubscription() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_no_sub");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        createTable(tablePath, descriptor, false);

        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            assertThatThrownBy(() -> scanner.poll(Duration.ofSeconds(1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not subscribed");
        }
    }

    @Test
    void testPollAfterClose() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_closed");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        createTable(tablePath, descriptor, false);

        MultiTable multiTable = conn.getMultiTable();
        MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner();
        scanner.close();
        assertThatThrownBy(() -> scanner.poll(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testPartitionedTableSubscribeAndPoll() throws Exception {
        // Create a partitioned log table with partition key "pt".
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_partitioned");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        long tableId = createTable(tablePath, descriptor, false);

        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        assertThat(partitionIdByNames).isNotEmpty();

        // Write data to each partition.
        int recordsPerPartition = 5;
        Map<Long, List<GenericRow>> expectedRowsByPartition = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (Map.Entry<String, Long> entry : partitionIdByNames.entrySet()) {
                String partitionName = entry.getKey();
                long partitionId = entry.getValue();
                for (int i = 0; i < recordsPerPartition; i++) {
                    GenericRow r = row(i, "p-" + i, partitionName);
                    expectedRowsByPartition
                            .computeIfAbsent(partitionId, k -> new ArrayList<>())
                            .add(r);
                    writer.append(r).get();
                }
            }
        }

        int totalRecords = recordsPerPartition * partitionIdByNames.size();
        // use a new connection to avoid sharing metadata.
        try (Connection newConnection = ConnectionFactory.createConnection(clientConf);
                MultiTableLogScanner scanner =
                        newConnection.getMultiTable().newMultiTableScan().createLogScanner()) {
            // Subscribe to each partition bucket.
            for (Long partitionId : partitionIdByNames.values()) {
                scanner.subscribeFromBeginning(tablePath, partitionId, 0);
            }

            Map<Long, List<GenericRow>> actualRowsByPartition = new HashMap<>();
            int totalPolled = 0;
            while (totalPolled < totalRecords) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets(tablePath)) {
                    for (MultiTableRecord record : records.records(tablePath, bucket)) {
                        assertThat(record.getTablePath()).isEqualTo(tablePath);
                        assertThat(record.getTableId()).isEqualTo(tableId);
                        assertThat(record.getSchemaId()).isGreaterThanOrEqualTo(0);
                        assertThat(record.getSchema().getRowType()).isEqualTo(schema.getRowType());
                        assertThat(bucket.getPartitionId()).isNotNull();
                        long partitionId = bucket.getPartitionId();
                        InternalRow r = record.getRow();
                        actualRowsByPartition
                                .computeIfAbsent(partitionId, k -> new ArrayList<>())
                                .add(row(r.getInt(0), r.getString(1), r.getString(2)));
                    }
                }
                totalPolled += records.count();
            }

            // Verify we received records for each partition.
            for (Map.Entry<Long, List<GenericRow>> entry : expectedRowsByPartition.entrySet()) {
                long partitionId = entry.getKey();
                assertThat(actualRowsByPartition).containsKey(partitionId);
                assertThat(actualRowsByPartition.get(partitionId))
                        .containsExactlyInAnyOrderElementsOf(entry.getValue());
            }
        }
    }

    @Test
    void testPartitionedAndNonPartitionedMixed() throws Exception {
        // Table 1: partitioned log table.
        TablePath tablePath1 = TablePath.of("test_db", "multi_scanner_part_mix1");
        Schema schema1 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor1 =
                TableDescriptor.builder()
                        .schema(schema1)
                        .distributedBy(1)
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        long tableId1 = createTable(tablePath1, descriptor1, false);

        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath1);
        assertThat(partitionIdByNames).isNotEmpty();

        // Table 2: non-partitioned log table.
        TablePath tablePath2 = TablePath.of("test_db", "multi_scanner_part_mix2");
        TableDescriptor descriptor2 =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        long tableId2 = createTable(tablePath2, descriptor2, true);
        waitAllReplicasReady(tableId2, 1);

        // Write data to the partitioned table.
        int recordsPerPartition = 3;
        int partitionedRecordCount = recordsPerPartition * partitionIdByNames.size();
        try (Table table1 = conn.getTable(tablePath1)) {
            AppendWriter writer = table1.newAppend().createWriter();
            for (String partitionName : partitionIdByNames.keySet()) {
                for (int i = 0; i < recordsPerPartition; i++) {
                    writer.append(row(i, "pv-" + i, partitionName)).get();
                }
            }
        }

        // Write data to the non-partitioned table.
        int nonPartitionedRecordCount = 5;
        List<GenericRow> expectedNonPartitioned = new ArrayList<>();
        try (Table table2 = conn.getTable(tablePath2)) {
            AppendWriter writer = table2.newAppend().createWriter();
            for (int i = 0; i < nonPartitionedRecordCount; i++) {
                GenericRow r = row(i, "np-" + i);
                expectedNonPartitioned.add(r);
                writer.append(r).get();
            }
        }

        int totalRecords = partitionedRecordCount + nonPartitionedRecordCount;
        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            // Subscribe to partitioned table buckets.
            for (Long partitionId : partitionIdByNames.values()) {
                scanner.subscribeFromBeginning(tablePath1, partitionId, 0);
            }
            // Subscribe to non-partitioned table bucket.
            scanner.subscribeFromBeginning(tablePath2, 0);

            List<GenericRow> actualNonPartitioned = new ArrayList<>();
            int partitionedPolled = 0;
            int totalPolled = 0;
            while (totalPolled < totalRecords) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord record : records) {
                    long tid = record.getTableId();
                    InternalRow r = record.getRow();
                    if (tid == tableId1) {
                        partitionedPolled++;
                    } else {
                        assertThat(tid).isEqualTo(tableId2);
                        actualNonPartitioned.add(row(r.getInt(0), r.getString(1)));
                    }
                }
                totalPolled += records.count();
            }

            assertThat(partitionedPolled).isEqualTo(partitionedRecordCount);
            assertThat(actualNonPartitioned)
                    .containsExactlyInAnyOrderElementsOf(expectedNonPartitioned);
        }
    }

    @Test
    void testMultipleBucketsOfSameTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_buckets");
        int bucketCount = 3;
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(bucketCount).build();
        long tableId = createTable(tablePath, descriptor, false);
        waitAllReplicasReady(tableId, bucketCount);

        int recordCount = 30;
        List<GenericRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow r = row(i, "val-" + i);
                expectedRows.add(r);
                writer.append(r).get();
            }
        }

        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            for (int i = 0; i < bucketCount; i++) {
                scanner.subscribeFromBeginning(tablePath, i);
            }

            List<GenericRow> actualRows = new ArrayList<>();
            while (actualRows.size() < recordCount) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord record : records) {
                    assertThat(record.getTableId()).isEqualTo(tableId);
                    InternalRow r = record.getRow();
                    actualRows.add(row(r.getInt(0), r.getString(1)));
                }
            }

            assertThat(actualRows).hasSize(recordCount);
            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }

    @Test
    void testReadDataBeforeAndAfterAddColumn() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_add_col");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, descriptor, false);
        waitAllReplicasReady(tableId, 1);

        // Phase 1: Write data with initial schema (id INT, name STRING).
        int preAddColumnCount = 5;
        List<GenericRow> expectedPreRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < preAddColumnCount; i++) {
                GenericRow r = row(i, "before-" + i);
                expectedPreRows.add(r);
                writer.append(r).get();
            }
        }

        // Add a new column "score BIGINT" to the table.
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "score",
                                        DataTypes.BIGINT(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();

        // Phase 2: Write data with new schema (id INT, name STRING, score BIGINT).
        int postAddColumnCount = 5;
        List<GenericRow> expectedPostRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < postAddColumnCount; i++) {
                GenericRow r = row(100 + i, "after-" + i, (long) (i * 10));
                expectedPostRows.add(r);
                writer.append(r).get();
            }
        }

        // Subscribe from the beginning and read all records.
        int totalRecords = preAddColumnCount + postAddColumnCount;
        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tablePath, 0);

            List<GenericRow> actualPreRows = new ArrayList<>();
            List<GenericRow> actualPostRows = new ArrayList<>();
            int totalPolled = 0;
            while (totalPolled < totalRecords) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord record : records) {
                    assertThat(record.getTableId()).isEqualTo(tableId);
                    InternalRow r = record.getRow();
                    RowType rowType = record.getSchema().getRowType();
                    if (rowType.getFieldCount() == 2) {
                        actualPreRows.add(row(r.getInt(0), r.getString(1)));
                    } else {
                        assertThat(rowType.getFieldCount()).isEqualTo(3);
                        actualPostRows.add(row(r.getInt(0), r.getString(1), r.getLong(2)));
                    }
                    totalPolled++;
                }
            }

            assertThat(actualPreRows).containsExactlyInAnyOrderElementsOf(expectedPreRows);
            assertThat(actualPostRows).containsExactlyInAnyOrderElementsOf(expectedPostRows);
        }
    }

    @Test
    void testSubscribeAfterDropRecreateFailsThenRecovers() throws Exception {
        // A table that is dropped and recreated under the same TablePath gets a new tableId. A
        // scanner that already subscribed the original table caches the old tableId; re-subscribing
        // the same path must detect the tableId change (once the shared connection metadata learns
        // the new id) and fail fast. Recovery: unsubscribe all stale buckets (which evicts the
        // cached registration) and subscribe again, which re-resolves the recreated table.
        TablePath tablePath = TablePath.of("test_db", "multi_scanner_drop_recreate");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        long oldTableId = createTable(tablePath, descriptor, false);
        waitAllReplicasReady(oldTableId, 1);

        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner()) {
            // 1. Subscribe to the original table; the scanner caches the old tableId.
            scanner.subscribeFromBeginning(tablePath, 0);

            // 2. Drop and recreate the same path -> new tableId.
            admin.dropTable(tablePath, false).get();
            long newTableId = createTable(tablePath, descriptor, true);
            waitAllReplicasReady(newTableId, 1);
            assertThat(newTableId).isNotEqualTo(oldTableId);

            // 3. Write a row to the recreated table through a writer on the SAME connection. This
            //    forces the shared connection metadata to learn the new tableId, which is the
            //    state the scanner inspects when validating its cached registration.
            GenericRow recreatedRow = row(1, "after-recreate");
            try (MultiTableWriter writer = multiTable.newMultiTableWrite().createWriter()) {
                int newSid = admin.getTableInfo(tablePath).get().getSchemaId();
                writer.write(
                                MultiTableWriteRecord.of(
                                        tablePath, ChangeType.INSERT, recreatedRow, newSid))
                        .get();
                writer.flush();
            }

            // 4. Re-subscribing the stale table now detects the recreate and fails fast.
            assertThatThrownBy(() -> scanner.subscribeFromBeginning(tablePath, 0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("has been recreated")
                    .hasMessageContaining(String.valueOf(oldTableId))
                    .hasMessageContaining(String.valueOf(newTableId));

            // 5. Recovery: unsubscribe the stale bucket (evicts the cached registration), then
            //    subscribe again -> re-resolves the recreated table and reads its data.
            scanner.unsubscribe(tablePath, 0);
            scanner.subscribeFromBeginning(tablePath, 0);

            List<GenericRow> actualRows = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (actualRows.isEmpty() && System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord record : records) {
                    assertThat(record.getTableId()).isEqualTo(newTableId);
                    InternalRow r = record.getRow();
                    actualRows.add(row(r.getInt(0), r.getString(1)));
                }
            }
            assertThat(actualRows).containsExactly(recreatedRow);
        }
    }
}
