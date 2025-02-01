/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link LogScannerImpl}. */
public class LogScannerITCase extends ClientToServerITCaseBase {

    @Test
    void testPoll() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_INFO.getTableDescriptor(), false);

        // append a batch of data.
        int recordSize = 10;
        List<IndexedRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < recordSize; i++) {
                IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {i, "a"});
                expectedRows.add(row);
                appendWriter.append(row).get();
            }

            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            List<InternalRow> rowList = new ArrayList<>();
            while (rowList.size() < recordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(
                            row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
                }
            }
            assertThat(rowList).hasSize(recordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }

    @Test
    void testPollWhileCreateTableNotReady() throws Exception {
        // create one table with 100 buckets.
        int bucketNumber = 100;
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(bucketNumber).build();
        createTable(DATA1_TABLE_PATH, tableDescriptor, false);

        // append a batch of data.
        int recordSize = 10;
        List<IndexedRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < recordSize; i++) {
                IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {i, "a"});
                expectedRows.add(row);
                appendWriter.append(row).get();
            }

            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            List<IndexedRow> rowList = new ArrayList<>();
            while (rowList.size() < recordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(
                            row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
                }
            }
            assertThat(rowList).hasSize(recordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }

    @Test
    void testLogScannerMultiThreadAccess() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_INFO.getTableDescriptor(), false);

        // append a batch of data.
        int recordSize = 10;
        List<IndexedRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < recordSize; i++) {
                IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {i, "a"});
                expectedRows.add(row);
                appendWriter.append(row).get();
            }

            LogScanner logScanner = table.newScan().createLogScanner();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            // subscribe in thread1
            executor.submit(() -> logScanner.subscribe(0, LogScanner.EARLIEST_OFFSET)).get();
            // subscribe again in main thread
            logScanner.subscribe(1, LogScanner.EARLIEST_OFFSET);
            // subscribe again in thread1
            executor.submit(() -> logScanner.subscribeFromBeginning(2)).get();

            // should be able to poll data from all buckets
            List<InternalRow> rowList = new ArrayList<>();
            while (rowList.size() < recordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(
                            row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
                }
            }
            assertThat(rowList).hasSize(recordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
            logScanner.close();
        }
    }

    @Test
    void testLogHeavyWriteAndScan() throws Exception {
        final String db = "db";
        final String tbl = "log_heavy_table";
        // create table
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("small_str", DataTypes.STRING())
                                        .column("bi", DataTypes.BIGINT())
                                        .column("long_str", DataTypes.STRING())
                                        .build())
                        .distributedBy(1) // 1 bucket for benchmark
                        .build();
        createTable(TablePath.of(db, tbl), descriptor, false);

        // produce logs
        // In the default configuration, every 15 records append will full a batch.
        // Besides, we inject a force flush every 100 records to have non-full batches.
        // This can reproduce the corner case bug.
        long recordSize = 1_000;
        RowType rowType = descriptor.getSchema().toRowType();
        try (Table table = conn.getTable(TablePath.of(db, tbl))) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (long i = 0; i < recordSize; i++) {
                final Object[] columns =
                        new Object[] {randomAlphanumeric(10), i, randomAlphanumeric(1000)};
                appendWriter.append(row(rowType, columns));
                if (i % 100 == 0) {
                    appendWriter.flush();
                }
            }
            appendWriter.flush();

            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            long scanned = 0;
            long total = 0;
            while (scanned < recordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    assertThat(scanRecord.getRow().getString(0).getSizeInBytes()).isEqualTo(10);
                    assertThat(scanRecord.getRow().getLong(1)).isEqualTo(scanned);
                    assertThat(scanRecord.getRow().getString(2).getSizeInBytes()).isEqualTo(1000);
                    scanned++;
                }
                total += scanRecords.count();
            }
            assertThat(scanned).isEqualTo(recordSize).isEqualTo(total);
            logScanner.close();
        }
    }

    @Test
    void testKvHeavyWriteAndScan() throws Exception {
        final String db = "db";
        final String tbl = "kv_heavy_table";
        // create table
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("small_str", DataTypes.STRING())
                                        .column("bi", DataTypes.BIGINT())
                                        .column("long_str", DataTypes.STRING())
                                        .primaryKey("bi")
                                        .build())
                        .distributedBy(1) // 1 bucket for benchmark
                        .build();
        createTable(TablePath.of(db, tbl), descriptor, false);

        // produce logs
        // In the default configuration, every 15 records append will full a batch.
        // Besides, we inject a force flush every 100 records to have non-full batches.
        // This can reproduce the corner case bug.
        long recordSize = 1_000;
        RowType rowType = descriptor.getSchema().toRowType();
        try (Table table = conn.getTable(TablePath.of(db, tbl))) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (long i = 0; i < recordSize; i++) {
                final Object[] columns =
                        new Object[] {randomAlphanumeric(10), i, randomAlphanumeric(1000)};
                upsertWriter.upsert(compactedRow(rowType, columns));
                if (i % 100 == 0) {
                    upsertWriter.flush();
                }
            }
            upsertWriter.flush();

            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            long scanned = 0;
            long total = 0;
            while (scanned < recordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.INSERT);
                    assertThat(scanRecord.getRow().getString(0).getSizeInBytes()).isEqualTo(10);
                    assertThat(scanRecord.getRow().getLong(1)).isEqualTo(scanned);
                    assertThat(scanRecord.getRow().getString(2).getSizeInBytes()).isEqualTo(1000);
                    scanned++;
                }
                total += scanRecords.count();
            }
            assertThat(scanned).isEqualTo(recordSize).isEqualTo(total);
            logScanner.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testScanFromStartTimestamp(boolean isPartitioned) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "test_db_1",
                        "test_scan_from_timestamp" + (isPartitioned ? "_partitioned" : ""));
        long tableId =
                createTable(
                        tablePath,
                        isPartitioned
                                ? DATA1_PARTITIONED_TABLE_INFO.getTableDescriptor()
                                : DATA1_TABLE_INFO.getTableDescriptor(),
                        false);

        String partitionName = null;
        Long partitionId = null;
        if (!isPartitioned) {
            FLUSS_CLUSTER_EXTENSION.waitUtilTableReady(tableId);
        } else {
            Map<String, Long> partitionNameAndIds =
                    FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(tablePath);
            // just pick one partition
            Map.Entry<String, Long> partitionNameAndIdEntry =
                    partitionNameAndIds.entrySet().iterator().next();
            partitionName = partitionNameAndIdEntry.getKey();
            partitionId = partitionNameAndIds.get(partitionName);
            FLUSS_CLUSTER_EXTENSION.waitUtilTablePartitionReady(tableId, partitionId);
        }

        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);

        long firstStartTimestamp = System.currentTimeMillis();
        int batchRecordSize = 10;
        List<IndexedRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            // 1. first write one batch of data.
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < batchRecordSize; i++) {
                IndexedRow row =
                        row(
                                DATA1_ROW_TYPE,
                                new Object[] {i, partitionName == null ? "a" : partitionName});
                expectedRows.add(row);
                appendWriter.append(row).get();
            }

            // sleep a while to avoid secondStartTimestamp is same with firstStartTimestamp
            Thread.sleep(10);

            // record second batch start timestamp, we move this before first scan to make it
            // as early as possible to avoid potential time backwards
            // as early as possible to avoid potential time backwards
            long secondStartTimestamp = System.currentTimeMillis();

            LogScanner logScanner = createLogScanner(table);
            // try to fetch from firstStartTimestamp, which smaller than the first batch commit
            // timestamp.
            subscribeFromTimestamp(
                    physicalTablePath, partitionId, table, logScanner, admin, firstStartTimestamp);
            List<InternalRow> rowList = new ArrayList<>();
            while (rowList.size() < batchRecordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(
                            row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
                }
            }
            assertThat(rowList).hasSize(batchRecordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);

            // 2. write the second batch.
            List<IndexedRow> nextExpectedRows = new ArrayList<>();
            for (int i = 0; i < batchRecordSize; i++) {
                IndexedRow row =
                        row(
                                DATA1_ROW_TYPE,
                                new Object[] {i, partitionName == null ? "a" : partitionName});
                nextExpectedRows.add(row);
                appendWriter.append(row).get();
            }

            // try to fetch from secondStartTimestamp, which larger than the second batch commit
            // timestamp, return the data of second batch.
            subscribeFromTimestamp(
                    physicalTablePath, partitionId, table, logScanner, admin, secondStartTimestamp);
            rowList = new ArrayList<>();
            while (rowList.size() < batchRecordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(
                            row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
                }
            }
            assertThat(rowList).hasSize(batchRecordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(nextExpectedRows);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testScanFromLatestOffsets(boolean isPartitioned) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "test_db_1",
                        "test_scan_from_latest_offsets" + (isPartitioned ? "_partitioned" : ""));
        long tableId =
                createTable(
                        tablePath,
                        isPartitioned
                                ? DATA1_PARTITIONED_TABLE_INFO.getTableDescriptor()
                                : DATA1_TABLE_INFO.getTableDescriptor(),
                        false);
        String partitionName = null;
        Long partitionId = null;
        if (!isPartitioned) {
            FLUSS_CLUSTER_EXTENSION.waitUtilTableReady(tableId);
        } else {
            Map<String, Long> partitionNameAndIds =
                    FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(tablePath);
            // just pick one partition
            partitionName = partitionNameAndIds.keySet().iterator().next();
            partitionId = partitionNameAndIds.get(partitionName);
            FLUSS_CLUSTER_EXTENSION.waitUtilTablePartitionReady(tableId, partitionId);
        }
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);

        int batchRecordSize = 10;
        try (Table table = conn.getTable(tablePath)) {
            // 1. first write one batch of data.
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < batchRecordSize; i++) {
                appendWriter
                        .append(
                                row(
                                        DATA1_ROW_TYPE,
                                        new Object[] {
                                            i, partitionName == null ? "a" : partitionName
                                        }))
                        .get();
            }

            LogScanner logScanner = createLogScanner(table);
            // try to fetch from the latest offsets. For the first batch, it cannot get any data.
            subscribeFromLatestOffset(physicalTablePath, partitionId, table, logScanner, admin);
            assertThat(logScanner.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();

            // 2. write the second batch.
            List<IndexedRow> expectedRows = new ArrayList<>();
            for (int i = 0; i < batchRecordSize; i++) {
                IndexedRow row =
                        row(
                                DATA1_ROW_TYPE,
                                new Object[] {i, partitionName == null ? "a" : partitionName});
                expectedRows.add(row);
                appendWriter.append(row).get();
            }

            List<InternalRow> rowList = new ArrayList<>();
            while (rowList.size() < batchRecordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(
                            row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
                }
            }
            assertThat(rowList).hasSize(batchRecordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }
}
