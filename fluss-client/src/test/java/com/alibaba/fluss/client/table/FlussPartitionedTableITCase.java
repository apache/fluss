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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.keyRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for Fluss partitioned table. */
class FlussPartitionedTableITCase extends ClientToServerITCaseBase {

    @Test
    void testPartitionedPrimaryKeyTable() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(DATA1_TABLE_PATH_PK);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        UpsertWriter upsertWriter = table.getUpsertWriter();
        int recordsPerPartition = 5;
        // now, put some data to the partitions
        Map<Long, List<InternalRow>> expectPutRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row =
                        compactedRow(schema.toRowType(), new Object[] {i, "a" + i, partition});
                upsertWriter.upsert(row);
                expectPutRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }
        upsertWriter.flush();

        // now, let's lookup the written data by look up
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow actualRow =
                        compactedRow(schema.toRowType(), new Object[] {i, "a" + i, partition});
                InternalRow lookupRow =
                        table.lookup(keyRow(schema, new Object[] {i, null, partition}))
                                .get()
                                .getRow();
                assertThat(lookupRow).isEqualTo(actualRow);
            }
        }

        // then, let's scan and check the cdc log
        verifyPartitionLogs(table, schema.toRowType(), expectPutRows);
    }

    @Test
    void testPartitionedLogTable() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH, false);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(DATA1_TABLE_PATH);
        Table table = conn.getTable(DATA1_TABLE_PATH);
        AppendWriter appendWriter = table.getAppendWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row = row(schema.toRowType(), new Object[] {i, "a" + i, partition});
                appendWriter.append(row);
                expectPartitionAppendRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }
        appendWriter.flush();

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.toRowType(), expectPartitionAppendRows);
    }

    @Test
    void testUnsubscribePartitionBucket() throws Exception {
        // write rows
        TablePath tablePath = TablePath.of("test_db_1", "unsubscribe_partition_bucket_table");
        Schema schema = createPartitionedTable(tablePath, false);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(tablePath);
        Table table = conn.getTable(tablePath);

        Map<Long, List<InternalRow>> expectPartitionAppendRows =
                writeRows(table, schema, partitionIdByNames);

        // then, let's verify the logs
        try (LogScanner logScanner = table.getLogScanner(new LogScan())) {
            for (Long partitionId : expectPartitionAppendRows.keySet()) {
                logScanner.subscribeFromBeginning(partitionId, 0);
            }
            int totalRecords = getRowsCount(expectPartitionAppendRows);
            Map<Long, List<InternalRow>> actualRows = pollRecords(logScanner, totalRecords);
            verifyRows(schema.toRowType(), actualRows, expectPartitionAppendRows);

            // now, unsubscribe some partitions
            long removedPartitionId = expectPartitionAppendRows.keySet().iterator().next();
            logScanner.unsubscribe(removedPartitionId, 0);

            // now, write some records again
            expectPartitionAppendRows = writeRows(table, schema, partitionIdByNames);
            // remove the removed partition
            expectPartitionAppendRows.remove(removedPartitionId);
            totalRecords = getRowsCount(expectPartitionAppendRows);
            actualRows = pollRecords(logScanner, totalRecords);
            verifyRows(schema.toRowType(), actualRows, expectPartitionAppendRows);
        }
    }

    private int getRowsCount(Map<Long, List<InternalRow>> rows) {
        return rows.values().stream().map(List::size).reduce(0, Integer::sum);
    }

    private Map<Long, List<InternalRow>> writeRows(
            Table table, Schema schema, Map<String, Long> partitionIdByNames) {
        AppendWriter appendWriter = table.getAppendWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row = row(schema.toRowType(), new Object[] {i, "a" + i, partition});
                appendWriter.append(row);
                expectPartitionAppendRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }
        appendWriter.flush();
        return expectPartitionAppendRows;
    }

    private Map<Long, List<InternalRow>> pollRecords(
            LogScanner logScanner, int expectRecordsCount) {
        int scanRecordCount = 0;
        Map<Long, List<InternalRow>> actualRows = new HashMap<>();
        while (scanRecordCount < expectRecordsCount) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket scanBucket : scanRecords.buckets()) {
                List<ScanRecord> records = scanRecords.records(scanBucket);
                for (ScanRecord scanRecord : records) {
                    actualRows
                            .computeIfAbsent(scanBucket.getPartitionId(), k -> new ArrayList<>())
                            .add(scanRecord.getRow());
                }
            }
            scanRecordCount += scanRecords.count();
        }
        return actualRows;
    }

    @Test
    void testOperateNotExistPartitionShouldThrowException() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);

        // test get for a not exist partition
        assertThatThrownBy(
                        () ->
                                table.lookup(
                                                keyRow(
                                                        schema,
                                                        new Object[] {
                                                            1, null, "notExistPartition"
                                                        }))
                                        .get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "notExistPartition"));

        // test write to not exist partition
        UpsertWriter upsertWriter = table.getUpsertWriter();
        InternalRow row = row(schema.toRowType(), new Object[] {1, "a", "notExistPartition"});
        assertThatThrownBy(() -> upsertWriter.upsert(row).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "notExistPartition"));

        // test scan a not exist partition's log
        LogScan logScan = new LogScan();
        LogScanner logScanner = table.getLogScanner(logScan);
        assertThatThrownBy(() -> logScanner.subscribe(100L, 0, 0))
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("Partition not exist for partition ids: [100]");

        // todo: test the case that client produce to a partition to a server, but
        // the server delete the partition at the time, the client should receive the
        // exception and won't retry again and again
    }

    private Schema createPartitionedTable(TablePath tablePath, boolean isPrimaryTable)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is third column");

        if (isPrimaryTable) {
            schemaBuilder.primaryKey("a", "c");
        }

        Schema schema = schemaBuilder.build();

        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        createTable(tablePath, partitionTableDescriptor, false);
        return schema;
    }
}
