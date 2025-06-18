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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.testutils.DataTestUtils.assertRowValueEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for auto partitioned table. */
class AutoPartitionedTableITCase extends ClientToServerITCaseBase {

    @BeforeEach
    void beforeEach() throws Exception {
        super.setup();
        // Auto partitioned table related tests will close dynamic partition creation default.
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);
    }

    @Test
    void testPartitionedPrimaryKeyTable() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(DATA1_TABLE_PATH_PK);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        int recordsPerPartition = 5;
        // now, put some data to the partitions
        Map<Long, List<InternalRow>> expectPutRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row = row(i, "a" + i, partition);
                upsertWriter.upsert(row);
                expectPutRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }
        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        // now, let's lookup the written data by look up
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow actualRow = row(i, "a" + i, partition);
                InternalRow lookupRow = lookuper.lookup(row(i, partition)).get().getSingletonRow();
                assertThatRow(lookupRow).withSchema(schema.getRowType()).isEqualTo(actualRow);
            }
        }

        // then, let's scan and check the cdc log
        verifyPartitionLogs(table, schema.getRowType(), expectPutRows);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPartitionedTablePrefixLookup(boolean isDataLakeEnabled) throws Exception {
        // This case partition key 'b' in both pk and bucket key (prefix key).
        TablePath tablePath = TablePath.of("test_db_1", "test_partitioned_table_prefix_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "a", "c")
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        // test data lake bucket assigner for prefix lookup
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, isDataLakeEnabled)
                        .build();
        RowType rowType = schema.getRowType();
        createTable(tablePath, descriptor, false);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        Table table = conn.getTable(tablePath);
        for (String partition : partitionIdByNames.keySet()) {
            verifyPutAndLookup(table, new Object[] {1, partition, 1L, "value1"});
            verifyPutAndLookup(table, new Object[] {1, partition, 2L, "value2"});
        }

        for (int i = 0; i < 3; i++) {
            // test prefix lookups with partition field (b) in different
            // position of the lookup columns
            List<String> lookupColumns =
                    i == 0
                            ? Arrays.asList("b", "a", "c")
                            : i == 1 ? Arrays.asList("a", "b", "c") : Arrays.asList("a", "c", "b");
            Lookuper prefixLookuper = table.newLookup().lookupBy(lookupColumns).createLookuper();
            for (String partition : partitionIdByNames.keySet()) {
                Object[] lookupRow =
                        i == 0
                                ? new Object[] {partition, 1, 1L}
                                : i == 1
                                        ? new Object[] {1, partition, 1L}
                                        : new Object[] {1, 1L, partition};
                CompletableFuture<LookupResult> result = prefixLookuper.lookup(row(lookupRow));
                LookupResult prefixLookupResult = result.get();
                assertThat(prefixLookupResult).isNotNull();
                List<InternalRow> rowList = prefixLookupResult.getRowList();
                assertThat(rowList.size()).isEqualTo(1);
                assertRowValueEquals(
                        rowType, rowList.get(0), new Object[] {1, partition, 1L, "value1"});
            }
        }
    }

    @Test
    void testInvalidPrefixLookupForPartitionedTable() throws Exception {
        // This case partition key 'c' only in pk but not in prefix key.
        TablePath tablePath = TablePath.of("test_db_1", "test_partitioned_table_prefix_lookup2");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "a", "b")
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        createTable(tablePath, descriptor, false);

        Table table = conn.getTable(tablePath);

        // test prefix lookup with (a, b).
        assertThatThrownBy(() -> table.newLookup().lookupBy("a", "b").createLookuper())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Can not perform prefix lookup on table 'test_db_1.test_partitioned_table_prefix_lookup2', "
                                + "because the lookup columns [a, b] must contain all partition fields [c].");
    }

    @Test
    void testPartitionedLogTable() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH, false);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(DATA1_TABLE_PATH);
        Table table = conn.getTable(DATA1_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                GenericRow row = row(i, "a" + i, partition);
                appendWriter.append(row);
                expectPartitionAppendRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }
        appendWriter.flush();

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
    }

    @Test
    void testUnsubscribePartitionBucket() throws Exception {
        // write rows
        TablePath tablePath = TablePath.of("test_db_1", "unsubscribe_partition_bucket_table");
        Schema schema = createPartitionedTable(tablePath, false);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        Table table = conn.getTable(tablePath);

        Map<Long, List<InternalRow>> expectPartitionAppendRows =
                writeRows(table, partitionIdByNames);

        // then, let's verify the logs
        try (LogScanner logScanner = table.newScan().createLogScanner()) {
            for (Long partitionId : expectPartitionAppendRows.keySet()) {
                logScanner.subscribeFromBeginning(partitionId, 0);
            }
            int totalRecords = getRowsCount(expectPartitionAppendRows);
            Map<Long, List<InternalRow>> actualRows = pollRecords(logScanner, totalRecords);
            verifyRows(schema.getRowType(), actualRows, expectPartitionAppendRows);

            // now, unsubscribe some partitions
            long removedPartitionId = expectPartitionAppendRows.keySet().iterator().next();
            logScanner.unsubscribe(removedPartitionId, 0);

            // now, write some records again
            expectPartitionAppendRows = writeRows(table, partitionIdByNames);
            // remove the removed partition
            expectPartitionAppendRows.remove(removedPartitionId);
            totalRecords = getRowsCount(expectPartitionAppendRows);
            actualRows = pollRecords(logScanner, totalRecords);
            verifyRows(schema.getRowType(), actualRows, expectPartitionAppendRows);
        }
    }

    private int getRowsCount(Map<Long, List<InternalRow>> rows) {
        return rows.values().stream().map(List::size).reduce(0, Integer::sum);
    }

    private Map<Long, List<InternalRow>> writeRows(
            Table table, Map<String, Long> partitionIdByNames) {
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                GenericRow row = row(i, "a" + i, partition);
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
        createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        String partitionName = "notExistPartition";
        Lookuper lookuper = table.newLookup().createLookuper();

        // test get for a not exist partition
        assertThatThrownBy(() -> lookuper.lookup(row(1, partitionName)).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK, partitionName));

        // test write to not exist partition
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        GenericRow row = row(1, "a", partitionName);
        assertThatThrownBy(() -> upsertWriter.upsert(row).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK, partitionName));

        // test scan a not exist partition's log
        LogScanner logScanner = table.newScan().createLogScanner();
        assertThatThrownBy(() -> logScanner.subscribe(100L, 0, 0))
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("Partition not exist for partition ids: [100]");
        logScanner.close();

        // todo: test the case that client produce to a partition to a server, but
        // the server delete the partition at the time, the client should receive the
        // exception and won't retry again and again
    }

    @Test
    void testAddPartitionForAutoPartitionedTable() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_auto_partition_table_add_partition_1");
        Schema schema = createPartitionedTable(tablePath, false);
        int currentYear = LocalDateTime.now().getYear();

        // create one partition.
        admin.createPartition(
                        tablePath, newPartitionSpec("c", String.valueOf(currentYear + 10)), false)
                .get();
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 3);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        List<String> expectedPartitions = new ArrayList<>(partitionIdByNames.keySet());
        assertThat(
                        partitionInfos.stream()
                                .map(PartitionInfo::getPartitionName)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expectedPartitions);

        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row = row(i, "a" + i, partition);
                appendWriter.append(row);
                expectPartitionAppendRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }
        appendWriter.flush();

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
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
