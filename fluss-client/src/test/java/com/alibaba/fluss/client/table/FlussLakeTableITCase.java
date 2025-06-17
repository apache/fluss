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

import com.alibaba.fluss.bucketing.BucketingFunction;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.TestData;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static com.alibaba.fluss.testutils.InternalRowListAssert.assertThatRows;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The IT case for lake table. Lake table is for the table that {@link
 * com.alibaba.fluss.config.ConfigOptions#TABLE_DATALAKE_FORMAT} is set.
 */
class FlussLakeTableITCase {

    private static final int DEFAULT_BUCKET_COUNT = 3;
    private static final int PARTITION_PRE_CREATE = 2;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    protected Connection conn;
    protected Admin admin;
    protected Configuration clientConf;

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.set(ConfigOptions.DEFAULT_BUCKET_NUMBER, 3);
        return configuration;
    }

    @BeforeEach
    protected void setup() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private static Stream<Arguments> testPrimaryKeyTableArgs() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false));
    }

    @Test
    void testDeleteOnPrimaryKeyTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_pk_table_1");
        createTable(tablePath, TestData.DATA1_TABLE_DESCRIPTOR_PK, false);
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter tableWriter = table.newUpsert().createWriter();
            tableWriter.upsert(row(1, "b1"));
            tableWriter.upsert(row(2, "b2"));
            tableWriter.upsert(row(3, "b3"));
            tableWriter.delete(row(2, null));
            tableWriter.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            List<InternalRow> row1 = lookuper.lookup(row(1)).get().getRowList();
            assertThatRows(row1)
                    .withSchema(TestData.DATA1_SCHEMA_PK.getRowType())
                    .isEqualTo(Collections.singletonList(row(1, "b1")));

            List<InternalRow> row2 = lookuper.lookup(row(2)).get().getRowList();
            assertThat(row2).isEmpty();

            List<InternalRow> row3 = lookuper.lookup(row(3)).get().getRowList();
            assertThatRows(row3)
                    .withSchema(TestData.DATA1_SCHEMA_PK.getRowType())
                    .isEqualTo(Collections.singletonList(row(3, "b3")));
        }
    }

    @ParameterizedTest
    @MethodSource("testPrimaryKeyTableArgs")
    void testPrimaryKeyTable(boolean isPartitioned, boolean isDefaultBucketKey) throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_primary_key_lake_table");
        Schema pkTableSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "c", "d")
                        .build();

        TableDescriptor.Builder pkTableBuilder = TableDescriptor.builder().schema(pkTableSchema);
        if (isPartitioned) {
            pkTableBuilder.partitionedBy("d");
            pkTableBuilder
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY)
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 2);
        }

        if (!isDefaultBucketKey) {
            pkTableBuilder.distributedBy(DEFAULT_BUCKET_COUNT, "a");
        }

        TableDescriptor pkTable = pkTableBuilder.build();
        createTable(tablePath, pkTable, false);

        // verify write will use the lake's bucket assigner
        Map<TableBucket, List<InternalRow>> writtenRows =
                writeRowsAndVerifyBucket(tablePath, pkTable);

        // verify lookup will use the lake's bucket assigner
        Set<InternalRow> allRows =
                writtenRows.values().stream().flatMap(List::stream).collect(Collectors.toSet());

        // init lookup columns
        List<String> lookUpColumns;
        if (isDefaultBucketKey) {
            lookUpColumns = Arrays.asList("a", "c", "d");
        } else {
            lookUpColumns =
                    isPartitioned ? Arrays.asList("a", "d") : Collections.singletonList("a");
        }
        List<InternalRow.FieldGetter> lookUpFieldGetter = new ArrayList<>(lookUpColumns.size());
        for (int columnIndex : pkTableSchema.getColumnIndexes(lookUpColumns)) {
            lookUpFieldGetter.add(
                    InternalRow.createFieldGetter(
                            pkTableSchema.getRowType().getTypeAt(columnIndex), columnIndex));
        }
        // lookup
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().lookupBy(lookUpColumns).createLookuper();
            for (InternalRow row : allRows) {
                GenericRow lookupKeyRow = new GenericRow(lookUpFieldGetter.size());
                for (int i = 0; i < lookUpFieldGetter.size(); i++) {
                    lookupKeyRow.setField(i, lookUpFieldGetter.get(i).getFieldOrNull(row));
                }
                // the written data are also unique on bucket key
                InternalRow actualRow = lookuper.lookup(lookupKeyRow).get().getSingletonRow();
                assertThatRow(actualRow).withSchema(pkTableSchema.getRowType()).isEqualTo(row);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogTable(boolean isPartitioned) throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_log_lake_table");
        Schema logTableSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.STRING())
                        .build();
        TableDescriptor.Builder logTableBuilder =
                TableDescriptor.builder()
                        .schema(logTableSchema)
                        .distributedBy(DEFAULT_BUCKET_COUNT, "b");
        if (isPartitioned) {
            logTableBuilder.partitionedBy("d");
            logTableBuilder
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, PARTITION_PRE_CREATE);
        }
        TableDescriptor logTable = logTableBuilder.build();
        createTable(tablePath, logTable, false);
        // verify write will use the lake's bucket assigner
        writeRowsAndVerifyBucket(tablePath, logTable);
    }

    private void createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, ignoreIfExists).get();
    }

    private Map<TableBucket, List<InternalRow>> writeRowsAndVerifyBucket(
            TablePath tablePath, TableDescriptor tableDescriptor) throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        long tableId = tableInfo.getTableId();
        DataLakeFormat dataLakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        int rowNums = 30;
        boolean isPartitioned = tableDescriptor.isPartitioned();
        RowType rowType = tableDescriptor.getSchema().getRowType();
        KeyEncoder keyEncoder =
                KeyEncoder.of(rowType, tableDescriptor.getBucketKeys(), dataLakeFormat);
        HashBucketAssigner bucketAssigner =
                new HashBucketAssigner(DEFAULT_BUCKET_COUNT, BucketingFunction.of(dataLakeFormat));
        Map<String, Long> partitionIdByNames = null;
        if (isPartitioned) {
            partitionIdByNames =
                    FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(
                            tablePath, PARTITION_PRE_CREATE);
        }
        int totalRows = partitionIdByNames != null ? rowNums * partitionIdByNames.size() : rowNums;

        // write rows
        Map<TableBucket, List<InternalRow>> expectedRows = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            TableWriter tableWriter =
                    tableDescriptor.hasPrimaryKey()
                            ? table.newUpsert().createWriter()
                            : table.newAppend().createWriter();
            if (partitionIdByNames != null) {
                for (String partition : partitionIdByNames.keySet()) {
                    for (int i = 0; i < rowNums; i++) {
                        InternalRow row = row(i, "b" + i, "c" + i, partition);
                        writeRow(tableWriter, row);
                        TableBucket assignedBucket =
                                new TableBucket(
                                        tableId,
                                        partitionIdByNames.get(partition),
                                        bucketAssigner.assignBucket(keyEncoder.encodeKey(row)));
                        expectedRows
                                .computeIfAbsent(assignedBucket, (k) -> new ArrayList<>())
                                .add(row);
                    }
                }
            } else {
                for (int i = 0; i < rowNums; i++) {
                    InternalRow row = row(i, "b" + i, "c" + i, "d" + i);
                    writeRow(tableWriter, row);
                    TableBucket assignedBucket =
                            new TableBucket(
                                    tableId,
                                    bucketAssigner.assignBucket(keyEncoder.encodeKey(row)));
                    expectedRows.computeIfAbsent(assignedBucket, (k) -> new ArrayList<>()).add(row);
                }
            }
            tableWriter.flush();
        }

        // scan rows from table and verify the buckets that the records fall into
        int scanCount = 0;
        Map<TableBucket, List<InternalRow>> actualRows = new HashMap<>();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            for (int bucket = 0; bucket < DEFAULT_BUCKET_COUNT; bucket++) {
                if (partitionIdByNames != null) {
                    for (long partitionId : partitionIdByNames.values()) {
                        logScanner.subscribeFromBeginning(partitionId, bucket);
                    }
                } else {
                    logScanner.subscribeFromBeginning(bucket);
                }
            }
            while (scanCount < totalRows) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket tableBucket : scanRecords.buckets()) {
                    actualRows
                            .computeIfAbsent(tableBucket, (k) -> new ArrayList<>())
                            .addAll(
                                    scanRecords.records(tableBucket).stream()
                                            .map(ScanRecord::getRow)
                                            .collect(Collectors.toList()));
                }
                scanCount += scanRecords.count();
            }
        }
        // verify the rows fall back the buckets calculated by lake bucket assigner
        assertThat(actualRows).hasSameSizeAs(expectedRows);
        for (Map.Entry<TableBucket, List<InternalRow>> actualRowEntry : actualRows.entrySet()) {
            List<InternalRow> actualRowList = actualRowEntry.getValue();
            List<InternalRow> expectedRowList = expectedRows.get(actualRowEntry.getKey());
            assertThat(actualRowList).hasSameSizeAs(expectedRowList);
            assertThatRows(actualRowList).withSchema(rowType).isEqualTo(expectedRowList);
        }
        return actualRows;
    }

    private void writeRow(TableWriter tableWriter, InternalRow row) {
        if (tableWriter instanceof AppendWriter) {
            ((AppendWriter) tableWriter).append(row);
        } else {
            ((UpsertWriter) tableWriter).upsert(row);
        }
    }
}
