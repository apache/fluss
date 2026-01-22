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
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.TestFileSystem;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.table.scanner.batch.BatchScanUtils.collectRows;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA3_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.assertRowValueEquals;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.keyRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link FlussTable}. */
class FlussTableITCase extends ClientToServerITCaseBase {

    @Test
    void testGetDescriptor() throws Exception {
        createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, false);
        // get table descriptor.
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        TableInfo tableInfo = table.getTableInfo();

        // the created table info will be applied with additional replica factor property
        TableDescriptor expected =
                DATA1_TABLE_DESCRIPTOR_PK
                        .withReplicationFactor(3)
                        .withDataLakeFormat(DataLakeFormat.PAIMON);
        assertThat(tableInfo.toTableDescriptor()).isEqualTo(expected);
    }

    @Test
    void testAppendOnly() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row(1, "a")).get();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Disabled("TODO, fix me in #116")
    void testAppendWithSmallBuffer(boolean indexedFormat) throws Exception {
        TableDescriptor desc =
                indexedFormat
                        ? TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(3)
                                .logFormat(LogFormat.INDEXED)
                                .build()
                        : DATA1_TABLE_DESCRIPTOR;
        createTable(DATA1_TABLE_PATH, desc, false);
        Configuration config = new Configuration(clientConf);
        // only 1kb memory size, and 64 bytes page size.
        config.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(2048));
        config.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(64));
        config.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(256));
        int expectedSize = 20;
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(DATA1_TABLE_PATH);
            AppendWriter appendWriter = table.newAppend().createWriter();
            BinaryString value = BinaryString.fromString(StringUtils.repeat("a", 100));
            // should exceed the buffer size, but append successfully
            for (int i = 0; i < expectedSize; i++) {
                appendWriter.append(row(1, value));
            }
            appendWriter.flush();

            // assert the written data
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    assertThat(row.getInt(0)).isEqualTo(1);
                    assertThat(row.getString(1)).isEqualTo(value);
                    count++;
                }
            }
            logScanner.close();
        }
    }

    @Test
    void testPollOnce() throws Exception {
        TableDescriptor desc = DATA1_TABLE_DESCRIPTOR;
        createTable(DATA1_TABLE_PATH, desc, false);
        Configuration config = new Configuration(clientConf);
        int expectedSize = 20;
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(DATA1_TABLE_PATH);
            AppendWriter appendWriter = table.newAppend().createWriter();
            BinaryString value = BinaryString.fromString(StringUtils.repeat("a", 100));
            // should exceed the buffer size, but append successfully
            for (int i = 0; i < expectedSize; i++) {
                appendWriter.append(row(1, value));
            }
            appendWriter.flush();

            // assert the written data
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                assertThat(scanRecords.isEmpty()).isFalse();
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    assertThat(row.getInt(0)).isEqualTo(1);
                    assertThat(row.getString(1)).isEqualTo(value);
                    count++;
                }
            }
            logScanner.close();
        }
    }

    @Test
    void testUpsertWithSmallBuffer() throws Exception {
        TableDescriptor desc =
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build();
        createTable(DATA1_TABLE_PATH, desc, false);
        Configuration config = new Configuration(clientConf);
        // only 1kb memory size, and 64 bytes page size.
        config.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(2048));
        config.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(64));
        config.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(256));
        int expectedSize = 20;
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(DATA1_TABLE_PATH);
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            BinaryString value = BinaryString.fromString(StringUtils.repeat("a", 100));
            // should exceed the buffer size, but append successfully
            for (int i = 0; i < expectedSize; i++) {
                upsertWriter.upsert(row(i, value));
            }
            upsertWriter.flush();

            // assert the written data
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                    InternalRow row = scanRecord.getRow();
                    assertThat(row.getInt(0)).isEqualTo(count);
                    assertThat(row.getString(1)).isEqualTo(value);
                    count++;
                }
            }
            logScanner.close();
        }
    }

    @Test
    void testPutAndLookup() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_put_and_lookup_table");
        createTable(tablePath, DATA1_TABLE_DESCRIPTOR_PK, false);

        Table table = conn.getTable(tablePath);
        verifyPutAndLookup(table, new Object[] {1, "a"});
        TableInfo tableInfo = table.getTableInfo();

        // test put/lookup data for primary table with pk index is not 0
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.STRING())
                        .withComment("a is first column")
                        .column("b", DataTypes.INT())
                        .withComment("b is second column")
                        .primaryKey("b")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "b").build();
        // create the table
        TablePath data1PkTablePath2 =
                TablePath.of(DATA1_TABLE_PATH_PK.getDatabaseName(), "test_pk_table_2");
        createTable(data1PkTablePath2, tableDescriptor, true);

        // now, check put/lookup data
        Table table2 = conn.getTable(data1PkTablePath2);
        verifyPutAndLookup(table2, new Object[] {"a", 1});

        // Test schema change: add new column which equals to DATA2_ROW_TYPE
        admin.alterTable(
                        tableInfo.getTablePath(),
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "c",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        Table newSchemaTable = conn.getTable(tableInfo.getTablePath());
        // schema change case1: read new data with new schema.
        verifyPutAndLookup(newSchemaTable, new Object[] {2, "b", "bb"});
        // schema change case2: read new data with old schema.
        assertThatRow(lookupRow(table.newLookup().createLookuper(), row(2)))
                .withSchema(tableInfo.getSchema().getRowType())
                .isEqualTo(row(2, "b"));
        // schema change case3: read old data with new schema.
        assertThatRow(lookupRow(newSchemaTable.newLookup().createLookuper(), row(1)))
                .withSchema(newSchemaTable.getTableInfo().getSchema().getRowType())
                .isEqualTo(row(1, "a", null));
    }

    @Test
    void testPutAndPrefixLookup() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_put_and_prefix_lookup_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a", "b").build();
        createTable(tablePath, descriptor, false);
        Table table = conn.getTable(tablePath);
        TableInfo tableInfo = table.getTableInfo();
        verifyPutAndLookup(table, new Object[] {1, "a", 1L, "value1"});
        verifyPutAndLookup(table, new Object[] {1, "a", 2L, "value2"});
        verifyPutAndLookup(table, new Object[] {1, "a", 3L, "value3"});
        verifyPutAndLookup(table, new Object[] {2, "a", 4L, "value4"});
        RowType rowType = schema.getRowType();

        // test prefix lookup.
        Schema prefixKeySchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        RowType prefixKeyRowType = prefixKeySchema.getRowType();
        Lookuper prefixLookuper =
                table.newLookup().lookupBy(prefixKeyRowType.getFieldNames()).createLookuper();
        CompletableFuture<LookupResult> result = prefixLookuper.lookup(row(1, "a"));
        LookupResult prefixLookupResult = result.get();
        assertThat(prefixLookupResult).isNotNull();
        List<InternalRow> rowList = prefixLookupResult.getRowList();
        assertThat(rowList.size()).isEqualTo(3);
        for (int i = 0; i < rowList.size(); i++) {
            assertRowValueEquals(
                    rowType, rowList.get(i), new Object[] {1, "a", i + 1L, "value" + (i + 1)});
        }

        result = prefixLookuper.lookup(row(2, "a"));
        prefixLookupResult = result.get();
        assertThat(prefixLookupResult).isNotNull();
        rowList = prefixLookupResult.getRowList();
        assertThat(rowList.size()).isEqualTo(1);
        assertRowValueEquals(rowType, rowList.get(0), new Object[] {2, "a", 4L, "value4"});

        result = prefixLookuper.lookup(compactedRow(prefixKeyRowType, new Object[] {3, "a"}));
        prefixLookupResult = result.get();
        assertThat(prefixLookupResult).isNotNull();
        rowList = prefixLookupResult.getRowList();
        assertThat(rowList.size()).isEqualTo(0);

        // Test schema change: add new column.
        Schema newSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .column("e", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        admin.alterTable(
                        tableInfo.getTablePath(),
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "e",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table newSchemaTable = connection.getTable(tableInfo.getTablePath())) {
            // schema change case1: read new data with new schema.
            verifyPutAndLookup(
                    newSchemaTable, new Object[] {1, "a", 4L, "value4", "add_column_value"});
            // schema change case2: read new data with old schema.
            result = prefixLookuper.lookup(row(1, "a"));
            prefixLookupResult = result.get();
            assertThat(prefixLookupResult).isNotNull();
            rowList = prefixLookupResult.getRowList();
            assertThat(rowList.size()).isEqualTo(4);
            for (int i = 0; i < rowList.size(); i++) {
                assertRowValueEquals(
                        rowType, rowList.get(i), new Object[] {1, "a", i + 1L, "value" + (i + 1)});
            }
            admin.getTableSchema(tablePath, 1).get();
            // schema change case3: read old data with new schema.
            Lookuper newPrefixLookuper =
                    newSchemaTable
                            .newLookup()
                            .lookupBy(prefixKeyRowType.getFieldNames())
                            .createLookuper();
            result = newPrefixLookuper.lookup(row(1, "a"));
            prefixLookupResult = result.get();
            assertThat(prefixLookupResult).isNotNull();
            rowList = prefixLookupResult.getRowList();
            assertThat(rowList.size()).isEqualTo(4);
            for (int i = 0; i < rowList.size(); i++) {
                assertRowValueEquals(
                        rowType,
                        rowList.get(i),
                        new Object[] {
                            1, "a", i + 1L, "value" + (i + 1), i == 3 ? "add_column_value" : null
                        });
            }
        }
    }

    @Test
    void testInvalidPrefixLookup() throws Exception {
        // First, test the bucket keys not a prefix subset of primary keys.
        TablePath tablePath = TablePath.of("test_db_1", "test_invalid_prefix_lookup_1");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a", "c").build();
        createTable(tablePath, descriptor, false);
        Table table = conn.getTable(tablePath);

        assertThatThrownBy(() -> table.newLookup().lookupBy("a", "c").createLookuper())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Can not perform prefix lookup on table 'test_db_1.test_invalid_prefix_lookup_1', "
                                + "because the bucket keys [a, c] is not a prefix subset of the "
                                + "physical primary keys [a, b, c] (excluded partition fields if present).");

        // Second, test the lookup column names in PrefixLookup not a subset of primary keys.
        tablePath = TablePath.of("test_db_1", "test_invalid_prefix_lookup_2");
        schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();

        descriptor = TableDescriptor.builder().schema(schema).distributedBy(3, "a", "b").build();
        createTable(tablePath, descriptor, true);
        Table table2 = conn.getTable(tablePath);

        // not match bucket key
        assertThatThrownBy(() -> table2.newLookup().lookupBy("a", "d").createLookuper())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Can not perform prefix lookup on table 'test_db_1.test_invalid_prefix_lookup_2', "
                                + "because the lookup columns [a, d] must contain all bucket keys [a, b] in order.");

        // wrong bucket key order
        assertThatThrownBy(() -> table2.newLookup().lookupBy("b", "a").createLookuper())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Can not perform prefix lookup on table 'test_db_1.test_invalid_prefix_lookup_2', "
                                + "because the lookup columns [b, a] must contain all bucket keys [a, b] in order.");
    }

    @Test
    void testLookupForNotReadyTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_lookup_unready_table_t1");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(10).build();
        long tableId = createTable(tablePath, descriptor, true);
        IndexedRow rowKey = keyRow(DATA1_SCHEMA_PK, new Object[] {1, "a"});
        // retry until all replica ready. Otherwise, the lookup maybe fail. To avoid test unstable,
        // if you want to test the lookup for not ready table, you can comment the following line.
        waitAllReplicasReady(tableId, 10);
        Table table = conn.getTable(tablePath);
        Lookuper lookuper = table.newLookup().createLookuper();
        assertThat(lookupRow(lookuper, rowKey)).isNull();
    }

    @Test
    void testLimitScanPrimaryTable() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1).build();
        long tableId = createTable(DATA1_TABLE_PATH_PK, descriptor, true);
        int insertSize = 10;
        int limitSize = 5;
        try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
            Table table = conn.getTable(DATA1_TABLE_PATH_PK);
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            List<Object[]> expectedRows = new ArrayList<>();
            for (int i = 0; i < insertSize; i++) {
                BinaryString value = BinaryString.fromString(StringUtils.repeat("a", i));
                upsertWriter.upsert(row(i, value));
                if (i < limitSize) {
                    expectedRows.add(new Object[] {i, StringUtils.repeat("a", i)});
                }
            }
            upsertWriter.flush();

            TableBucket tb = new TableBucket(tableId, 0);
            List<InternalRow> actualRows =
                    collectRows(table.newScan().limit(limitSize).createBatchScanner(tb));
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.getRowType(), actualRows.get(i), expectedRows.get(i));
            }

            // test projection scan
            int[] projectedFields = new int[] {1};
            for (int i = 0; i < limitSize; i++) {
                expectedRows.set(i, new Object[] {expectedRows.get(i)[1]});
            }
            actualRows =
                    collectRows(
                            table.newScan()
                                    .limit(limitSize)
                                    .project(projectedFields)
                                    .createBatchScanner(tb));
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.getRowType().project(projectedFields),
                        actualRows.get(i),
                        expectedRows.get(i));
            }
        }
    }

    @Test
    void testLimitScanLogTable() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        long tableId = createTable(DATA1_TABLE_PATH, descriptor, true);

        int insertSize = 10;
        int limitSize = 5;
        try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
            Table table = conn.getTable(DATA1_TABLE_PATH);
            AppendWriter appendWriter = table.newAppend().createWriter();

            List<Object[]> expectedRows = new ArrayList<>();
            for (int i = 0; i < insertSize; i++) {
                BinaryString value = BinaryString.fromString(StringUtils.repeat("a", i));
                appendWriter.append(row(i, value));
                // limit log scan read the latest limit number of record.
                if (i >= insertSize - limitSize) {
                    expectedRows.add(new Object[] {i, StringUtils.repeat("a", i)});
                }
            }
            appendWriter.flush();

            TableBucket tb = new TableBucket(tableId, 0);
            List<InternalRow> actualRows =
                    collectRows(table.newScan().limit(limitSize).createBatchScanner(tb));
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.getRowType(), actualRows.get(i), expectedRows.get(i));
            }

            // test projection scan
            int[] projectedFields = new int[] {1};
            for (int i = 0; i < limitSize; i++) {
                expectedRows.set(i, new Object[] {expectedRows.get(i)[1]});
            }
            actualRows =
                    collectRows(
                            table.newScan()
                                    .limit(limitSize)
                                    .project(projectedFields)
                                    .createBatchScanner(tb));
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.getRowType().project(projectedFields),
                        actualRows.get(i),
                        expectedRows.get(i));
            }
        }
    }

    @Test
    void testPartialPutAndDelete() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .column("d", DataTypes.BOOLEAN())
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();
        createTable(DATA1_TABLE_PATH_PK, tableDescriptor, true);

        // test put a full row
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        verifyPutAndLookup(table, new Object[] {1, "a", 1, true});

        // partial update columns: a, b
        UpsertWriter upsertWriter =
                table.newUpsert().partialUpdate(new int[] {0, 1}).createWriter();
        upsertWriter.upsert(row(1, "aaa", null, null)).get();
        Lookuper lookuper = table.newLookup().createLookuper();

        // check the row
        GenericRow rowKey = row(1);
        assertThat(lookupRow(lookuper, rowKey))
                .isEqualTo(compactedRow(schema.getRowType(), new Object[] {1, "aaa", 1, true}));

        // partial update columns columns: a,b,c
        upsertWriter = table.newUpsert().partialUpdate("a", "b", "c").createWriter();
        upsertWriter.upsert(row(1, "bbb", 222, null)).get();

        // lookup the row
        assertThat(lookupRow(lookuper, rowKey))
                .isEqualTo(compactedRow(schema.getRowType(), new Object[] {1, "bbb", 222, true}));

        // test partial delete, target column is a,b,c
        upsertWriter.delete(row(1, "bbb", 222, null)).get();
        assertThat(lookupRow(lookuper, rowKey))
                .isEqualTo(compactedRow(schema.getRowType(), new Object[] {1, null, null, true}));

        // partial delete, target column is d
        upsertWriter = table.newUpsert().partialUpdate("a", "d").createWriter();
        upsertWriter.delete(row(1, null, null, true)).get();

        // the row should be deleted, shouldn't get the row again
        assertThat(lookupRow(lookuper, rowKey)).isNull();

        table.close();
    }

    @Test
    void testInvalidPartialUpdate() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", new StringType(true))
                        .column("c", new BigIntType(false))
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();
        createTable(DATA1_TABLE_PATH_PK, tableDescriptor, true);

        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            // the target columns doesn't contain the primary column, should
            // throw exception
            assertThatThrownBy(() -> table.newUpsert().partialUpdate("b").createWriter())
                    .hasMessage(
                            "The target write columns [b] must contain the primary key columns [a].");

            // the column not in the primary key is nullable, should throw exception
            assertThatThrownBy(() -> table.newUpsert().partialUpdate("a", "b").createWriter())
                    .hasMessage(
                            "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
            assertThatThrownBy(() -> table.newUpsert().partialUpdate("a", "c").createWriter())
                    .hasMessage(
                            "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
            assertThatThrownBy(() -> table.newUpsert().partialUpdate("a", "d").createWriter())
                    .hasMessage(
                            "Can not find target column: d for table test_db_1.test_pk_table_1.");
            assertThatThrownBy(
                            () -> table.newUpsert().partialUpdate(new int[] {0, 3}).createWriter())
                    .hasMessage(
                            "Invalid target column index: 3 for table test_db_1.test_pk_table_1. The table only has 3 columns.");
        }

        // test invalid auto increment column upsert
        schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .primaryKey("a")
                        .enableAutoIncrement("c")
                        .build();
        tableDescriptor = TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();
        TablePath tablePath =
                TablePath.of("test_db_1", "test_invalid_auto_increment_column_upsert");
        createTable(tablePath, tableDescriptor, true);
        try (Table table = conn.getTable(tablePath)) {
            assertThatThrownBy(() -> table.newUpsert().createWriter())
                    .hasMessage(
                            "This table has auto increment column [c]. Explicitly specifying values for an auto increment column is not allowed. Please specify non-auto-increment columns as target columns using partialUpdate first.");

            assertThatThrownBy(() -> table.newUpsert().partialUpdate("a", "c").createWriter())
                    .hasMessage(
                            "Explicitly specifying values for the auto increment column c is not allowed.");
        }
    }

    @Test
    void testDelete() throws Exception {
        createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, false);

        // put key.
        InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row).get();
            Lookuper lookuper = table.newLookup().createLookuper();

            // lookup this key.
            IndexedRow keyRow = keyRow(DATA1_SCHEMA_PK, new Object[] {1, "a"});
            assertThat(lookupRow(lookuper, keyRow)).isEqualTo(row);

            // delete this key.
            upsertWriter.delete(row).get();
            // lookup this key again, will return null.
            assertThat(lookupRow(lookuper, keyRow)).isNull();
        }
    }

    @Test
    void testAppendWhileTableMaybeNotReady() throws Exception {
        // Create table request will complete if the table info was registered in zk, but the table
        // maybe not ready immediately. So, the metadata request possibly get incomplete table info,
        // like the unknown leader. In this case, the append request need retry until the table is
        // ready.
        int bucketNumber = 10;
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(bucketNumber).build();
        createTable(DATA1_TABLE_PATH, tableDescriptor, false);

        // append data.
        GenericRow row = row(1, "a");
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row).get();

            // fetch data.
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            InternalRow result = null;
            while (result == null) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    result = scanRecord.getRow();
                }
            }
            assertThatRow(result).withSchema(DATA1_ROW_TYPE).isEqualTo(row);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "ARROW", "COMPACTED"})
    void testAppendAndPoll(String format) throws Exception {
        verifyAppendOrPut(true, format, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "COMPACTED"})
    void testPutAndPoll(String kvFormat) throws Exception {
        verifyAppendOrPut(false, "ARROW", kvFormat);
    }

    @Test
    void testPutAndPollCompacted() throws Exception {
        verifyAppendOrPut(false, "COMPACTED", "COMPACTED");
    }

    void verifyAppendOrPut(boolean append, String logFormat, @Nullable String kvFormat)
            throws Exception {
        Schema schema =
                append
                        ? Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.INT())
                                .column("c", DataTypes.STRING())
                                .column("d", DataTypes.BIGINT())
                                .build()
                        : Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.INT())
                                .column("c", DataTypes.STRING())
                                .column("d", DataTypes.BIGINT())
                                .primaryKey("a")
                                .build();
        TableDescriptor.Builder builder =
                TableDescriptor.builder().schema(schema).logFormat(LogFormat.fromString(logFormat));
        if (kvFormat != null) {
            builder.kvFormat(KvFormat.fromString(kvFormat));
        }
        TableDescriptor tableDescriptor = builder.build();
        createTable(DATA1_TABLE_PATH, tableDescriptor, false);

        int expectedSize = 30;
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            TableWriter tableWriter;
            if (append) {
                tableWriter = table.newAppend().createWriter();
            } else {
                tableWriter = table.newUpsert().createWriter();
            }
            for (int i = 0; i < expectedSize; i++) {
                String value = i % 2 == 0 ? "hello, friend" + i : null;
                GenericRow row = row(i, 100, value, i * 10L);
                if (tableWriter instanceof AppendWriter) {
                    ((AppendWriter) tableWriter).append(row);
                } else {
                    ((UpsertWriter) tableWriter).upsert(row);
                }
                if (i % 10 == 0) {
                    // insert 3 bathes, each batch has 10 rows
                    tableWriter.flush();
                }
            }
        }

        // fetch data.
        try (Table table = conn.getTable(DATA1_TABLE_PATH);
                LogScanner logScanner = createLogScanner(table)) {
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    if (append) {
                        assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    } else {
                        assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                    }
                    assertThat(scanRecord.getRow().getFieldCount()).isEqualTo(4);
                    assertThat(scanRecord.getRow().getInt(0)).isEqualTo(count);
                    assertThat(scanRecord.getRow().getInt(1)).isEqualTo(100);
                    if (count % 2 == 0) {
                        assertThat(scanRecord.getRow().getString(2).toString())
                                .isEqualTo("hello, friend" + count);
                    } else {
                        // check null values
                        assertThat(scanRecord.getRow().isNullAt(2)).isTrue();
                    }
                    assertThat(scanRecord.getRow().getLong(3)).isEqualTo(count * 10L);
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedSize);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "ARROW"})
    void testAppendAndProject(String format) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.BIGINT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .logFormat(LogFormat.fromString(format))
                        .build();
        TablePath tablePath = TablePath.of("test_db_1", "test_append_and_project");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            int expectedSize = 30;
            for (int i = 0; i < expectedSize; i++) {
                String value = i % 2 == 0 ? "hello, friend" + i : null;
                GenericRow row = row(i, 100, value, i * 10L);
                appendWriter.append(row);
                if (i % 10 == 0) {
                    // insert 3 bathes, each batch has 10 rows
                    appendWriter.flush();
                }
            }

            // fetch data.
            LogScanner logScanner = createLogScanner(table, new int[] {0, 2});
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    assertThat(scanRecord.getRow().getFieldCount()).isEqualTo(2);
                    assertThat(scanRecord.getRow().getInt(0)).isEqualTo(count);
                    if (count % 2 == 0) {
                        assertThat(scanRecord.getRow().getString(1).toString())
                                .isEqualTo("hello, friend" + count);
                    } else {
                        // check null values
                        assertThat(scanRecord.getRow().isNullAt(1)).isTrue();
                    }
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedSize);
            logScanner.close();

            // fetch data with projection reorder.
            logScanner = createLogScanner(table, new int[] {2, 0});
            subscribeFromBeginning(logScanner, table);
            count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    assertThat(scanRecord.getRow().getFieldCount()).isEqualTo(2);
                    assertThat(scanRecord.getRow().getInt(1)).isEqualTo(count);
                    if (count % 2 == 0) {
                        assertThat(scanRecord.getRow().getString(0).toString())
                                .isEqualTo("hello, friend" + count);
                    } else {
                        // check null values
                        assertThat(scanRecord.getRow().isNullAt(0)).isTrue();
                    }
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedSize);
            logScanner.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ARROW", "COMPACTED"})
    void testPutAndProject(String changelogFormat) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_LOG_FORMAT.key(), changelogFormat)
                        .build();
        TablePath tablePath = TablePath.of("test_db_1", "test_pk_table_1");
        createTable(tablePath, tableDescriptor, false);

        int batches = 3;
        int keyId = 0;
        int expectedSize = 0;
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int b = 0; b < batches; b++) {
                // insert 10 rows
                for (int i = keyId; i < keyId + 10; i++) {
                    InternalRow row = row(i, 100, "hello, friend" + i, i * 10L);
                    upsertWriter.upsert(row);
                    expectedSize += 1;
                }
                // update 5 rows: [keyId, keyId+4]
                for (int i = keyId; i < keyId + 5; i++) {
                    InternalRow row = row(i, 200, "HELLO, FRIEND" + i, i * 10L);
                    upsertWriter.upsert(row);
                    expectedSize += 2;
                }
                // delete 1 row: [keyId+5]
                int deleteKey = keyId + 5;
                InternalRow row = row(deleteKey, 100, "hello, friend" + deleteKey, deleteKey * 10L);
                upsertWriter.delete(row);
                expectedSize += 1;
                // flush the mutation batch
                upsertWriter.flush();
                keyId += 10;
            }
        }

        // fetch data.
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = createLogScanner(table, new int[] {0, 2, 1})) {
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            int id = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                Iterator<ScanRecord> iterator = scanRecords.iterator();
                while (iterator.hasNext()) {
                    // 10 inserts
                    for (int i = 0; i < 10; i++) {
                        ScanRecord scanRecord = iterator.next();
                        assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                        assertThat(scanRecord.getRow().getFieldCount()).isEqualTo(3);
                        assertThat(scanRecord.getRow().getInt(0)).isEqualTo(id);
                        assertThat(scanRecord.getRow().getString(1).toString())
                                .isEqualTo("hello, friend" + id);
                        assertThat(scanRecord.getRow().getInt(2)).isEqualTo(100);
                        count++;
                        id++;
                    }
                    id -= 10;
                    // 10 updates
                    for (int i = 0; i < 5; i++) {
                        ScanRecord beforeRecord = iterator.next();
                        assertThat(beforeRecord.getChangeType())
                                .isEqualTo(ChangeType.UPDATE_BEFORE);
                        assertThat(beforeRecord.getRow().getFieldCount()).isEqualTo(3);
                        assertThat(beforeRecord.getRow().getInt(0)).isEqualTo(id);
                        assertThat(beforeRecord.getRow().getString(1).toString())
                                .isEqualTo("hello, friend" + id);
                        assertThat(beforeRecord.getRow().getInt(2)).isEqualTo(100);

                        ScanRecord afterRecord = iterator.next();
                        assertThat(afterRecord.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
                        assertThat(afterRecord.getRow().getFieldCount()).isEqualTo(3);
                        assertThat(afterRecord.getRow().getInt(0)).isEqualTo(id);
                        assertThat(afterRecord.getRow().getString(1).toString())
                                .isEqualTo("HELLO, FRIEND" + id);
                        assertThat(afterRecord.getRow().getInt(2)).isEqualTo(200);

                        id++;
                        count += 2;
                    }

                    // 1 delete
                    ScanRecord beforeRecord = iterator.next();
                    assertThat(beforeRecord.getChangeType()).isEqualTo(ChangeType.DELETE);
                    assertThat(beforeRecord.getRow().getFieldCount()).isEqualTo(3);
                    assertThat(beforeRecord.getRow().getInt(0)).isEqualTo(id);
                    assertThat(beforeRecord.getRow().getString(1).toString())
                            .isEqualTo("hello, friend" + id);
                    assertThat(beforeRecord.getRow().getInt(2)).isEqualTo(100);
                    count++;
                    id += 5;
                }
            }
            assertThat(count).isEqualTo(expectedSize);
        }
    }

    @Test
    void testPutAndProjectDuringAddColumn() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
        TablePath tablePath = TablePath.of("test_db_1", "test_pk_table_1");
        createTable(tablePath, tableDescriptor, false);

        int batches = 3;
        int keyId = 0;
        int expectedSize = 0;

        try (Table table = conn.getTable(tablePath)) {
            // produce data with new schema.
            // Test schema change: add new column which equals to DATA2_ROW_TYPE
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "d",
                                            DataTypes.BIGINT(),
                                            "add new column",
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();
            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table newSchemaTable = connection.getTable(tablePath)) {
                UpsertWriter oldSchemaUpsertWriter = table.newUpsert().createWriter();
                UpsertWriter newSchemaUpsertWriter = newSchemaTable.newUpsert().createWriter();
                for (int b = 0; b < batches; b++) {
                    // insert 10 rows with old schema.
                    for (int i = keyId; i < keyId + 10; i++) {
                        InternalRow row = row(i, 100, "hello, friend" + i);
                        oldSchemaUpsertWriter.upsert(row);
                        expectedSize += 1;
                        oldSchemaUpsertWriter.flush();
                    }
                    // update 5 rows with new schema: [keyId, keyId+4]
                    for (int i = keyId; i < keyId + 5; i++) {
                        InternalRow row = row(i, 100, "HELLO, FRIEND" + i, i * 10L);
                        newSchemaUpsertWriter.upsert(row);
                        expectedSize += 2;
                        newSchemaUpsertWriter.flush();
                    }
                    // delete 1 row with old schema: [keyId+5]
                    int deleteKey = keyId + 5;
                    InternalRow row = row(deleteKey, 100, "hello, friend" + deleteKey);
                    oldSchemaUpsertWriter.delete(row);
                    expectedSize += 1;
                    // flush the mutation batch
                    oldSchemaUpsertWriter.flush();
                    keyId += 10;
                }

                // read with new schema
                try (LogScanner logScanner = createLogScanner(newSchemaTable, new int[] {2, 0})) {
                    subscribeFromBeginning(logScanner, table);
                    int count = 0;
                    int id = 0;
                    while (count < expectedSize) {
                        ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                        Iterator<ScanRecord> iterator = scanRecords.iterator();
                        while (iterator.hasNext()) {
                            // 10 inserts
                            for (int i = 0; i < 10; i++) {
                                ScanRecord scanRecord = iterator.next();
                                assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                                assertThat(scanRecord.getRow().getFieldCount()).isEqualTo(2);
                                assertThat(scanRecord.getRow().getInt(1)).isEqualTo(id);
                                assertThat(scanRecord.getRow().getString(0).toString())
                                        .isEqualTo("hello, friend" + id);
                                count++;
                                id++;
                            }
                            id -= 10;
                            // 10 updates
                            for (int i = 0; i < 5; i++) {
                                ScanRecord beforeRecord = iterator.next();
                                assertThat(beforeRecord.getChangeType())
                                        .isEqualTo(ChangeType.UPDATE_BEFORE);
                                assertThat(beforeRecord.getRow().getFieldCount()).isEqualTo(2);
                                assertThat(beforeRecord.getRow().getInt(1)).isEqualTo(id);
                                assertThat(beforeRecord.getRow().getString(0).toString())
                                        .isEqualTo("hello, friend" + id);

                                ScanRecord afterRecord = iterator.next();
                                assertThat(afterRecord.getChangeType())
                                        .isEqualTo(ChangeType.UPDATE_AFTER);
                                assertThat(afterRecord.getRow().getFieldCount()).isEqualTo(2);
                                assertThat(afterRecord.getRow().getInt(1)).isEqualTo(id);
                                assertThat(afterRecord.getRow().getString(0).toString())
                                        .isEqualTo("HELLO, FRIEND" + id);

                                id++;
                                count += 2;
                            }

                            // 1 delete
                            ScanRecord beforeRecord = iterator.next();
                            assertThat(beforeRecord.getChangeType()).isEqualTo(ChangeType.DELETE);
                            assertThat(beforeRecord.getRow().getFieldCount()).isEqualTo(2);
                            assertThat(beforeRecord.getRow().getInt(1)).isEqualTo(id);
                            assertThat(beforeRecord.getRow().getString(0).toString())
                                    .isEqualTo("hello, friend" + id);
                            count++;
                            id += 5;
                        }
                    }
                    assertThat(count).isEqualTo(expectedSize);
                }
            }
        }
    }

    @Test
    void testInvalidColumnProjection() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).logFormat(LogFormat.INDEXED).build();
        createTable(DATA1_TABLE_PATH, tableDescriptor, false);
        Table table = conn.getTable(DATA1_TABLE_PATH);

        // validation on projection
        assertThatThrownBy(() -> createLogScanner(table, new int[] {1, 2, 3, 4, 5}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Projected field index 2 is out of bound for schema ROW<`a` INT, `b` STRING>");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFirstRowMergeEngine(boolean doProjection) throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.FIRST_ROW)
                        .build();
        RowType rowType = DATA1_SCHEMA_PK.getRowType();
        String tableName =
                String.format(
                        "test_first_row_merge_engine_with_%s",
                        doProjection ? "projection" : "no_projection");
        TablePath tablePath = TablePath.of("test_db_1", tableName);
        createTable(tablePath, tableDescriptor, false);

        int rows = 5;
        int duplicateNum = 10;
        int batchSize = 3;
        int count = 0;
        try (Table table = conn.getTable(tablePath)) {
            // first, put rows
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            List<InternalRow> expectedScanRows = new ArrayList<>(rows);
            List<InternalRow> expectedLookupRows = new ArrayList<>(rows);
            for (int id = 0; id < rows; id++) {
                for (int num = 0; num < duplicateNum; num++) {
                    upsertWriter.upsert(row(id, "value_" + num));
                    if (count++ > batchSize) {
                        upsertWriter.flush();
                        count = 0;
                    }
                }

                expectedLookupRows.add(row(id, "value_0"));
                expectedScanRows.add(doProjection ? row(id) : row(id, "value_0"));
            }

            upsertWriter.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            // now, get rows by lookup
            for (int id = 0; id < rows; id++) {
                InternalRow gotRow = lookuper.lookup(row(id)).get().getSingletonRow();
                assertThatRow(gotRow).withSchema(rowType).isEqualTo(expectedLookupRows.get(id));
            }

            Scan scan = table.newScan();
            if (doProjection) {
                scan = scan.project(new int[] {0}); // do projection.
            }
            LogScanner logScanner = scan.createLogScanner();

            logScanner.subscribeFromBeginning(0);
            List<ScanRecord> actualLogRecords = new ArrayList<>(0);
            while (actualLogRecords.size() < rows) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                scanRecords.forEach(actualLogRecords::add);
            }
            logScanner.close();
            assertThat(actualLogRecords).hasSize(rows);
            for (int i = 0; i < actualLogRecords.size(); i++) {
                ScanRecord scanRecord = actualLogRecords.get(i);
                assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThatRow(scanRecord.getRow())
                        .withSchema(doProjection ? rowType.project(new int[] {0}) : rowType)
                        .isEqualTo(expectedScanRows.get(i));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"none,3", "lz4_frame,3", "zstd,3", "zstd,9"})
    void testArrowCompressionAndProject(String compression, String level) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.BIGINT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_TYPE.key(), compression)
                        .property(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL.key(), level)
                        .build();
        TablePath tablePath = TablePath.of("test_db_1", "test_arrow_" + compression + level);
        createTable(tablePath, tableDescriptor, false);

        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            int expectedSize = 30;
            for (int i = 0; i < expectedSize; i++) {
                String value = i % 2 == 0 ? "hello, friend " + i : null;
                InternalRow row = row(i, 100, value, i * 10L);
                appendWriter.append(row);
                if (i % 10 == 0) {
                    // insert 3 bathes, each batch has 10 rows
                    appendWriter.flush();
                }
            }

            // fetch data without project.
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {

                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    assertThat(scanRecord.getRow().getInt(0)).isEqualTo(count);
                    assertThat(scanRecord.getRow().getInt(1)).isEqualTo(100);
                    if (count % 2 == 0) {
                        assertThat(scanRecord.getRow().getString(2).toString())
                                .isEqualTo("hello, friend " + count);
                    } else {
                        // check null values
                        assertThat(scanRecord.getRow().isNullAt(2)).isTrue();
                    }
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedSize);
            logScanner.close();

            // fetch data with project.
            logScanner = createLogScanner(table, new int[] {0, 2});
            subscribeFromBeginning(logScanner, table);
            count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    assertThat(scanRecord.getRow().getFieldCount()).isEqualTo(2);
                    assertThat(scanRecord.getRow().getInt(0)).isEqualTo(count);
                    if (count % 2 == 0) {
                        assertThat(scanRecord.getRow().getString(1).toString())
                                .isEqualTo("hello, friend " + count);
                    } else {
                        // check null values
                        assertThat(scanRecord.getRow().isNullAt(1)).isTrue();
                    }
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedSize);
            logScanner.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMergeEngineWithVersion(boolean doProjection) throws Exception {
        // Create table.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA3_SCHEMA_PK)
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.VERSIONED)
                        .property(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN, "b")
                        .build();
        RowType rowType = DATA3_SCHEMA_PK.getRowType();
        String tableName =
                String.format(
                        "test_merge_engine_with_version_with_%s",
                        doProjection ? "projection" : "no_projection");
        TablePath tablePath = TablePath.of("test_db_1", tableName);
        createTable(tablePath, tableDescriptor, false);

        int rows = 3;
        try (Table table = conn.getTable(tablePath)) {
            // put rows.
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            List<ScanRecord> expectedScanRecords = new ArrayList<>(rows);
            // init rows.
            for (int id = 0; id < rows; id++) {
                upsertWriter.upsert(row(id, 1000L));

                expectedScanRecords.add(
                        doProjection ? new ScanRecord(row(id)) : new ScanRecord(row(id, 1000L)));
            }
            upsertWriter.flush();

            // update row if id=0 and version < 1000L, will not update
            int oldVersionRecordCount = 20;
            int batchSize = 3;
            int count = 0;
            for (int i = 0; i < oldVersionRecordCount; i++) {
                upsertWriter.upsert(row(0, 999L));
                if (count++ > batchSize) {
                    upsertWriter.flush();
                    count = 0;
                }
            }

            // update if version> 1000L
            upsertWriter.upsert(row(1, 1001L));
            // update_before record, don't care about offset/timestamp
            expectedScanRecords.add(
                    new ScanRecord(
                            -1,
                            -1,
                            ChangeType.UPDATE_BEFORE,
                            doProjection ? row(1) : row(1, 1000L)));
            // update_after record
            expectedScanRecords.add(
                    new ScanRecord(
                            -1,
                            -1,
                            ChangeType.UPDATE_AFTER,
                            doProjection ? row(1) : row(1, 1001L)));
            rows = rows + 2;

            upsertWriter.flush();

            Scan scan = table.newScan();
            if (doProjection) {
                scan = scan.project(new int[] {0}); // do projection.
            }
            LogScanner logScanner = scan.createLogScanner();
            logScanner.subscribeFromBeginning(0);
            List<ScanRecord> actualLogRecords = new ArrayList<>(rows);
            while (actualLogRecords.size() < rows) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                scanRecords.forEach(actualLogRecords::add);
            }
            logScanner.close();

            assertThat(actualLogRecords).hasSize(rows);
            for (int i = 0; i < rows; i++) {
                ScanRecord actualScanRecord = actualLogRecords.get(i);
                ScanRecord expectedRecord = expectedScanRecords.get(i);
                assertThat(actualScanRecord.getChangeType())
                        .isEqualTo(expectedRecord.getChangeType());
                assertThatRow(actualScanRecord.getRow())
                        .withSchema(doProjection ? rowType.project(new int[] {0}) : rowType)
                        .isEqualTo(expectedRecord.getRow());
            }
        }
    }

    @Test
    void testFileSystemRecognizeConnectionConf() throws Exception {
        Configuration config = new Configuration(clientConf);
        config.setString("client.fs.test.key", "fs_test_value");
        config.setString("client.test.key", "client_test_value");
        try (Connection ignore = ConnectionFactory.createConnection(config)) {
            FsPath fsPath = new FsPath("test:///f1");
            TestFileSystem testFileSystem = (TestFileSystem) fsPath.getFileSystem();
            Configuration filesystemConf = testFileSystem.getConfiguration();
            assertThat(filesystemConf.toMap())
                    .containsExactlyEntriesOf(
                            Collections.singletonMap("client.fs.test.key", "fs_test_value"));
        }
    }

    // ---------------------- PK with COMPACTED log tests ----------------------
    @Test
    void testPkUpsertAndPollWithCompactedLog() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .kvFormat(KvFormat.COMPACTED)
                        .logFormat(LogFormat.COMPACTED)
                        .build();
        TablePath tablePath = TablePath.of("test_db_1", "test_pk_compacted_upsert_poll");
        createTable(tablePath, tableDescriptor, false);

        int expectedSize = 30;
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < expectedSize; i++) {
                String value = i % 2 == 0 ? "hello, friend" + i : null;
                GenericRow r = row(i, 100, value, i * 10L);
                upsertWriter.upsert(r);
                if (i % 10 == 0) {
                    upsertWriter.flush();
                }
            }
            upsertWriter.flush();

            // normal scan
            try (LogScanner logScanner = createLogScanner(table)) {
                subscribeFromBeginning(logScanner, table);
                int count = 0;
                while (count < expectedSize) {
                    ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                    for (ScanRecord scanRecord : scanRecords) {
                        assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                        InternalRow rr = scanRecord.getRow();
                        assertThat(rr.getFieldCount()).isEqualTo(4);
                        assertThat(rr.getInt(0)).isEqualTo(count);
                        assertThat(rr.getInt(1)).isEqualTo(100);
                        if (count % 2 == 0) {
                            assertThat(rr.getString(2).toString())
                                    .isEqualTo("hello, friend" + count);
                        } else {
                            assertThat(rr.isNullAt(2)).isTrue();
                        }
                        assertThat(rr.getLong(3)).isEqualTo(count * 10L);
                        count++;
                    }
                }
                assertThat(count).isEqualTo(expectedSize);
            }

            // Creating a projected log scanner for COMPACTED should work
            try (LogScanner scanner = createLogScanner(table, new int[] {0, 2})) {
                subscribeFromBeginning(scanner, table);
                int count = 0;
                while (count < expectedSize) {
                    ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                    for (ScanRecord record : records) {
                        InternalRow row = record.getRow();
                        assertThat(row.getFieldCount()).isEqualTo(2);
                        assertThat(row.getInt(0)).isEqualTo(count);
                        if (count % 2 == 0) {
                            assertThat(row.getString(1).toString())
                                    .isEqualTo("hello, friend" + count);
                        } else {
                            assertThat(row.isNullAt(1)).isTrue();
                        }
                        count++;
                    }
                }
                assertThat(count).isEqualTo(expectedSize);
            }
        }
    }

    @Test
    void testPkUpdateAndDeleteWithCompactedLog() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .kvFormat(KvFormat.COMPACTED)
                        .logFormat(LogFormat.COMPACTED)
                        .build();
        TablePath tablePath = TablePath.of("test_db_1", "test_pk_compacted_update_delete");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            // initial insert
            upsertWriter.upsert(row(1, 10));
            upsertWriter.flush();
            // update same key
            upsertWriter.upsert(row(1, 20));
            upsertWriter.flush();
            // delete the key
            upsertWriter.delete(row(1, 20));
            upsertWriter.flush();

            LogScanner scanner = createLogScanner(table);
            subscribeFromBeginning(scanner, table);
            // Expect: +I(1,10), -U(1,10), +U(1,20), -D(1,20)
            ChangeType[] expected = {
                ChangeType.INSERT,
                ChangeType.UPDATE_BEFORE,
                ChangeType.UPDATE_AFTER,
                ChangeType.DELETE
            };
            int seen = 0;
            while (seen < expected.length) {
                ScanRecords recs = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord r : recs) {
                    assertThat(r.getChangeType()).isEqualTo(expected[seen]);
                    InternalRow row = r.getRow();
                    assertThat(row.getInt(0)).isEqualTo(1);
                    // value field present
                    if (expected[seen] == ChangeType.UPDATE_AFTER
                            || expected[seen] == ChangeType.DELETE) {
                        assertThat(row.getInt(1)).isEqualTo(20);
                    } else {
                        assertThat(row.getInt(1)).isEqualTo(10);
                    }
                    seen++;
                }
            }
            assertThat(seen).isEqualTo(expected.length);
            scanner.close();
        }
    }

    @Test
    void testPkCompactedPollFromLatestNoRecords() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .build();
        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(schema)
                        .kvFormat(KvFormat.COMPACTED)
                        .logFormat(LogFormat.COMPACTED)
                        .build();
        TablePath path = TablePath.of("test_db_1", "test_pk_compacted_latest");
        createTable(path, td, false);

        try (Table table = conn.getTable(path)) {
            LogScanner scanner = createLogScanner(table);
            subscribeFromLatestOffset(path, null, null, table, scanner, admin);
            // Now write a few rows and ensure only these are seen
            UpsertWriter upsert = table.newUpsert().createWriter();
            for (int i = 0; i < 5; i++) {
                upsert.upsert(row(i, i));
            }
            upsert.flush();

            int seen = 0;
            while (seen < 5) {
                ScanRecords recs = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord r : recs) {
                    assertThat(r.getChangeType()).isEqualTo(ChangeType.INSERT);
                    assertThat(r.getRow().getInt(0)).isBetween(0, 4);
                    seen++;
                }
            }

            // delete non-existent key
            upsert.delete(row(42, 0));
            upsert.flush();
            // poll a few times to ensure no accidental records
            int total = 0;
            for (int i = 0; i < 3; i++) {
                total += scanner.poll(Duration.ofSeconds(1)).count();
            }
            assertThat(total).isEqualTo(0);

            scanner.close();
        }
    }
}
