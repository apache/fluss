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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWrite;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.utils.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.testutils.DataTestUtils.assertRowValueEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.keyRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link FlussTable}. */
class FlussTableITCase extends ClientToServerITCaseBase {

    @Test
    void testGetDescriptor() throws Exception {
        createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK.getTableDescriptor(), false);
        // get table descriptor.
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        TableDescriptor tableDescriptor = table.getDescriptor();
        assertThat(tableDescriptor).isEqualTo(DATA1_TABLE_INFO_PK.getTableDescriptor());
    }

    @Test
    void testAppendOnly() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_INFO.getTableDescriptor(), false);
        // append data.
        InternalRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});

        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.getAppendWriter();
            appendWriter.append(row).get();
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
            AppendWriter appendWriter = table.getAppendWriter();
            BinaryString value = BinaryString.fromString(StringUtils.repeat("a", 100));
            // should exceed the buffer size, but append successfully
            for (int i = 0; i < expectedSize; i++) {
                appendWriter.append(row(DATA1_ROW_TYPE, new Object[] {1, value}));
            }
            appendWriter.flush();

            // assert the written data
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
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
            UpsertWriter upsertWriter = table.getUpsertWriter();
            BinaryString value = BinaryString.fromString(StringUtils.repeat("a", 100));
            // should exceed the buffer size, but append successfully
            for (int i = 0; i < expectedSize; i++) {
                upsertWriter.upsert(compactedRow(DATA1_ROW_TYPE, new Object[] {i, value}));
            }
            upsertWriter.flush();

            // assert the written data
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            int count = 0;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.INSERT);
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
        createTable(tablePath, DATA1_TABLE_INFO_PK.getTableDescriptor(), false);

        Table table = conn.getTable(tablePath);
        verifyPutAndLookup(table, DATA1_SCHEMA_PK, new Object[] {1, "a"});

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
        verifyPutAndLookup(table2, schema, new Object[] {"a", 1});
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
        waitAllReplicasReady(tableId, descriptor);
        Table table = conn.getTable(tablePath);
        assertThat(lookupRow(table, rowKey)).isNull();
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
            UpsertWriter upsertWriter = table.getUpsertWriter();

            List<Object[]> expectedRows = new ArrayList<>();
            for (int i = 0; i < insertSize; i++) {
                BinaryString value = BinaryString.fromString(StringUtils.repeat("a", i));
                CompactedRow compactedRow = compactedRow(DATA1_ROW_TYPE, new Object[] {i, value});
                upsertWriter.upsert(compactedRow);
                if (i < limitSize) {
                    expectedRows.add(new Object[] {i, StringUtils.repeat("a", i)});
                }
            }
            upsertWriter.flush();

            TableBucket tb = new TableBucket(tableId, 0);
            List<InternalRow> actualRows =
                    table.limitScan(tb, limitSize, null).get().stream()
                            .map(ScanRecord::getRow)
                            .collect(Collectors.toList());
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.toRowType(), actualRows.get(i), expectedRows.get(i));
            }

            // test projection scan
            int[] projectedFields = new int[] {1};
            for (int i = 0; i < limitSize; i++) {
                expectedRows.set(i, new Object[] {expectedRows.get(i)[1]});
            }
            actualRows =
                    table.limitScan(tb, limitSize, projectedFields).get().stream()
                            .map(ScanRecord::getRow)
                            .collect(Collectors.toList());
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.toRowType().project(projectedFields),
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
            AppendWriter appendWriter = table.getAppendWriter();

            List<Object[]> expectedRows = new ArrayList<>();
            for (int i = 0; i < insertSize; i++) {
                BinaryString value = BinaryString.fromString(StringUtils.repeat("a", i));
                CompactedRow compactedRow = compactedRow(DATA1_ROW_TYPE, new Object[] {i, value});
                appendWriter.append(compactedRow);
                // limit log scan read the latest limit number of record.
                if (i >= insertSize - limitSize) {
                    expectedRows.add(new Object[] {i, StringUtils.repeat("a", i)});
                }
            }
            appendWriter.flush();

            TableBucket tb = new TableBucket(tableId, 0);
            List<InternalRow> actualRows =
                    table.limitScan(tb, limitSize, null).get().stream()
                            .map(ScanRecord::getRow)
                            .collect(Collectors.toList());
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.toRowType(), actualRows.get(i), expectedRows.get(i));
            }

            // test projection scan
            int[] projectedFields = new int[] {1};
            for (int i = 0; i < limitSize; i++) {
                expectedRows.set(i, new Object[] {expectedRows.get(i)[1]});
            }
            actualRows =
                    table.limitScan(tb, limitSize, projectedFields).get().stream()
                            .map(ScanRecord::getRow)
                            .collect(Collectors.toList());
            assertThat(actualRows.size()).isEqualTo(limitSize);
            for (int i = 0; i < limitSize; i++) {
                assertRowValueEquals(
                        DATA1_SCHEMA.toRowType().project(projectedFields),
                        actualRows.get(i),
                        expectedRows.get(i));
            }
        }
    }

    void verifyPutAndLookup(Table table, Schema tableSchema, Object[] fields) throws Exception {
        // put data.
        InternalRow row = compactedRow(tableSchema.toRowType(), fields);
        UpsertWriter upsertWriter = table.getUpsertWriter();
        // put data.
        upsertWriter.upsert(row);
        upsertWriter.flush();
        // lookup this key.
        IndexedRow keyRow = keyRow(tableSchema, fields);
        assertThat(lookupRow(table, keyRow)).isEqualTo(row);
    }

    private InternalRow lookupRow(Table table, IndexedRow keyRow) throws Exception {
        // lookup this key.
        return table.lookup(keyRow).get().getRow();
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
        RowType pkRowType = RowType.of(DataTypes.INT());
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();
        createTable(DATA1_TABLE_PATH_PK, tableDescriptor, true);

        // test put a full row
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        verifyPutAndLookup(table, schema, new Object[] {1, "a", 1, true});

        // partial update columns: a, b
        UpsertWrite partialUpdate = new UpsertWrite().withPartialUpdate(new int[] {0, 1});
        UpsertWriter upsertWriter = table.getUpsertWriter(partialUpdate);
        upsertWriter
                .upsert(compactedRow(schema.toRowType(), new Object[] {1, "aaa", null, null}))
                .get();

        // check the row
        IndexedRow rowKey = row(pkRowType, new Object[] {1});
        assertThat(lookupRow(table, rowKey))
                .isEqualTo(compactedRow(schema.toRowType(), new Object[] {1, "aaa", 1, true}));

        // partial update columns columns: a,b,c
        partialUpdate = new UpsertWrite().withPartialUpdate(new int[] {0, 1, 2});
        upsertWriter = table.getUpsertWriter(partialUpdate);
        upsertWriter
                .upsert(compactedRow(schema.toRowType(), new Object[] {1, "bbb", 222, null}))
                .get();

        // lookup the row
        assertThat(lookupRow(table, rowKey))
                .isEqualTo(compactedRow(schema.toRowType(), new Object[] {1, "bbb", 222, true}));

        // test partial delete, target column is a,b,c
        upsertWriter
                .delete(compactedRow(schema.toRowType(), new Object[] {1, "bbb", 222, null}))
                .get();
        assertThat(lookupRow(table, rowKey))
                .isEqualTo(compactedRow(schema.toRowType(), new Object[] {1, null, null, true}));

        // partial delete, target column is d
        partialUpdate = new UpsertWrite().withPartialUpdate(new int[] {0, 3});
        upsertWriter = table.getUpsertWriter(partialUpdate);
        upsertWriter
                .delete(compactedRow(schema.toRowType(), new Object[] {1, null, null, true}))
                .get();

        // the row should be deleted, shouldn't get the row again
        assertThat(lookupRow(table, rowKey)).isNull();

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
            assertThatThrownBy(
                            () ->
                                    table.getUpsertWriter(
                                            new UpsertWrite().withPartialUpdate(new int[] {1})))
                    .hasMessage(
                            "The target write columns [b] must contain the primary key columns [a].");

            // the column not in the primary key is nullable, should throw exception
            assertThatThrownBy(
                            () ->
                                    table.getUpsertWriter(
                                            new UpsertWrite().withPartialUpdate(new int[] {0, 1})))
                    .hasMessage(
                            "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
            assertThatThrownBy(
                            () ->
                                    table.getUpsertWriter(
                                            new UpsertWrite().withPartialUpdate(new int[] {0, 2})))
                    .hasMessage(
                            "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
        }
    }

    @Test
    void testDelete() throws Exception {
        createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK.getTableDescriptor(), false);

        // put key.
        InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            UpsertWriter upsertWriter = table.getUpsertWriter();
            upsertWriter.upsert(row).get();

            // lookup this key.
            IndexedRow keyRow = keyRow(DATA1_SCHEMA_PK, new Object[] {1, "a"});
            assertThat(lookupRow(table, keyRow)).isEqualTo(row);

            // delete this key.
            upsertWriter.delete(row).get();
            // lookup this key again, will return null.
            assertThat(lookupRow(table, keyRow)).isNull();
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
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.getAppendWriter();
            appendWriter.append(row).get();

            // fetch data.
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);
            InternalRow result = null;
            while (result == null) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    result = scanRecord.getRow();
                }
            }
            assertThatRow(result).withSchema(DATA1_ROW_TYPE).isEqualTo(row);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "ARROW"})
    void testAppendAndPoll(String format) throws Exception {
        verifyAppendOrPut(true, format, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "COMPACTED"})
    void testPutAndPoll(String kvFormat) throws Exception {
        verifyAppendOrPut(false, "ARROW", kvFormat);
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
                tableWriter = table.getAppendWriter();
            } else {
                tableWriter = table.getUpsertWriter();
            }
            for (int i = 0; i < expectedSize; i++) {
                String value = i % 2 == 0 ? "hello, friend" + i : null;
                InternalRow row;
                if (append) {
                    row = row(schema.toRowType(), new Object[] {i, 100, value, i * 10L});
                } else {
                    Preconditions.checkNotNull(kvFormat);
                    KvFormat format = KvFormat.fromString(kvFormat);
                    if (format == KvFormat.COMPACTED) {
                        row =
                                compactedRow(
                                        schema.toRowType(), new Object[] {i, 100, value, i * 10L});
                    } else {
                        row = row(schema.toRowType(), new Object[] {i, 100, value, i * 10L});
                    }
                }
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
                        assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                    } else {
                        assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.INSERT);
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
            AppendWriter appendWriter = table.getAppendWriter();
            int expectedSize = 30;
            for (int i = 0; i < expectedSize; i++) {
                String value = i % 2 == 0 ? "hello, friend" + i : null;
                InternalRow row = row(schema.toRowType(), new Object[] {i, 100, value, i * 10L});
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
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
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
                    assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
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

    @Test
    void testPutAndProject() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();
        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
        TablePath tablePath = TablePath.of("test_db_1", "test_pk_table_1");
        createTable(tablePath, tableDescriptor, false);

        int batches = 3;
        int keyId = 0;
        int expectedSize = 0;
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.getUpsertWriter();
            for (int b = 0; b < batches; b++) {
                // insert 10 rows
                for (int i = keyId; i < keyId + 10; i++) {
                    InternalRow row =
                            compactedRow(
                                    schema.toRowType(),
                                    new Object[] {i, 100, "hello, friend" + i, i * 10L});
                    upsertWriter.upsert(row);
                    expectedSize += 1;
                }
                // update 5 rows: [keyId, keyId+4]
                for (int i = keyId; i < keyId + 5; i++) {
                    InternalRow row =
                            compactedRow(
                                    schema.toRowType(),
                                    new Object[] {i, 200, "HELLO, FRIEND" + i, i * 10L});
                    upsertWriter.upsert(row);
                    expectedSize += 2;
                }
                // delete 1 row: [keyId+5]
                int deleteKey = keyId + 5;
                InternalRow row =
                        compactedRow(
                                schema.toRowType(),
                                new Object[] {
                                    deleteKey, 100, "hello, friend" + deleteKey, deleteKey * 10L
                                });
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
                        assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.INSERT);
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
                        assertThat(beforeRecord.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
                        assertThat(beforeRecord.getRow().getFieldCount()).isEqualTo(3);
                        assertThat(beforeRecord.getRow().getInt(0)).isEqualTo(id);
                        assertThat(beforeRecord.getRow().getString(1).toString())
                                .isEqualTo("hello, friend" + id);
                        assertThat(beforeRecord.getRow().getInt(2)).isEqualTo(100);

                        ScanRecord afterRecord = iterator.next();
                        assertThat(afterRecord.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
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
                    assertThat(beforeRecord.getRowKind()).isEqualTo(RowKind.DELETE);
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
}
