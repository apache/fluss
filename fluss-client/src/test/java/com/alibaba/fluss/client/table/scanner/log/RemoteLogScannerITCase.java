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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA2_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link LogScannerImpl} as scan log from remote. */
public class RemoteLogScannerITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private Connection conn;
    private Admin admin;

    @BeforeEach
    protected void setup() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @Test
    void testScanFromRemote() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        long tableId = createTable(DATA1_TABLE_PATH, tableDescriptor);

        // append a batch of data.
        int recordSize = 20;
        List<InternalRow> expectedRows = new ArrayList<>();
        Table table = conn.getTable(DATA1_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();
        for (int i = 0; i < recordSize; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "aaaaa"});
            expectedRows.add(row);
            appendWriter.append(row).get();
        }

        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(new TableBucket(tableId, 0));

        // test fetch.
        LogScanner logScanner = table.newScan().createLogScanner();
        logScanner.subscribeFromBeginning(0);
        List<InternalRow> rowList = new ArrayList<>();
        while (rowList.size() < recordSize) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (ScanRecord scanRecord : scanRecords) {
                assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                InternalRow row = scanRecord.getRow();
                rowList.add(row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
            }
        }
        assertThat(rowList).hasSize(recordSize);
        assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
        logScanner.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "ARROW"})
    void testScanFromRemoteAndProject(String format) throws Exception {
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
                        .distributedBy(1)
                        .logFormat(LogFormat.fromString(format))
                        .build();
        long tableId = createTable(DATA1_TABLE_PATH, tableDescriptor);

        // append a batch of data.
        List<InternalRow> expectedRows = new ArrayList<>();
        Table table = conn.getTable(DATA1_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int expectedSize = 30;
        for (int i = 0; i < expectedSize; i++) {
            String value = i % 2 == 0 ? "hello, friend" + i : null;
            InternalRow row = row(schema.getRowType(), new Object[] {i, 100, value, i * 10L});
            appendWriter.append(row);
            if (i % 10 == 0) {
                // insert 3 bathes, each batch has 10 rows
                appendWriter.flush();
            }
        }

        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(new TableBucket(tableId, 0));

        // test fetch.
        LogScanner logScanner = createLogScanner(table, new int[] {0, 2});
        logScanner.subscribeFromBeginning(0);
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
        logScanner.subscribeFromBeginning(0);
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

    @Test
    void testPartitionTableFetchFromRemote() throws Exception {
        final Schema data2NonPkSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is adding column")
                        .build();
        final TablePath tablePath = DATA2_TABLE_PATH;

        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(data2NonPkSchema)
                        .distributedBy(1)
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        long tableId = createTable(tablePath, partitionTableDescriptor);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(tablePath);
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row = row(DATA2_ROW_TYPE, new Object[] {i, "aaaab" + i, partition});
                appendWriter.append(row).get();
                expectPartitionAppendRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }

        for (long id : partitionIdByNames.values()) {
            FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(
                    new TableBucket(tableId, id, 0));
        }

        ClientToServerITCaseBase.verifyPartitionLogs(
                table, DATA2_ROW_TYPE, expectPartitionAppendRows);
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

    private long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false).get();
        admin.createTable(tablePath, tableDescriptor, false).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));
        return conf;
    }

    private static LogScanner createLogScanner(Table table, int[] projectFields) {
        return table.newScan().project(projectFields).createLogScanner();
    }
}
