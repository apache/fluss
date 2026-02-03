/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli.sql;

import org.apache.fluss.cli.config.ConnectionConfig;
import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorSelectTest {

    @Test
    void testSelectWithLookup() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Lookup lookup = mock(Lookup.class);
        Lookuper lookuper = mock(Lookuper.class);
        when(table.newLookup()).thenReturn(lookup);
        when(lookup.createLookuper()).thenReturn(lookuper);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        LookupResult result = new LookupResult(row);
        when(lookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        String output = executeSql(connection, "SELECT * FROM db1.tbl WHERE id = 1");

        assertThat(output).contains("Using point query optimization");
        assertThat(output).contains("Alice");
    }

    @Test
    void testSelectWithScan() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        ScanRecord record = new ScanRecord(row);
        Map<TableBucket, java.util.List<ScanRecord>> recordsMap = new HashMap<>();
        recordsMap.put(new TableBucket(1L, 0), Collections.singletonList(record));
        ScanRecords scanRecords = new ScanRecords(recordsMap);

        when(logScanner.poll(any(Duration.class)))
                .thenReturn(scanRecords)
                .thenReturn(ScanRecords.EMPTY);

        String output = executeSql(connection, "SELECT * FROM db1.tbl");

        assertThat(output).contains("Alice");
    }

    @Test
    void testSelectWithLimit() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        GenericRow row1 = new GenericRow(2);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        GenericRow row2 = new GenericRow(2);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        GenericRow row3 = new GenericRow(2);
        row3.setField(0, 3);
        row3.setField(1, BinaryString.fromString("Charlie"));

        ScanRecord record1 = new ScanRecord(row1);
        ScanRecord record2 = new ScanRecord(row2);
        ScanRecord record3 = new ScanRecord(row3);
        Map<TableBucket, java.util.List<ScanRecord>> recordsMap = new HashMap<>();
        recordsMap.put(new TableBucket(1L, 0), java.util.Arrays.asList(record1, record2, record3));
        ScanRecords scanRecords = new ScanRecords(recordsMap);

        when(logScanner.poll(any(Duration.class)))
                .thenReturn(scanRecords)
                .thenReturn(ScanRecords.EMPTY);

        String output = executeSql(connection, "SELECT * FROM db1.tbl LIMIT 2");

        assertThat(output).contains("Alice");
        assertThat(output).contains("Bob");
        assertThat(output).contains("2 row(s)");
    }

    @Test
    void testSelectWithLimitZero() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Lookup lookup = mock(Lookup.class);
        Lookuper lookuper = mock(Lookuper.class);
        when(table.newLookup()).thenReturn(lookup);
        when(lookup.createLookuper()).thenReturn(lookuper);

        String output = executeSql(connection, "SELECT * FROM db1.tbl WHERE id = 1 LIMIT 0");

        assertThat(output).contains("LIMIT 0: Skipping query execution");
        assertThat(output).contains("0 row(s)");
    }

    @Test
    void testSelectLogTableWithoutLimit() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "log_tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        when(logScanner.poll(any(Duration.class))).thenReturn(ScanRecords.EMPTY);

        String output = executeSql(connection, "SELECT * FROM db1.log_tbl");

        assertThat(output).contains("Streaming mode: Continuously polling for new data");
        assertThat(output).contains("Idle timeout: 30 seconds");
    }

    @Test
    void testSelectWithQuietModeHidesStatusMessages() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        ScanRecord record = new ScanRecord(row);
        Map<TableBucket, java.util.List<ScanRecord>> recordsMap = new HashMap<>();
        recordsMap.put(new TableBucket(1L, 0), Collections.singletonList(record));
        ScanRecords scanRecords = new ScanRecords(recordsMap);

        when(logScanner.poll(any(Duration.class)))
                .thenReturn(scanRecords)
                .thenReturn(ScanRecords.EMPTY);

        String output = executeSqlWithQuiet(connection, "SELECT * FROM db1.tbl");

        assertThat(output).doesNotContain("Executing SELECT");
        assertThat(output).contains("Alice");
        assertThat(output).contains("1 row(s)");
    }

    @Test
    void testSelectLookupWithQuietModeHidesOptimization() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Lookup lookup = mock(Lookup.class);
        Lookuper lookuper = mock(Lookuper.class);
        when(table.newLookup()).thenReturn(lookup);
        when(lookup.createLookuper()).thenReturn(lookuper);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        LookupResult result = new LookupResult(row);
        when(lookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        String output = executeSqlWithQuiet(connection, "SELECT * FROM db1.tbl WHERE id = 1");

        assertThat(output).doesNotContain("Using point query optimization");
        assertThat(output).contains("Alice");
    }

    @Test
    void testStreamingWithQuietModeHidesWarnings() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "log_tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        when(logScanner.poll(any(Duration.class))).thenReturn(ScanRecords.EMPTY);

        String output = executeSqlWithQuiet(connection, "SELECT * FROM db1.log_tbl");

        assertThat(output).doesNotContain("Streaming mode");
        assertThat(output).doesNotContain("Idle timeout");
    }

    @Test
    void testQuietModeWithCsvFormat() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        ScanRecord record = new ScanRecord(row);
        Map<TableBucket, java.util.List<ScanRecord>> recordsMap = new HashMap<>();
        recordsMap.put(new TableBucket(1L, 0), Collections.singletonList(record));
        ScanRecords scanRecords = new ScanRecords(recordsMap);

        when(logScanner.poll(any(Duration.class)))
                .thenReturn(scanRecords)
                .thenReturn(ScanRecords.EMPTY);

        String output =
                executeSqlWithFormat(
                        connection,
                        "SELECT * FROM db1.tbl",
                        org.apache.fluss.cli.format.OutputFormat.CSV,
                        true);

        assertThat(output).doesNotContain("Executing SELECT");
        assertThat(output).contains("1,Alice");
    }

    @Test
    void testQuietModeWithJsonFormat() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        GenericRow row = new GenericRow(2);
        row.setField(0, 42);
        row.setField(1, BinaryString.fromString("Bob"));
        ScanRecord record = new ScanRecord(row);
        Map<TableBucket, java.util.List<ScanRecord>> recordsMap = new HashMap<>();
        recordsMap.put(new TableBucket(1L, 0), Collections.singletonList(record));
        ScanRecords scanRecords = new ScanRecords(recordsMap);

        when(logScanner.poll(any(Duration.class)))
                .thenReturn(scanRecords)
                .thenReturn(ScanRecords.EMPTY);

        String output =
                executeSqlWithFormat(
                        connection,
                        "SELECT * FROM db1.tbl",
                        org.apache.fluss.cli.format.OutputFormat.JSON,
                        true);

        assertThat(output).doesNotContain("Executing SELECT");
        assertThat(output).contains("{\"id\":42,\"name\":\"Bob\"}");
    }

    @Test
    void testCustomStreamingTimeout60Seconds() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "log_tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        when(logScanner.poll(any(Duration.class))).thenReturn(ScanRecords.EMPTY);

        String output = executeSqlWithTimeout(connection, "SELECT * FROM db1.log_tbl", 60);

        assertThat(output).contains("Idle timeout: 60 seconds");
        assertThat(output).doesNotContain("30 seconds");
    }

    @Test
    void testCustomStreamingTimeout10Seconds() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "log_tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        when(logScanner.poll(any(Duration.class))).thenReturn(ScanRecords.EMPTY);

        String output = executeSqlWithTimeout(connection, "SELECT * FROM db1.log_tbl", 10);

        assertThat(output).contains("Idle timeout: 10 seconds");
    }

    @Test
    void testCombineQuietAndCustomTimeout() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "log_tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Scan scan = mock(Scan.class);
        LogScanner logScanner = mock(LogScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        when(logScanner.poll(any(Duration.class))).thenReturn(ScanRecords.EMPTY);

        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor =
                new SqlExecutor(
                        connectionManager,
                        new PrintWriter(writer),
                        org.apache.fluss.cli.format.OutputFormat.TABLE,
                        true,
                        45);
        executor.executeSql("SELECT * FROM db1.log_tbl");
        String output = writer.toString();

        assertThat(output).doesNotContain("Streaming mode");
        assertThat(output).doesNotContain("Idle timeout");
    }

    private static String executeSql(Connection connection, String sql) throws Exception {
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor = new SqlExecutor(connectionManager, new PrintWriter(writer));
        executor.executeSql(sql);
        return writer.toString();
    }

    private static String executeSqlWithQuiet(Connection connection, String sql) throws Exception {
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor =
                new SqlExecutor(
                        connectionManager,
                        new PrintWriter(writer),
                        org.apache.fluss.cli.format.OutputFormat.TABLE,
                        true);
        executor.executeSql(sql);
        return writer.toString();
    }

    private static String executeSqlWithFormat(
            Connection connection,
            String sql,
            org.apache.fluss.cli.format.OutputFormat format,
            boolean quiet)
            throws Exception {
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor =
                new SqlExecutor(connectionManager, new PrintWriter(writer), format, quiet);
        executor.executeSql(sql);
        return writer.toString();
    }

    private static String executeSqlWithTimeout(
            Connection connection, String sql, long timeoutSeconds) throws Exception {
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor =
                new SqlExecutor(
                        connectionManager,
                        new PrintWriter(writer),
                        org.apache.fluss.cli.format.OutputFormat.TABLE,
                        false,
                        timeoutSeconds);
        executor.executeSql(sql);
        return writer.toString();
    }

    private static class StubConnectionManager extends ConnectionManager {
        private final Connection connection;

        StubConnectionManager(Connection connection) {
            super(new ConnectionConfig(new org.apache.fluss.config.Configuration()));
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }
    }
}
