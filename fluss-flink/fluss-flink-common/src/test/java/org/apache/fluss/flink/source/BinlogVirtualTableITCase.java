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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for $binlog virtual table functionality. */
abstract class BinlogVirtualTableITCase extends AbstractTestBase {

    protected static final ManualClock CLOCK = new ManualClock();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(new Configuration())
                    .setNumOfTabletServers(1)
                    .setClock(CLOCK)
                    .build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "test_binlog_db";
    protected StreamExecutionEnvironment execEnv;
    protected StreamTableEnvironment tEnv;
    protected static Connection conn;
    protected static Admin admin;

    protected static Configuration clientConf;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void before() {
        // Initialize Flink environment
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        // Initialize catalog and database
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
        // reset clock before each test
        CLOCK.advanceTime(-CLOCK.milliseconds(), TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    /** Deletes rows from a primary key table using the proper delete API. */
    protected static void deleteRows(
            Connection connection, TablePath tablePath, List<InternalRow> rows) throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (InternalRow row : rows) {
                writer.delete(row);
            }
            writer.flush();
        }
    }

    @Test
    public void testDescribeBinlogTable() throws Exception {
        // Create a table with various data types to test complex schema
        tEnv.executeSql(
                "CREATE TABLE describe_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        // Test DESCRIBE on binlog virtual table
        CloseableIterator<Row> describeResult =
                tEnv.executeSql("DESCRIBE describe_test$binlog").collect();

        List<String> schemaRows = new ArrayList<>();
        while (describeResult.hasNext()) {
            schemaRows.add(describeResult.next().toString());
        }

        // Should have 5 columns: _change_type, _log_offset, _commit_timestamp, before, after
        assertThat(schemaRows).hasSize(5);

        // Verify metadata columns are listed first
        assertThat(schemaRows.get(0))
                .isEqualTo("+I[_change_type, STRING, false, null, null, null]");
        assertThat(schemaRows.get(1)).isEqualTo("+I[_log_offset, BIGINT, false, null, null, null]");
        assertThat(schemaRows.get(2))
                .isEqualTo("+I[_commit_timestamp, TIMESTAMP_LTZ(3), false, null, null, null]");

        // Verify before and after are ROW types with original columns
        assertThat(schemaRows.get(3))
                .isEqualTo(
                        "+I[before, ROW<`id` INT NOT NULL, `name` STRING, `amount` BIGINT>, true, null, null, null]");
        assertThat(schemaRows.get(4))
                .isEqualTo(
                        "+I[after, ROW<`id` INT NOT NULL, `name` STRING, `amount` BIGINT>, true, null, null, null]");
    }

    @Test
    public void testBinlogUnsupportedForLogTable() throws Exception {
        // Create a log table (no primary key)
        tEnv.executeSql(
                "CREATE TABLE log_table ("
                        + "  event_id INT,"
                        + "  event_type STRING"
                        + ") WITH ('bucket.num' = '1')");

        // $binlog should fail for log tables
        assertThatThrownBy(() -> tEnv.executeSql("DESCRIBE log_table$binlog").collect())
                .hasMessageContaining("only supported for primary key tables");
    }

    @Test
    public void testBinlogWithAllChangeTypes() throws Exception {
        // Create a primary key table with 1 bucket for consistent log_offset numbers
        tEnv.executeSql(
                "CREATE TABLE binlog_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "binlog_test");

        // Start binlog scan
        String query =
                "SELECT _change_type, _log_offset, "
                        + "before.id, before.name, before.amount, "
                        + "after.id, after.name, after.amount "
                        + "FROM binlog_test$binlog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Test INSERT
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(
                conn,
                tablePath,
                Arrays.asList(row(1, "Item-1", 100L), row(2, "Item-2", 200L)),
                false);

        // Collect inserts - each INSERT produces one binlog row
        List<String> insertResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(insertResults).hasSize(2);

        // INSERT: before=null, after=row data
        // Format: +I[_change_type, _log_offset, before.id, before.name, before.amount,
        //            after.id, after.name, after.amount]
        assertThat(insertResults.get(0))
                .isEqualTo("+I[insert, 0, null, null, null, 1, Item-1, 100]");
        assertThat(insertResults.get(1))
                .isEqualTo("+I[insert, 1, null, null, null, 2, Item-2, 200]");

        // Test UPDATE - should merge -U and +U into single binlog row
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1-Updated", 150L)), false);

        // UPDATE produces ONE binlog row (not two like changelog)
        List<String> updateResults = collectRowsWithTimeout(rowIter, 1, false);
        assertThat(updateResults).hasSize(1);

        // UPDATE: before=old row, after=new row, offset=from -U record
        assertThat(updateResults.get(0))
                .isEqualTo("+I[update, 2, 1, Item-1, 100, 1, Item-1-Updated, 150]");

        // Test DELETE
        CLOCK.advanceTime(Duration.ofMillis(1000));
        deleteRows(conn, tablePath, Arrays.asList(row(2, "Item-2", 200L)));

        // DELETE produces one binlog row
        List<String> deleteResults = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(deleteResults).hasSize(1);

        // DELETE: before=row data, after=null
        assertThat(deleteResults.get(0))
                .isEqualTo("+I[delete, 4, 2, Item-2, 200, null, null, null]");
    }

    @Test
    public void testBinlogSelectStar() throws Exception {
        // Test SELECT * which returns the full binlog structure
        tEnv.executeSql(
                "CREATE TABLE star_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "star_test");

        String query = "SELECT * FROM star_test$binlog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Insert a row
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Alice")), false);

        List<String> results = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(results).hasSize(1);

        // SELECT * returns: _change_type, _log_offset, _commit_timestamp, before, after
        // before is null for INSERT, after contains the row
        assertThat(results.get(0))
                .isEqualTo("+I[insert, 0, 1970-01-01T00:00:01Z, null, +I[1, Alice]]");
    }

    @Test
    public void testBinlogWithPartitionedTable() throws Exception {
        // Create a partitioned primary key table
        tEnv.executeSql(
                "CREATE TABLE partitioned_binlog_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  region STRING NOT NULL,"
                        + "  PRIMARY KEY (id, region) NOT ENFORCED"
                        + ") PARTITIONED BY (region) WITH ('bucket.num' = '1')");

        // Insert data into different partitions using Flink SQL
        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql(
                        "INSERT INTO partitioned_binlog_test VALUES "
                                + "(1, 'Item-1', 'us'), "
                                + "(2, 'Item-2', 'eu')")
                .await();

        // Query binlog with nested field access
        String query =
                "SELECT _change_type, after.id, after.name, after.region "
                        + "FROM partitioned_binlog_test$binlog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        List<String> results = collectRowsWithTimeout(rowIter, 2, false);
        // Sort results for deterministic assertion (partitions may return in any order)
        Collections.sort(results);
        assertThat(results)
                .isEqualTo(Arrays.asList("+I[insert, 1, Item-1, us]", "+I[insert, 2, Item-2, eu]"));

        // Update a record in a specific partition
        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql("INSERT INTO partitioned_binlog_test VALUES (1, 'Item-1-Updated', 'us')")
                .await();

        List<String> updateResults = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(updateResults).hasSize(1);
        assertThat(updateResults.get(0)).isEqualTo("+I[update, 1, Item-1-Updated, us]");
    }

    @Test
    public void testBinlogScanStartupMode() throws Exception {
        // Create a primary key table with 1 bucket
        tEnv.executeSql(
                "CREATE TABLE startup_binlog_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "startup_binlog_test");

        // Write first batch
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, tablePath, Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3")), false);

        // Write second batch
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, tablePath, Arrays.asList(row(4, "v4"), row(5, "v5")), false);

        // Test scan.startup.mode='earliest' - should read all records from beginning
        String optionsEarliest = " /*+ OPTIONS('scan.startup.mode' = 'earliest') */";
        String queryEarliest =
                "SELECT _change_type, after.id, after.name FROM startup_binlog_test$binlog"
                        + optionsEarliest;
        CloseableIterator<Row> rowIterEarliest = tEnv.executeSql(queryEarliest).collect();
        List<String> earliestResults = collectRowsWithTimeout(rowIterEarliest, 5, true);
        // Sort results for deterministic assertion
        Collections.sort(earliestResults);
        assertThat(earliestResults)
                .isEqualTo(
                        Arrays.asList(
                                "+I[insert, 1, v1]",
                                "+I[insert, 2, v2]",
                                "+I[insert, 3, v3]",
                                "+I[insert, 4, v4]",
                                "+I[insert, 5, v5]"));

        // Test scan.startup.mode='timestamp' - should read from specific timestamp
        String optionsTimestamp =
                " /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '150') */";
        String queryTimestamp =
                "SELECT _change_type, after.id, after.name FROM startup_binlog_test$binlog"
                        + optionsTimestamp;
        CloseableIterator<Row> rowIterTimestamp = tEnv.executeSql(queryTimestamp).collect();
        List<String> timestampResults = collectRowsWithTimeout(rowIterTimestamp, 2, true);
        Collections.sort(timestampResults);
        assertThat(timestampResults)
                .isEqualTo(Arrays.asList("+I[insert, 4, v4]", "+I[insert, 5, v5]"));
    }
}
