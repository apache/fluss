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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for $changelog virtual table functionality. */
abstract class ChangelogVirtualTableITCase extends AbstractTestBase {

    protected static final ManualClock CLOCK = new ManualClock();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(new Configuration())
                    .setNumOfTabletServers(1)
                    .setClock(CLOCK)
                    .build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "test_changelog_db";
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
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
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
    public void testDescribeChangelogTable() throws Exception {
        // Create a table with various data types to test complex schema
        tEnv.executeSql(
                "CREATE TABLE complex_table ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  score DOUBLE,"
                        + "  is_active BOOLEAN,"
                        + "  created_date DATE,"
                        + "  metadata MAP<STRING, STRING>,"
                        + "  tags ARRAY<STRING>,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        // Test DESCRIBE on changelog virtual table
        CloseableIterator<Row> describeResult =
                tEnv.executeSql("DESCRIBE complex_table$changelog").collect();

        List<String> schemaRows = new ArrayList<>();
        while (describeResult.hasNext()) {
            schemaRows.add(describeResult.next().toString());
        }

        // Should have 3 metadata columns + 7 data columns = 10 total
        assertThat(schemaRows).hasSize(10);

        // Verify metadata columns are listed first
        // DESCRIBE format: +I[name, type, null, key, extras, watermark]
        assertThat(schemaRows.get(0))
                .isEqualTo("+I[_change_type, STRING, false, null, null, null]");
        assertThat(schemaRows.get(1)).isEqualTo("+I[_log_offset, BIGINT, false, null, null, null]");
        assertThat(schemaRows.get(2))
                .isEqualTo("+I[_commit_timestamp, TIMESTAMP_LTZ(3), false, null, null, null]");

        // Verify data columns maintain their types
        // Note: Primary key info is not preserved in $changelog virtual table
        assertThat(schemaRows.get(3)).isEqualTo("+I[id, INT, false, null, null, null]");
        assertThat(schemaRows.get(4)).isEqualTo("+I[name, STRING, true, null, null, null]");
        assertThat(schemaRows.get(5)).isEqualTo("+I[score, DOUBLE, true, null, null, null]");
        assertThat(schemaRows.get(6)).isEqualTo("+I[is_active, BOOLEAN, true, null, null, null]");
        assertThat(schemaRows.get(7)).isEqualTo("+I[created_date, DATE, true, null, null, null]");
        assertThat(schemaRows.get(8))
                .isEqualTo("+I[metadata, MAP<STRING NOT NULL, STRING>, true, null, null, null]");
        assertThat(schemaRows.get(9)).isEqualTo("+I[tags, ARRAY<STRING>, true, null, null, null]");

        // Test SHOW CREATE TABLE on changelog virtual table
        CloseableIterator<Row> showCreateResult =
                tEnv.executeSql("SHOW CREATE TABLE complex_table$changelog").collect();

        StringBuilder createTableStatement = new StringBuilder();
        while (showCreateResult.hasNext()) {
            createTableStatement.append(showCreateResult.next().toString());
        }

        String createStatement = createTableStatement.toString();
        // Verify metadata columns are included in the CREATE TABLE statement
        assertThat(createStatement)
                .contains(
                        "CREATE TABLE `testcatalog`.`test_changelog_db`.`complex_table$changelog` (\n"
                                + "  `_change_type` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `_log_offset` BIGINT NOT NULL,\n"
                                + "  `_commit_timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,\n"
                                + "  `id` INT NOT NULL,\n"
                                + "  `name` VARCHAR(2147483647),\n"
                                + "  `score` DOUBLE,\n"
                                + "  `is_active` BOOLEAN,\n"
                                + "  `created_date` DATE,\n"
                                + "  `metadata` MAP<VARCHAR(2147483647) NOT NULL, VARCHAR(2147483647)>,\n"
                                + "  `tags` ARRAY<VARCHAR(2147483647)>\n"
                                // with options contains random properties, skip checking
                                + ")");
    }

    @Test
    public void testChangelogVirtualTableWithNonPrimaryKeyTable() {
        // Create a non-primary key table (log table)
        tEnv.executeSql(
                "CREATE TABLE events ("
                        + "  event_id INT,"
                        + "  event_type STRING,"
                        + "  event_time TIMESTAMP"
                        + ")");

        // Attempt to query changelog virtual table should fail
        String query = "SELECT * FROM events$changelog";

        // The error message is wrapped in a CatalogException, so we check for the root cause
        assertThatThrownBy(() -> tEnv.executeSql(query).await())
                .hasRootCauseMessage(
                        "Virtual $changelog tables are only supported for primary key tables. "
                                + "Table test_changelog_db.events does not have a primary key.");
    }

    @Test
    public void testProjectionOnChangelogTable() throws Exception {
        // Create a primary key table with 1 bucket and extra columns to test projection
        tEnv.executeSql(
                "CREATE TABLE projection_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  description STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "projection_test");

        // Select only _change_type, id, and name (skip amount and description)
        String query = "SELECT _change_type, id, name FROM projection_test$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Test INSERT
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1", 100L, "Desc-1")), false);
        List<String> insertResult = collectRowsWithTimeout(rowIter, 1, false);
        assertThat(insertResult.get(0)).isEqualTo("+I[+I, 1, Item-1]");

        // Test UPDATE
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(
                conn,
                tablePath,
                Arrays.asList(row(1, "Item-1-Updated", 150L, "Desc-1-Updated")),
                false);
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults.get(0)).isEqualTo("+I[-U, 1, Item-1]");
        assertThat(updateResults.get(1)).isEqualTo("+I[+U, 1, Item-1-Updated]");

        // Test DELETE
        CLOCK.advanceTime(Duration.ofMillis(100));
        deleteRows(
                conn, tablePath, Arrays.asList(row(1, "Item-1-Updated", 150L, "Desc-1-Updated")));
        List<String> deleteResult = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(deleteResult.get(0)).isEqualTo("+I[-D, 1, Item-1-Updated]");
    }

    @Test
    public void testChangelogScanWithAllChangeTypes() throws Exception {
        // Create a primary key table with 1 bucket for consistent log_offset numbers
        tEnv.executeSql(
                "CREATE TABLE scan_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "scan_test");

        // Start changelog scan
        String query = "SELECT * FROM scan_test$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Insert initial data with controlled timestamp
        CLOCK.advanceTime(Duration.ofMillis(1000));
        List<InternalRow> initialData =
                Arrays.asList(row(1, "Item-1", 100L), row(2, "Item-2", 200L));
        writeRows(conn, tablePath, initialData, false);

        // Collect and validate inserts - with 1 bucket, offsets are predictable (0, 1)
        List<String> results = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(results).hasSize(2);

        // With ManualClock and 1 bucket, we can assert exact row values
        // Format: +I[_change_type, _log_offset, _commit_timestamp, id, name, amount]
        assertThat(results.get(0)).isEqualTo("+I[+I, 0, 1970-01-01T00:00:01Z, 1, Item-1, 100]");
        assertThat(results.get(1)).isEqualTo("+I[+I, 1, 1970-01-01T00:00:01Z, 2, Item-2, 200]");

        // Test UPDATE operation with new timestamp
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1-Updated", 150L)), false);

        // Collect update records (should get -U and +U)
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults).hasSize(2);
        assertThat(updateResults.get(0))
                .isEqualTo("+I[-U, 2, 1970-01-01T00:00:02Z, 1, Item-1, 100]");
        assertThat(updateResults.get(1))
                .isEqualTo("+I[+U, 3, 1970-01-01T00:00:02Z, 1, Item-1-Updated, 150]");

        // Test DELETE operation with new timestamp
        CLOCK.advanceTime(Duration.ofMillis(1000));
        deleteRows(conn, tablePath, Arrays.asList(row(2, "Item-2", 200L)));

        // Collect delete record
        List<String> deleteResult = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(deleteResult).hasSize(1);
        assertThat(deleteResult.get(0))
                .isEqualTo("+I[-D, 4, 1970-01-01T00:00:03Z, 2, Item-2, 200]");
    }

    @Test
    public void testChangelogWithScanStartupMode() throws Exception {
        // Create a primary key table with 1 bucket for consistent log_offset numbers
        tEnv.executeSql(
                "CREATE TABLE startup_mode_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "startup_mode_test");

        // Write first batch of data
        CLOCK.advanceTime(Duration.ofMillis(100));
        List<InternalRow> batch1 = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(conn, tablePath, batch1, false);

        // Write second batch of data
        CLOCK.advanceTime(Duration.ofMillis(100));
        List<InternalRow> batch2 = Arrays.asList(row(4, "v4"), row(5, "v5"));
        writeRows(conn, tablePath, batch2, false);

        // 1. Test scan.startup.mode='earliest' - should read all records from beginning
        String optionsEarliest = " /*+ OPTIONS('scan.startup.mode' = 'earliest') */";
        String queryEarliest =
                "SELECT _change_type, id, name FROM startup_mode_test$changelog" + optionsEarliest;
        CloseableIterator<Row> rowIterEarliest = tEnv.executeSql(queryEarliest).collect();
        List<String> earliestResults = collectRowsWithTimeout(rowIterEarliest, 5, true);
        assertThat(earliestResults).hasSize(5);
        // All should be INSERT change types
        for (String result : earliestResults) {
            assertThat(result).startsWith("+I[+I,");
        }

        // 2. Test scan.startup.mode='timestamp' - should read records from specific timestamp
        // read between batch1 and batch2
        String optionsTimestamp =
                " /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '150') */";
        String queryTimestamp = "SELECT * FROM startup_mode_test$changelog " + optionsTimestamp;
        CloseableIterator<Row> rowIterTimestamp = tEnv.executeSql(queryTimestamp).collect();
        List<String> timestampResults = collectRowsWithTimeout(rowIterTimestamp, 2, true);
        assertThat(timestampResults).hasSize(2);
        // Should contain records from batch2 only
        assertThat(timestampResults)
                .containsExactlyInAnyOrder(
                        "+I[+I, 3, 1970-01-01T00:00:00.200Z, 4, v4]",
                        "+I[+I, 4, 1970-01-01T00:00:00.200Z, 5, v5]");
    }

    @Test
    public void testChangelogWithPartitionedTable() throws Exception {
        // Create a partitioned primary key table with 1 bucket per partition
        tEnv.executeSql(
                "CREATE TABLE partitioned_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  region STRING NOT NULL,"
                        + "  PRIMARY KEY (id, region) NOT ENFORCED"
                        + ") PARTITIONED BY (region) WITH ('bucket.num' = '1')");

        // Insert data into different partitions using Flink SQL
        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql(
                        "INSERT INTO partitioned_test VALUES "
                                + "(1, 'Item-1', 'us'), "
                                + "(2, 'Item-2', 'us'), "
                                + "(3, 'Item-3', 'eu')")
                .await();

        // Query the changelog virtual table for all partitions
        String query = "SELECT _change_type, id, name, region FROM partitioned_test$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Collect initial inserts
        List<String> results = collectRowsWithTimeout(rowIter, 3, false);
        assertThat(results)
                .containsExactlyInAnyOrder(
                        "+I[+I, 1, Item-1, us]", "+I[+I, 2, Item-2, us]", "+I[+I, 3, Item-3, eu]");

        // Update a record in a specific partition
        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql("INSERT INTO partitioned_test VALUES (1, 'Item-1-Updated', 'us')").await();
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults)
                .containsExactly("+I[-U, 1, Item-1, us]", "+I[+U, 1, Item-1-Updated, us]");

        rowIter.close();
    }
}
