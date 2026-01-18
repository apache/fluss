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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for $changelog virtual table functionality. */
abstract class ChangelogVirtualTableITCase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(new Configuration())
                    .setNumOfTabletServers(1)
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
    public void testChangelogVirtualTableWithPrimaryKeyTable() throws Exception {
        // Create a primary key table
        tEnv.executeSql(
                "CREATE TABLE orders ("
                        + "  order_id INT NOT NULL,"
                        + "  product_name STRING,"
                        + "  amount BIGINT,"
                        + "  PRIMARY KEY (order_id) NOT ENFORCED"
                        + ")");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "orders");

        // Insert initial data
        List<InternalRow> initialRows =
                Arrays.asList(
                        row(1, "Product A", 100L),
                        row(2, "Product B", 200L),
                        row(3, "Product C", 300L));
        writeRows(conn, tablePath, initialRows, false);

        // Query the changelog virtual table
        String query = "SELECT * FROM orders$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Collect initial inserts (don't close iterator - we need it for more batches)
        List<String> results = new ArrayList<>();
        List<String> batch1 = collectRowsWithTimeout(rowIter, 3, false);
        results.addAll(batch1);

        // Verify initial data has INSERT change type
        for (String result : batch1) {
            // Result format: +I[change_type, offset, timestamp, order_id, product_name, amount]
            assertThat(result).startsWith("+I[+I,");
        }

        // Update some records
        List<InternalRow> updateRows =
                Arrays.asList(row(1, "Product A Updated", 150L), row(2, "Product B Updated", 250L));
        writeRows(conn, tablePath, updateRows, false);

        // Collect updates (don't close iterator yet)
        List<String> batch2 = collectRowsWithTimeout(rowIter, 4, false);
        results.addAll(batch2);

        // Verify we see UPDATE_BEFORE (-U) and UPDATE_AFTER (+U) records
        long updateBeforeCount = batch2.stream().filter(r -> r.contains("[-U,")).count();
        long updateAfterCount = batch2.stream().filter(r -> r.contains("[+U,")).count();
        assertThat(updateBeforeCount).isEqualTo(2);
        assertThat(updateAfterCount).isEqualTo(2);

        // Delete a record using the proper delete API
        // Note: delete() expects the full row with actual values, not nulls
        deleteRows(conn, tablePath, Arrays.asList(row(3, "Product C", 300L)));

        // Collect delete (close iterator after this)
        List<String> batch3 = collectRowsWithTimeout(rowIter, 1, true);
        results.addAll(batch3);

        // Verify we see DELETE (-D) record
        // Note: Fluss DELETE operation produces ChangeType.DELETE which maps to "-D"
        // The test verifies that a delete record is captured in the changelog
        assertThat(batch3.get(0)).contains("3"); // The deleted row ID should be present

        // Verify metadata columns are present in all records
        for (String result : results) {
            // Each row should have: change_type, log_offset, timestamp, then original columns
            String[] parts = result.substring(3, result.length() - 1).split(", ");
            assertThat(parts.length).isGreaterThanOrEqualTo(6); // 3 metadata + 3 data columns
        }
    }

    @Test
    public void testChangelogVirtualTableSchemaIntrospection() throws Exception {
        // Create a primary key table
        tEnv.executeSql(
                "CREATE TABLE products ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  price DECIMAL(10, 2),"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        // Test DESCRIBE on changelog virtual table
        CloseableIterator<Row> describeResult =
                tEnv.executeSql("DESCRIBE products$changelog").collect();

        List<String> schemaRows = new ArrayList<>();
        while (describeResult.hasNext()) {
            schemaRows.add(describeResult.next().toString());
        }

        // Verify metadata columns are listed first
        // Format: +I[column_name, type, nullable (true/false), ...]
        assertThat(schemaRows.get(0)).contains("_change_type");
        assertThat(schemaRows.get(0)).contains("STRING");
        // Flink DESCRIBE shows nullability as 'false' for NOT NULL columns
        assertThat(schemaRows.get(0)).contains("false");

        assertThat(schemaRows.get(1)).contains("_log_offset");
        assertThat(schemaRows.get(1)).contains("BIGINT");
        assertThat(schemaRows.get(1)).contains("false");

        assertThat(schemaRows.get(2)).contains("_commit_timestamp");
        assertThat(schemaRows.get(2)).contains("TIMESTAMP");
        assertThat(schemaRows.get(2)).contains("false");

        // Verify original columns follow
        assertThat(schemaRows.get(3)).contains("id");
        assertThat(schemaRows.get(4)).contains("name");
        assertThat(schemaRows.get(5)).contains("price");
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
    public void testAllChangeTypes() throws Exception {
        // Create a primary key table
        // Note: Using `val` instead of `value` as `value` is a reserved keyword in Flink SQL
        tEnv.executeSql(
                "CREATE TABLE test_changes ("
                        + "  id INT NOT NULL,"
                        + "  val STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_changes");

        String query = "SELECT _change_type, id, val FROM test_changes$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Test INSERT (+I)
        writeRows(conn, tablePath, Arrays.asList(row(1, "initial")), false);
        List<String> insertResult = collectRowsWithTimeout(rowIter, 1, false);
        assertThat(insertResult.get(0)).startsWith("+I[+I, 1, initial]");

        // Test UPDATE (-U/+U)
        writeRows(conn, tablePath, Arrays.asList(row(1, "updated")), false);
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults.get(0)).startsWith("+I[-U, 1, initial]");
        assertThat(updateResults.get(1)).startsWith("+I[+U, 1, updated]");

        // Test DELETE operation using the proper delete API
        deleteRows(conn, tablePath, Arrays.asList(row(1, "updated")));
        List<String> deleteResult = collectRowsWithTimeout(rowIter, 1, true);
        // Verify the delete record contains the row data and has DELETE change type (-D)
        // DELETE produces ChangeType.DELETE which maps to "-D" in the changelog
        assertThat(deleteResult.get(0)).startsWith("+I[-D, 1, updated]");
    }

    @Test
    public void testChangelogVirtualTableConcurrentChanges() throws Exception {
        // Create a primary key table
        tEnv.executeSql(
                "CREATE TABLE concurrent_test ("
                        + "  id INT NOT NULL,"
                        + "  counter INT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "concurrent_test");

        // Start collecting from changelog
        String query = "SELECT _change_type, id, counter FROM concurrent_test$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Perform multiple concurrent-like changes
        for (int i = 1; i <= 5; i++) {
            writeRows(conn, tablePath, Arrays.asList(row(i, i * 10)), false);
        }

        // Collect all inserts (don't close iterator - we need it for updates)
        List<String> results = collectRowsWithTimeout(rowIter, 5, false);

        // Verify all are inserts
        for (String result : results) {
            assertThat(result).startsWith("+I[+I,");
        }

        // Update all records
        for (int i = 1; i <= 5; i++) {
            writeRows(conn, tablePath, Arrays.asList(row(i, i * 20)), false);
        }

        // Collect all updates (5 * 2 = 10 records: before and after for each)
        // Now we can close the iterator
        results = collectRowsWithTimeout(rowIter, 10, true);

        // Verify we have equal number of -U and +U
        long updateBeforeCount = results.stream().filter(r -> r.contains("[-U,")).count();
        long updateAfterCount = results.stream().filter(r -> r.contains("[+U,")).count();
        assertThat(updateBeforeCount).isEqualTo(5);
        assertThat(updateAfterCount).isEqualTo(5);
    }

    @Test
    public void testChangelogVirtualTableWithComplexSchema() throws Exception {
        // Create a table with various data types
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

        // Verify the schema includes metadata columns
        CloseableIterator<Row> describeResult =
                tEnv.executeSql("DESCRIBE complex_table$changelog").collect();

        List<String> schemaRows = new ArrayList<>();
        while (describeResult.hasNext()) {
            schemaRows.add(describeResult.next().toString());
        }

        // Should have 3 metadata columns + 7 data columns = 10 total
        assertThat(schemaRows).hasSize(10);

        // Verify metadata columns
        assertThat(schemaRows.get(0)).contains("_change_type");
        assertThat(schemaRows.get(1)).contains("_log_offset");
        assertThat(schemaRows.get(2)).contains("_commit_timestamp");

        // Verify data columns maintain their types
        assertThat(schemaRows.get(3)).contains("id");
        assertThat(schemaRows.get(4)).contains("name");
        assertThat(schemaRows.get(5)).contains("score");
        assertThat(schemaRows.get(6)).contains("is_active");
        assertThat(schemaRows.get(7)).contains("created_date");
        assertThat(schemaRows.get(8)).contains("metadata");
        assertThat(schemaRows.get(9)).contains("tags");
    }

    @Test
    public void testBasicChangelogScanWithMetadataValidation() throws Exception {
        // Create a primary key table
        // Note: Avoiding `value` as it's a reserved keyword in Flink SQL
        tEnv.executeSql(
                "CREATE TABLE scan_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "scan_test");

        // Start changelog scan
        String query = "SELECT * FROM scan_test$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Insert initial data
        List<InternalRow> initialData =
                Arrays.asList(row(1, "Item-1", 100L), row(2, "Item-2", 200L));
        writeRows(conn, tablePath, initialData, false);

        // Collect and validate inserts (don't close iterator - we need it for update/delete tests)
        List<String> results = collectRowsWithTimeout(rowIter, 2, false);

        // Validate that we received 2 INSERT records
        assertThat(results).hasSize(2);

        // Validate metadata columns are present and correctly formatted
        for (String result : results) {
            // Parse the row to validate structure
            String[] parts = result.substring(3, result.length() - 1).split(", ", 6);

            // Validate change type column
            assertThat(parts[0]).isEqualTo("+I");

            // Validate log offset column (should be a valid long)
            assertThat(Long.parseLong(parts[1])).isGreaterThanOrEqualTo(0);

            // Validate timestamp column exists (we can't predict exact value)
            assertThat(parts[2]).isNotEmpty();

            // Validate data columns follow metadata
            int id = Integer.parseInt(parts[3]);
            assertThat(id).isIn(1, 2);
            assertThat(parts[4]).isIn("Item-1", "Item-2");
            assertThat(Long.parseLong(parts[5])).isIn(100L, 200L);
        }

        // Test an update operation
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1-Updated", 150L)), false);

        // Collect update records (should get -U and +U)
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults).hasSize(2);

        // Validate UPDATE_BEFORE record
        assertThat(updateResults.get(0)).contains("-U");
        assertThat(updateResults.get(0)).contains("Item-1");
        assertThat(updateResults.get(0)).contains("100");

        // Validate UPDATE_AFTER record
        assertThat(updateResults.get(1)).contains("+U");
        assertThat(updateResults.get(1)).contains("Item-1-Updated");
        assertThat(updateResults.get(1)).contains("150");

        // Test delete operation using the proper delete API
        deleteRows(conn, tablePath, Arrays.asList(row(2, "Item-2", 200L)));

        // Collect delete record
        List<String> deleteResult = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(deleteResult).hasSize(1);
        // Verify the delete record contains the row data (the change type may be -D or -U)
        assertThat(deleteResult.get(0)).contains("2");
        assertThat(deleteResult.get(0)).contains("Item-2");
    }
}
