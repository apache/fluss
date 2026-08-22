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
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectBatchRows;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for $changelog virtual table functionality. */
abstract class ChangelogVirtualTableITCase {

    protected static final ManualClock CLOCK = new ManualClock();
    @TempDir public static File checkpointDir;
    @TempDir public static File savepointDir;

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
    protected Connection conn;
    protected Admin admin;
    protected Configuration clientConf;
    protected MiniClusterWithClientResource cluster;

    @BeforeEach
    protected void beforeEach() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getFileBasedCheckpointsConfig(savepointDir))
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        cluster.before();

        // Initialize Flink environment
        tEnv = initTableEnvironment(null);
        // reset clock before each test
        CLOCK.advanceTime(-CLOCK.milliseconds(), TimeUnit.MILLISECONDS);
    }

    @AfterEach
    protected void afterEach() throws Exception {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    // init table environment from savepointPath
    private StreamTableEnvironment initTableEnvironment(@Nullable String savepointPath) {
        return initTableEnvironment(savepointPath, EnvironmentSettings.inStreamingMode());
    }

    private StreamTableEnvironment initBatchTableEnvironment() {
        return initTableEnvironment(null, EnvironmentSettings.inBatchMode());
    }

    private StreamTableEnvironment initTableEnvironment(
            @Nullable String savepointPath, EnvironmentSettings environmentSettings) {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
        }
        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        execEnv.setParallelism(1);
        execEnv.enableCheckpointing(1000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, environmentSettings);
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("create database if not exists " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);

        return tEnv;
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
    @MultiVersionTest
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
    public void testBatchReadChangelogTable() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE batch_changelog_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "batch_changelog_test");
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Alice"), row(2, "Bob")), false);
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Alice-updated")), false);
        CLOCK.advanceTime(Duration.ofMillis(1000));
        deleteRows(conn, tablePath, Arrays.asList(row(2, "Bob")));

        tEnv = initBatchTableEnvironment();

        // The default FULL startup mode maps to the earliest log offset for virtual tables. The
        // batch source captures the latest offsets as stopping offsets and terminates after
        // replaying the bounded changelog.
        List<String> allChanges =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, _log_offset, id, name "
                                                + "FROM batch_changelog_test$changelog "
                                                + "ORDER BY _log_offset")
                                .collect());
        assertThat(allChanges)
                .containsExactly(
                        "+I[insert, 0, 1, Alice]",
                        "+I[insert, 1, 2, Bob]",
                        "+I[update_before, 2, 1, Alice]",
                        "+I[update_after, 3, 1, Alice-updated]",
                        "+I[delete, 4, 2, Bob]");

        List<String> primaryKeyFilteredAndLimited =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, _log_offset, id, name "
                                                + "FROM batch_changelog_test$changelog "
                                                + "WHERE id = 1 "
                                                + "ORDER BY _log_offset LIMIT 2")
                                .collect());
        assertThat(primaryKeyFilteredAndLimited)
                .containsExactly("+I[insert, 0, 1, Alice]", "+I[update_before, 2, 1, Alice]");

        // Filters on _commit_timestamp are covered by
        // testBatchReadChangelogTableWithCommitTimestampFilter, which is disabled on Flink 1.18
        // (FLINK-35318).
        List<String> metadataFiltered =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, _log_offset, id "
                                                + "FROM batch_changelog_test$changelog "
                                                + "WHERE _change_type = 'delete' "
                                                + "AND _log_offset = 4")
                                .collect());
        assertThat(metadataFiltered).containsExactly("+I[delete, 4, 2]");

        List<String> timestampStartup =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, _log_offset "
                                                + "FROM batch_changelog_test$changelog "
                                                + "/*+ OPTIONS("
                                                + "'scan.startup.mode' = 'timestamp', "
                                                + "'scan.startup.timestamp' = '1500') */ "
                                                + "ORDER BY _log_offset")
                                .collect());
        assertThat(timestampStartup)
                .containsExactly("+I[update_before, 2]", "+I[update_after, 3]", "+I[delete, 4]");

        tEnv.executeSql(
                "CREATE TABLE partitioned_batch_changelog_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  region STRING NOT NULL,"
                        + "  PRIMARY KEY (id, region) NOT ENFORCED"
                        + ") PARTITIONED BY (region) WITH ('bucket.num' = '1')");
        CLOCK.advanceTime(Duration.ofMillis(1000));
        tEnv.executeSql(
                        "INSERT INTO partitioned_batch_changelog_test VALUES "
                                + "(1, 'Item-1', 'us'), "
                                + "(2, 'Item-2', 'us'), "
                                + "(3, 'Item-3', 'eu')")
                .await();

        List<String> partitionedChanges =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, id, name, region "
                                                + "FROM partitioned_batch_changelog_test$changelog "
                                                + "ORDER BY region, id")
                                .collect());
        assertThat(partitionedChanges)
                .containsExactly(
                        "+I[insert, 3, Item-3, eu]",
                        "+I[insert, 1, Item-1, us]",
                        "+I[insert, 2, Item-2, us]");
    }

    /**
     * Bounded reads with a predicate on the {@code _commit_timestamp} metadata column.
     *
     * <p>This is kept apart from {@code testBatchReadChangelogTable} because Flink 1.18 rebuilds
     * the remaining TIMESTAMP_LTZ filter using the session time zone instead of UTC after {@code
     * applyFilters()} (FLINK-35318, fixed in Flink 1.19.2 and 1.20.0), which shifts the literal and
     * drops all rows in non-UTC environments. The Flink 1.18 subclass therefore disables this test
     * only, keeping the rest of the batch coverage.
     */
    @Test
    public void testBatchReadChangelogTableWithCommitTimestampFilter() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE batch_changelog_ts_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "batch_changelog_ts_test");
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Alice"), row(2, "Bob")), false);
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Alice-updated")), false);
        CLOCK.advanceTime(Duration.ofMillis(1000));
        deleteRows(conn, tablePath, Arrays.asList(row(2, "Bob")));

        tEnv = initBatchTableEnvironment();

        List<String> timestampRangeFiltered =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, _log_offset, id, name "
                                                + "FROM batch_changelog_ts_test$changelog "
                                                + "WHERE _commit_timestamp >= TO_TIMESTAMP_LTZ(2000, 3) "
                                                + "AND _commit_timestamp < TO_TIMESTAMP_LTZ(3000, 3) "
                                                + "ORDER BY _log_offset")
                                .collect());
        assertThat(timestampRangeFiltered)
                .containsExactly(
                        "+I[update_before, 2, 1, Alice]", "+I[update_after, 3, 1, Alice-updated]");

        List<String> metadataFiltered =
                collectBatchRows(
                        tEnv.executeSql(
                                        "SELECT _change_type, _log_offset, id "
                                                + "FROM batch_changelog_ts_test$changelog "
                                                + "WHERE _change_type = 'delete' "
                                                + "AND _log_offset = 4 "
                                                + "AND _commit_timestamp = TO_TIMESTAMP_LTZ(3000, 3)")
                                .collect());
        assertThat(metadataFiltered).containsExactly("+I[delete, 4, 2]");
    }

    @Test
    public void testChangelogVirtualTableWithLogTable() throws Exception {
        // Create a log table (no primary key) with 1 bucket for predictable offsets
        tEnv.executeSql(
                "CREATE TABLE events ("
                        + "  event_id INT,"
                        + "  event_type STRING"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "events");

        // Query the changelog virtual table
        String query = "SELECT * FROM events$changelog";
        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        // Insert data into log table - log tables only have APPEND_ONLY (+A) change type
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "click"), row(2, "view")), true);

        // Collect and validate - log table changelog should have +A change type
        List<String> results = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(results).hasSize(2);

        // Format: +I[_change_type, _log_offset, _commit_timestamp, event_id, event_type]
        // Log tables use insert (append-only) change type
        assertThat(results.get(0)).isEqualTo("+I[insert, 0, 1970-01-01T00:00:01Z, 1, click]");
        assertThat(results.get(1)).isEqualTo("+I[insert, 1, 1970-01-01T00:00:01Z, 2, view]");

        // Insert more data with new timestamp
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(3, "purchase")), true);

        List<String> moreResults = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(moreResults.get(0))
                .isEqualTo("+I[insert, 2, 1970-01-01T00:00:02Z, 3, purchase]");
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
        assertThat(insertResult.get(0)).isEqualTo("+I[insert, 1, Item-1]");

        // Test UPDATE
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(
                conn,
                tablePath,
                Arrays.asList(row(1, "Item-1-Updated", 150L, "Desc-1-Updated")),
                false);
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults.get(0)).isEqualTo("+I[update_before, 1, Item-1]");
        assertThat(updateResults.get(1)).isEqualTo("+I[update_after, 1, Item-1-Updated]");

        // Test DELETE
        CLOCK.advanceTime(Duration.ofMillis(100));
        deleteRows(
                conn, tablePath, Arrays.asList(row(1, "Item-1-Updated", 150L, "Desc-1-Updated")));
        List<String> deleteResult = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(deleteResult.get(0)).isEqualTo("+I[delete, 1, Item-1-Updated]");
    }

    @Test
    public void testReorderedProjectionOnChangelogTable() throws Exception {
        // Reordered projection: data columns out of their original order with a metadata column
        // in between. This exercises data projection plus FlinkSource output projection end to end
        // through the real planner.
        tEnv.executeSql(
                "CREATE TABLE reordered_projection_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "reordered_projection_test");

        String query = "SELECT amount, _change_type, id FROM reordered_projection_test$changelog";
        // The planner pushes the reordered projection into the scan as-is (no Calc reorder), so
        // the source receives the out-of-order indices end to end.
        assertThat(tEnv.explainSql(query)).contains("project=[amount, _change_type, id]");

        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1", 100L)), false);
        List<String> insertResult = collectRowsWithTimeout(rowIter, 1, false);
        assertThat(insertResult).containsExactly("+I[100, insert, 1]");

        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1", 250L)), false);
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, true);
        assertThat(updateResults)
                .containsExactly("+I[100, update_before, 1]", "+I[250, update_after, 1]");
    }

    @Test
    public void testChangelogDataColumnFilterAndProjectionPushdown() throws Exception {
        // Statistics columns are only supported on log tables (not PK tables), so use a log table.
        // A log table's changelog is append-only, which is sufficient to exercise pushdown.
        tEnv.executeSql(
                "CREATE TABLE data_filter_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  amount BIGINT"
                        + ") WITH ('bucket.num' = '1', 'table.statistics.columns' = 'amount')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "data_filter_test");

        String query =
                "SELECT _change_type, id, amount FROM data_filter_test$changelog WHERE amount > 150";

        // Projection and the data-column filter should be pushed into the source scan. Assert the
        // exact pushed projection and predicate, because "filter=[" would also match an empty
        // "filter=[]" digest when nothing is pushed down.
        String plan = tEnv.explainSql(query);
        assertThat(plan).contains("project=[_change_type, id, amount]");
        assertThat(plan).contains("filter=[>(amount, 150)]");
        // The filter is still retained in the Calc operator as a safety net (FLINK-38635).
        assertThat(plan).contains("where=");

        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(
                conn,
                tablePath,
                Arrays.asList(
                        row(1, "Item-1", 100L), row(2, "Item-2", 200L), row(3, "Item-3", 300L)),
                true);

        // Only amount > 150 rows pass the filter (safety net + pushdown produce identical results).
        List<String> results = collectRowsWithTimeout(rowIter, 2, true);
        assertThat(results).containsExactly("+I[insert, 2, 200]", "+I[insert, 3, 300]");
        rowIter.close();
    }

    @Test
    public void testChangelogPartitionFilterPushdown() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE partition_filter_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  region STRING NOT NULL,"
                        + "  PRIMARY KEY (id, region) NOT ENFORCED"
                        + ") PARTITIONED BY (region) WITH ('bucket.num' = '1')");

        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql(
                        "INSERT INTO partition_filter_test VALUES "
                                + "(1, 'Item-1', 'us'), "
                                + "(2, 'Item-2', 'us'), "
                                + "(3, 'Item-3', 'eu')")
                .await();

        String query =
                "SELECT _change_type, id, name, region FROM partition_filter_test$changelog "
                        + "WHERE region = 'us'";

        // The partition-key filter should be pushed into the source scan (assert the exact pushed
        // predicate), and retained in the Calc operator as a safety net (FLINK-38635).
        String plan = tEnv.explainSql(query);
        assertThat(plan).contains("filter=[=(region, _UTF-16LE'us'");
        assertThat(plan).contains("where=");

        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();
        List<String> results = collectRowsWithTimeout(rowIter, 2, true);
        assertThat(results)
                .containsExactly("+I[insert, 1, Item-1, us]", "+I[insert, 2, Item-2, us]");
        rowIter.close();
    }

    @Test
    public void testChangelogMetadataOnlyProjection() throws Exception {
        // Projecting only metadata columns leaves no data column to scan. The source normalizes the
        // scan projection to null (full data row scan) to avoid a zero-column scan that produces no
        // records, then emits only the metadata columns. Verify this runtime path works end to end.
        tEnv.executeSql(
                "CREATE TABLE metadata_only_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING"
                        + ") WITH ('bucket.num' = '1')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "metadata_only_test");

        String query = "SELECT _change_type, _log_offset FROM metadata_only_test$changelog";
        assertThat(tEnv.explainSql(query)).contains("project=[_change_type, _log_offset]");

        CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1"), row(2, "Item-2")), true);

        List<String> results = collectRowsWithTimeout(rowIter, 2, true);
        assertThat(results).containsExactly("+I[insert, 0]", "+I[insert, 1]");
        rowIter.close();
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
        assertThat(results.get(0)).isEqualTo("+I[insert, 0, 1970-01-01T00:00:01Z, 1, Item-1, 100]");
        assertThat(results.get(1)).isEqualTo("+I[insert, 1, 1970-01-01T00:00:01Z, 2, Item-2, 200]");

        // Test UPDATE operation with new timestamp
        CLOCK.advanceTime(Duration.ofMillis(1000));
        writeRows(conn, tablePath, Arrays.asList(row(1, "Item-1-Updated", 150L)), false);

        // Collect update records (should get update_before and update_after)
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults).hasSize(2);
        assertThat(updateResults.get(0))
                .isEqualTo("+I[update_before, 2, 1970-01-01T00:00:02Z, 1, Item-1, 100]");
        assertThat(updateResults.get(1))
                .isEqualTo("+I[update_after, 3, 1970-01-01T00:00:02Z, 1, Item-1-Updated, 150]");

        // Test DELETE operation with new timestamp
        CLOCK.advanceTime(Duration.ofMillis(1000));
        deleteRows(conn, tablePath, Arrays.asList(row(2, "Item-2", 200L)));

        // Collect delete record
        List<String> deleteResult = collectRowsWithTimeout(rowIter, 1, true);
        assertThat(deleteResult).hasSize(1);
        assertThat(deleteResult.get(0))
                .isEqualTo("+I[delete, 4, 1970-01-01T00:00:03Z, 2, Item-2, 200]");
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
            assertThat(result).startsWith("+I[insert,");
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
                .containsExactly(
                        "+I[insert, 3, 1970-01-01T00:00:00.200Z, 4, v4]",
                        "+I[insert, 4, 1970-01-01T00:00:00.200Z, 5, v5]");
    }

    @Test
    public void testChangelogWithLatestScanStartupMode() throws Exception {
        // Create source and result tables
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ('bucket.num' = '1')");

        tEnv.executeSql(
                "CREATE TABLE result_table ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")");

        TablePath sourcePath = TablePath.of(DEFAULT_DB, "source_table");

        // Write first batch of data.
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, sourcePath, Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3")), false);

        // Pre-populate the result table with some existing records.
        String optionsLatest = "/*+ OPTIONS('scan.startup.mode' = 'latest') */";
        TableResult insertResult =
                tEnv.executeSql(
                        "INSERT INTO result_table SELECT id, name FROM source_table$changelog "
                                + optionsLatest);

        // Wait for at least one checkpoint to complete before creating savepoint
        waitForCheckpoint(insertResult.getJobClient().get().getJobID());

        CloseableIterator<Row> rowIterLatest =
                tEnv.executeSql("SELECT * FROM result_table").collect();

        // now, stop the job with save point
        String savepointPath =
                insertResult
                        .getJobClient()
                        .get()
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get(60, TimeUnit.SECONDS);

        // Init env with savepoint Path
        tEnv = initTableEnvironment(savepointPath);
        insertResult =
                tEnv.executeSql(
                        "INSERT INTO result_table SELECT id, name FROM source_table$changelog "
                                + optionsLatest);

        // Write the third batch of data, ensure to get the lastest value
        CLOCK.advanceTime(Duration.ofMillis(100));
        writeRows(conn, sourcePath, Arrays.asList(row(4, "v4"), row(5, "v5")), false);

        // Should contain records from the third batch only
        List<String> latestResults = collectRowsWithTimeout(rowIterLatest, 2, true);
        assertThat(latestResults).hasSize(2);
        assertThat(latestResults).containsExactly("+I[4, v4]", "+I[5, v5]");

        // Cleanup job
        insertResult.getJobClient().get().cancel().get();
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

        // Collect initial inserts. Records from different partitions may arrive in any order,
        // so the assertion must be order-insensitive.
        List<String> results = collectRowsWithTimeout(rowIter, 3, false);
        assertThat(results)
                .containsExactlyInAnyOrder(
                        "+I[insert, 1, Item-1, us]",
                        "+I[insert, 2, Item-2, us]",
                        "+I[insert, 3, Item-3, eu]");

        // Update a record in a specific partition
        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql("INSERT INTO partitioned_test VALUES (1, 'Item-1-Updated', 'us')").await();
        List<String> updateResults = collectRowsWithTimeout(rowIter, 2, false);
        assertThat(updateResults)
                .containsExactly(
                        "+I[update_before, 1, Item-1, us]",
                        "+I[update_after, 1, Item-1-Updated, us]");

        rowIter.close();
    }

    @Test
    public void testShowPartitionsOnChangelogVirtualTable() throws Exception {
        // Create a partitioned primary key table
        tEnv.executeSql(
                "CREATE TABLE partitioned_show_test ("
                        + "  id INT NOT NULL,"
                        + "  name STRING,"
                        + "  region STRING NOT NULL,"
                        + "  PRIMARY KEY (id, region) NOT ENFORCED"
                        + ") PARTITIONED BY (region) WITH ('bucket.num' = '1')");

        // Insert data to create partitions
        CLOCK.advanceTime(Duration.ofMillis(100));
        tEnv.executeSql(
                        "INSERT INTO partitioned_show_test VALUES "
                                + "(1, 'Item-1', 'us'), "
                                + "(2, 'Item-2', 'eu')")
                .await();

        // SHOW PARTITIONS on base table — should work
        List<String> basePartitions = new ArrayList<>();
        try (CloseableIterator<Row> iter =
                tEnv.executeSql("SHOW PARTITIONS partitioned_show_test").collect()) {
            while (iter.hasNext()) {
                basePartitions.add(iter.next().toString());
            }
        }
        assertThat(basePartitions).containsExactlyInAnyOrder("+I[region=us]", "+I[region=eu]");

        // SHOW PARTITIONS on $changelog virtual table — should return same partitions
        // Without the fix, this throws TableNotExistException
        List<String> changelogPartitions = new ArrayList<>();
        try (CloseableIterator<Row> iter =
                tEnv.executeSql("SHOW PARTITIONS partitioned_show_test$changelog").collect()) {
            while (iter.hasNext()) {
                changelogPartitions.add(iter.next().toString());
            }
        }
        assertThat(changelogPartitions).containsExactlyInAnyOrder("+I[region=us]", "+I[region=eu]");
    }

    private static org.apache.flink.configuration.Configuration getFileBasedCheckpointsConfig(
            File savepointDir) {
        return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
    }

    private static org.apache.flink.configuration.Configuration getFileBasedCheckpointsConfig(
            final String savepointDir) {
        final org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        return config;
    }

    protected static void waitForCheckpoint(JobID jobId) {
        String jobIdStr = jobId.toHexString();
        waitUntil(
                () -> {
                    File jobCheckpointDir = new File(checkpointDir, jobIdStr);
                    if (!jobCheckpointDir.exists()) {
                        return false;
                    }
                    File[] checkpoints =
                            jobCheckpointDir.listFiles(
                                    f -> f.isDirectory() && f.getName().startsWith("chk-"));
                    return checkpoints != null && checkpoints.length > 0;
                },
                Duration.ofSeconds(60),
                "Timeout waiting for checkpoint for job " + jobIdStr);
    }
}
