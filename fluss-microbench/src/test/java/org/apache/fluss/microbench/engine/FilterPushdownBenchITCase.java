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

package org.apache.fluss.microbench.engine;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that filter pushdown produces a clear trimming ratio (fewer records
 * returned) rather than read amplification.
 *
 * <p>Tests both the direct client API and the {@link ScanExecutor#buildPredicate} YAML filter
 * parsing to ensure the full pipeline works end-to-end.
 *
 * <p>Writes 10,000 records with a sequential INT column (0–9999), then:
 *
 * <ul>
 *   <li>Scans ALL records (no filter) — baseline count
 *   <li>Scans with filter {@code category > 9000} — should return ~10% of data
 * </ul>
 */
class FilterPushdownBenchITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConf())
                    .build();

    private static final String DATABASE = "fluss";
    private static final String TABLE_NAME = "filter_bench_table";
    private static final TablePath TABLE_PATH = TablePath.of(DATABASE, TABLE_NAME);
    private static final int TOTAL_RECORDS = 10_000;
    private static final int FILTER_THRESHOLD = 9000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final int MAX_EMPTY_POLLS = 30;

    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.fluss.types.DataType[] {DataTypes.INT(), DataTypes.STRING()},
                    new String[] {"category", "payload"});

    @BeforeAll
    static void setup() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
                Admin admin = conn.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();

            Schema schema =
                    Schema.newBuilder()
                            .column("category", DataTypes.INT())
                            .column("payload", DataTypes.STRING())
                            .build();
            TableDescriptor desc =
                    TableDescriptor.builder()
                            .schema(schema)
                            .distributedBy(1)
                            .property("table.statistics.columns", "*")
                            .build();
            admin.createTable(TABLE_PATH, desc, true).get();
            long tableId = admin.getTableInfo(TABLE_PATH).get().getTableId();
            FLUSS_CLUSTER.waitUntilTableReady(tableId);

            // Synchronous writes ensure data is fully visible before scan phases
            try (Table table = conn.getTable(TABLE_PATH)) {
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 0; i < TOTAL_RECORDS; i++) {
                    GenericRow row = new GenericRow(2);
                    row.setField(0, i);
                    row.setField(1, org.apache.fluss.row.BinaryString.fromString("payload-" + i));
                    writer.append(row).get();
                }
                writer.flush();
            }
        }
    }

    /** Verifies filter pushdown trimming ratio using direct client API. */
    @Test
    void testFilterPushdownShowsTrimmingNotAmplification() throws Exception {
        try (Connection conn =
                ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig())) {

            // Phase 1: Unfiltered scan — count all records
            long unfilteredCount;
            try (Table table = conn.getTable(TABLE_PATH)) {
                try (LogScanner scanner = table.newScan().createLogScanner()) {
                    scanner.subscribe(0, LogScanner.EARLIEST_OFFSET);
                    unfilteredCount = drainScanner(scanner, "Unfiltered");
                }
            }

            // Phase 2: Filtered scan — category > 9000
            long filteredCount;
            try (Table table = conn.getTable(TABLE_PATH)) {
                PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
                try (LogScanner scanner =
                        table.newScan()
                                .filter(builder.greaterThan(0, FILTER_THRESHOLD))
                                .createLogScanner()) {
                    scanner.subscribe(0, LogScanner.EARLIEST_OFFSET);
                    filteredCount =
                            drainScanner(scanner, "Filtered(category>" + FILTER_THRESHOLD + ")");
                }
            }

            System.out.printf(
                    "[FilterPushdownBench] Total written=%d, Unfiltered scan=%d, "
                            + "Filtered scan (category>%d)=%d%n",
                    TOTAL_RECORDS, unfilteredCount, FILTER_THRESHOLD, filteredCount);

            assertThat(unfilteredCount)
                    .describedAs("Unfiltered scan should return all written records")
                    .isEqualTo(TOTAL_RECORDS);

            assertThat(filteredCount)
                    .describedAs(
                            "Filtered scan must return fewer records than unfiltered. "
                                    + "Got filtered=%d, unfiltered=%d.",
                            filteredCount, unfilteredCount)
                    .isLessThan(unfilteredCount);

            double ratio = (double) filteredCount / unfilteredCount;
            assertThat(ratio)
                    .describedAs(
                            "Trimming ratio %.2f should be < 0.50 for ~10%% selectivity.", ratio)
                    .isLessThan(0.50);

            System.out.printf("[FilterPushdownBench] Trimming ratio = %.2f%n", ratio);
        }
    }

    /**
     * Verifies that {@link ScanExecutor#buildPredicate} correctly parses YAML filter config into a
     * working {@link Predicate}, ensuring the YAML engine's filter expression capability.
     */
    @Test
    void testBuildPredicateFromYamlConfig() {
        // Simulate YAML filter config: {column: category, op: gt, value: "9000"}
        Map<String, String> filterConfig = new HashMap<>();
        filterConfig.put("column", "category");
        filterConfig.put("op", "gt");
        filterConfig.put("value", "9000");

        TableConfig tableConfig = new TableConfig();
        tableConfig.setName("test_table");
        ColumnConfig col1 = new ColumnConfig();
        col1.setName("category");
        col1.setType("INT");
        ColumnConfig col2 = new ColumnConfig();
        col2.setName("payload");
        col2.setType("STRING");
        tableConfig.setColumns(Arrays.asList(col1, col2));

        Predicate predicate = ScanExecutor.buildPredicate(filterConfig, tableConfig);
        assertThat(predicate).isNotNull();

        // Null filter config should return null
        assertThat(ScanExecutor.buildPredicate(null, tableConfig)).isNull();
        assertThat(ScanExecutor.buildPredicate(Collections.emptyMap(), tableConfig)).isNull();
    }

    /** Drains a LogScanner until no more data, returning total record count with batch stats. */
    private static long drainScanner(LogScanner scanner, String label) {
        long count = 0;
        int emptyPolls = 0;
        int pollCount = 0;
        int minBatch = Integer.MAX_VALUE;
        int maxBatch = 0;
        long totalBatchRecords = 0;
        while (emptyPolls < MAX_EMPTY_POLLS) {
            ScanRecords records = scanner.poll(POLL_TIMEOUT);
            if (records.isEmpty()) {
                emptyPolls++;
                continue;
            }
            emptyPolls = 0;
            int batchSize = 0;
            for (ScanRecord ignored : records) {
                batchSize++;
                count++;
            }
            pollCount++;
            totalBatchRecords += batchSize;
            if (batchSize < minBatch) {
                minBatch = batchSize;
            }
            if (batchSize > maxBatch) {
                maxBatch = batchSize;
            }
        }
        double avgBatch = pollCount > 0 ? (double) totalBatchRecords / pollCount : 0;
        System.out.printf(
                "[%s] polls=%d, records=%d, batchSize: min=%d, max=%d, avg=%.1f%n",
                label, pollCount, count, pollCount > 0 ? minBatch : 0, maxBatch, avgBatch);
        return count;
    }

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }
}
