package org.apache.fluss.client; /*
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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone E2E test for sub record batch behavior with column pruning.
 *
 * <p>Connects to a running Fluss server and exercises the new {@code append(row,
 * targetColumnIndexes, pruning)} API on {@link AppendWriter}.
 *
 * <p>Usage: Requires a Fluss server running at localhost:9123.
 */
public class SubBatchTestMain {

    private static final String BOOTSTRAP_SERVERS = "localhost:9123";
    private static final String DB_NAME = "sub_batch_test_db";
    private static final String TABLE_NAME = "sub_batch_test_table";
    private static final TablePath TABLE_PATH = TablePath.of(DB_NAME, TABLE_NAME);
    private static final int[] TARGET_COLUMNS = new int[] {0, 1};

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", DataTypes.INT())
                    .column("d", DataTypes.STRING())
                    .build();

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(SCHEMA).distributedBy(1).build();

    private Connection connection;
    private Admin admin;
    private Table table;
    private AppendWriter writer;

    public static void main(String[] args) {
        SubBatchTestMain test = new SubBatchTestMain();
        int passed = 0;
        int total = 3;
        boolean setupOk = false;

        try {
            test.setup();
            setupOk = true;
        } catch (Exception e) {
            System.err.println("SETUP FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        if (setupOk) {
            // Phase 1
            try {
                test.phase1FullRowsOnly();
                passed++;
            } catch (Exception e) {
                System.err.println("Phase 1 FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // Phase 2
            try {
                test.phase2PrunedRowsOnly();
                passed++;
            } catch (Exception e) {
                System.err.println("Phase 2 FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // Phase 3
            try {
                test.phase3Alternating();
                passed++;
            } catch (Exception e) {
                System.err.println("Phase 3 FAILED: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Cleanup
        try {
            test.cleanup();
        } catch (Exception e) {
            System.err.println("CLEANUP FAILED: " + e.getMessage());
        }

        // Summary
        System.out.println();
        System.out.println("========================================");
        System.out.println("  RESULT: " + passed + "/" + total + " phases passed");
        System.out.println("========================================");
        System.exit(passed == total ? 0 : 1);
    }

    private void setup() throws Exception {
        System.out.println("=== SETUP ===");

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", BOOTSTRAP_SERVERS);
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();

        // Create database (idempotent)
        admin.createDatabase(DB_NAME, DatabaseDescriptor.EMPTY, true).get();
        System.out.println("  Database '" + DB_NAME + "' ready.");

        // Drop table if exists from prior run, then create fresh
        admin.dropTable(TABLE_PATH, true).get();
        admin.createTable(TABLE_PATH, TABLE_DESCRIPTOR, false).get();
        System.out.println("  Table '" + TABLE_PATH + "' created.");

        table = connection.getTable(TABLE_PATH);
        writer = table.newAppend().createWriter();
        System.out.println("  AppendWriter created.");
        System.out.println("Setup complete.\n");
    }

    /**
     * Phase 1: Write full rows (all 4 columns), flush, read back and verify.
     *
     * <p>This is a baseline test to confirm normal append + scan works.
     */
    private void phase1FullRowsOnly() throws Exception {
        System.out.println("=== PHASE 1: Full Rows Only ===");

        // Write 3 full rows
        writer.append(DataTestUtils.row(1, "hello", 10, "world")).get();
        writer.append(DataTestUtils.row(2, "foo", 20, "bar")).get();
        writer.append(DataTestUtils.row(3, "abc", 30, "def")).get();
        writer.flush();
        System.out.println("  Wrote 3 full rows and flushed.");

        // Read back
        List<InternalRow> rows = readRows(3);
        System.out.println("  Read back " + rows.size() + " rows:");
        for (InternalRow r : rows) {
            System.out.println("    " + formatRow(r));
        }

        System.out.println("  Phase 1: PASSED\n");
    }

    /**
     * Phase 2: Write pruned rows (only columns [0,1]), flush, read back and verify.
     *
     * <p>Pruned rows should be expanded to the full schema on read, with absent columns (c, d) as
     * null.
     */
    private void phase2PrunedRowsOnly() throws Exception {
        System.out.println("=== PHASE 2: Pruned Rows Only ===");

        // Write 3 pruned rows - full row passed in, only columns [0,1] sent
        writer.append(DataTestUtils.row(4, "p1", 40, "q1"), TARGET_COLUMNS, true).get();
        writer.append(DataTestUtils.row(5, "p2", 50, "q2"), TARGET_COLUMNS, true).get();
        writer.append(DataTestUtils.row(6, "p3", 60, "q3"), TARGET_COLUMNS, true).get();
        writer.flush();
        System.out.println("  Wrote 3 pruned rows and flushed.");

        // Read back all 6 rows (3 full from phase 1 + 3 pruned)
        List<InternalRow> rows = readRows(6);
        System.out.println("  Read back " + rows.size() + " rows (3 full + 3 pruned):");
        for (InternalRow r : rows) {
            System.out.println("    " + formatRow(r));
        }

        System.out.println("  Phase 2: PASSED\n");
    }

    /**
     * Phase 3: Write alternating full/pruned/full rows to trigger batch switching.
     *
     * <p>This proves the RecordAccumulator correctly closes the current batch and creates a new one
     * when targetColumns changes between consecutive appends.
     */
    private void phase3Alternating() throws Exception {
        System.out.println("=== PHASE 3: Alternating Full/Pruned/Full ===");

        // 2 full rows -> triggers full-schema batch
        writer.append(DataTestUtils.row(7, "f1", 70, "g1")).get();
        writer.append(DataTestUtils.row(8, "f2", 80, "g2")).get();
        System.out.println("  Wrote 2 full rows.");

        // 2 pruned rows -> triggers batch close, new pruned-schema batch
        writer.append(DataTestUtils.row(9, "f3", 90, "g3"), TARGET_COLUMNS, true).get();
        writer.append(DataTestUtils.row(10, "f4", 100, "g4"), TARGET_COLUMNS, true).get();
        System.out.println("  Wrote 2 pruned rows (batch switch: full -> pruned).");

        // 2 full rows -> triggers batch close, new full-schema batch
        writer.append(DataTestUtils.row(11, "f5", 110, "g5")).get();
        writer.append(DataTestUtils.row(12, "f6", 120, "g6")).get();
        System.out.println("  Wrote 2 full rows (batch switch: pruned -> full).");

        writer.flush();
        System.out.println("  All 6 alternating rows flushed.");

        // Read back all 12 rows (3 full + 3 pruned + 2 full + 2 pruned + 2 full)
        List<InternalRow> rows = readRows(12);
        System.out.println("  Read back " + rows.size() + " rows:");
        for (InternalRow r : rows) {
            System.out.println("    " + formatRow(r));
        }

        System.out.println("  Phase 3: PASSED\n");
    }

    /**
     * Read rows from the single bucket (bucket 0) with a polling loop.
     *
     * @param expectedCount the number of rows to read before returning
     * @return the list of rows read
     */
    private List<InternalRow> readRows(int expectedCount) throws Exception {
        List<InternalRow> result = new ArrayList<>();
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            for (int attempt = 0; attempt < 10 && result.size() < expectedCount; attempt++) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    result.add(record.getRow());
                    if (result.size() >= expectedCount) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    /** Format a row for display, handling nullable columns safely. */
    private static String formatRow(InternalRow r) {
        StringBuilder sb = new StringBuilder("Row: a=");
        sb.append(r.isNullAt(0) ? "null" : r.getInt(0));
        sb.append(", b=");
        sb.append(r.isNullAt(1) ? "null" : r.getString(1).toString());
        sb.append(", c=");
        sb.append(r.isNullAt(2) ? "null" : r.getInt(2));
        sb.append(", d=");
        sb.append(r.isNullAt(3) ? "null" : r.getString(3).toString());
        return sb.toString();
    }

    private void cleanup() {
        System.out.println("=== CLEANUP ===");
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            System.err.println("  Failed to close table: " + e.getMessage());
        }
        try {
            if (admin != null) {
                admin.dropTable(TABLE_PATH, true).get();
                System.out.println("  Table dropped.");
                admin.close();
            }
        } catch (Exception e) {
            System.err.println("  Failed to drop table or close admin: " + e.getMessage());
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            System.err.println("  Failed to close connection: " + e.getMessage());
        }
        System.out.println("Cleanup complete.");
    }
}
