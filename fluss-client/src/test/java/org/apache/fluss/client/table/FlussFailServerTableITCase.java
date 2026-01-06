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

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.InternalRowListAssert.assertThatRows;

/** IT case for {@link FlussTable} in the case of one tablet server fails. */
class FlussFailServerTableITCase extends ClientToServerITCaseBase {

    private static final int SERVER = 0;

    @BeforeEach
    void beforeEach() throws Exception {
        // since we kill and start one tablet server in each test,
        // we need to wait for metadata to be updated to servers
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        super.setup();
    }

    @Test
    void testAppend() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            GenericRow row = row(1, "a");

            // append a row
            appendWriter.append(row).get();

            // now, kill one server and try to append data again
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // append some rows again, should success
                for (int i = 0; i < 10; i++) {
                    appendWriter.append(row).get();
                }
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    @Test
    void testPut() throws Exception {
        createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, false);
        // put one row
        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            InternalRow row = row(1, "a");
            upsertWriter.upsert(row).get();

            // kill first tablet server
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // append some rows again, should success
                for (int i = 0; i < 10; i++) {
                    // mock a row
                    upsertWriter.upsert(row(i, "a" + i)).get();
                }
            } finally {
                // todo: try to get value when get is re-triable in FLUSS-56857409
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    @Test
    void testLogScan() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        // append one row.
        GenericRow row = row(1, "a");
        try (Table table = conn.getTable(DATA1_TABLE_PATH);
                LogScanner logScanner = createLogScanner(table)) {
            subscribeFromBeginning(logScanner, table);
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row).get();

            // poll data util we get one record
            ScanRecords scanRecords;
            do {
                scanRecords = logScanner.poll(Duration.ofSeconds(1));
            } while (scanRecords.isEmpty());

            int rowCount = 10;
            // append some rows
            List<GenericRow> expectRows = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                appendWriter.append(row).get();
                expectRows.add(row);
            }
            // kill first tablet server
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // now, poll records utils we can get all the records
                int counts = 0;
                int expectCounts = 10;
                List<InternalRow> actualRows = new ArrayList<>(rowCount);
                do {
                    scanRecords = logScanner.poll(Duration.ofSeconds(1));
                    actualRows.addAll(toRows(scanRecords));
                    counts += scanRecords.count();
                } while (counts < expectCounts);
                assertThatRows(actualRows).withSchema(DATA1_ROW_TYPE).isEqualTo(expectRows);
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    @Test
    void testPutAutoIncColumnAndLookup() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("col1", DataTypes.STRING())
                        .withComment("col1 is first column")
                        .column("col2", DataTypes.BIGINT())
                        .withComment("col2 is second column, auto increment column")
                        .column("col3", DataTypes.STRING())
                        .withComment("col3 is third column")
                        .enableAutoIncrement("col2")
                        .primaryKey("col1")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(2, "col1").build();
        createTable(DATA1_TABLE_PATH_PK, tableDescriptor, false);
        Object[][] records = {
            {"a", 0L, "batch1"},
            {"b", 100000L, "batch1"},
            {"c", 1L, "batch1"},
            {"d", 100001L, "batch1"}
        };

        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            UpsertWriter upsertWriter =
                    table.newUpsert().partialUpdate("col1", "col3").createWriter();
            for (Object[] record : records) {
                upsertWriter.upsert(row(record[0], null, record[2])).get();
            }

            Lookuper lookuper = table.newLookup().createLookuper();
            ProjectedRow keyRow = ProjectedRow.from(schema.getPrimaryKeyIndexes());
            for (Object[] record : records) {
                assertThatRow(lookupRow(lookuper, keyRow.replaceRow(row(record))))
                        .withSchema(schema.getRowType())
                        .isEqualTo(row(record));
            }

            // kill and restart all tablet server
            for (int i = 0; i < 3; i++) {
                FLUSS_CLUSTER_EXTENSION.stopTabletServer(i);
                FLUSS_CLUSTER_EXTENSION.startTabletServer(i);
            }

            for (Object[] record : records) {
                assertThatRow(lookupRow(lookuper, keyRow.replaceRow(row(record))))
                        .withSchema(schema.getRowType())
                        .isEqualTo(row(record));
            }
            upsertWriter.upsert(row("e", null, "batch2")).get();
            // The auto-increment column should start from a new segment for now, and local cached
            // IDs have been discarded.
            assertThatRow(lookupRow(lookuper, keyRow.replaceRow(row("e", null, null))))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row("e", 200000L, "batch2"));
        }
    }

    private List<InternalRow> toRows(ScanRecords scanRecords) {
        List<InternalRow> rows = new ArrayList<>();
        for (ScanRecord scanRecord : scanRecords) {
            rows.add(scanRecord.getRow());
        }
        return rows;
    }
}
