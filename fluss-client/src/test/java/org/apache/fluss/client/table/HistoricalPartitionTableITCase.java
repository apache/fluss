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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for writing to expired partitions on data-lake-enabled tables. Verifies that
 * expired partition writes are redirected to the {@code __historical__} partition for both log
 * tables and primary key tables.
 */
class HistoricalPartitionTableITCase extends ClientToServerITCaseBase {

    private static final TablePath LOG_TABLE_PATH =
            TablePath.of("test_db_1", "test_historical_log_table");

    private static final TablePath PK_TABLE_PATH =
            TablePath.of("test_db_1", "test_historical_pk_table");

    @BeforeEach
    void beforeEach() throws Exception {
        super.setup();
        // Disable dynamic partition creation so only historical redirect works
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);
    }

    // ---- Log table tests ----

    @Test
    void testLogTableWriteExpiredPartition() throws Exception {
        Schema schema = createDataLakeLogTable();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(LOG_TABLE_PATH);

        Table table = conn.getTable(LOG_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();

        // Write to an expired partition (year 2000 is well past the 7-year retention)
        int numRecords = 3;
        for (int i = 0; i < numRecords; i++) {
            appendWriter.append(row(i, "val" + i, "2000"));
        }
        appendWriter.flush();

        verifyHistoricalPartitionData(LOG_TABLE_PATH, table, schema, numRecords);
    }

    @Test
    void testLogTableWriteNonExpiredNonExistentThrows() throws Exception {
        createDataLakeLogTable();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(LOG_TABLE_PATH);

        Table table = conn.getTable(LOG_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();

        // "9000" is a future year - not expired, and not pre-created - should throw
        assertThatThrownBy(() -> appendWriter.append(row(1, "val", "9000")))
                .rootCause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("does not exist");
    }

    // ---- PK table tests ----

    @Test
    void testPkTableWriteExpiredPartition() throws Exception {
        Schema schema = createDataLakePkTable();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(PK_TABLE_PATH);

        Table table = conn.getTable(PK_TABLE_PATH);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();

        // Write initial values to an expired partition (year 2000 is well past the 7-year
        // retention)
        int numRecords = 3;
        for (int i = 0; i < numRecords; i++) {
            upsertWriter.upsert(row(i, "val" + i, "2000"));
        }
        upsertWriter.flush();

        // Overwrite with updated values using the same keys
        for (int i = 0; i < numRecords; i++) {
            upsertWriter.upsert(row(i, "updated" + i, "2000"));
        }
        upsertWriter.flush();

        // Verify __historical__ partition was created
        List<PartitionInfo> partitions = admin.listPartitionInfos(PK_TABLE_PATH).get();
        Optional<PartitionInfo> historicalPartition =
                partitions.stream()
                        .filter(p -> HISTORICAL_PARTITION_VALUE.equals(p.getPartitionName()))
                        .findFirst();
        assertThat(historicalPartition).isPresent();

        // Log contains: 3 inserts (+I) + 3 update-before (-U) + 3 update-after (+U) = 9
        long historicalPartitionId = historicalPartition.get().getPartitionId();
        int totalLogRecords = numRecords * 3;
        List<GenericRow> scannedRows =
                scanHistoricalPartition(table, historicalPartitionId, totalLogRecords);
        assertThat(scannedRows).hasSize(totalLogRecords);

        // 3 inserts
        for (int i = 0; i < numRecords; i++) {
            assertThatRow(scannedRows.get(i))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "val" + i, "2000"));
        }
        // 3 pairs of (-U old, +U new)
        for (int i = 0; i < numRecords; i++) {
            int base = numRecords + i * 2;
            assertThatRow(scannedRows.get(base))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "val" + i, "2000"));
            assertThatRow(scannedRows.get(base + 1))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "updated" + i, "2000"));
        }
    }

    @Test
    void testPkTableWriteNonExpiredNonExistentThrows() throws Exception {
        createDataLakePkTable();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(PK_TABLE_PATH);

        Table table = conn.getTable(PK_TABLE_PATH);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();

        // "9000" is a future year - not expired, and not pre-created - should throw
        assertThatThrownBy(() -> upsertWriter.upsert(row(1, "val", "9000")))
                .rootCause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("does not exist");
    }

    // ---- Helper methods ----

    /**
     * Verifies that the {@code __historical__} partition exists and contains the expected records
     * with the original partition column value "2000" preserved.
     */
    private void verifyHistoricalPartitionData(
            TablePath tablePath, Table table, Schema schema, int numRecords) throws Exception {
        List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
        Optional<PartitionInfo> historicalPartition =
                partitions.stream()
                        .filter(p -> HISTORICAL_PARTITION_VALUE.equals(p.getPartitionName()))
                        .findFirst();
        assertThat(historicalPartition).isPresent();

        long historicalPartitionId = historicalPartition.get().getPartitionId();
        List<GenericRow> scannedRows =
                scanHistoricalPartition(table, historicalPartitionId, numRecords);

        assertThat(scannedRows).hasSize(numRecords);
        for (int i = 0; i < numRecords; i++) {
            assertThatRow(scannedRows.get(i))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "val" + i, "2000"));
        }
    }

    /**
     * Scans records from the historical partition until the expected number of records is reached.
     */
    private List<GenericRow> scanHistoricalPartition(
            Table table, long partitionId, int expectedRecords) throws Exception {
        List<GenericRow> scannedRows = new ArrayList<>();
        try (LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.subscribeFromBeginning(partitionId, 0);
            while (scannedRows.size() < expectedRecords) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(2));
                for (TableBucket bucket : scanRecords.buckets()) {
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        scannedRows.add(
                                row(
                                        record.getRow().getInt(0),
                                        record.getRow().getString(1).toString(),
                                        record.getRow().getString(2).toString()));
                    }
                }
            }
        }
        return scannedRows;
    }

    /**
     * Creates a data-lake-enabled, auto-partitioned log table (no primary key) with YEAR time unit
     * and 7-year retention.
     */
    private Schema createDataLakeLogTable() throws Exception {
        Schema tableSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(tableSchema)
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 7)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        createTable(LOG_TABLE_PATH, descriptor, false);
        return tableSchema;
    }

    /**
     * Creates a data-lake-enabled, auto-partitioned primary key table with YEAR time unit and
     * 7-year retention.
     */
    private Schema createDataLakePkTable() throws Exception {
        Schema tableSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "c")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(tableSchema)
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 7)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        createTable(PK_TABLE_PATH, descriptor, false);
        return tableSchema;
    }
}
