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
 * Integration tests for writing to expired partitions on data-lake-enabled log tables. Verifies
 * that expired partition writes are redirected to the {@code __historical__} partition.
 */
class HistoricalPartitionLogTableITCase extends ClientToServerITCaseBase {

    private static final TablePath LOG_TABLE_PATH =
            TablePath.of("test_db_1", "test_historical_log_table");

    private Schema schema;

    @BeforeEach
    void beforeEach() throws Exception {
        super.setup();
        // Enable dynamic partition creation so historical partition can be created
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);

        schema = createDataLakeLogTable();
    }

    @Test
    void testWriteExpiredPartition() throws Exception {
        // Wait for pre-created partitions to be ready
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(LOG_TABLE_PATH);

        Table table = conn.getTable(LOG_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();

        // Write to an expired partition (year 2000 is well past the 7-year retention)
        int numRecords = 3;
        for (int i = 0; i < numRecords; i++) {
            appendWriter.append(row(i, "val" + i, "2000"));
        }
        appendWriter.flush();

        // Verify __historical__ partition was created and contains the data
        List<PartitionInfo> partitions = admin.listPartitionInfos(LOG_TABLE_PATH).get();
        Optional<PartitionInfo> historicalPartition =
                partitions.stream()
                        .filter(p -> HISTORICAL_PARTITION_VALUE.equals(p.getPartitionName()))
                        .findFirst();
        assertThat(historicalPartition).isPresent();

        // Scan data from the historical partition
        long historicalPartitionId = historicalPartition.get().getPartitionId();
        List<GenericRow> scannedRows = new ArrayList<>();
        try (LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.subscribeFromBeginning(historicalPartitionId, 0);
            while (scannedRows.size() < numRecords) {
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

        assertThat(scannedRows).hasSize(numRecords);
        // Verify that the original partition column value "2000" is preserved in the row
        for (int i = 0; i < numRecords; i++) {
            assertThatRow(scannedRows.get(i))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "val" + i, "2000"));
        }
    }

    @Test
    void testWriteNonExpiredNonExistentThrows() throws Exception {
        // Wait for pre-created partitions to be ready
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(LOG_TABLE_PATH);

        Table table = conn.getTable(LOG_TABLE_PATH);
        AppendWriter appendWriter = table.newAppend().createWriter();

        // "9000" is a future year → not expired, and not pre-created → should throw
        // (dynamicPartitionEnabled is false).
        // The PartitionNotExistException is wrapped in FlussRuntimeException by WriterClient.
        assertThatThrownBy(() -> appendWriter.append(row(1, "val", "9000")))
                .rootCause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("does not exist");
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
}
