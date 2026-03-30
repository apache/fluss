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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tests that require manual cluster management. */
class CustomFlussClusterITCase {

    @Test
    void testProjectionPushdownWithEmptyBatches() throws Exception {
        Configuration conf = initConfig();
        // Configuration to reproduce the issue described in
        // https://github.com/apache/fluss/issues/2369:
        // 1. Disable remote log task to prevent reading from remote log storage.
        // 2. Set LOG_SEGMENT_FILE_SIZE to ensure that the segment before the last segment
        //    contains an empty batch at the end.
        // This setup causes the scanner to block indefinitely if it incorrectly skips empty batches
        // during projection pushdown, as it will wait forever for non-empty data that never
        // arrives.
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        conf.set(
                ConfigOptions.LOG_SEGMENT_FILE_SIZE,
                new MemorySize(2 * V0_RECORD_BATCH_HEADER_SIZE));
        conf.set(
                ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET,
                new MemorySize(2 * V0_RECORD_BATCH_HEADER_SIZE));
        final FlussClusterExtension flussClusterExtension =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(3)
                        .setClusterConf(conf)
                        .build();
        flussClusterExtension.start();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.FIRST_ROW)
                        .distributedBy(1)
                        .build();
        RowType rowType = DATA1_SCHEMA_PK.getRowType();
        TablePath tablePath =
                TablePath.of("test_db_1", "test_projection_pushdown_with_empty_batches");

        int rows = 5;
        int duplicateNum = 2;
        int batchSize = 3;
        int count = 0;
        // Case1: Test normal update to generator not empty cdc logs.
        Table table = null;
        LogScanner logScanner = null;
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                flussClusterExtension.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false)
                    .get();
            admin.createTable(tablePath, tableDescriptor, false).get();
            table = connection.getTable(tablePath);
            // first, put rows
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            List<InternalRow> expectedScanRows = new ArrayList<>(rows);
            List<InternalRow> expectedLookupRows = new ArrayList<>(rows);
            for (int id = 0; id < rows; id++) {
                for (int num = 0; num < duplicateNum; num++) {
                    upsertWriter.upsert(row(id, "value_" + num));
                    if (count++ > batchSize) {
                        upsertWriter.flush();
                        count = 0;
                    }
                }

                expectedLookupRows.add(row(id, "value_0"));
                expectedScanRows.add(row(id));
            }

            upsertWriter.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            // now, get rows by lookup
            for (int id = 0; id < rows; id++) {
                InternalRow gotRow = lookuper.lookup(row(id)).get().getSingletonRow();
                assertThatRow(gotRow).withSchema(rowType).isEqualTo(expectedLookupRows.get(id));
            }

            Scan scan = table.newScan().project(new int[] {0});
            logScanner = scan.createLogScanner();

            logScanner.subscribeFromBeginning(0);
            List<ScanRecord> actualLogRecords = new ArrayList<>(0);
            while (actualLogRecords.size() < rows) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                scanRecords.forEach(actualLogRecords::add);
            }
            assertThat(actualLogRecords).hasSize(rows);
            for (int i = 0; i < actualLogRecords.size(); i++) {
                ScanRecord scanRecord = actualLogRecords.get(i);
                assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThatRow(scanRecord.getRow())
                        .withSchema(rowType.project(new int[] {0}))
                        .isEqualTo(expectedScanRows.get(i));
            }

            // Case2: Test all the update in the write batch are duplicate(Thus generate empty cdc
            // logs).
            // insert duplicate rows again to generate empty cdc log.
            for (int num = 0; num < duplicateNum; num++) {
                upsertWriter.upsert(row(0, "value_" + num));
                upsertWriter.flush();
            }

            // insert a new row.
            upsertWriter.upsert(row(rows + 1, "new_value"));

            actualLogRecords = new ArrayList<>(0);
            while (actualLogRecords.isEmpty()) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                scanRecords.forEach(actualLogRecords::add);
            }
            logScanner.close();
            assertThat(actualLogRecords).hasSize(1);
            ScanRecord scanRecord = actualLogRecords.get(0);
            assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
            assertThatRow(scanRecord.getRow())
                    .withSchema(rowType.project(new int[] {0}))
                    .isEqualTo(row(rows + 1));
        } finally {
            if (logScanner != null) {
                logScanner.close();
            }
            if (table != null) {
                table.close();
            }
            flussClusterExtension.close();
        }
    }

    @Test
    void testConcurrentKvSnapshotLeaseOperations() throws Exception {
        Configuration conf = initConfig();
        // Use a short snapshot interval for stress testing
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        final FlussClusterExtension flussClusterExtension =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(3)
                        .setClusterConf(conf)
                        .build();
        flussClusterExtension.start();

        TablePath tablePath = TablePath.of("test_stress_db", "test_concurrent_kv_snapshot_lease");

        try (Connection connection =
                        ConnectionFactory.createConnection(
                                flussClusterExtension.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false)
                    .get();
            admin.createTable(tablePath, DATA1_TABLE_DESCRIPTOR_PK, false).get();

            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            int bucketNum = 3;

            // Write initial data
            try (Table table = connection.getTable(tablePath)) {
                UpsertWriter upsertWriter = table.newUpsert().createWriter();
                for (int i = 0; i < 20; i++) {
                    upsertWriter.upsert(row(i, "value_" + i));
                }
                upsertWriter.flush();
            }

            // Trigger and wait for snapshot
            flussClusterExtension.triggerAndWaitSnapshot(tablePath);

            // Prepare snapshot IDs for lease operations
            Map<TableBucket, Long> bucketSnapshotIds = new HashMap<>();
            for (int bucket = 0; bucket < bucketNum; bucket++) {
                bucketSnapshotIds.put(new TableBucket(tableId, bucket), 0L);
            }

            int numLeases = 5;
            int numConcurrentOps = 3;
            ExecutorService executor = Executors.newFixedThreadPool(numLeases * numConcurrentOps);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            // Concurrently acquire, renew, and drop leases
            for (int i = 0; i < numLeases; i++) {
                final String leaseId = "stress-lease-" + i;
                final long leaseDuration = Duration.ofDays(1).toMillis();

                CompletableFuture<Void> future =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        KvSnapshotLease lease =
                                                admin.createKvSnapshotLease(leaseId, leaseDuration);

                                        // Acquire snapshots
                                        lease.acquireSnapshots(bucketSnapshotIds).get();

                                        // Renew the lease multiple times
                                        for (int r = 0; r < 3; r++) {
                                            lease.renew().get();
                                        }

                                        // Drop the lease
                                        lease.dropLease().get();

                                        successCount.incrementAndGet();
                                    } catch (Exception e) {
                                        errorCount.incrementAndGet();
                                    }
                                },
                                executor);
                futures.add(future);
            }

            // Wait for all concurrent operations to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(2, TimeUnit.MINUTES);

            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

            // Verify all operations succeeded
            assertThat(successCount.get()).isEqualTo(numLeases);
            assertThat(errorCount.get()).isEqualTo(0);

            // Verify all leases are cleaned up
            ZooKeeperClient zkClient = flussClusterExtension.getZooKeeperClient();
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(zkClient.getKvSnapshotLeasesList()).isEmpty());

            // Verify data is still consistent after concurrent operations
            try (Table table = connection.getTable(tablePath)) {
                Lookuper lookuper = table.newLookup().createLookuper();
                for (int i = 0; i < 20; i++) {
                    InternalRow gotRow = lookuper.lookup(row(i)).get().getSingletonRow();
                    assertThatRow(gotRow)
                            .withSchema(DATA1_SCHEMA_PK.getRowType())
                            .isEqualTo(row(i, "value_" + i));
                }
            }
        } finally {
            flussClusterExtension.close();
        }
    }

    protected static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.setString("datalake.paimon.jdbc.user", "admin");
        conf.setString("datalake.paimon.jdbc.password", "pass");

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.MAX_PARTITION_NUM, 10);
        conf.set(ConfigOptions.MAX_BUCKET_NUM, 30);

        conf.set(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS, 1);
        return conf;
    }
}
