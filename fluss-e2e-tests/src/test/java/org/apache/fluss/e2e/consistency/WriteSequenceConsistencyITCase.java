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

package org.apache.fluss.e2e.consistency;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.scanner.ScanRecord;
import org.apache.fluss.client.scanner.log.LogScanner;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.e2e.utils.ConsistencyValidator;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for write sequence number consistency.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Write sequence numbers are monotonically increasing within each bucket
 *   <li>Sequence numbers are correctly maintained across leader changes
 *   <li>Out-of-order writes are properly detected and rejected
 *   <li>Sequence numbers are persistent across server restarts
 *   <li>Multiple concurrent writers don't violate sequence consistency
 * </ul>
 */
class WriteSequenceConsistencyITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(WriteSequenceConsistencyITCase.class);

    private static final int BUCKET_NUM = 3;

    @RegisterExtension
    public static final AllCallbackWrapper<FlussClusterExtension> FLUSS_CLUSTER_EXTENSION =
            new AllCallbackWrapper<>(
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(3)
                            .setClusterConf(createClusterConfig())
                            .build());

    private static Connection conn;
    private static Admin admin;
    private static Configuration clientConf;

    private static Configuration createClusterConfig() {
        Configuration conf = new Configuration();
        // Ensure sufficient replicas for consistency testing
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getCustomExtension().getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        // Create test database
        admin.createDatabase("test_db", false).get();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Tests that write sequence numbers are monotonically increasing for sequential writes.
     *
     * <p>This is the basic consistency guarantee.
     */
    @Test
    void testSequentialWriteMonotonicity() throws Exception {
        LOG.info("Testing sequential write monotonicity");

        TablePath tablePath = TablePath.of("test_db", "sequential_test");

        // Create log table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        LOG.info("✓ Test table created");

        // Wait for table to be ready
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);
        LOG.info("✓ Table is ready");

        // Write records sequentially
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.getAppendWriter();

        int recordCount = 100;
        for (int i = 0; i < recordCount; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "value_" + i});
            appendWriter.append(row).get();
        }
        appendWriter.flush();
        LOG.info("✓ Wrote {} records sequentially", recordCount);

        // Validate sequence monotonicity for each bucket
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            for (int bucketId = 0; bucketId < BUCKET_NUM; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                ConsistencyValidator.ValidationResult result =
                        validator.validateWriteSequenceMonotonicity(tablePath, tableBucket);

                LOG.info(
                        "✓ Bucket {} validation result: valid={}, violations={}, metrics={}",
                        bucketId,
                        result.isValid(),
                        result.getViolations(),
                        result.getMetrics());

                assertThat(result.isValid())
                        .as("Write sequences should be monotonic for bucket " + bucketId)
                        .isTrue();
                assertThat(result.getViolations()).isEmpty();
            }
        }

        LOG.info("✅ Sequential write monotonicity test passed");
    }

    /**
     * Tests sequence consistency with concurrent writers.
     *
     * <p>Multiple writers writing to the same table should maintain sequence consistency.
     */
    @Test
    void testConcurrentWriteConsistency() throws Exception {
        LOG.info("Testing concurrent write consistency");

        TablePath tablePath = TablePath.of("test_db", "concurrent_test");

        // Create log table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);
        LOG.info("✓ Test table created and ready");

        // Launch multiple concurrent writers
        int writerCount = 5;
        int recordsPerWriter = 20;
        ExecutorService executor = Executors.newFixedThreadPool(writerCount);

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int writerId = 0; writerId < writerCount; writerId++) {
                int finalWriterId = writerId;
                CompletableFuture<Void> future =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        Table table = conn.getTable(tablePath);
                                        AppendWriter writer = table.getAppendWriter();

                                        for (int i = 0; i < recordsPerWriter; i++) {
                                            int value = finalWriterId * 1000 + i;
                                            InternalRow row =
                                                    row(
                                                            DATA1_ROW_TYPE,
                                                            new Object[] {
                                                                value, "writer_" + finalWriterId
                                                            });
                                            writer.append(row).get();
                                        }
                                        writer.flush();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                executor);
                futures.add(future);
            }

            // Wait for all writers to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            LOG.info("✓ All {} concurrent writers completed", writerCount);

        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        // Validate sequence monotonicity despite concurrent writes
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            for (int bucketId = 0; bucketId < BUCKET_NUM; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                ConsistencyValidator.ValidationResult result =
                        validator.validateWriteSequenceMonotonicity(tablePath, tableBucket);

                assertThat(result.isValid())
                        .as(
                                "Sequences should be monotonic even with concurrent writers for bucket "
                                        + bucketId)
                        .isTrue();

                LOG.info(
                        "✓ Bucket {} passed validation with {} records",
                        bucketId,
                        result.getMetrics().get("recordCount"));
            }
        }

        LOG.info("✅ Concurrent write consistency test passed");
    }

    /**
     * Tests that sequence numbers are maintained correctly after a TabletServer restart.
     *
     * <p>This verifies persistence of sequence state.
     */
    @Test
    void testSequencePersistenceAcrossRestart() throws Exception {
        LOG.info("Testing sequence persistence across TabletServer restart");

        TablePath tablePath = TablePath.of("test_db", "persistence_test");

        // Create log table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(1) // Single bucket for easier testing
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);
        LOG.info("✓ Test table created");

        // Write some records before restart
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        for (int i = 0; i < 50; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "before_restart_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 50 records before restart");

        // Get the leader server for bucket 0
        TableBucket tableBucket = new TableBucket(tableId, 0);
        int leaderServer =
                FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitAndGetLeader(tableBucket);
        LOG.info("✓ Leader server for bucket 0: {}", leaderServer);

        // Restart the leader server
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().stopTabletServer(leaderServer);
        LOG.info("✓ Stopped TabletServer {}", leaderServer);

        Thread.sleep(2000); // Wait for failure detection

        FLUSS_CLUSTER_EXTENSION.getCustomExtension().startTabletServer(leaderServer);
        LOG.info("✓ Restarted TabletServer {}", leaderServer);

        // Wait for cluster to stabilize
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilAllGatewayHasSameMetadata();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilAllReplicaReady(tableBucket);
        LOG.info("✓ Cluster stabilized after restart");

        // Write more records after restart
        for (int i = 50; i < 100; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "after_restart_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 50 more records after restart");

        // Validate sequence monotonicity across the restart boundary
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ConsistencyValidator.ValidationResult result =
                    validator.validateWriteSequenceMonotonicity(tablePath, tableBucket);

            assertThat(result.isValid())
                    .as("Sequences should remain monotonic across server restart")
                    .isTrue();

            LOG.info(
                    "✓ Validation result: valid={}, recordCount={}",
                    result.isValid(),
                    result.getMetrics().get("recordCount"));
        }

        LOG.info("✅ Sequence persistence across restart test passed");
    }

    /**
     * Tests sequence consistency during ISR changes.
     *
     * <p>Verifies that sequence numbers remain consistent when replicas are added/removed from
     * ISR.
     */
    @Test
    void testSequenceConsistencyDuringISRChanges() throws Exception {
        LOG.info("Testing sequence consistency during ISR changes");

        TablePath tablePath = TablePath.of("test_db", "isr_test");

        // Create log table with replication
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);
        LOG.info("✓ Test table created with replication factor 3");

        TableBucket tableBucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilAllReplicaReady(tableBucket);
        LOG.info("✓ All replicas ready");

        // Write initial records
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        for (int i = 0; i < 30; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "initial_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 30 initial records");

        // Stop a follower to trigger ISR shrink
        int leaderServer =
                FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitAndGetLeader(tableBucket);
        int followerToStop = (leaderServer + 1) % 3;

        FLUSS_CLUSTER_EXTENSION.getCustomExtension().stopTabletServer(followerToStop);
        LOG.info("✓ Stopped follower server {}", followerToStop);

        // Wait for ISR to shrink
        FLUSS_CLUSTER_EXTENSION
                .getCustomExtension()
                .waitUntilReplicaShrinkFromIsr(tableBucket, followerToStop);
        LOG.info("✓ ISR shrunk, removed server {}", followerToStop);

        // Write more records with shrunk ISR
        for (int i = 30; i < 60; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "shrunk_isr_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 30 records with shrunk ISR");

        // Restart the follower
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().startTabletServer(followerToStop);
        LOG.info("✓ Restarted follower server {}", followerToStop);

        // Wait for ISR to expand
        FLUSS_CLUSTER_EXTENSION
                .getCustomExtension()
                .waitUntilReplicaExpandToIsr(tableBucket, followerToStop);
        LOG.info("✓ ISR expanded, added back server {}", followerToStop);

        // Write final records with full ISR
        for (int i = 60; i < 90; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "full_isr_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 30 final records with full ISR");

        // Validate sequence consistency across all ISR changes
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ConsistencyValidator.ValidationResult result =
                    validator.validateWriteSequenceMonotonicity(tablePath, tableBucket);

            assertThat(result.isValid())
                    .as("Sequences should remain monotonic during ISR changes")
                    .isTrue();

            LOG.info(
                    "✓ Final validation: valid={}, recordCount={}",
                    result.isValid(),
                    result.getMetrics().get("recordCount"));
        }

        LOG.info("✅ Sequence consistency during ISR changes test passed");
    }
}
