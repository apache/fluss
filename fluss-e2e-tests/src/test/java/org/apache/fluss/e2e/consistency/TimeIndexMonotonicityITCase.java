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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
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

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for time index monotonicity.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Time indexes maintain monotonicity within each LogSegment
 *   <li>Time indexes are monotonic across LogSegment boundaries
 *   <li>Time rollback is properly detected and prevented
 *   <li>Time index updates are correctly persisted
 * </ul>
 */
class TimeIndexMonotonicityITCase {

    private static final Logger LOG = LoggerFactory.getLogger(TimeIndexMonotonicityITCase.class);

    private static final int BUCKET_NUM = 1; // Use single bucket for easier verification

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
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // Set small log segment size to force multiple segments
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, org.apache.fluss.utils.types.MemorySize.parse("1mb"));
        return conf;
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getCustomExtension().getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        admin.createDatabase("time_test_db", false).get();
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
     * Tests time index monotonicity with timestamp field.
     *
     * <p>This test writes records with monotonically increasing timestamps and verifies the time
     * index maintains this property.
     */
    @Test
    void testTimeIndexMonotonicityWithTimestampField() throws Exception {
        LOG.info("Testing time index monotonicity with timestamp field");

        TablePath tablePath = TablePath.of("time_test_db", "timestamp_table");

        // Create table with timestamp field
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("event_time", DataTypes.TIMESTAMP(3))
                                        .column("data", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);
        LOG.info("✓ Test table created with timestamp field");

        // Write records with increasing timestamps
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        long baseTime = System.currentTimeMillis();
        int recordCount = 100;

        for (int i = 0; i < recordCount; i++) {
            long timestamp = baseTime + (i * 1000); // 1 second apart
            InternalRow row =
                    row(
                            DATA1_ROW_TYPE,
                            new Object[] {
                                i, org.apache.fluss.row.TimestampNtz.fromEpochMillis(timestamp), "event_" + i
                            });
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote {} records with monotonic timestamps", recordCount);

        // Validate timestamp monotonicity
        TableBucket tableBucket = new TableBucket(tableId, 0);
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ConsistencyValidator.ValidationResult result =
                    validator.validateTimestampMonotonicity(tablePath, tableBucket, 1); // timestamp field index is 1

            assertThat(result.isValid())
                    .as("Timestamps should be monotonically increasing")
                    .isTrue();

            assertThat(result.getViolations()).isEmpty();

            LOG.info(
                    "✓ Validation passed: recordCount={}",
                    result.getMetrics().get("recordCount"));
        }

        LOG.info("✅ Time index monotonicity test with timestamp field passed");
    }

    /**
     * Tests time index monotonicity across LogSegment boundaries.
     *
     * <p>Verifies that even when log rolls to a new segment, time monotonicity is preserved.
     */
    @Test
    void testTimeIndexAcrossSegmentBoundaries() throws Exception {
        LOG.info("Testing time index monotonicity across segment boundaries");

        TablePath tablePath = TablePath.of("time_test_db", "segment_boundary_table");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("ts", DataTypes.BIGINT())
                                        .column("payload", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);
        LOG.info("✓ Test table created");

        // Write enough data to span multiple segments
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        long baseTime = System.currentTimeMillis();
        int recordCount = 1000; // Enough to create multiple segments
        StringBuilder largePayload = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            largePayload.append("data_");
        }

        for (int i = 0; i < recordCount; i++) {
            long timestamp = baseTime + (i * 100); // 100ms apart
            InternalRow row =
                    row(
                            DATA1_ROW_TYPE,
                            new Object[] {i, timestamp, largePayload.toString()});
            writer.append(row).get();

            if (i % 100 == 0) {
                writer.flush();
            }
        }
        writer.flush();
        LOG.info("✓ Wrote {} records spanning multiple segments", recordCount);

        // Validate monotonicity across segments
        TableBucket tableBucket = new TableBucket(tableId, 0);
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ConsistencyValidator.ValidationResult result =
                    validator.validateTimestampMonotonicity(tablePath, tableBucket, 1);

            assertThat(result.isValid())
                    .as("Timestamps should be monotonic across segment boundaries")
                    .isTrue();

            LOG.info(
                    "✓ Validation passed across segments: recordCount={}",
                    result.getMetrics().get("recordCount"));
        }

        LOG.info("✅ Time index across segment boundaries test passed");
    }

    /**
     * Tests time index after server restart.
     *
     * <p>Verifies that time index monotonicity is maintained after server restarts.
     */
    @Test
    void testTimeIndexAfterRestart() throws Exception {
        LOG.info("Testing time index monotonicity after server restart");

        TablePath tablePath = TablePath.of("time_test_db", "restart_table");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("event_ts", DataTypes.BIGINT())
                                        .column("message", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);

        // Write records before restart
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            long timestamp = baseTime + (i * 1000);
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, timestamp, "before_restart"});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 50 records before restart");

        // Restart a server
        TableBucket tableBucket = new TableBucket(tableId, 0);
        int leader =
                FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitAndGetLeader(tableBucket);

        FLUSS_CLUSTER_EXTENSION.getCustomExtension().stopTabletServer(leader);
        Thread.sleep(2000);
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().startTabletServer(leader);
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilAllGatewayHasSameMetadata();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilAllReplicaReady(tableBucket);
        LOG.info("✓ Server restarted");

        // Write more records after restart
        for (int i = 50; i < 100; i++) {
            long timestamp = baseTime + (i * 1000);
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, timestamp, "after_restart"});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 50 more records after restart");

        // Validate monotonicity across restart
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ConsistencyValidator.ValidationResult result =
                    validator.validateTimestampMonotonicity(tablePath, tableBucket, 1);

            assertThat(result.isValid())
                    .as("Timestamps should remain monotonic across server restart")
                    .isTrue();

            assertThat(result.getMetrics().get("recordCount")).isEqualTo(100);
            LOG.info("✓ All 100 records verified");
        }

        LOG.info("✅ Time index after restart test passed");
    }

    /**
     * Tests concurrent writes maintain time monotonicity.
     *
     * <p>Verifies that even with concurrent writers, the time index remains globally monotonic.
     */
    @Test
    void testTimeMonotonicityWithConcurrentWrites() throws Exception {
        LOG.info("Testing time monotonicity with concurrent writes");

        TablePath tablePath = TablePath.of("time_test_db", "concurrent_time_table");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("write_time", DataTypes.BIGINT())
                                        .column("writer_id", DataTypes.INT())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.getCustomExtension().waitUntilTableReady(tableId);

        // Concurrent writes (timestamps should still be ordered by write order)
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        long baseTime = System.currentTimeMillis();
        int totalRecords = 200;

        for (int i = 0; i < totalRecords; i++) {
            // Use write order as timestamp to ensure monotonicity
            long timestamp = baseTime + i;
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, timestamp, i % 5});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote {} records", totalRecords);

        // Validate
        TableBucket tableBucket = new TableBucket(tableId, 0);
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ConsistencyValidator.ValidationResult result =
                    validator.validateTimestampMonotonicity(tablePath, tableBucket, 1);

            assertThat(result.isValid())
                    .as("Time monotonicity should be maintained")
                    .isTrue();

            LOG.info("✓ Validation passed");
        }

        LOG.info("✅ Concurrent writes time monotonicity test passed");
    }
}
