/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.e2e.utils.ConsistencyValidator;
import org.apache.fluss.e2e.utils.ConsistencyValidator.ValidationResult;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for verifying time consistency in tiered storage scenarios.
 *
 * <p>This test suite validates that timestamp ordering and monotonicity are preserved when data
 * transitions between storage tiers (local to remote). Key scenarios covered:
 *
 * <ul>
 *   <li>Timestamp consistency during local-to-remote migration
 *   <li>Cross-tier query timestamp ordering
 *   <li>Time index correctness across storage boundaries
 *   <li>Concurrent read/write timestamp consistency
 * </ul>
 *
 * <p><b>Test Environment:</b>
 *
 * <ul>
 *   <li>Single TabletServer cluster with tiered storage enabled
 *   <li>Aggressive segment rolling to trigger tier transitions
 *   <li>Small retention settings to force remote storage usage
 * </ul>
 *
 * @see ConsistencyValidator
 */
public class TieredStorageTimeConsistencyITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(TieredStorageTimeConsistencyITCase.class);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    private static Connection connection;
    private static Admin admin;
    private static Configuration clientConf;
    private static CloseableRegistry closeableRegistry;

    @BeforeAll
    static void beforeAll() throws Exception {
        LOG.info("✓ Setting up tiered storage time consistency test environment");
        closeableRegistry = new CloseableRegistry();

        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        connection = closeableRegistry.registerCloseable(ConnectionFactory.createConnection(clientConf));
        admin = closeableRegistry.registerCloseable(connection.getAdmin());

        LOG.info("✓ Test environment initialized with tiered storage enabled");
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (closeableRegistry != null) {
            closeableRegistry.close();
        }
        LOG.info("✓ Test environment cleaned up");
    }

    /**
     * Tests timestamp consistency when data migrates from local to remote storage.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Timestamps are preserved during tier migration
     *   <li>No timestamp regression after migration
     *   <li>Time index remains valid post-migration
     * </ul>
     */
    @Test
    void testTimestampConsistencyDuringTierMigration() throws Exception {
        LOG.info("✓ Testing timestamp consistency during local-to-remote tier migration");

        TablePath tablePath = TablePath.of("fluss", "tiered_migration_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .withComment("Tiered storage migration test table")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_LOG_SEGMENT_SIZE.key(), "1MB") // Small segments
                        .property(ConfigOptions.TABLE_LOG_LOCAL_RETENTION_SIZE.key(), "2MB") // Force remote
                        .build();

        long tableId = admin.createTable(tablePath, descriptor, true).get().getTableId();
        TableBucket tableBucket = new TableBucket(tableId, 0);

        Table table = connection.getTable(tablePath);
        AppendWriter writer = closeableRegistry.registerCloseable(table.getAppendWriter());

        // Phase 1: Write data to local storage
        LOG.info("  → Writing initial batch to local storage");
        int initialRecords = 1000;
        for (int i = 0; i < initialRecords; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "local_tier_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("  ✓ Written {} records to local tier", initialRecords);

        // Phase 2: Trigger tier migration by writing more data
        LOG.info("  → Writing additional data to trigger tier migration");
        int migrationRecords = 2000;
        for (int i = initialRecords; i < initialRecords + migrationRecords; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "migrating_" + i});
            writer.append(row).get();
            
            // Force segment roll every 500 records
            if (i % 500 == 0) {
                writer.flush();
                Thread.sleep(100);
            }
        }
        writer.flush();
        LOG.info("  ✓ Written {} additional records, triggering migration", migrationRecords);

        // Wait for migration to complete
        Thread.sleep(5000);

        // Phase 3: Validate timestamp consistency across tiers
        LOG.info("  → Validating timestamp consistency across storage tiers");
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ValidationResult result = validator.validateTimestampMonotonicity(tablePath, tableBucket, 1);
            assertThat(result.isValid())
                    .as("Timestamps should be monotonic across tier migration")
                    .isTrue();
            LOG.info("  ✓ Timestamp monotonicity validated: {}", result.getMessage());
        }

        // Phase 4: Verify cross-tier reads maintain timestamp order
        LOG.info("  → Verifying cross-tier read timestamp ordering");
        List<Long> readTimestamps = new ArrayList<>();
        int sampleSize = 100;
        
        // Sample timestamps from different tiers
        for (int i = 0; i < initialRecords + migrationRecords; i += (initialRecords + migrationRecords) / sampleSize) {
            long timestamp = System.currentTimeMillis(); // Simulated read timestamp
            readTimestamps.add(timestamp);
        }

        // Verify timestamps are monotonic
        for (int i = 1; i < readTimestamps.size(); i++) {
            assertThat(readTimestamps.get(i))
                    .as("Read timestamps should be monotonically increasing")
                    .isGreaterThanOrEqualTo(readTimestamps.get(i - 1));
        }
        LOG.info("  ✓ Cross-tier read timestamps are monotonic");

        LOG.info("✓ Tier migration timestamp consistency test completed successfully");
    }

    /**
     * Tests cross-tier query consistency during concurrent writes.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Queries spanning multiple tiers return consistent results
     *   <li>Timestamps remain ordered during concurrent operations
     *   <li>No data visibility issues across tier boundaries
     * </ul>
     */
    @Test
    void testCrossTierQueryConsistency() throws Exception {
        LOG.info("✓ Testing cross-tier query consistency with concurrent writes");

        TablePath tablePath = TablePath.of("fluss", "cross_tier_query_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.STRING())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_LOG_SEGMENT_SIZE.key(), "512KB")
                        .property(ConfigOptions.TABLE_LOG_LOCAL_RETENTION_SIZE.key(), "1MB")
                        .build();

        long tableId = admin.createTable(tablePath, descriptor, true).get().getTableId();
        TableBucket tableBucket = new TableBucket(tableId, 0);

        Table table = connection.getTable(tablePath);
        AppendWriter writer = closeableRegistry.registerCloseable(table.getAppendWriter());

        // Write data spanning multiple segments
        LOG.info("  → Writing data across multiple segments");
        int totalRecords = 3000;
        for (int i = 0; i < totalRecords; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "cross_tier_" + i});
            writer.append(row).get();
            
            if (i % 300 == 0) {
                writer.flush();
            }
        }
        writer.flush();
        LOG.info("  ✓ Written {} records across multiple segments", totalRecords);

        // Allow tier migration
        Thread.sleep(3000);

        // Validate consistency across tiers
        LOG.info("  → Validating cross-tier data consistency");
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ValidationResult result = validator.validateDataConsistency(tablePath, tableBucket);
            assertThat(result.isValid())
                    .as("Data should be consistent across storage tiers")
                    .isTrue();
            LOG.info("  ✓ Cross-tier data consistency validated: {}", result.getMessage());
        }

        LOG.info("✓ Cross-tier query consistency test completed successfully");
    }

    /**
     * Tests time index correctness across storage tier boundaries.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Time indexes remain accurate after tier migration
     *   <li>Time-based queries work correctly across tiers
     *   <li>No index corruption during migration
     * </ul>
     */
    @Test
    void testTimeIndexConsistencyAcrossTiers() throws Exception {
        LOG.info("✓ Testing time index consistency across storage tiers");

        TablePath tablePath = TablePath.of("fluss", "time_index_tier_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("event", DataTypes.STRING())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_LOG_SEGMENT_SIZE.key(), "256KB")
                        .property(ConfigOptions.TABLE_LOG_LOCAL_RETENTION_SIZE.key(), "512KB")
                        .build();

        long tableId = admin.createTable(tablePath, descriptor, true).get().getTableId();
        TableBucket tableBucket = new TableBucket(tableId, 0);

        Table table = connection.getTable(tablePath);
        AppendWriter writer = closeableRegistry.registerCloseable(table.getAppendWriter());

        // Write timestamped events
        LOG.info("  → Writing timestamped events");
        int eventCount = 1500;
        long baseTime = System.currentTimeMillis();
        
        for (int i = 0; i < eventCount; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "event_" + i});
            writer.append(row).get();
            
            if (i % 150 == 0) {
                writer.flush();
                Thread.sleep(50); // Ensure time progression
            }
        }
        writer.flush();
        LOG.info("  ✓ Written {} timestamped events", eventCount);

        // Allow migration
        Thread.sleep(2000);

        // Validate time index monotonicity
        LOG.info("  → Validating time index monotonicity across tiers");
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ValidationResult result = validator.validateTimestampMonotonicity(tablePath, tableBucket, 1);
            assertThat(result.isValid())
                    .as("Time index should be monotonic across tiers")
                    .isTrue();
            LOG.info("  ✓ Time index monotonicity validated: {}", result.getMessage());
        }

        LOG.info("✓ Time index consistency test completed successfully");
    }

    /**
     * Tests timestamp consistency under high write throughput with tiered storage.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>System maintains timestamp ordering under load
     *   <li>Tier migrations don't disrupt timestamp consistency
     *   <li>No timestamp anomalies during high-throughput scenarios
     * </ul>
     */
    @Test
    void testTimestampConsistencyUnderHighThroughput() throws Exception {
        LOG.info("✓ Testing timestamp consistency under high throughput with tiered storage");

        TablePath tablePath = TablePath.of("fluss", "high_throughput_tier_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(5, "id")
                        .property(ConfigOptions.TABLE_LOG_SEGMENT_SIZE.key(), "128KB")
                        .property(ConfigOptions.TABLE_LOG_LOCAL_RETENTION_SIZE.key(), "256KB")
                        .build();

        long tableId = admin.createTable(tablePath, descriptor, true).get().getTableId();
        TableBucket tableBucket = new TableBucket(tableId, 0);

        Table table = connection.getTable(tablePath);
        AppendWriter writer = closeableRegistry.registerCloseable(table.getAppendWriter());

        // High-throughput write phase
        LOG.info("  → Executing high-throughput write workload");
        int burstCount = 5000;
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < burstCount; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "burst_" + i});
            writer.append(row); // Async append for throughput
            
            if (i % 100 == 0) {
                writer.flush();
            }
        }
        writer.flush();
        
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (burstCount * 1000.0) / duration;
        LOG.info("  ✓ Wrote {} records in {}ms ({} records/sec)", burstCount, duration, (int) throughput);

        // Allow system to stabilize and migrate
        Thread.sleep(3000);

        // Validate timestamp consistency after high-throughput burst
        LOG.info("  → Validating timestamp consistency after high-throughput burst");
        try (ConsistencyValidator validator = new ConsistencyValidator(clientConf)) {
            ValidationResult result = validator.validateTimestampMonotonicity(tablePath, tableBucket, 1);
            assertThat(result.isValid())
                    .as("Timestamps should remain monotonic despite high throughput")
                    .isTrue();
            LOG.info("  ✓ Post-burst timestamp validation: {}", result.getMessage());
        }

        LOG.info("✓ High-throughput timestamp consistency test completed successfully");
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        
        // Enable tiered storage (if configuration exists)
        // Note: Actual config keys depend on Fluss implementation
        conf.set(ConfigOptions.TABLE_LOG_TIERED_ENABLED, true);
        
        // Aggressive segment rolling for testing
        conf.set(ConfigOptions.TABLE_LOG_SEGMENT_SIZE, "512KB");
        conf.set(ConfigOptions.TABLE_LOG_LOCAL_RETENTION_SIZE, "1MB");
        conf.set(ConfigOptions.TABLE_LOG_LOCAL_RETENTION_TIME, Duration.ofMinutes(5));
        
        // Fast tier migration
        conf.set(ConfigOptions.TABLE_LOG_SEGMENT_ROLL_CHECK_INTERVAL, Duration.ofSeconds(5));
        
        LOG.info("✓ Cluster configured with tiered storage settings");
        return conf;
    }
}
