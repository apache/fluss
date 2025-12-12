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

package org.apache.fluss.client;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Connection recovery mode functionality.
 *
 * <p>This test class verifies that:
 *
 * <ul>
 *   <li>Recovery connections can be created
 *   <li>Recovery mode bypasses the merge engine (uses overwrite mode)
 *   <li>Connections can switch from recovery mode to normal mode
 *   <li>The mode switch is one-way and can only happen once
 *   <li>Normal connections use normal mode
 * </ul>
 */
public class ConnectionRecoveryModeTest extends ClientToServerITCaseBase {

    private static final String DEFAULT_DB = "test_db";
    private static final int NUM_BUCKETS = 4;

    /**
     * Test that a recovery connection can be created successfully.
     *
     * <p>Verifies that {@link ConnectionFactory#createRecoveryConnection} returns a valid
     * connection in recovery mode.
     */
    @Test
    public void testCreateRecoveryConnection() throws Exception {
        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(clientConf)) {
            assertThat(recoveryConn).isNotNull();
            assertThat(recoveryConn.isRecoveryMode()).isTrue();
        }
    }

    /**
     * Test that recovery mode bypasses the merge engine and directly overwrites values.
     *
     * <p>When using an aggregate table with SUM aggregation, a normal connection should aggregate
     * values, but a recovery connection should overwrite them.
     */
    @Test
    public void testRecoveryConnectionBypassesMergeEngine() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_recovery_bypass_merge_engine");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("amount", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        // Create aggregate table with SUM aggregation
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.AGGREGATE)
                        .property("table.merge-engine.aggregate.amount", "sum")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Step 1: Write initial value with normal connection (amount = 100)
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            Table table = normalConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 100L));
            writer.flush();

            // Verify: amount should be 100
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 100L));
        }

        // Step 2: Write with normal connection again (amount = 50)
        // Should perform SUM aggregation: 100 + 50 = 150
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            Table table = normalConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 50L));
            writer.flush();

            // Verify: amount should be 150 (aggregated)
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 150L));
        }

        // Step 3: Write with recovery connection (amount = 200)
        // Should bypass merge engine and overwrite directly: 200 (not 150 + 200 = 350)
        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(clientConf)) {
            Table table = recoveryConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 200L));
            writer.flush();

            // Verify: amount should be 200 (overwritten, not aggregated)
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 200L));
        }

        // Step 4: Write with normal connection again (amount = 30)
        // Should resume normal aggregation: 200 + 30 = 230
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            Table table = normalConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 30L));
            writer.flush();

            // Verify: amount should be 230 (aggregated)
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 230L));
        }
    }

    /**
     * Test that a recovery connection can switch to normal mode.
     *
     * <p>After calling {@link Connection#switchToNormalMode()}, subsequent writes should use the
     * merge engine (aggregation) instead of overwrite mode.
     */
    @Test
    public void testSwitchToNormalMode() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_switch_to_normal_mode");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("amount", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.AGGREGATE)
                        .property("table.merge-engine.aggregate.amount", "sum")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(clientConf)) {
            Table table = recoveryConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();

            // Step 1: Write initial value in recovery mode (amount = 100)
            // Should overwrite
            assertThat(recoveryConn.isRecoveryMode()).isTrue();
            writer.upsert(row(1, 100L));
            writer.flush();

            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 100L));

            // Step 2: Write again in recovery mode (amount = 50)
            // Should overwrite (not aggregate): result = 50
            writer.upsert(row(1, 50L));
            writer.flush();

            result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 50L));

            // Step 3: Switch to normal mode
            recoveryConn.switchToNormalMode();
            assertThat(recoveryConn.isRecoveryMode()).isFalse();

            // Step 4: Write after switching (amount = 30)
            // Should use normal merge engine (aggregation): 50 + 30 = 80
            writer.upsert(row(1, 30L));
            writer.flush();

            result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 80L));

            // Step 5: Write again in normal mode (amount = 20)
            // Should continue aggregating: 80 + 20 = 100
            writer.upsert(row(1, 20L));
            writer.flush();

            result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 100L));
        }
    }

    /**
     * Test that {@link Connection#switchToNormalMode()} can only be called once.
     *
     * <p>Calling it a second time should throw {@link IllegalStateException}.
     */
    @Test
    public void testSwitchToNormalModeOnlyOnce() throws Exception {
        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(clientConf)) {
            assertThat(recoveryConn.isRecoveryMode()).isTrue();

            // First call should succeed
            recoveryConn.switchToNormalMode();
            assertThat(recoveryConn.isRecoveryMode()).isFalse();

            // Second call should throw IllegalStateException
            assertThatThrownBy(recoveryConn::switchToNormalMode)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("switchToNormalMode() can only be called once");
        }
    }

    /**
     * Test that {@link Connection#isRecoveryMode()} correctly reports the connection mode.
     *
     * <p>Verifies the state transitions: recovery mode → normal mode (after switch).
     */
    @Test
    public void testIsRecoveryMode() throws Exception {
        // Test recovery connection
        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(clientConf)) {
            assertThat(recoveryConn.isRecoveryMode()).isTrue();

            recoveryConn.switchToNormalMode();
            assertThat(recoveryConn.isRecoveryMode()).isFalse();
        }

        // Test normal connection
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            assertThat(normalConn.isRecoveryMode()).isFalse();

            // switchToNormalMode on normal connection should be idempotent (no effect)
            normalConn.switchToNormalMode();
            assertThat(normalConn.isRecoveryMode()).isFalse();
        }
    }

    /**
     * Test that KvRecordBatch created by a recovery connection has the overwrite flag set.
     *
     * <p>This test verifies that the overwrite flag is correctly propagated from the connection to
     * the record batch. We test this indirectly by verifying the behavior with an aggregate merge
     * engine table.
     */
    @Test
    public void testKvRecordBatchOverwriteFlag() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_kv_batch_overwrite_flag");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.AGGREGATE)
                        .property("table.merge-engine.aggregate.value", "sum")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Write with recovery connection (overwrite flag = true)
        try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(clientConf)) {
            Table table = recoveryConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 100L));
            writer.upsert(row(1, 200L)); // Should overwrite to 200, not aggregate to 300
            writer.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            // If overwrite flag is set correctly, the result should be 200 (last value)
            // If overwrite flag is NOT set, the result would be 300 (sum aggregation)
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 200L));
        }

        // Write with normal connection (overwrite flag = false)
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            Table table = normalConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(2, 100L));
            writer.upsert(row(2, 200L)); // Should aggregate: 100 + 200 = 300
            writer.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(2)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(2, 300L));
        }
    }

    /**
     * Test that a normal connection uses normal mode by default.
     *
     * <p>Verifies that {@link ConnectionFactory#createConnection} creates a connection in normal
     * mode (not recovery mode).
     */
    @Test
    public void testNormalConnectionUsesNormalMode() throws Exception {
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            assertThat(normalConn.isRecoveryMode()).isFalse();
        }

        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_normal_connection_mode");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("amount", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.AGGREGATE)
                        .property("table.merge-engine.aggregate.amount", "sum")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Write with normal connection and verify aggregation works
        try (Connection normalConn = ConnectionFactory.createConnection(clientConf)) {
            assertThat(normalConn.isRecoveryMode()).isFalse();

            Table table = normalConn.getTable(tablePath);
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 100L));
            writer.upsert(row(1, 50L)); // Should aggregate: 100 + 50 = 150
            writer.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(1)).get().getSingletonRow();
            assertThatRow(result).withSchema(schema.getRowType()).isEqualTo(row(1, 150L));
        }
    }
}
