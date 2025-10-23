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

package org.apache.fluss.server.log;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for handling recovery from residual data of already dropped tables. */
final class DroppedTableRecoveryTest extends LogTestBase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;
    private @TempDir File tempDir;
    private TablePath tablePath;
    private TableBucket tableBucket;
    private LogManager logManager;
    private KvManager kvManager;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    public void setup() throws Exception {
        super.before();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());

        String dbName = "test_db";
        tablePath = TablePath.of(dbName, "dropped_table");
        tableBucket = new TableBucket(DATA1_TABLE_ID, 1);

        registerTableInZkClient();
        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        logManager.startup();

        kvManager =
                KvManager.create(
                        conf, zkClient, logManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager.startup();
    }

    private void registerTableInZkClient() throws Exception {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
        zkClient.registerTable(
                tablePath, TableRegistration.newTable(DATA1_TABLE_ID, DATA1_TABLE_DESCRIPTOR));
        zkClient.registerSchema(tablePath, DATA1_SCHEMA);
    }

    @AfterEach
    public void tearDown() {
        if (kvManager != null) {
            kvManager.shutdown();
        }
        if (logManager != null) {
            logManager.shutdown();
        }
    }

    @Test
    void testMultipleLogTabletResidualDataDirectoriesCleanup() throws Exception {
        // Create multiple logs for the same table
        TableBucket tableBucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket tableBucket2 = new TableBucket(DATA1_TABLE_ID, 2);

        LogTablet log1 =
                logManager.getOrCreateLog(
                        PhysicalTablePath.of(tablePath), tableBucket1, LogFormat.ARROW, 1, false);
        LogTablet log2 =
                logManager.getOrCreateLog(
                        PhysicalTablePath.of(tablePath), tableBucket2, LogFormat.ARROW, 1, false);

        // Write some data to both logs
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        log1.appendAsLeader(records);
        log2.appendAsLeader(records);
        log1.flush(false);
        log2.flush(false);

        // Get log directories before shutdown
        String logDir1 = log1.getLogDir().getAbsolutePath();
        String logDir2 = log2.getLogDir().getAbsolutePath();

        // Shutdown log manager first
        logManager.shutdown();

        // Simulate table drop: remove metadata from ZooKeeper
        zkClient.deleteTable(tablePath);

        // Start LogManager again
        LogManager newLogManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        newLogManager.startup();

        // Verify that both residual data directories were cleaned up
        assertThat(new File(logDir1)).doesNotExist();
        assertThat(new File(logDir2)).doesNotExist();

        newLogManager.shutdown();
    }

    @Test
    void testLogTabletResidualDataCleanupWithPartitionedTable() throws Exception {
        // Create a partitioned table log
        TableBucket partitionedTableBucket = new TableBucket(DATA1_TABLE_ID, 2024L, 1);
        PhysicalTablePath partitionedTablePath =
                PhysicalTablePath.of(tablePath.getDatabaseName(), tablePath.getTableName(), "2024");

        LogTablet log =
                logManager.getOrCreateLog(
                        partitionedTablePath, partitionedTableBucket, LogFormat.ARROW, 1, false);

        // Write some data to the log
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        log.appendAsLeader(records);
        log.flush(false);

        String logDir = log.getLogDir().getAbsolutePath();

        // Shutdown log manager first
        logManager.shutdown();

        // Simulate table drop: remove metadata from ZooKeeper
        zkClient.deleteTable(tablePath);

        // Start LogManager again
        LogManager newLogManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        newLogManager.startup();

        // Verify that the residual data directory was cleaned up
        assertThat(new File(logDir)).doesNotExist();

        newLogManager.shutdown();
    }

    @Test
    void testMultipleKvTabletResidualDataDirectoriesCleanup() throws Exception {
        // Create multiple logs and KV tablets for the same table
        TableBucket tableBucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket tableBucket2 = new TableBucket(DATA1_TABLE_ID, 2);

        LogTablet log1 =
                logManager.getOrCreateLog(
                        PhysicalTablePath.of(tablePath), tableBucket1, LogFormat.ARROW, 1, false);
        LogTablet log2 =
                logManager.getOrCreateLog(
                        PhysicalTablePath.of(tablePath), tableBucket2, LogFormat.ARROW, 1, false);

        // Write some data to both logs
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        log1.appendAsLeader(records);
        log2.appendAsLeader(records);
        log1.flush(false);
        log2.flush(false);

        // Create KV tablets
        TableConfig tableConfig =
                new TableConfig(Configuration.fromMap(DATA1_TABLE_DESCRIPTOR.getProperties()));
        KvTablet kvTablet1 =
                kvManager.getOrCreateKv(
                        PhysicalTablePath.of(tablePath),
                        tableBucket1,
                        log1,
                        KvFormat.COMPACTED,
                        DATA1_SCHEMA,
                        tableConfig,
                        DEFAULT_COMPRESSION);
        KvTablet kvTablet2 =
                kvManager.getOrCreateKv(
                        PhysicalTablePath.of(tablePath),
                        tableBucket2,
                        log2,
                        KvFormat.COMPACTED,
                        DATA1_SCHEMA,
                        tableConfig,
                        DEFAULT_COMPRESSION);

        // Get directories before shutdown
        String kvDir1 = kvTablet1.getKvTabletDir().getAbsolutePath();
        String kvDir2 = kvTablet2.getKvTabletDir().getAbsolutePath();
        String logDir1 = log1.getLogDir().getAbsolutePath();
        String logDir2 = log2.getLogDir().getAbsolutePath();

        // Shutdown managers first
        kvManager.shutdown();
        logManager.shutdown();

        // Simulate table drop: remove metadata from ZooKeeper
        zkClient.deleteTable(tablePath);

        // Start managers again
        LogManager newLogManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        newLogManager.startup(); // Should clean up log directories

        KvManager newKvManager =
                KvManager.create(
                        conf, zkClient, newLogManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        newKvManager.startup();

        // KV tablet directories should be cleaned up by LogManager automatically

        newKvManager.shutdown();
        newLogManager.shutdown();

        // Verify that all residual data directories were cleaned up
        assertThat(new File(logDir1)).doesNotExist();
        assertThat(new File(logDir2)).doesNotExist();
        assertThat(new File(kvDir1)).doesNotExist(); // Also cleaned by LogManager
        assertThat(new File(kvDir2)).doesNotExist();
    }

    @Test
    void testKvTabletResidualDataCleanupWithPartitionedTable() throws Exception {
        // Create a partitioned table KV tablet
        TableBucket partitionedTableBucket = new TableBucket(DATA1_TABLE_ID, 2024L, 1);
        PhysicalTablePath partitionedTablePath =
                PhysicalTablePath.of(tablePath.getDatabaseName(), tablePath.getTableName(), "2024");

        LogTablet log =
                logManager.getOrCreateLog(
                        partitionedTablePath, partitionedTableBucket, LogFormat.ARROW, 1, false);

        // Write some data to the log
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        log.appendAsLeader(records);
        log.flush(false);

        // Create KV tablet
        TableConfig tableConfig =
                new TableConfig(Configuration.fromMap(DATA1_TABLE_DESCRIPTOR.getProperties()));
        KvTablet kvTablet =
                kvManager.getOrCreateKv(
                        partitionedTablePath,
                        partitionedTableBucket,
                        log,
                        KvFormat.COMPACTED,
                        DATA1_SCHEMA,
                        tableConfig,
                        DEFAULT_COMPRESSION);

        String kvDir = kvTablet.getKvTabletDir().getAbsolutePath();
        String logDir = log.getLogDir().getAbsolutePath();

        // Shutdown managers first
        kvManager.shutdown();
        logManager.shutdown();

        // Simulate table drop: remove metadata from ZooKeeper
        zkClient.deleteTable(tablePath);

        // Start managers again
        LogManager newLogManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        newLogManager.startup();

        KvManager newKvManager =
                KvManager.create(
                        conf, zkClient, newLogManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        newKvManager.startup();

        // KV tablet directory should be cleaned up by LogManager automatically

        newKvManager.shutdown();
        newLogManager.shutdown();

        // Verify that both residual data directories were cleaned up
        assertThat(new File(logDir)).doesNotExist();
        assertThat(new File(kvDir)).doesNotExist(); // Also cleaned by LogManager
    }
}
