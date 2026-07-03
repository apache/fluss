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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.server.testutils.ServerTestTags;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.record.TestData.ANOTHER_DATA1;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.log.LogManager.CLEAN_SHUTDOWN_FILE;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogManager}. */
final class LogManagerTest extends LogTestBase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;
    private @TempDir File tempDir;
    private TablePath tablePath1;
    private TablePath tablePath2;
    private TableBucket tableBucket1;
    private TableBucket tableBucket2;
    private LocalDiskManager localDiskManager;
    private LogManager logManager;

    // TODO add more tests refer to kafka's LogManagerTest.

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        super.before();
        if (testInfo.getTags().contains(ServerTestTags.JBOD_MULTI_DIR_TAG)) {
            conf.set(
                    ConfigOptions.DATA_DIRS,
                    Arrays.asList(
                            new File(tempDir, "data-1").getAbsolutePath(),
                            new File(tempDir, "data-2").getAbsolutePath()));
        } else {
            conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());
        }
        conf.setString(ConfigOptions.COORDINATOR_HOST, "localhost");
        conf.set(ConfigOptions.TABLET_SERVER_ID, 1);

        String dbName = "db1";
        tablePath1 = TablePath.of(dbName, "t1");
        tablePath2 = TablePath.of(dbName, "t2");

        registerTableInZkClient();
        localDiskManager = LocalDiskManager.create(conf);
        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        logManager.startup();
    }

    private void registerTableInZkClient() throws Exception {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
        zkClient.registerTable(
                tablePath1,
                TableRegistration.newTable(
                        DATA1_TABLE_ID, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        zkClient.registerFirstSchema(tablePath1, DATA1_SCHEMA);
        zkClient.registerTable(
                tablePath2,
                TableRegistration.newTable(
                        DATA2_TABLE_ID, DEFAULT_REMOTE_DATA_DIR, DATA2_TABLE_DESCRIPTOR));
        zkClient.registerFirstSchema(tablePath2, DATA2_SCHEMA);
    }

    static List<String> partitionProvider() {
        return Arrays.asList(null, "2024");
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testCreateLog(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        LogTablet log1 = getOrCreateLog(tablePath1, partitionName, tableBucket1);
        LogTablet log2 = getOrCreateLog(tablePath2, partitionName, tableBucket2);

        MemoryLogRecords mr1 = genMemoryLogRecordsByObject(DATA1);
        log1.appendAsLeader(mr1);

        MemoryLogRecords mr2 = genMemoryLogRecordsByObject(DATA1);
        log2.appendAsLeader(mr2);

        LogTablet newLog1 = getOrCreateLog(tablePath1, partitionName, tableBucket1);
        LogTablet newLog2 = getOrCreateLog(tablePath2, partitionName, tableBucket2);

        FetchDataInfo fetchDataInfo1 = readLog(newLog1);
        FetchDataInfo fetchDataInfo2 = readLog(newLog2);

        assertLogRecordsEquals(DATA1_ROW_TYPE, fetchDataInfo1.getRecords(), DATA1);
        assertLogRecordsEquals(DATA1_ROW_TYPE, fetchDataInfo2.getRecords(), DATA1);
    }

    @Test
    void testGetNonExistentLog() {
        Optional<LogTablet> log = logManager.getLog(new TableBucket(1001, 1));
        assertThat(log.isPresent()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testCheckpointRecoveryPoints(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        LogTablet log1 = getOrCreateLog(tablePath1, partitionName, tableBucket1);
        for (int i = 0; i < 50; i++) {
            MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
            log1.appendAsLeader(mr);
        }
        log1.flush(false);

        LogTablet log2 = getOrCreateLog(tablePath2, partitionName, tableBucket2);
        for (int i = 0; i < 50; i++) {
            MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
            log2.appendAsLeader(mr);
        }
        log2.flush(false);

        logManager.checkpointRecoveryOffsets(tempDir);
        Map<TableBucket, Long> checkpoints =
                new OffsetCheckpointFile(
                                new File(tempDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                        .read();

        assertThat(checkpoints.get(tableBucket1)).isEqualTo(log1.getRecoveryPoint());
        assertThat(checkpoints.get(tableBucket2)).isEqualTo(log2.getRecoveryPoint());
    }

    @Test
    void testRecoveryAfterLogManagerShutdown() throws Exception {
        initTableBuckets(null);
        LogTablet log1 = getOrCreateLog(tablePath1, null, tableBucket1);
        for (int i = 0; i < 50; i++) {
            MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
            log1.appendAsLeader(mr);
        }

        LogTablet log2 = getOrCreateLog(tablePath2, null, tableBucket2);
        for (int i = 0; i < 50; i++) {
            MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
            log2.appendAsLeader(mr);
        }

        logManager.shutdown();
        logManager = null;

        LogManager newLogManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        newLogManager.startup();
        logManager = newLogManager;
        log1 = getOrCreateLog(tablePath1, null, tableBucket1);
        log2 = getOrCreateLog(tablePath2, null, tableBucket2);
        Map<TableBucket, Long> checkpoints =
                new OffsetCheckpointFile(
                                new File(tempDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                        .read();

        assertThat(checkpoints.get(tableBucket1)).isEqualTo(log1.getRecoveryPoint());
        assertThat(checkpoints.get(tableBucket2)).isEqualTo(log2.getRecoveryPoint());

        newLogManager.shutdown();
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testHasCleanShutdownMarkerAfterLogManagerShutdown(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        LogTablet log1 = getOrCreateLog(tablePath1, partitionName, tableBucket1);
        for (int i = 0; i < 50; i++) {
            MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
            log1.appendAsLeader(mr);
        }

        LogTablet log2 = getOrCreateLog(tablePath2, partitionName, tableBucket2);
        for (int i = 0; i < 50; i++) {
            MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
            log2.appendAsLeader(mr);
        }

        // test clean shutdown.
        logManager.shutdown();
        logManager = null;

        String dataDir = conf.getString(ConfigOptions.DATA_DIR);
        assertThat(new File(dataDir, CLEAN_SHUTDOWN_FILE).exists()).isTrue();

        LogManager newLogManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        assertThat(new File(dataDir, CLEAN_SHUTDOWN_FILE).exists()).isTrue();
        newLogManager.startup();
        logManager = newLogManager;
        assertThat(new File(dataDir, CLEAN_SHUTDOWN_FILE).exists()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testSameTableNameInDifferentDb(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        LogTablet log1 = getOrCreateLog(TablePath.of("db1", "t1"), partitionName, tableBucket1);
        MemoryLogRecords mr1 = genMemoryLogRecordsByObject(DATA1);
        log1.appendAsLeader(mr1);

        // Different db with same table name.
        LogTablet log2 =
                getOrCreateLog(
                        TablePath.of("db2", "t1"),
                        partitionName,
                        new TableBucket(15002L, tableBucket1.getPartitionId(), 2));
        MemoryLogRecords mr2 = genMemoryLogRecordsByObject(ANOTHER_DATA1);
        log2.appendAsLeader(mr2);

        FetchDataInfo fetchDataInfo1 = readLog(log1);
        FetchDataInfo fetchDataInfo2 = readLog(log2);

        assertLogRecordsEquals(DATA1_ROW_TYPE, fetchDataInfo1.getRecords(), DATA1);
        assertLogRecordsEquals(DATA1_ROW_TYPE, fetchDataInfo2.getRecords(), ANOTHER_DATA1);
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testDeleteLog(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        LogTablet log1 = getOrCreateLog(tablePath1, partitionName, tableBucket1);
        logManager.dropLog(log1.getTableBucket());

        assertThat(log1.getLogDir().exists()).isFalse();
        assertThat(logManager.getLog(log1.getTableBucket()).isPresent()).isFalse();

        log1 = getOrCreateLog(tablePath1, partitionName, tableBucket1);
        assertThat(logManager.getLog(log1.getTableBucket()).isPresent()).isTrue();
    }

    @Test
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testCheckpointRecoveryPointsAreWrittenPerDirectory() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        initTableBuckets(null);

        LogTablet log1 = createLog(tablePath1, tableBucket1, dataDir1);
        LogTablet log2 = createLog(tablePath2, tableBucket2, dataDir2);

        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        log1.appendAsLeader(records);
        log2.appendAsLeader(records);
        log1.flush(false);
        log2.flush(false);

        logManager.checkpointRecoveryOffsets(dataDir1);
        logManager.checkpointRecoveryOffsets(dataDir2);

        Map<TableBucket, Long> dir1Checkpoints =
                new OffsetCheckpointFile(
                                new File(dataDir1, LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                        .read();
        Map<TableBucket, Long> dir2Checkpoints =
                new OffsetCheckpointFile(
                                new File(dataDir2, LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                        .read();

        assertThat(dir1Checkpoints).containsOnlyKeys(tableBucket1);
        assertThat(dir1Checkpoints.get(tableBucket1)).isEqualTo(log1.getRecoveryPoint());
        assertThat(dir2Checkpoints).containsOnlyKeys(tableBucket2);
        assertThat(dir2Checkpoints.get(tableBucket2)).isEqualTo(log2.getRecoveryPoint());
    }

    @Test
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testPerDirectoryCleanShutdownAndRecovery() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        initTableBuckets(null);

        createLog(tablePath1, tableBucket1, dataDir1)
                .appendAsLeader(genMemoryLogRecordsByObject(DATA1));
        createLog(tablePath2, tableBucket2, dataDir2)
                .appendAsLeader(genMemoryLogRecordsByObject(DATA1));

        logManager.shutdown();
        logManager = null;
        localDiskManager.close();

        assertThat(new File(dataDir1, LogManager.CLEAN_SHUTDOWN_FILE)).exists();
        assertThat(new File(dataDir2, LogManager.CLEAN_SHUTDOWN_FILE)).exists();

        localDiskManager = LocalDiskManager.create(conf);
        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        logManager.startup();

        assertThat(new File(dataDir1, LogManager.CLEAN_SHUTDOWN_FILE)).doesNotExist();
        assertThat(new File(dataDir2, LogManager.CLEAN_SHUTDOWN_FILE)).doesNotExist();
        assertThat(logManager.getLog(tableBucket1)).isPresent();
        assertThat(logManager.getLog(tableBucket1).get().getDataDir())
                .isEqualTo(dataDir1.getAbsoluteFile());
        assertThat(logManager.getLog(tableBucket2)).isPresent();
        assertThat(logManager.getLog(tableBucket2).get().getDataDir())
                .isEqualTo(dataDir2.getAbsoluteFile());
    }

    private LogTablet getOrCreateLog(
            TablePath tablePath, String partitionName, TableBucket tableBucket) throws Exception {
        return logManager.getOrCreateLog(
                tempDir,
                PhysicalTablePath.of(
                        tablePath.getDatabaseName(), tablePath.getTableName(), partitionName),
                tableBucket,
                LogFormat.ARROW,
                1,
                false);
    }

    private LogTablet createLog(TablePath tablePath, TableBucket tableBucket, File dataDir)
            throws Exception {
        return logManager.getOrCreateLog(
                dataDir, PhysicalTablePath.of(tablePath), tableBucket, LogFormat.ARROW, 1, false);
    }

    private void initTableBuckets(@Nullable String partitionName) {
        if (partitionName == null) {
            tableBucket1 = new TableBucket(DATA1_TABLE_ID, 1);
            tableBucket2 = new TableBucket(DATA2_TABLE_ID, 2);
        } else {
            tableBucket1 = new TableBucket(DATA1_TABLE_ID, 11L, 1);
            tableBucket2 = new TableBucket(DATA2_TABLE_ID, 11L, 2);
        }
    }

    private FetchDataInfo readLog(LogTablet log) throws Exception {
        return log.read(0, Integer.MAX_VALUE, FetchIsolation.LOG_END, true, null, null);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (logManager != null) {
            logManager.shutdown();
        }
        if (localDiskManager != null) {
            localDiskManager.close();
        }
    }
}
