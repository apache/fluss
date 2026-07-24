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
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
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

    @Test
    void testResidualKvAndLogRemovedWhenTableDropped() throws Exception {
        initTableBuckets(null);
        // Create the on-disk log dir and a corresponding kv tablet dir.
        LogTablet log1 = getOrCreateLog(tablePath1, null, tableBucket1);
        File logDir = log1.getLogDir();
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath1);
        File kvTabletDir = FlussPaths.kvTabletDir(tempDir, physicalTablePath, tableBucket1);
        assertThat(kvTabletDir.mkdirs()).isTrue();
        assertThat(logDir).exists();
        assertThat(kvTabletDir).exists();

        // Drop the table from ZK so that loadLog sees the schema is gone (residual data).
        logManager.shutdown();
        logManager = null;
        zkClient.deleteTable(tablePath1);

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

        // Both the kv and log tablet directories should be removed.
        assertThat(kvTabletDir).doesNotExist();
        assertThat(logDir).doesNotExist();
    }

    @Test
    void testLogKeptWhenResidualKvDeletionFails() throws Exception {
        initTableBuckets(null);
        // Create the on-disk log dir, then place a regular FILE where the kv tablet dir would be,
        // so FileUtils.deleteDirectory fails (it is not a directory).
        LogTablet log1 = getOrCreateLog(tablePath1, null, tableBucket1);
        File logDir = log1.getLogDir();
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath1);
        File kvTabletDir = FlussPaths.kvTabletDir(tempDir, physicalTablePath, tableBucket1);
        // The table dir already exists (created alongside the log dir); create the kv path as a
        // regular file so FileUtils.deleteDirectory fails (it is not a directory).
        assertThat(kvTabletDir.createNewFile()).isTrue();
        assertThat(kvTabletDir).isFile();
        assertThat(logDir).exists();

        // Drop the table from ZK so that loadLog sees the schema is gone (residual data).
        logManager.shutdown();
        logManager = null;
        zkClient.deleteTable(tablePath1);

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

        // KV deletion failed, so the log must be kept as an anchor for a later retry.
        assertThat(logDir).exists();
        assertThat(kvTabletDir).exists();
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

    @Test
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testSkipCleanShutdownMarkerForDataDirWithFailedLogClose() throws Exception {
        File failedDataDir = new File(tempDir, "data-1");
        File healthyDataDir = new File(tempDir, "data-2");
        initTableBuckets(null);

        LogTablet failedLog = createLog(tablePath1, tableBucket1, failedDataDir);
        failedLog.appendAsLeader(genMemoryLogRecordsByObject(DATA1));
        createLog(tablePath2, tableBucket2, healthyDataDir)
                .appendAsLeader(genMemoryLogRecordsByObject(DATA1));

        // Make the shutdown flush fail by closing the underlying file channels first.
        failedLog.close();
        logManager.shutdown();
        logManager = null;

        assertThat(new File(failedDataDir, CLEAN_SHUTDOWN_FILE)).doesNotExist();
        assertThat(new File(healthyDataDir, CLEAN_SHUTDOWN_FILE)).exists();
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
    void testPeriodicLogManagerTasks() throws Exception {
        logManager.shutdown();
        logManager = null;
        localDiskManager.close();
        localDiskManager = LocalDiskManager.create(conf);

        RecordingScheduler scheduler = new RecordingScheduler();
        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        scheduler,
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        logManager.startup();

        RecordingScheduledTask checkpointTask =
                scheduler.getTask("fluss-recovery-point-checkpoint");
        assertThat((Object) checkpointTask).isNotNull();
        assertThat(checkpointTask.getDelayMs()).isEqualTo(60_000L);
        assertThat(checkpointTask.getPeriodMs()).isEqualTo(60_000L);

        RecordingScheduledTask retentionTask =
                scheduler.getTask("fluss-tiered-local-log-retention");
        assertThat((Object) retentionTask).isNotNull();
        assertThat(retentionTask.getDelayMs()).isEqualTo(300_000L);
        assertThat(retentionTask.getPeriodMs()).isEqualTo(300_000L);

        initTableBuckets(null);
        LogTablet log1 = getOrCreateLog(tablePath1, null, tableBucket1);
        log1.appendAsLeader(genMemoryLogRecordsByObject(DATA1));
        log1.flush(false);

        checkpointTask.run();

        Map<TableBucket, Long> checkpoints =
                new OffsetCheckpointFile(
                                new File(tempDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                        .read();
        assertThat(checkpoints).containsEntry(tableBucket1, log1.getRecoveryPoint());

        logManager.shutdown();
        logManager = null;
        assertThat(checkpointTask.isCancelled()).isTrue();
        assertThat(retentionTask.isCancelled()).isTrue();
    }

    @Test
    void testLogManagerPeriodicallyCheckpointsRecoveryPoints() throws Exception {
        logManager.shutdown();
        logManager = null;
        localDiskManager.close();

        conf.set(ConfigOptions.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL, Duration.ofMillis(10));
        localDiskManager = LocalDiskManager.create(conf);

        FlussScheduler scheduler = new FlussScheduler(1);
        scheduler.startup();
        try {
            logManager =
                    LogManager.create(
                            conf,
                            zkClient,
                            scheduler,
                            SystemClock.getInstance(),
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            localDiskManager);
            logManager.startup();

            initTableBuckets(null);
            LogTablet log1 = getOrCreateLog(tablePath1, null, tableBucket1);
            log1.appendAsLeader(genMemoryLogRecordsByObject(DATA1));
            log1.flush(false);

            waitUntil(
                    () -> {
                        Map<TableBucket, Long> checkpoints =
                                new OffsetCheckpointFile(
                                                new File(
                                                        tempDir,
                                                        LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                                        .read();
                        return checkpoints.containsKey(tableBucket1)
                                && checkpoints.get(tableBucket1).equals(log1.getRecoveryPoint());
                    },
                    Duration.ofSeconds(2),
                    "Timed out waiting for periodic recovery point checkpoint.");
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void testRecoveryPointCheckpointSkipsFlushedSegmentsDuringRecovery() throws Exception {
        initTableBuckets(null);
        LogTablet log1 = getOrCreateLog(tablePath1, null, tableBucket1);
        log1.appendAsLeader(genMemoryLogRecordsByObject(DATA1));

        File firstSegmentFile = FlussPaths.logFile(log1.getLogDir(), 0L);
        assertThat(firstSegmentFile).exists();
        long cleanSegmentSize = firstSegmentFile.length();

        log1.roll(Optional.empty());
        long rolledSegmentBaseOffset = log1.activeLogSegment().getBaseOffset();
        log1.appendAsLeader(genMemoryLogRecordsByObject(ANOTHER_DATA1));
        log1.flush(false);
        long recoveryPoint = log1.getRecoveryPoint();
        assertThat(recoveryPoint).isGreaterThan(rolledSegmentBaseOffset);

        logManager.checkpointRecoveryOffsets(tempDir);
        Map<TableBucket, Long> checkpoints =
                new OffsetCheckpointFile(
                                new File(tempDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE))
                        .read();
        assertThat(checkpoints).containsEntry(tableBucket1, recoveryPoint);

        logManager.shutdown();
        logManager = null;
        Files.deleteIfExists(new File(tempDir, CLEAN_SHUTDOWN_FILE).toPath());

        assertThat(firstSegmentFile.length()).isEqualTo(cleanSegmentSize);
        appendInvalidBytes(firstSegmentFile);
        long corruptSegmentSize = firstSegmentFile.length();
        assertThat(corruptSegmentSize).isGreaterThan(cleanSegmentSize);

        localDiskManager.close();
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

        assertThat(logManager.getLog(tableBucket1)).isPresent();
        assertThat(firstSegmentFile.length()).isEqualTo(corruptSegmentSize);
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

    private static void appendInvalidBytes(File file) throws IOException {
        try (FileOutputStream outputStream = new FileOutputStream(file, true)) {
            outputStream.write(new byte[] {0, 1, 2, 3});
        }
    }

    private static final class RecordingScheduler implements Scheduler {
        private final Map<String, RecordingScheduledTask> tasks = new HashMap<>();

        @Override
        public void startup() {}

        @Override
        public void shutdown() {}

        @Override
        public ScheduledFuture<?> schedule(
                String name, Runnable task, long delayMs, long periodMs) {
            RecordingScheduledTask scheduledTask =
                    new RecordingScheduledTask(task, delayMs, periodMs);
            tasks.put(name, scheduledTask);
            return scheduledTask;
        }

        private RecordingScheduledTask getTask(String name) {
            return tasks.get(name);
        }
    }

    private static final class RecordingScheduledTask implements ScheduledFuture<Void> {
        private final Runnable task;
        private final long delayMs;
        private final long periodMs;
        private boolean cancelled;

        private RecordingScheduledTask(Runnable task, long delayMs, long periodMs) {
            this.task = task;
            this.delayMs = delayMs;
            this.periodMs = periodMs;
        }

        private long getDelayMs() {
            return delayMs;
        }

        private long getPeriodMs() {
            return periodMs;
        }

        private void run() {
            task.run();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }
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
