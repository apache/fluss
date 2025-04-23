/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogTestBase;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.server.log.checkpoint.OffsetCheckpointFile;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import static com.alibaba.fluss.record.TestData.ANOTHER_DATA1;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA2_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_ID;
import static com.alibaba.fluss.server.log.LogManager.CLEAN_SHUTDOWN_FILE;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
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
    public void setup() throws Exception {
        super.before();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());
        conf.setString(ConfigOptions.COORDINATOR_HOST, "localhost");

        String dbName = "db1";
        tablePath1 = TablePath.of(dbName, "t1");
        tablePath2 = TablePath.of(dbName, "t2");

        registerTableInZkClient();
        logManager =
                LogManager.create(conf, zkClient, new FlussScheduler(1), SystemClock.getInstance());
        logManager.startup();
    }

    private void registerTableInZkClient() throws Exception {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
        zkClient.registerTable(
                tablePath1, TableRegistration.newTable(DATA1_TABLE_ID, DATA1_TABLE_DESCRIPTOR));
        zkClient.registerSchema(tablePath1, DATA1_SCHEMA);
        zkClient.registerTable(
                tablePath2, TableRegistration.newTable(DATA2_TABLE_ID, DATA2_TABLE_DESCRIPTOR));
        zkClient.registerSchema(tablePath2, DATA2_SCHEMA);
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

        logManager.checkpointRecoveryOffsets();
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
                LogManager.create(conf, zkClient, new FlussScheduler(1), SystemClock.getInstance());
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
                LogManager.create(conf, zkClient, new FlussScheduler(1), SystemClock.getInstance());
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

    private LogTablet getOrCreateLog(
            TablePath tablePath, String partitionName, TableBucket tableBucket) throws Exception {
        return logManager.getOrCreateLog(
                PhysicalTablePath.of(
                        tablePath.getDatabaseName(), tablePath.getTableName(), partitionName),
                tableBucket,
                LogFormat.ARROW,
                1,
                false);
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
        return log.read(0, Integer.MAX_VALUE, FetchIsolation.LOG_END, true, null);
    }

    @AfterEach
    public void tearDown() {
        if (logManager != null) {
            logManager.shutdown();
        }
    }
}
