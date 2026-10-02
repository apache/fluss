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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Test for {@link KvManager} . */
final class KvManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private final RowType baseRowType = TestData.DATA1_ROW_TYPE;
    private static final short schemaId = 1;
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(schemaId);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(baseRowType);

    private static ZooKeeperClient zkClient;

    private @TempDir File tempDir;
    private TablePath tablePath1;
    private TablePath tablePath2;

    private TableBucket tableBucket1;
    private TableBucket tableBucket2;

    private LocalDiskManager localDiskManager;
    private LogManager logManager;
    private KvManager kvManager;
    private Configuration conf;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setup() throws Exception {
        conf = new Configuration();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());
        conf.set(ConfigOptions.TABLET_SERVER_ID, 1);

        String dbName = "db1";
        tablePath1 = TablePath.of(dbName, "t1");
        tablePath2 = TablePath.of(dbName, "t2");

        createManagers();
    }

    private void createManagers() throws Exception {
        // we need a log manager for kv manager
        localDiskManager = LocalDiskManager.create(conf);
        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        kvManager =
                KvManager.create(
                        conf,
                        zkClient,
                        logManager,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        kvManager.startup();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (kvManager != null) {
            kvManager.shutdown();
        }
        if (logManager != null) {
            logManager.shutdown();
        }
        if (localDiskManager != null) {
            localDiskManager.close();
        }
    }

    static List<String> partitionProvider() {
        return Arrays.asList(null, "2024");
    }

    @Test
    void testStartupCleanupAcrossDisks(@TempDir File secondDataDir) throws Exception {
        configureTwoDataDirs(secondDataDir);

        List<Path> staleDirs = new ArrayList<>();
        List<Path> retainedFiles = new ArrayList<>();
        List<Path> emptyParentDirs = new ArrayList<>();
        for (File dataDir : localDiskManager.dataDirs()) {
            for (String path :
                    Arrays.asList(
                            "db/table-1/kv-0",
                            "db/table-2/20260917-p2/kv-1",
                            "dropped/table-3/kv-0",
                            "dropped/table-4/20260917-p4/kv-2")) {
                Path kvDir = dataDir.toPath().resolve(path);
                Files.createDirectories(kvDir.resolve("db"));
                Files.write(kvDir.resolve("db/000001.sst"), new byte[] {1, 2, 3});
                staleDirs.add(kvDir);
            }
            emptyParentDirs.add(dataDir.toPath().resolve("dropped/table-3"));
            emptyParentDirs.add(dataDir.toPath().resolve("dropped/table-4"));
            emptyParentDirs.add(dataDir.toPath().resolve("dropped"));
            for (String path :
                    Arrays.asList(
                            "db/table-1/log-0/segment.log",
                            "db/table-2/20260917-p2/log-1/segment.log",
                            "db/table-1/backup/file",
                            FlussPaths.HISTORICAL_LOOKUP_CACHE_DIR_NAME + "/table-1/kv-0/file",
                            FlussPaths.REMOTE_LOG_INDEX_LOCAL_CACHE + "/table-1/kv-0/file",
                            "recovery-point-offset-checkpoint")) {
                Path retainedFile = dataDir.toPath().resolve(path);
                Files.createDirectories(retainedFile.getParent());
                Files.write(retainedFile, new byte[] {4, 5, 6});
                retainedFiles.add(retainedFile);
            }
        }

        kvManager.startup();
        // A second startup cleanup must also succeed when the orphan directories are gone.
        kvManager.startup();

        for (Path staleDir : staleDirs) {
            assertThat(staleDir).doesNotExist();
        }
        for (Path parentDir : emptyParentDirs) {
            assertThat(parentDir).doesNotExist();
        }
        for (Path retainedFile : retainedFiles) {
            assertThat(Files.readAllBytes(retainedFile)).containsExactly(4, 5, 6);
        }
        for (File dataDir : localDiskManager.dataDirs()) {
            assertThat(new File(dataDir, LocalDiskManager.DISK_PROPERTIES_FILE_NAME)).isFile();
            assertThat(new File(dataDir, LocalDiskManager.LOCK_FILE_NAME)).isFile();
        }
    }

    private void configureTwoDataDirs(File secondDataDir) throws Exception {
        tearDown();
        kvManager = null;
        logManager = null;
        localDiskManager = null;
        conf.set(
                ConfigOptions.DATA_DIRS,
                Arrays.asList(tempDir.getAbsolutePath(), secondDataDir.getAbsolutePath()));
        createManagers();
    }

    @Test
    void testStartupCleanupRejectsOpenTablets() throws Exception {
        initTableBuckets(null);
        KvTablet kv = getOrCreateKv(tablePath1, null, tableBucket1);
        byte[] key = "live-key".getBytes(StandardCharsets.UTF_8);
        KvRecord record = kvRecordFactory.ofRecord(key, new Object[] {1, "value"});
        put(kv, record);

        assertThatThrownBy(kvManager::startup)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot clean KV directories while KV tablets are open.");

        assertThat(kv.getKvTabletDir()).isDirectory();
        verifyMultiGet(kv, key, valueOf(record));
    }

    @Test
    void testStartupCleanupContinuesOnOtherDisksAfterDeletionFailure(@TempDir File secondDataDir)
            throws Exception {
        configureTwoDataDirs(secondDataDir);
        Path otherKvDir =
                Files.createDirectories(secondDataDir.toPath().resolve("db/table-2/kv-0"));
        Path kvDir = tempDir.toPath().resolve("db/table-1/kv-0");
        Path dbDir = Files.createDirectories(kvDir.resolve("db"));
        Files.write(dbDir.resolve("data"), new byte[] {1});
        File tableDir = kvDir.getParent().toFile();
        try {
            assertThat(dbDir.toFile().setWritable(false)).isTrue();
            assumeThat(Files.isWritable(dbDir)).isFalse();

            kvManager.startup();
            assertThat(kvDir).doesNotExist();
            File[] pendingDirs = tableDir.listFiles(File::isDirectory);
            assertThat(pendingDirs).hasSize(1);
            assertThat(pendingDirs[0].getName()).endsWith(FlussPaths.DELETED_FILE_SUFFIX);
            assertThat(Files.readAllBytes(pendingDirs[0].toPath().resolve("db/data")))
                    .containsExactly(1);
            assertThat(otherKvDir).doesNotExist();
        } finally {
            makeKvStoreDirectoriesWritable(tableDir);
        }

        // Cleanup can be retried once the disk problem is resolved.
        kvManager.startup();
        assertThat(tableDir).doesNotExist();
    }

    @Test
    void testStartupCleanupRetriesPendingDeletionWithoutBlockingNewTablet() throws Exception {
        Path kvDir = Files.createDirectories(tempDir.toPath().resolve("db/table-1/kv-0"));
        Files.write(kvDir.resolve("data"), new byte[] {1});
        Path pendingDir = Files.createDirectory(kvDir.resolveSibling("kv-0.deleted"));
        Path pendingFile = Files.write(pendingDir.resolve("data"), new byte[] {2});
        try {
            assertThat(pendingDir.toFile().setWritable(false)).isTrue();
            assumeThat(Files.isWritable(pendingDir)).isFalse();

            kvManager.startup();
            // Retrying an older deletion must neither rename it again nor prevent isolation of
            // the current tablet, regardless of directory enumeration order.
            kvManager.startup();

            assertThat(kvDir).doesNotExist();
            assertThat(kvDir.getParent().toFile().listFiles()).containsExactly(pendingDir.toFile());
            assertThat(Files.readAllBytes(pendingFile)).containsExactly(2);
        } finally {
            assertThat(pendingDir.toFile().setWritable(true)).isTrue();
        }

        kvManager.startup();
        assertThat(kvDir.getParent()).doesNotExist();
    }

    @Test
    void testStartupCleanupKeepsOriginalDirectoryWhenRenameFails() throws Exception {
        Path kvDir = Files.createDirectories(tempDir.toPath().resolve("db/table-1/kv-0"));
        Path retainedFile = Files.write(kvDir.resolve("data"), new byte[] {1, 2, 3});
        File tableDir = kvDir.getParent().toFile();
        Path otherKvDir = Files.createDirectories(tempDir.toPath().resolve("db/table-2/kv-0"));
        try {
            assertThat(tableDir.setWritable(false)).isTrue();
            assumeThat(tableDir.canWrite()).isFalse();

            kvManager.startup();

            assertThat(Files.readAllBytes(retainedFile)).containsExactly(1, 2, 3);
            assertThat(tableDir.listFiles()).containsExactly(kvDir.toFile());
            assertThat(otherKvDir).doesNotExist();
        } finally {
            assertThat(tableDir.setWritable(true)).isTrue();
        }

        kvManager.startup();
        assertThat(tableDir).doesNotExist();
    }

    private void makeKvStoreDirectoriesWritable(File directory) {
        File[] children = directory.listFiles(File::isDirectory);
        if (children != null) {
            for (File child : children) {
                File dbDir = new File(child, "db");
                if (dbDir.exists()) {
                    assertThat(dbDir.setWritable(true)).isTrue();
                }
            }
        }
    }

    @Test
    void testStartupCleanupWithSymbolicDataRoot(@TempDir Path linkDir) throws Exception {
        tearDown();
        kvManager = null;
        logManager = null;
        localDiskManager = null;
        Path dataLink = Files.createSymbolicLink(linkDir.resolve("data"), tempDir.toPath());
        conf.set(ConfigOptions.DATA_DIR, dataLink.toString());
        createManagers();
        Path staleDir = tempDir.toPath().resolve("db/table-1/kv-0");
        Files.createDirectories(staleDir);
        Files.write(staleDir.resolve("data"), new byte[] {1});

        kvManager.startup();

        assertThat(staleDir).doesNotExist();
        assertThat(Files.isSymbolicLink(dataLink)).isTrue();
        assertThat(tempDir).isDirectory();
    }

    @ParameterizedTest
    @CsvSource({
        "db, table-1/kv-0",
        "db/table-1, kv-0",
        "db/table-1/partition-p1, kv-0",
        "db/table-1/kv-0, db",
        "db/table-1/partition-p1/kv-0, db"
    })
    void testStartupCleanupSkipsSymbolicLinks(
            String linkPath, String targetPath, @TempDir Path outsideDir) throws Exception {
        Path outsideKvDir = Files.createDirectories(outsideDir.resolve(targetPath));
        Path outsideFile = Files.write(outsideKvDir.resolve("data"), new byte[] {1, 2, 3});
        Path link = tempDir.toPath().resolve(linkPath);
        Files.createDirectories(link.getParent());
        Files.createSymbolicLink(link, outsideDir);
        Path staleDir = Files.createDirectories(tempDir.toPath().resolve("other/table-2/kv-0"));

        kvManager.startup();

        assertThat(staleDir).doesNotExist();
        assertThat(Files.readAllBytes(outsideFile)).containsExactly(1, 2, 3);
        assertThat(Files.isSymbolicLink(link)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"db", "db/table-1", "db/table-1/partition-p1"})
    void testStartupCleanupContinuesOnOtherDisksAfterListingFailure(
            String unreadablePath, @TempDir File secondDataDir) throws Exception {
        configureTwoDataDirs(secondDataDir);
        Path otherKvDir =
                Files.createDirectories(secondDataDir.toPath().resolve("db/table-2/kv-0"));
        Path kvDir = tempDir.toPath().resolve("db/table-1/partition-p1/kv-0");
        Files.createDirectories(kvDir);
        Path retainedFile = Files.write(kvDir.resolve("data"), new byte[] {1});
        File unreadableDir = tempDir.toPath().resolve(unreadablePath).toFile();
        try {
            assertThat(unreadableDir.setReadable(false)).isTrue();
            assumeThat(unreadableDir.canRead()).isFalse();

            kvManager.startup();
            assertThat(otherKvDir).doesNotExist();
        } finally {
            assertThat(unreadableDir.setReadable(true)).isTrue();
        }

        assertThat(retainedFile).exists();
        kvManager.startup();
        assertThat(kvDir).doesNotExist();
    }

    @Test
    void testStartupCleanupLeavesExcludedSymbolicLinks(@TempDir Path outsideDir) throws Exception {
        Path outsideFile = Files.write(outsideDir.resolve("data"), new byte[] {1, 2, 3});
        Path kvDir = tempDir.toPath().resolve("db/table-1/kv-0");
        Files.createDirectories(kvDir);
        List<Path> links = new ArrayList<>();
        for (String path :
                Arrays.asList(
                        FlussPaths.HISTORICAL_LOOKUP_CACHE_DIR_NAME,
                        FlussPaths.REMOTE_LOG_INDEX_LOCAL_CACHE,
                        "db/table-1/log-0",
                        "db/table-1/backup")) {
            links.add(Files.createSymbolicLink(tempDir.toPath().resolve(path), outsideDir));
        }

        kvManager.startup();

        assertThat(kvDir).doesNotExist();
        assertThat(Files.readAllBytes(outsideFile)).containsExactly(1, 2, 3);
        for (Path link : links) {
            assertThat(Files.isSymbolicLink(link)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"db/foo/kv-0", "db/table-1/kv-abc", "db/foo/partition-p1/kv-abc"})
    void testStartupCleanupDoesNotRequireValidTabletIds(String path) throws Exception {
        Path staleDir = Files.createDirectories(tempDir.toPath().resolve(path));
        Files.write(staleDir.resolve("data"), new byte[] {1});

        kvManager.startup();

        assertThat(staleDir).doesNotExist();
        assertThat(tempDir.toPath().resolve("db")).doesNotExist();
        assertThat(tempDir).isDirectory();
    }

    @Test
    void testStartupCleanupDoesNotFollowLinksInsideKv(@TempDir Path outsideDir) throws Exception {
        Path outsideFile = Files.write(outsideDir.resolve("data"), new byte[] {1, 2, 3});
        Path staleDir = Files.createDirectories(tempDir.toPath().resolve("db/table-1/kv-0"));
        Files.createSymbolicLink(staleDir.resolve("db"), outsideDir);

        kvManager.startup();

        assertThat(staleDir).doesNotExist();
        assertThat(Files.readAllBytes(outsideFile)).containsExactly(1, 2, 3);
    }

    @Test
    void testPositiveSharedBlockCacheSizeEnablesSharedCache() throws Exception {
        kvManager.shutdown();
        kvManager = null;
        conf.set(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE, MemorySize.parse("64mb"));
        kvManager =
                KvManager.create(
                        conf,
                        zkClient,
                        logManager,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        kvManager.startup();

        initTableBuckets(null);
        KvTablet firstKv = getOrCreateKv(tablePath1, null, tableBucket1);
        KvTablet secondKv = getOrCreateKv(tablePath2, null, tableBucket2);

        assertThat(firstKv.getRocksDBKv().getBlockCache())
                .isSameAs(secondKv.getRocksDBKv().getBlockCache());
    }

    @Test
    void testSharedWriteBufferConfiguredThroughKvManagerCreateAndLoad() throws Exception {
        assertThat(
                        gaugeValue(
                                TestingMetricGroups.TABLET_SERVER_METRICS,
                                MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE))
                .isEqualTo(0L);
        assertThat(
                        gaugeValue(
                                TestingMetricGroups.TABLET_SERVER_METRICS,
                                MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_CAPACITY))
                .isEqualTo(0L);

        kvManager.shutdown();
        kvManager = null;
        MemorySize capacity = MemorySize.ofMebiBytes(64);
        conf.set(ConfigOptions.KV_SHARED_WRITE_BUFFER_SIZE, capacity);
        TabletServerMetricGroup metricGroup =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "cluster", "rack", "host", 1);
        kvManager = KvManager.create(conf, zkClient, logManager, metricGroup, localDiskManager);
        kvManager.startup();

        initTableBuckets(null);
        KvTablet firstKv = getOrCreateKv(tablePath1, null, tableBucket1);
        byte[] value = new byte[512 * 1024];
        firstKv.getRocksDBKv().put("first-key".getBytes(), value);
        long firstUsage = gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE);

        KvTablet secondKv = getOrCreateKv(tablePath2, null, tableBucket2);
        byte[] secondKey = "second-key".getBytes();
        secondKv.getRocksDBKv().put(secondKey, value);
        long secondUsage = gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE);

        assertThat(firstUsage).isPositive();
        assertThat(secondUsage).isGreaterThan(firstUsage);
        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_CAPACITY))
                .isEqualTo(capacity.getBytes());

        kvManager.dropKv(tableBucket1);
        KvRecord remainingRecord =
                kvRecordFactory.ofRecord("remaining-key".getBytes(), new Object[] {3, "remaining"});
        put(secondKv, remainingRecord);
        assertThat(secondKv.getRocksDBKv().get(secondKey)).isEqualTo(value);
        verifyMultiGet(secondKv, "remaining-key".getBytes(), valueOf(remainingRecord));

        File secondKvDir = secondKv.getKvTabletDir();
        zkClient.registerSchema(tablePath2, DATA1_SCHEMA_PK, schemaId);
        long currentTime = System.currentTimeMillis();
        zkClient.registerTable(
                tablePath2,
                new TableRegistration(
                        tableBucket2.getTableId(),
                        null,
                        Collections.emptyList(),
                        new TableDescriptor.TableDistribution(1, Collections.emptyList()),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        currentTime,
                        currentTime),
                false);

        metricGroup.close();
        kvManager.shutdown();
        kvManager = null;
        TabletServerMetricGroup recoveredMetricGroup =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "cluster", "rack", "host", 1);
        kvManager =
                KvManager.create(
                        conf, zkClient, logManager, recoveredMetricGroup, localDiskManager);
        // Reopen preserved local state directly, without TabletServer startup cleanup.
        KvTablet loadedKv =
                kvManager.loadKv(
                        secondKvDir,
                        new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, schemaId)),
                        null);
        loadedKv.getRocksDBKv().put("loaded-key".getBytes(), value);

        assertThat(gaugeValue(recoveredMetricGroup, MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE))
                .isPositive();
        assertThat(
                        gaugeValue(
                                recoveredMetricGroup,
                                MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_CAPACITY))
                .isEqualTo(capacity.getBytes());
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testCreateKv(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        KvTablet kv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);

        byte[] k1 = "k1".getBytes();
        KvRecord kvRecord1 = kvRecordFactory.ofRecord(k1, new Object[] {1, "a"});
        put(kv1, kvRecord1);

        byte[] k2 = "k2".getBytes();
        KvRecord kvRecord2 = kvRecordFactory.ofRecord(k2, new Object[] {2, "b"});
        put(kv2, kvRecord2);

        KvTablet newKv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        KvTablet newKv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);
        verifyMultiGet(newKv1, k1, valueOf(kvRecord1));
        verifyMultiGet(newKv2, k2, valueOf(kvRecord2));
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testReopenKvDirectlyWithoutStartupCleanup(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        int kvRecordCount = 50;
        KvRecord[] kvRecords1 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords1[i] = kvRecordFactory.ofRecord(("key" + i).getBytes(), new Object[] {i, "a"});
        }
        put(kv1, kvRecords1);

        KvTablet kv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);
        KvRecord[] kvRecords2 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords2[i] = kvRecordFactory.ofRecord(("key" + i).getBytes(), new Object[] {i, "b"});
        }
        put(kv2, kvRecords2);

        // Close and directly reopen the preserved local state.
        kvManager.shutdown();
        kvManager =
                KvManager.create(
                        conf,
                        zkClient,
                        logManager,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        // Reopen preserved local state directly, without TabletServer startup cleanup.
        kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        kv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);

        List<byte[]> kv1Keys = new ArrayList<>(kvRecordCount);
        List<byte[]> kv1Values = new ArrayList<>(kvRecordCount);
        List<byte[]> kv2Keys = new ArrayList<>(kvRecordCount);
        List<byte[]> kv2Values = new ArrayList<>(kvRecordCount);

        for (int i = 0; i < kvRecordCount; i++) {
            kv1Keys.add(("key" + i).getBytes());
            kv1Values.add(valueOf(kvRecords1[i]));
            kv2Keys.add(("key" + i).getBytes());
            kv2Values.add(valueOf(kvRecords2[i]));
        }

        // check kv1
        assertThat(toByteArrays(kv1.multiGet(kv1Keys))).containsExactlyElementsOf(kv1Values);
        // check kv2
        assertThat(toByteArrays(kv2.multiGet(kv2Keys))).containsExactlyElementsOf(kv2Values);
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testDirectReopenAfterDiscardShutdownDoesNotPersistUnflushedState(String partitionName)
            throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        byte[] key = "discarded-key".getBytes(StandardCharsets.UTF_8);
        put(kv, kvRecordFactory.ofRecord(key, new Object[] {1, "value"}));

        kvManager.shutdown(KvCloseMode.DISCARD_UNPERSISTED_STATE);
        kvManager =
                KvManager.create(
                        conf,
                        zkClient,
                        logManager,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        // Reopen preserved local state directly, without TabletServer startup cleanup.

        KvTablet reopened = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        assertThat(toByteArrays(reopened.multiGet(Collections.singletonList(key))))
                .containsExactly((byte[]) null);
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testDirectReopenWithSchemaChangeWithoutStartupCleanup(String partitionName)
            throws Exception {
        TestingSchemaGetter testingSchemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, 1));
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1, testingSchemaGetter);
        int kvRecordCount = 50;

        // Insert before closing the manager.
        KvRecord[] kvRecords1 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords1[i] = kvRecordFactory.ofRecord(("key" + i).getBytes(), new Object[] {i, "a"});
        }
        put(kv1, kvRecords1);

        // Close and directly reopen with a schema change.
        short newSchemaId = 2;
        kvManager.shutdown();
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(DATA2_SCHEMA, newSchemaId));
        kvManager =
                KvManager.create(
                        conf,
                        zkClient,
                        logManager,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        // Reopen preserved local state directly, without TabletServer startup cleanup.

        // Insert again after reopening the local state.
        kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1, testingSchemaGetter);
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryOfSchema2 =
                KvRecordTestUtils.KvRecordBatchFactory.of(newSchemaId);
        KvRecordTestUtils.KvRecordFactory kvRecordFactoryOfSchema2 =
                KvRecordTestUtils.KvRecordFactory.of(DATA2_SCHEMA.getRowType());
        KvRecord[] kvRecords2 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords2[i] =
                    kvRecordFactoryOfSchema2.ofRecord(
                            ("key" + (i + kvRecordCount)).getBytes(),
                            new Object[] {i + kvRecordCount, "b", "c"});
        }
        put(kv1, batchFactoryOfSchema2, kvRecords2);

        // check result.
        List<byte[]> kvKeys = new ArrayList<>(kvRecordCount);
        List<byte[]> kvValues = new ArrayList<>(kvRecordCount);

        for (int i = 0; i < kvRecordCount; i++) {
            kvKeys.add(("key" + i).getBytes());
            kvValues.add(valueOf(kvRecords1[i]));
            kvKeys.add(("key" + (i + kvRecordCount)).getBytes());
            kvValues.add(ValueEncoder.encodeValue(newSchemaId, kvRecords2[i].getRow()));
        }

        // check kv1
        assertThat(toByteArrays(kv1.multiGet(kvKeys))).containsExactlyElementsOf(kvValues);
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testSameTableNameInDifferentDb(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        byte[] k1 = "k1".getBytes();
        KvRecord kvRecord1 = kvRecordFactory.ofRecord(k1, new Object[] {1, "a"});
        put(kv1, kvRecord1);

        // different db with same table name
        TablePath anotherDbTablePath = TablePath.of("db2", tablePath1.getTableName());
        KvTablet kv2 = getOrCreateKv(anotherDbTablePath, partitionName, tableBucket2);
        KvRecord kvRecord2 = kvRecordFactory.ofRecord(k1, new Object[] {2, "b"});
        put(kv2, kvRecord2);

        KvTablet newKv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        KvTablet newKv2 = getOrCreateKv(anotherDbTablePath, partitionName, tableBucket2);
        verifyMultiGet(newKv1, k1, valueOf(kvRecord1));
        verifyMultiGet(newKv2, k1, valueOf(kvRecord2));
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testDropKv(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        byte[] key = "dropped-key".getBytes(StandardCharsets.UTF_8);
        put(kv1, kvRecordFactory.ofRecord(key, new Object[] {1, "old"}));
        kvManager.dropKv(kv1.getTableBucket());

        assertThat(kv1.getKvTabletDir()).doesNotExist();
        assertThat(kvManager.getKv(tableBucket1)).isNotPresent();

        kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        assertThat(kv1.getKvTabletDir()).exists();
        assertThat(toByteArrays(kv1.multiGet(Collections.singletonList(key))))
                .containsExactly((byte[]) null);
        assertThat(kvManager.getKv(tableBucket1)).isPresent();
    }

    @Test
    void testKvRunConcurrentlyForDifferentBuckets() throws Exception {
        initTableBuckets(null);
        BlockingSchemaGetter blockingSchemaGetter = new BlockingSchemaGetter();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<KvTablet> blockedCreation =
                executor.submit(
                        () -> getOrCreateKv(tablePath1, null, tableBucket1, blockingSchemaGetter));
        try {
            blockingSchemaGetter.awaitBlocked();

            Future<KvTablet> otherBucketCreation =
                    executor.submit(() -> getOrCreateKv(tablePath2, null, tableBucket2));
            assertThat(otherBucketCreation.get(10, TimeUnit.SECONDS)).isNotNull();
        } finally {
            blockingSchemaGetter.unblock();
            blockedCreation.get(10, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }

    @Test
    void testDropKvRacingGetOrCreateKv() throws Exception {
        initTableBuckets(null);
        KvTablet oldKv = getOrCreateKv(tablePath1, null, tableBucket1);
        ResourceGuard resourceGuard = oldKv.getRocksDBKv().getResourceGuard();
        ResourceGuard.Lease lease = resourceGuard.acquireResource();
        BlockingSchemaGetter blockingSchemaGetter = new BlockingSchemaGetter();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<?> drop = executor.submit(() -> kvManager.dropKv(tableBucket1));
            waitUntil(
                    resourceGuard::isClosed,
                    Duration.ofSeconds(10),
                    "The drop should wait for the outstanding lease.");

            AtomicReference<Thread> waitingThread = new AtomicReference<>();
            Future<KvTablet> waitingCreation =
                    executor.submit(
                            () -> {
                                waitingThread.set(Thread.currentThread());
                                return getOrCreateKv(
                                        tablePath1, null, tableBucket1, blockingSchemaGetter);
                            });
            waitUntil(
                    () ->
                            waitingThread.get() != null
                                    && waitingThread.get().getState() == Thread.State.BLOCKED,
                    Duration.ofSeconds(10),
                    "The first creation should wait on the lock held by dropKv.");

            lease.close();
            drop.get(10, TimeUnit.SECONDS);
            blockingSchemaGetter.awaitBlocked();

            // This arrival must share the replacement lock with the waiter on the old lock.
            AtomicReference<Thread> concurrentThread = new AtomicReference<>();
            Future<KvTablet> concurrentCreation =
                    executor.submit(
                            () -> {
                                concurrentThread.set(Thread.currentThread());
                                return getOrCreateKv(tablePath1, null, tableBucket1);
                            });
            waitUntil(
                    () ->
                            concurrentCreation.isDone()
                                    || (concurrentThread.get() != null
                                            && concurrentThread.get().getState()
                                                    == Thread.State.BLOCKED),
                    Duration.ofSeconds(10),
                    "The second creation should wait on the replacement lock.");
            assertThat(concurrentCreation.isDone()).isFalse();

            blockingSchemaGetter.unblock();
            KvTablet recreatedKv = waitingCreation.get(10, TimeUnit.SECONDS);
            assertThat(recreatedKv).isNotSameAs(oldKv);
            assertThat(concurrentCreation.get(10, TimeUnit.SECONDS)).isSameAs(recreatedKv);
            assertThat(kvManager.getKv(tableBucket1)).contains(recreatedKv);
        } finally {
            lease.close();
            blockingSchemaGetter.unblock();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testKvLockRemovedWhenBucketHasNoKv() throws Exception {
        initTableBuckets(null);
        getOrCreateKv(tablePath1, null, tableBucket1);
        assertThat(kvManager.hasKvLock(tableBucket1)).isTrue();

        kvManager.dropKv(tableBucket1);
        assertThat(kvManager.hasKvLock(tableBucket1)).isFalse();

        kvManager.dropKv(tableBucket2);
        assertThat(kvManager.hasKvLock(tableBucket2)).isFalse();

        SchemaGetter failingSchemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, 1)) {
                    @Override
                    public SchemaInfo getLatestSchemaInfo() {
                        throw new FlussRuntimeException("Failed schema lookup.");
                    }
                };
        assertThatThrownBy(() -> getOrCreateKv(tablePath1, null, tableBucket1, failingSchemaGetter))
                .hasMessageContaining("Failed schema lookup.");
        assertThat(kvManager.getKv(tableBucket1)).isNotPresent();
        assertThat(kvManager.hasKvLock(tableBucket1)).isFalse();
    }

    @Test
    void testGetNonExistentKv() {
        initTableBuckets(null);
        Optional<KvTablet> kv = kvManager.getKv(tableBucket1);
        assertThat(kv).isNotPresent();
    }

    @Test
    void testShutdownRejectsNullCloseModeBeforeClosingTablets() throws Exception {
        initTableBuckets(null);
        KvTablet kv = getOrCreateKv(tablePath1, null, tableBucket1);
        byte[] key = "still-usable-key".getBytes(StandardCharsets.UTF_8);
        KvRecord record = kvRecordFactory.ofRecord(key, new Object[] {1, "value"});
        put(kv, record);

        assertThatThrownBy(() -> kvManager.shutdown(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("closeMode");

        verifyMultiGet(kv, key, valueOf(record));

        kvManager.shutdown();
        assertThatThrownBy(kv.getRocksDBKv()::checkIfRocksDBClosed)
                .isInstanceOf(FlussRuntimeException.class);
        kvManager = null;
    }

    @Test
    void testShutdownClosesKvTabletsConcurrently() throws Exception {
        int maxClosingThreads = 2;
        recreateKvManager(maxClosingThreads);

        List<KvTablet> kvTablets = new ArrayList<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            kvTablets.add(getOrCreateKv(tablePath1, null, new TableBucket(16001L, bucket)));
        }

        List<ResourceGuard.Lease> leases = new ArrayList<>();
        for (KvTablet kvTablet : kvTablets) {
            leases.add(kvTablet.getRocksDBKv().getResourceGuard().acquireResource());
        }

        KvManager managerToShutdown = kvManager;
        kvManager = null;
        ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor();
        Future<?> shutdownFuture = shutdownExecutor.submit(() -> managerToShutdown.shutdown());
        try {
            waitUntil(
                    () ->
                            kvTablets.stream()
                                            .filter(
                                                    kvTablet ->
                                                            kvTablet.getRocksDBKv()
                                                                    .getResourceGuard()
                                                                    .isClosed())
                                            .count()
                                    == maxClosingThreads,
                    Duration.ofSeconds(10),
                    "The configured number of KvTablets should start closing concurrently.");
            assertThat(
                            kvTablets.stream()
                                    .filter(
                                            kvTablet ->
                                                    kvTablet.getRocksDBKv()
                                                            .getResourceGuard()
                                                            .isClosed())
                                    .count())
                    .isEqualTo(maxClosingThreads);
            assertThat(shutdownFuture.isDone()).isFalse();

            int releasedLease = -1;
            for (int index = 0; index < kvTablets.size(); index++) {
                if (kvTablets.get(index).getRocksDBKv().getResourceGuard().isClosed()) {
                    leases.get(index).close();
                    releasedLease = index;
                    break;
                }
            }
            assertThat(releasedLease).isNotNegative();
            waitUntil(
                    () ->
                            kvTablets.stream()
                                            .filter(
                                                    kvTablet ->
                                                            kvTablet.getRocksDBKv()
                                                                    .getResourceGuard()
                                                                    .isClosed())
                                            .count()
                                    == kvTablets.size(),
                    Duration.ofSeconds(10),
                    "The queued KvTablet should start closing after a worker becomes available.");
            assertThat(shutdownFuture.isDone()).isFalse();
        } finally {
            leases.forEach(ResourceGuard.Lease::close);
            try {
                shutdownFuture.get(30, TimeUnit.SECONDS);
            } finally {
                shutdownExecutor.shutdownNow();
            }
        }

        assertThat(kvTablets)
                .allSatisfy(
                        kvTablet ->
                                assertThatThrownBy(kvTablet.getRocksDBKv()::checkIfRocksDBClosed)
                                        .isInstanceOf(FlussRuntimeException.class));
    }

    private void recreateKvManager(int closingThreads) throws Exception {
        KvManager previousKvManager = kvManager;
        kvManager = null;
        previousKvManager.shutdown();
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, closingThreads);
        kvManager =
                KvManager.create(
                        conf,
                        zkClient,
                        logManager,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        localDiskManager);
        kvManager.startup();
    }

    private void initTableBuckets(@Nullable String partitionName) {
        if (partitionName == null) {
            tableBucket1 = new TableBucket(15001L, 1);
            tableBucket2 = new TableBucket(15002L, 2);
        } else {
            tableBucket1 = new TableBucket(15001L, 11L, 1);
            tableBucket2 = new TableBucket(15002L, 11L, 1);
        }
    }

    private void put(KvTablet kvTablet, KvRecord... kvRecords) throws Exception {
        put(kvTablet, kvRecordBatchFactory, kvRecords);
    }

    private void put(
            KvTablet kvTablet,
            KvRecordTestUtils.KvRecordBatchFactory factory,
            KvRecord... kvRecords)
            throws Exception {
        KvRecordBatch kvRecordBatch = factory.ofRecords(Arrays.asList(kvRecords));
        kvTablet.putAsLeader(kvRecordBatch, null);
        // flush to make sure data is visible
        flushAndWait(kvTablet, Long.MAX_VALUE);
    }

    private KvTablet getOrCreateKv(
            TablePath tablePath, @Nullable String partitionName, TableBucket tableBucket)
            throws Exception {
        return getOrCreateKv(
                tablePath,
                partitionName,
                tableBucket,
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, 1)));
    }

    private KvTablet getOrCreateKv(
            TablePath tablePath,
            @Nullable String partitionName,
            TableBucket tableBucket,
            SchemaGetter schemaGetter)
            throws Exception {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(
                        tablePath.getDatabaseName(), tablePath.getTableName(), partitionName);
        LogTablet logTablet =
                logManager.getOrCreateLog(
                        tempDir, physicalTablePath, tableBucket, LogFormat.ARROW, 1, true);
        return kvManager.getOrCreateKv(
                physicalTablePath,
                tableBucket,
                logTablet,
                KvFormat.COMPACTED,
                schemaGetter,
                new TableConfig(new Configuration()),
                DEFAULT_COMPRESSION,
                null);
    }

    private byte[] valueOf(KvRecord kvRecord) {
        return ValueEncoder.encodeValue(schemaId, kvRecord.getRow());
    }

    private void verifyMultiGet(KvTablet kvTablet, byte[] key, byte[] expectedValue)
            throws IOException {
        List<byte[]> gotValues = toByteArrays(kvTablet.multiGet(Collections.singletonList(key)));
        assertThat(gotValues).containsExactly(expectedValue);
    }

    private static List<byte[]> toByteArrays(List<ByteArraySlice> slices) {
        List<byte[]> values = new ArrayList<>(slices.size());
        for (ByteArraySlice slice : slices) {
            values.add(slice == null ? null : slice.toByteArray());
        }
        return values;
    }

    private static long gaugeValue(TabletServerMetricGroup metricGroup, String metricName) {
        return ((Number) ((Gauge<?>) metricGroup.getMetrics().get(metricName)).getValue())
                .longValue();
    }

    private static final class BlockingSchemaGetter extends TestingSchemaGetter {
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch unblock = new CountDownLatch(1);

        private BlockingSchemaGetter() {
            super(new SchemaInfo(DATA1_SCHEMA_PK, 1));
        }

        @Override
        public SchemaInfo getLatestSchemaInfo() {
            blocked.countDown();
            try {
                if (!unblock.await(30, TimeUnit.SECONDS)) {
                    throw new FlussRuntimeException("Timed out waiting to unblock schema lookup.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException("Interrupted while blocking schema lookup.", e);
            }
            return super.getLatestSchemaInfo();
        }

        private void awaitBlocked() throws InterruptedException {
            assertThat(blocked.await(10, TimeUnit.SECONDS)).isTrue();
        }

        private void unblock() {
            unblock.countDown();
        }
    }
}
