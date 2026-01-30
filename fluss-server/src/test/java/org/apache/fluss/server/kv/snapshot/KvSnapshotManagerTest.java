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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.KvSnapshotResource;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.function.FunctionWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.fluss.shaded.guava32.com.google.common.collect.Iterators.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvSnapshotManager} . */
class KvSnapshotManagerTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final long periodicMaterializeDelay = 10_000L;
    private static ZooKeeperClient zkClient;
    private final TableBucket tableBucket = new TableBucket(1, 1);
    private ManuallyTriggeredScheduledExecutorService scheduledExecutorService;
    private ManuallyTriggeredScheduledExecutorService asyncSnapshotExecutorService;
    private KvSnapshotResource kvSnapshotResource;
    private KvSnapshotManager kvSnapshotManager;
    private DefaultSnapshotContext snapshotContext;
    private Configuration conf;
    private ManualClock manualClock;
    private @TempDir File tmpKvDir;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void before() {
        conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofMillis(periodicMaterializeDelay));
        scheduledExecutorService = new ManuallyTriggeredScheduledExecutorService();
        asyncSnapshotExecutorService = new ManuallyTriggeredScheduledExecutorService();
        ExecutorService dataTransferThreadPool = Executors.newFixedThreadPool(1);
        kvSnapshotResource =
                new KvSnapshotResource(
                        scheduledExecutorService,
                        new KvSnapshotDataUploader(dataTransferThreadPool),
                        new KvSnapshotDataDownloader(dataTransferThreadPool),
                        asyncSnapshotExecutorService);
        snapshotContext =
                DefaultSnapshotContext.create(
                        zkClient,
                        new TestingCompletedKvSnapshotCommitter(),
                        kvSnapshotResource,
                        conf);
        manualClock = new ManualClock(System.currentTimeMillis());
    }

    @AfterEach
    void close() {
        if (kvSnapshotManager != null) {
            kvSnapshotManager.close();
        }
    }

    @Test
    void testInitialDelay() {
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(NopUploadSnapshotTarget.INSTANCE);
        checkOnlyOneScheduledTasks();
    }

    @Test
    void testInitWithNonPositiveSnapshotInterval() {
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofMillis(0));
        snapshotContext =
                DefaultSnapshotContext.create(
                        zkClient,
                        new TestingCompletedKvSnapshotCommitter(),
                        kvSnapshotResource,
                        conf);
        kvSnapshotManager = createSnapshotManager(snapshotContext);
        startPeriodicUploadSnapshot(NopUploadSnapshotTarget.INSTANCE);
        // periodic snapshot is disabled when periodicMaterializeDelay is not positive
        Assertions.assertEquals(0, scheduledExecutorService.getAllScheduledTasks().size());
    }

    @Test
    void testPeriodicSnapshot() {
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(NopUploadSnapshotTarget.INSTANCE);
        // check only one schedule task
        checkOnlyOneScheduledTasks();
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        // after trigger, should still remain one task
        checkOnlyOneScheduledTasks();
    }

    @Test
    void testSnapshot() {
        // use local filesystem to make the FileSystem plugin happy
        String snapshotDir = "file:/test/snapshot1";
        TestUploadSnapshotTarget target = new TestUploadSnapshotTarget(new FsPath(snapshotDir));
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(target);
        // trigger schedule
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        // trigger async snapshot
        asyncSnapshotExecutorService.trigger();

        // now, check the result
        assertThat(target.getCollectedRemoteDirs())
                .isEqualTo(Collections.singletonList(snapshotDir));
    }

    @Test
    void testSnapshotWithException() {
        // use local filesystem to make the FileSystem plugin happy
        String remoteDir = "file:/test/snapshot1";
        String exceptionMessage = "Exception while initializing Materialization";
        TestUploadSnapshotTarget target =
                new TestUploadSnapshotTarget(new FsPath(remoteDir), exceptionMessage);
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(target);
        // trigger schedule
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();

        // trigger async snapshot
        asyncSnapshotExecutorService.trigger();

        assertThat(target.getCause())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage(exceptionMessage);
    }

    @Test
    void testDownloadSnapshotByIdSuccessfully() throws Exception {
        // Create a snapshot manager with successful downloader
        SuccessfulDownloadSnapshotContext successContext =
                new SuccessfulDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, successContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();

        // Create a mock completed snapshot with specific snapshotId
        long snapshotId = 123L;
        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot(snapshotId);
        successContext.setMockSnapshot(mockSnapshot);

        // Download by specific snapshotId should succeed
        kvSnapshotManager.downloadSnapshot(snapshotId);

        // Verify no temp directories left behind
        File[] tempDirs = tmpKvDir.listFiles((dir, name) -> name.startsWith(".tmp_snapshot_"));
        assertThat(tempDirs).isEmpty();

        // Verify files are in the final db directory
        Path dbPath = tmpKvDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        assertThat(Files.exists(dbPath)).isTrue();

        // Verify downloaded SST files cache is updated
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).isNotNull();
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(1);

        // Verify downloaded misc files cache is updated
        assertThat(kvSnapshotManager.getDownloadedMiscFiles()).isNotNull();
        assertThat(kvSnapshotManager.getDownloadedMiscFiles()).hasSize(1);

        // Verify standby snapshot size is updated
        assertThat(kvSnapshotManager.getStandbySnapshotSize())
                .isEqualTo(mockSnapshot.getSnapshotSize());
    }

    @Test
    void testDownloadSnapshotByIdFailure() {
        // Create a snapshot manager with failing downloader
        FailingSnapshotContext failingContext =
                new FailingSnapshotContext(
                        snapshotContext, new IOException("Simulated download failure"));
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, failingContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();

        long snapshotId = 456L;
        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot(snapshotId);
        failingContext.setMockSnapshot(mockSnapshot);

        // Attempt to download by snapshotId - should fail
        assertThatThrownBy(() -> kvSnapshotManager.downloadSnapshot(snapshotId))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Fail to download kv snapshot");

        // Verify temp directories are cleaned up
        File[] tempDirs = tmpKvDir.listFiles((dir, name) -> name.startsWith(".tmp_snapshot_"));
        assertThat(tempDirs).isEmpty();
    }

    @Test
    void testDownloadLatestSnapshotWhenNoSnapshotExists() throws Exception {
        // Create a snapshot context that returns null for latest snapshot
        NoSnapshotContext noSnapshotContext = new NoSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, noSnapshotContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();

        // Download latest snapshot should return empty Optional when no snapshot exists
        Optional<CompletedSnapshot> result = kvSnapshotManager.downloadLatestSnapshot();
        assertThat(result).isEmpty();

        // Verify downloaded SST files cache is not initialized (still null)
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).isNull();
    }

    @Test
    void testDownloadLatestSnapshotSuccessfullyUpdatesCache() throws Exception {
        // Create a snapshot manager with successful downloader
        SuccessfulDownloadSnapshotContext successContext =
                new SuccessfulDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, successContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();

        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot();
        successContext.setMockSnapshot(mockSnapshot);

        // Download latest snapshot
        Optional<CompletedSnapshot> result = kvSnapshotManager.downloadLatestSnapshot();
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(mockSnapshot);

        // Verify downloaded files caches are updated
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).isNotNull();
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(1);
        assertThat(kvSnapshotManager.getDownloadedMiscFiles()).isNotNull();
        assertThat(kvSnapshotManager.getDownloadedMiscFiles()).hasSize(1);
    }

    @Test
    void testIncrementalDownloadSkipsExistingFiles() throws Exception {
        // Create a snapshot manager with successful downloader
        SuccessfulDownloadSnapshotContext successContext =
                new SuccessfulDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, successContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();

        // First download
        CompletedSnapshot firstSnapshot = createMockCompletedSnapshot(1L);
        successContext.setMockSnapshot(firstSnapshot);
        kvSnapshotManager.downloadSnapshot(1L);

        // Verify first download
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(1);

        // Second download with same files - should be incremental (no new files to download)
        CompletedSnapshot secondSnapshot = createMockCompletedSnapshot(2L);
        successContext.setMockSnapshot(secondSnapshot);
        kvSnapshotManager.downloadSnapshot(2L);

        // Cache should still have one SST file
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(1);
    }

    @Test
    void testIncrementalDownloadWithNewSstFiles() throws Exception {
        // Create a snapshot manager with successful downloader
        SuccessfulDownloadSnapshotContext successContext =
                new SuccessfulDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, successContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();
        Path dbPath = tmpKvDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);

        // First download: snapshot contains only test.sst
        List<String> firstSstFiles = Collections.singletonList("test.sst");
        CompletedSnapshot firstSnapshot =
                createMockCompletedSnapshotWithSstFiles(1L, firstSstFiles);
        successContext.setMockSnapshot(firstSnapshot);
        kvSnapshotManager.downloadLatestSnapshot();

        // Verify first download: 1 SST file
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(1);
        assertThat(
                        kvSnapshotManager.getDownloadedSstFiles().stream()
                                .map(Path::toString)
                                .collect(Collectors.toSet()))
                .containsExactly(dbPath.resolve("test.sst").toString());

        // Second download: snapshot contains test.sst and new.sst
        List<String> secondSstFiles = Arrays.asList("test.sst", "new.sst");
        CompletedSnapshot secondSnapshot =
                createMockCompletedSnapshotWithSstFiles(2L, secondSstFiles);
        successContext.setMockSnapshot(secondSnapshot);
        kvSnapshotManager.downloadLatestSnapshot();

        // Verify second download: 2 SST files (incremental download got new.sst)
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(2);
        assertThat(
                        kvSnapshotManager.getDownloadedSstFiles().stream()
                                .map(Path::toString)
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(
                        dbPath.resolve("test.sst").toString(),
                        dbPath.resolve("new.sst").toString());

        // Verify files exist in db directory
        assertThat(Files.exists(dbPath.resolve("test.sst"))).isTrue();
        assertThat(Files.exists(dbPath.resolve("new.sst"))).isTrue();
    }

    @Test
    void testIncrementalDownloadDeletesObsoleteSstFiles() throws Exception {
        // Create a snapshot manager with successful downloader
        SuccessfulDownloadSnapshotContext successContext =
                new SuccessfulDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, successContext, manualClock);
        // Must call becomeStandby() first to create db directory
        kvSnapshotManager.becomeStandby();
        Path dbPath = tmpKvDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);

        // First download: snapshot contains old.sst and common.sst
        List<String> firstSstFiles = Arrays.asList("old.sst", "common.sst");
        CompletedSnapshot firstSnapshot =
                createMockCompletedSnapshotWithSstFiles(1L, firstSstFiles);
        successContext.setMockSnapshot(firstSnapshot);
        kvSnapshotManager.downloadLatestSnapshot();

        // Verify first download: 2 SST files
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(2);
        assertThat(Files.exists(dbPath.resolve("old.sst"))).isTrue();
        assertThat(Files.exists(dbPath.resolve("common.sst"))).isTrue();

        // Second download: snapshot contains common.sst and new.sst (old.sst is compacted away)
        List<String> secondSstFiles = Arrays.asList("common.sst", "new.sst");
        CompletedSnapshot secondSnapshot =
                createMockCompletedSnapshotWithSstFiles(2L, secondSstFiles);
        successContext.setMockSnapshot(secondSnapshot);
        kvSnapshotManager.downloadLatestSnapshot();

        // Verify second download: 2 SST files (old.sst deleted, new.sst added)
        assertThat(kvSnapshotManager.getDownloadedSstFiles()).hasSize(2);
        assertThat(
                        kvSnapshotManager.getDownloadedSstFiles().stream()
                                .map(Path::toString)
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(
                        dbPath.resolve("common.sst").toString(),
                        dbPath.resolve("new.sst").toString());

        // Verify old.sst is deleted from db directory
        assertThat(Files.exists(dbPath.resolve("old.sst"))).isFalse();
        assertThat(Files.exists(dbPath.resolve("common.sst"))).isTrue();
        assertThat(Files.exists(dbPath.resolve("new.sst"))).isTrue();
    }

    @Test
    void testAtomicDownloadCleansUpTempDirOnDownloadFailure() {
        // Create a snapshot manager with failing downloader
        FailingSnapshotContext failingContext =
                new FailingSnapshotContext(
                        snapshotContext, new IOException("Simulated download failure"));
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, failingContext, manualClock);
        kvSnapshotManager.becomeStandby();

        // Create a mock completed snapshot
        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot();
        failingContext.setMockSnapshot(mockSnapshot);

        // Attempt to download - should fail
        assertThatThrownBy(() -> kvSnapshotManager.downloadLatestSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Fail to download kv snapshot");

        // Verify no temp directories left behind
        File[] tempDirs = tmpKvDir.listFiles((dir, name) -> name.startsWith(".tmp_snapshot_"));
        assertThat(tempDirs).as("Temp directories should be cleaned up on failure").isEmpty();

        // Verify db directory is not corrupted (should be empty or not exist)
        Path dbPath = tmpKvDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        if (Files.exists(dbPath)) {
            File[] dbFiles = dbPath.toFile().listFiles();
            assertThat(dbFiles).as("DB directory should be empty after failed download").isEmpty();
        }
    }

    @Test
    void testAtomicDownloadVerifiesSnapshotCompleteness() {
        // Create a snapshot manager with a downloader that creates incomplete files
        IncompleteDownloadSnapshotContext incompleteContext =
                new IncompleteDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, incompleteContext, manualClock);
        kvSnapshotManager.becomeStandby();

        // Create a mock completed snapshot expecting certain files
        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot();
        incompleteContext.setMockSnapshot(mockSnapshot);

        // Attempt to download - should fail verification
        assertThatThrownBy(() -> kvSnapshotManager.downloadLatestSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Snapshot verification failed");

        // Verify temp directories are cleaned up
        File[] tempDirs = tmpKvDir.listFiles((dir, name) -> name.startsWith(".tmp_snapshot_"));
        assertThat(tempDirs)
                .as("Temp directories should be cleaned up on verification failure")
                .isEmpty();
    }

    @Test
    void testAtomicDownloadSuccessfullyMovesFiles() throws Exception {
        // Create a snapshot manager with successful downloader
        SuccessfulDownloadSnapshotContext successContext =
                new SuccessfulDownloadSnapshotContext(snapshotContext);
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, successContext, manualClock);
        kvSnapshotManager.becomeStandby();

        // Create a mock completed snapshot
        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot();
        successContext.setMockSnapshot(mockSnapshot);

        // Download should succeed
        Optional<CompletedSnapshot> result = kvSnapshotManager.downloadLatestSnapshot();
        assertThat(result).isPresent();

        // Verify no temp directories left behind
        File[] tempDirs = tmpKvDir.listFiles((dir, name) -> name.startsWith(".tmp_snapshot_"));
        assertThat(tempDirs).isEmpty();

        // Verify files are in the final db directory
        Path dbPath = tmpKvDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        assertThat(Files.exists(dbPath)).isTrue();
    }

    @Test
    void testDownloadedSstFilesCacheNotUpdatedOnFailure() {
        // Create a snapshot manager with failing downloader
        FailingSnapshotContext failingContext =
                new FailingSnapshotContext(
                        snapshotContext, new IOException("Simulated download failure"));
        kvSnapshotManager =
                new KvSnapshotManager(tableBucket, tmpKvDir, failingContext, manualClock);
        kvSnapshotManager.becomeStandby();

        CompletedSnapshot mockSnapshot = createMockCompletedSnapshot();
        failingContext.setMockSnapshot(mockSnapshot);

        // Attempt to download - should fail
        assertThatThrownBy(() -> kvSnapshotManager.downloadLatestSnapshot())
                .isInstanceOf(IOException.class);

        // Verify cache is not updated (should be null since this is first download)
        assertThat(kvSnapshotManager.getDownloadedSstFiles())
                .as("Downloaded SST files cache should be empty on failure")
                .isEmpty();
    }

    private void checkOnlyOneScheduledTasks() {
        assertThat(
                        getOnlyElement(scheduledExecutorService.getAllScheduledTasks().iterator())
                                .getDelay(MILLISECONDS))
                .as(
                        String.format(
                                "task for initial materialization should be scheduled with a 0..%d delay",
                                periodicMaterializeDelay))
                .isLessThanOrEqualTo(periodicMaterializeDelay);
    }

    private KvSnapshotManager createSnapshotManager(boolean isLeader) {
        KvSnapshotManager snapshotManager = createSnapshotManager(snapshotContext);
        if (isLeader) {
            snapshotManager.becomeLeader();
        }
        return snapshotManager;
    }

    private KvSnapshotManager createSnapshotManager(SnapshotContext context) {
        return new KvSnapshotManager(tableBucket, tmpKvDir, context, manualClock);
    }

    private void startPeriodicUploadSnapshot(KvSnapshotManager.UploadSnapshotTarget target) {
        kvSnapshotManager.startPeriodicUploadSnapshot(
                org.apache.fluss.utils.concurrent.Executors.directExecutor(), target);
    }

    private static class NopUploadSnapshotTarget implements KvSnapshotManager.UploadSnapshotTarget {
        private static final NopUploadSnapshotTarget INSTANCE = new NopUploadSnapshotTarget();

        @Override
        public long currentSnapshotId() {
            return 0;
        }

        @Override
        public Optional<KvSnapshotManager.SnapshotRunnable> initSnapshot() {
            return Optional.empty();
        }

        @Override
        public void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult) {}

        @Override
        public void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {}

        @Override
        public long getSnapshotSize() {
            return 0L;
        }
    }

    private static class TestUploadSnapshotTarget
            implements KvSnapshotManager.UploadSnapshotTarget {

        private final FsPath snapshotPath;
        private final SnapshotLocation snapshotLocation;
        private final List<String> collectedRemoteDirs;
        private final String exceptionMessage;
        private Throwable cause;

        public TestUploadSnapshotTarget(FsPath snapshotPath) {
            this(snapshotPath, null);
        }

        public TestUploadSnapshotTarget(FsPath snapshotPath, String exceptionMessage) {
            this.snapshotPath = snapshotPath;
            this.collectedRemoteDirs = new ArrayList<>();
            this.exceptionMessage = exceptionMessage;
            try {
                this.snapshotLocation =
                        new SnapshotLocation(
                                snapshotPath.getFileSystem(), snapshotPath, snapshotPath, 1024);
            } catch (IOException e) {
                throw new FlussRuntimeException(e);
            }
        }

        @Override
        public long currentSnapshotId() {
            return 0;
        }

        @Override
        public Optional<KvSnapshotManager.SnapshotRunnable> initSnapshot() {
            RunnableFuture<SnapshotResult> runnableFuture =
                    new FutureTask<>(
                            () -> {
                                if (exceptionMessage != null) {
                                    throw new FlussRuntimeException(exceptionMessage);
                                } else {
                                    final long logOffset = 0;
                                    return new SnapshotResult(null, snapshotPath, logOffset);
                                }
                            });
            int snapshotId = 1;
            int coordinatorEpoch = 0;
            int leaderEpoch = 0;
            return Optional.of(
                    new KvSnapshotManager.SnapshotRunnable(
                            runnableFuture,
                            snapshotId,
                            coordinatorEpoch,
                            leaderEpoch,
                            snapshotLocation));
        }

        @Override
        public void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult) {
            collectedRemoteDirs.add(snapshotResult.getSnapshotPath().toString());
        }

        @Override
        public void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {
            this.cause = cause;
        }

        @Override
        public long getSnapshotSize() {
            return 0L;
        }

        private List<String> getCollectedRemoteDirs() {
            return collectedRemoteDirs;
        }

        private Throwable getCause() {
            return this.cause;
        }
    }

    private CompletedSnapshot createMockCompletedSnapshot() {
        return createMockCompletedSnapshot(1L);
    }

    private CompletedSnapshot createMockCompletedSnapshot(long snapshotId) {
        return createMockCompletedSnapshotWithSstFiles(
                snapshotId, Collections.singletonList("test.sst"));
    }

    private CompletedSnapshot createMockCompletedSnapshotWithSstFiles(
            long snapshotId, List<String> sstFileNames) {
        Path dbPath = tmpKvDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        List<KvFileHandleAndLocalPath> sstFiles = new ArrayList<>();
        long totalSstSize = 0;
        for (String sstFileName : sstFileNames) {
            long fileSize = 1024;
            sstFiles.add(
                    KvFileHandleAndLocalPath.of(
                            new KvFileHandle("file:/remote/" + sstFileName, fileSize),
                            dbPath.resolve(sstFileName).toString()));
            totalSstSize += fileSize;
        }

        List<KvFileHandleAndLocalPath> miscFiles = new ArrayList<>();
        miscFiles.add(
                KvFileHandleAndLocalPath.of(
                        new KvFileHandle("file:/remote/CURRENT", 16),
                        dbPath.resolve("CURRENT").toString()));

        long totalSize = totalSstSize + 16;
        KvSnapshotHandle handle = new KvSnapshotHandle(sstFiles, miscFiles, totalSize);
        return new CompletedSnapshot(
                tableBucket, snapshotId, new FsPath("file:/remote"), handle, totalSize);
    }

    /** A SnapshotContext wrapper that simulates download failures. */
    private static class FailingSnapshotContext implements SnapshotContext {
        private final SnapshotContext delegate;
        private final Exception failureException;
        private CompletedSnapshot mockSnapshot;

        FailingSnapshotContext(SnapshotContext delegate, Exception failureException) {
            this.delegate = delegate;
            this.failureException = failureException;
        }

        void setMockSnapshot(CompletedSnapshot snapshot) {
            this.mockSnapshot = snapshot;
        }

        @Override
        public KvSnapshotDataDownloader getSnapshotDataDownloader() {
            return new FailingKvSnapshotDataDownloader(failureException);
        }

        @Override
        public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
                getLatestCompletedSnapshotProvider() {
            return (tb) -> mockSnapshot;
        }

        @Override
        public ZooKeeperClient getZooKeeperClient() {
            return delegate.getZooKeeperClient();
        }

        @Override
        public ExecutorService getAsyncOperationsThreadPool() {
            return delegate.getAsyncOperationsThreadPool();
        }

        @Override
        public KvSnapshotDataUploader getSnapshotDataUploader() {
            return delegate.getSnapshotDataUploader();
        }

        @Override
        public ScheduledExecutorService getSnapshotScheduler() {
            return delegate.getSnapshotScheduler();
        }

        @Override
        public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
            return delegate.getCompletedSnapshotReporter();
        }

        @Override
        public long getSnapshotIntervalMs() {
            return delegate.getSnapshotIntervalMs();
        }

        @Override
        public int getSnapshotFsWriteBufferSize() {
            return delegate.getSnapshotFsWriteBufferSize();
        }

        @Override
        public FsPath getRemoteKvDir() {
            return delegate.getRemoteKvDir();
        }

        @Override
        public CompletedSnapshot getCompletedSnapshotProvider(
                TableBucket tableBucket, long snapshotId) {
            return mockSnapshot;
        }

        @Override
        public void handleSnapshotBroken(CompletedSnapshot snapshot) {}

        @Override
        public int maxFetchLogSizeInRecoverKv() {
            return delegate.maxFetchLogSizeInRecoverKv();
        }
    }

    /** A KvSnapshotDataDownloader that always fails. */
    private static class FailingKvSnapshotDataDownloader extends KvSnapshotDataDownloader {
        private final Exception failureException;

        FailingKvSnapshotDataDownloader(Exception failureException) {
            super(Executors.newSingleThreadExecutor());
            this.failureException = failureException;
        }

        @Override
        public void transferAllDataToDirectory(
                KvSnapshotDownloadSpec downloadSpec, CloseableRegistry closeableRegistry)
                throws Exception {
            throw failureException;
        }
    }

    /** A SnapshotContext that creates incomplete downloads (missing files). */
    private static class IncompleteDownloadSnapshotContext implements SnapshotContext {
        private final SnapshotContext delegate;
        private CompletedSnapshot mockSnapshot;

        IncompleteDownloadSnapshotContext(SnapshotContext delegate) {
            this.delegate = delegate;
        }

        void setMockSnapshot(CompletedSnapshot snapshot) {
            this.mockSnapshot = snapshot;
        }

        @Override
        public KvSnapshotDataDownloader getSnapshotDataDownloader() {
            // Return a downloader that creates files but with wrong sizes
            return new IncompleteKvSnapshotDataDownloader();
        }

        @Override
        public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
                getLatestCompletedSnapshotProvider() {
            return (tb) -> mockSnapshot;
        }

        @Override
        public ZooKeeperClient getZooKeeperClient() {
            return delegate.getZooKeeperClient();
        }

        @Override
        public ExecutorService getAsyncOperationsThreadPool() {
            return delegate.getAsyncOperationsThreadPool();
        }

        @Override
        public KvSnapshotDataUploader getSnapshotDataUploader() {
            return delegate.getSnapshotDataUploader();
        }

        @Override
        public ScheduledExecutorService getSnapshotScheduler() {
            return delegate.getSnapshotScheduler();
        }

        @Override
        public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
            return delegate.getCompletedSnapshotReporter();
        }

        @Override
        public long getSnapshotIntervalMs() {
            return delegate.getSnapshotIntervalMs();
        }

        @Override
        public int getSnapshotFsWriteBufferSize() {
            return delegate.getSnapshotFsWriteBufferSize();
        }

        @Override
        public FsPath getRemoteKvDir() {
            return delegate.getRemoteKvDir();
        }

        @Override
        public CompletedSnapshot getCompletedSnapshotProvider(
                TableBucket tableBucket, long snapshotId) {
            return mockSnapshot;
        }

        @Override
        public void handleSnapshotBroken(CompletedSnapshot snapshot) {}

        @Override
        public int maxFetchLogSizeInRecoverKv() {
            return delegate.maxFetchLogSizeInRecoverKv();
        }
    }

    /** A downloader that creates files but doesn't download any content (files will be empty). */
    private static class IncompleteKvSnapshotDataDownloader extends KvSnapshotDataDownloader {
        IncompleteKvSnapshotDataDownloader() {
            super(Executors.newSingleThreadExecutor());
        }

        @Override
        public void transferAllDataToDirectory(
                KvSnapshotDownloadSpec downloadSpec, CloseableRegistry closeableRegistry)
                throws Exception {
            // Don't create any files - this simulates incomplete download
            // The verification will fail because expected files don't exist
        }
    }

    /** A SnapshotContext that successfully downloads files. */
    private static class SuccessfulDownloadSnapshotContext implements SnapshotContext {
        private final SnapshotContext delegate;
        private CompletedSnapshot mockSnapshot;

        SuccessfulDownloadSnapshotContext(SnapshotContext delegate) {
            this.delegate = delegate;
        }

        void setMockSnapshot(CompletedSnapshot snapshot) {
            this.mockSnapshot = snapshot;
        }

        @Override
        public KvSnapshotDataDownloader getSnapshotDataDownloader() {
            return new SuccessfulKvSnapshotDataDownloader(mockSnapshot);
        }

        @Override
        public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
                getLatestCompletedSnapshotProvider() {
            return (tb) -> mockSnapshot;
        }

        @Override
        public ZooKeeperClient getZooKeeperClient() {
            return delegate.getZooKeeperClient();
        }

        @Override
        public ExecutorService getAsyncOperationsThreadPool() {
            return delegate.getAsyncOperationsThreadPool();
        }

        @Override
        public KvSnapshotDataUploader getSnapshotDataUploader() {
            return delegate.getSnapshotDataUploader();
        }

        @Override
        public ScheduledExecutorService getSnapshotScheduler() {
            return delegate.getSnapshotScheduler();
        }

        @Override
        public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
            return delegate.getCompletedSnapshotReporter();
        }

        @Override
        public long getSnapshotIntervalMs() {
            return delegate.getSnapshotIntervalMs();
        }

        @Override
        public int getSnapshotFsWriteBufferSize() {
            return delegate.getSnapshotFsWriteBufferSize();
        }

        @Override
        public FsPath getRemoteKvDir() {
            return delegate.getRemoteKvDir();
        }

        @Override
        public CompletedSnapshot getCompletedSnapshotProvider(
                TableBucket tableBucket, long snapshotId) {
            return mockSnapshot;
        }

        @Override
        public void handleSnapshotBroken(CompletedSnapshot snapshot) {}

        @Override
        public int maxFetchLogSizeInRecoverKv() {
            return delegate.maxFetchLogSizeInRecoverKv();
        }
    }

    /** A downloader that successfully creates files matching the snapshot metadata. */
    private static class SuccessfulKvSnapshotDataDownloader extends KvSnapshotDataDownloader {
        private final CompletedSnapshot snapshot;

        SuccessfulKvSnapshotDataDownloader(CompletedSnapshot snapshot) {
            super(Executors.newSingleThreadExecutor());
            this.snapshot = snapshot;
        }

        @Override
        public void transferAllDataToDirectory(
                KvSnapshotDownloadSpec downloadSpec, CloseableRegistry closeableRegistry)
                throws Exception {
            Path destDir = downloadSpec.getDownloadDestination();
            Files.createDirectories(destDir);

            // Create all expected files with correct sizes
            KvSnapshotHandle handle = snapshot.getKvSnapshotHandle();
            for (KvFileHandleAndLocalPath fileHandle : handle.getSharedKvFileHandles()) {
                createFileWithSize(
                        destDir, fileHandle.getLocalPath(), fileHandle.getKvFileHandle().getSize());
            }
            for (KvFileHandleAndLocalPath fileHandle : handle.getPrivateFileHandles()) {
                createFileWithSize(
                        destDir, fileHandle.getLocalPath(), fileHandle.getKvFileHandle().getSize());
            }
        }

        private void createFileWithSize(Path dir, String localPath, long size) throws IOException {
            String fileName = java.nio.file.Paths.get(localPath).getFileName().toString();
            Path filePath = dir.resolve(fileName);
            byte[] content = new byte[(int) size];
            Files.write(filePath, content);
        }
    }

    /** A SnapshotContext that returns null for latest snapshot (no snapshot exists). */
    private static class NoSnapshotContext implements SnapshotContext {
        private final SnapshotContext delegate;

        NoSnapshotContext(SnapshotContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public KvSnapshotDataDownloader getSnapshotDataDownloader() {
            return delegate.getSnapshotDataDownloader();
        }

        @Override
        public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
                getLatestCompletedSnapshotProvider() {
            // Return null to simulate no snapshot exists
            return (tb) -> null;
        }

        @Override
        public ZooKeeperClient getZooKeeperClient() {
            return delegate.getZooKeeperClient();
        }

        @Override
        public ExecutorService getAsyncOperationsThreadPool() {
            return delegate.getAsyncOperationsThreadPool();
        }

        @Override
        public KvSnapshotDataUploader getSnapshotDataUploader() {
            return delegate.getSnapshotDataUploader();
        }

        @Override
        public ScheduledExecutorService getSnapshotScheduler() {
            return delegate.getSnapshotScheduler();
        }

        @Override
        public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
            return delegate.getCompletedSnapshotReporter();
        }

        @Override
        public long getSnapshotIntervalMs() {
            return delegate.getSnapshotIntervalMs();
        }

        @Override
        public int getSnapshotFsWriteBufferSize() {
            return delegate.getSnapshotFsWriteBufferSize();
        }

        @Override
        public FsPath getRemoteKvDir() {
            return delegate.getRemoteKvDir();
        }

        @Override
        public CompletedSnapshot getCompletedSnapshotProvider(
                TableBucket tableBucket, long snapshotId) {
            return null;
        }

        @Override
        public void handleSnapshotBroken(CompletedSnapshot snapshot) {}

        @Override
        public int maxFetchLogSizeInRecoverKv() {
            return delegate.maxFetchLogSizeInRecoverKv();
        }
    }
}
