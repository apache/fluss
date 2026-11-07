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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogStorage.IndexType;
import org.apache.fluss.shaded.guava32.com.google.common.io.Files;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultRemoteLogStorage}. */
class DefaultRemoteLogStorageTest extends RemoteLogTestBase {
    private DefaultRemoteLogStorage remoteLogStorageManager;
    private ExecutorService ioExecutor;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        ioExecutor = Executors.newSingleThreadExecutor();
        remoteLogStorageManager = new DefaultRemoteLogStorage(conf, ioExecutor);
    }

    @AfterEach
    public void teardown() throws Exception {
        remoteLogStorageManager.close();
        if (ioExecutor != null) {
            ioExecutor.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCopyLogSegmentFiles(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();
        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).isNotNull();
        assertThat(remoteFiles).hasSize(4);

        File[] allFilesForLocalLog = logTablet.getLogDir().listFiles();
        assertThat(allFilesForLocalLog).isNotNull();
        for (File remoteFile : remoteFiles) {
            File localFile = getLocalFileByName(remoteFile.getName(), allFilesForLocalLog);
            assertThat(localFile).isNotNull();
            assertThat(Files.equal(remoteFile, localFile)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteRemoteLogSegment(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();
        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).hasSize(4);

        remoteLogStorageManager.deleteLogSegmentFiles(
                FlussPaths.remoteLogDir(conf), remoteLogSegment);
        remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchIndex(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();

        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).isNotNull();
        assertThat(remoteFiles).hasSize(4);

        File tmpIndexFile = new File(tempDir, "tmp-index");
        try (InputStream inputStream =
                remoteLogStorageManager.fetchIndex(
                        FlussPaths.remoteLogDir(conf), remoteLogSegment, IndexType.OFFSET)) {
            java.nio.file.Files.copy(
                    inputStream, tmpIndexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        File[] allFilesForLocalLog = logTablet.getLogDir().listFiles();
        assertThat(allFilesForLocalLog).isNotNull();
        for (File remoteFile : remoteFiles) {
            File localFile = getLocalFileByName(remoteFile.getName(), allFilesForLocalLog);
            if (localFile
                    .getName()
                    .endsWith(RemoteLogStorage.IndexType.getFileSuffix(IndexType.OFFSET))) {
                assertThat(Files.equal(remoteFile, tmpIndexFile)).isTrue();
                assertThat(Files.equal(tmpIndexFile, localFile)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadWriteDeleteRemoteLogManifestSnapshot(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // do snapshot.
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        remoteLogTablet.loadRemoteLogManifest(
                new RemoteLogManifest(
                        logTablet.getPhysicalTablePath(),
                        logTablet.getTableBucket(),
                        remoteLogSegmentList));
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap())
                .hasSize(remoteLogSegmentList.size());
        RemoteLogManifest manifestSnapshot = remoteLogTablet.currentManifest();
        assertThat(manifestSnapshot.getRemoteLogSegmentList()).isNotEmpty();

        FsPath remoteSnapshotDir =
                remoteLogStorageManager.writeRemoteLogManifestSnapshot(
                        FlussPaths.remoteLogDir(conf), manifestSnapshot);
        assertThat(remoteSnapshotDir).isNotNull();
        File snapshotFile = new File(remoteSnapshotDir.getPath());
        assertThat(snapshotFile.exists()).isTrue();

        RemoteLogManifest result =
                remoteLogStorageManager.readRemoteLogManifestSnapshot(remoteSnapshotDir);
        assertThat(result).isEqualTo(manifestSnapshot);

        remoteLogStorageManager.deleteRemoteLogManifestSnapshot(remoteSnapshotDir);
        snapshotFile = new File(remoteSnapshotDir.getPath());
        assertThat(snapshotFile.exists()).isFalse();

        assertThatThrownBy(
                        () ->
                                remoteLogStorageManager.readRemoteLogManifestSnapshot(
                                        remoteSnapshotDir))
                .isInstanceOf(RemoteStorageException.class)
                .hasMessageContaining("Failed to read remote log manifest from " + snapshotFile);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteTable(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();
        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).hasSize(4);

        PhysicalTablePath physicalTablePath = logTablet.getPhysicalTablePath();
        TableBucket tableBucket = logTablet.getTableBucket();
        File remoteDirForBucket =
                new File(
                        FlussPaths.remoteLogTabletDir(
                                        FlussPaths.remoteLogDir(conf),
                                        physicalTablePath,
                                        tableBucket)
                                .toString());
        assertThat(remoteDirForBucket.exists()).isTrue();

        remoteLogStorageManager.deleteTableBucket(
                FlussPaths.remoteLogDir(conf), physicalTablePath, tableBucket);
        assertThat(remoteDirForBucket.exists()).isFalse();
        assertThatThrownBy(
                        () ->
                                remoteLogStorageManager.fetchIndex(
                                        FlussPaths.remoteLogDir(conf),
                                        remoteLogSegment,
                                        IndexType.OFFSET))
                .isInstanceOf(RemoteStorageException.class)
                .hasMessageContaining("Failed to fetch index file type: OFFSET from path");
    }

    @Test
    void testFailedCopyWaitsForOtherUploadsBeforeCleanup() throws Exception {
        ControlledUploadExecutor executor = new ControlledUploadExecutor(4);
        DefaultRemoteLogStorage storage = new DefaultRemoteLogStorage(conf, executor);
        RemoteLogSegment segment =
                createRemoteLogSegmentList(makeLogTabletAndAddSegments(false)).get(0);
        LogSegmentFiles files = createUploadFiles(segment);
        java.nio.file.Files.delete(files.logSegment());
        FutureTask<Void> copy =
                new FutureTask<>(
                        () -> {
                            storage.copyLogSegmentFiles(
                                    FlussPaths.remoteLogDir(conf), segment, files);
                            return null;
                        });
        Thread caller = new Thread(copy);
        caller.start();
        List<Runnable> uploads = executor.takeUploads(4);
        try {
            uploads.get(0).run();
            assertThatThrownBy(() -> copy.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
        } finally {
            uploads.forEach(Runnable::run);
            caller.join(5000);
        }
        assertThat(caller.isAlive()).isFalse();
        assertThatThrownBy(copy::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RemoteStorageException.class);
        storage.deleteLogSegmentFiles(FlussPaths.remoteLogDir(conf), segment);
        uploads.forEach(Runnable::run);
        assertThat(getTestingRemoteLogSegmentDir(segment)).doesNotExist();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAbortedCopyPreventsQueuedUploads(boolean interrupt) throws Exception {
        ControlledUploadExecutor executor = new ControlledUploadExecutor(interrupt ? 4 : 2);
        DefaultRemoteLogStorage storage = new DefaultRemoteLogStorage(conf, executor);
        RemoteLogSegment segment =
                createRemoteLogSegmentList(makeLogTabletAndAddSegments(false)).get(0);
        LogSegmentFiles files = createUploadFiles(segment);
        AtomicBoolean interrupted = new AtomicBoolean();
        FutureTask<Void> copy =
                new FutureTask<>(
                        () -> {
                            try {
                                storage.copyLogSegmentFiles(
                                        FlussPaths.remoteLogDir(conf), segment, files);
                                return null;
                            } finally {
                                interrupted.set(Thread.currentThread().isInterrupted());
                            }
                        });
        Thread caller = new Thread(copy);
        caller.start();
        List<Runnable> uploads = executor.takeUploads(interrupt ? 4 : 2);
        if (interrupt) {
            caller.interrupt();
        }
        try {
            assertThatThrownBy(() -> copy.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(RemoteStorageException.class);
            assertThat(interrupted.get()).isEqualTo(interrupt);
            storage.deleteLogSegmentFiles(FlussPaths.remoteLogDir(conf), segment);
            uploads.forEach(Runnable::run);
            assertThat(getTestingRemoteLogSegmentDir(segment)).doesNotExist();
        } finally {
            uploads.forEach(Runnable::run);
            caller.join(5000);
        }
        assertThat(caller.isAlive()).isFalse();
    }

    private LogSegmentFiles createUploadFiles(RemoteLogSegment segment) throws Exception {
        Path directory = tempDir.toPath().resolve("upload-source");
        java.nio.file.Files.createDirectories(directory);
        Path log =
                directory.resolve(
                        FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                                + ".log");
        Path index =
                directory.resolve(
                        FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                                + ".index");
        Path time =
                directory.resolve(
                        FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                                + ".timeindex");
        Path writer =
                directory.resolve(
                        FlussPaths.filenamePrefixFromOffset(segment.remoteLogEndOffset())
                                + ".writer_snapshot");
        for (Path file : java.util.Arrays.asList(log, index, time, writer)) {
            java.nio.file.Files.write(file, new byte[] {1, 2, 3});
        }
        return new LogSegmentFiles(log, index, time, writer);
    }

    private static class ControlledUploadExecutor extends AbstractExecutorService {
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        private final int accepted;
        private int submitted;

        private ControlledUploadExecutor(int accepted) {
            this.accepted = accepted;
        }

        @Override
        public void execute(Runnable command) {
            if (submitted++ >= accepted) {
                throw new RejectedExecutionException("Injected upload submission failure");
            }
            tasks.add(command);
        }

        private List<Runnable> takeUploads(int count) throws Exception {
            List<Runnable> result = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Runnable task = tasks.poll(5, TimeUnit.SECONDS);
                assertThat(task).isNotNull();
                result.add(task);
            }
            return result;
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
    }

    private File getTestingRemoteLogSegmentDir(RemoteLogSegment remoteLogSegment) {
        return new File(
                FlussPaths.remoteLogSegmentDir(
                                FlussPaths.remoteLogTabletDir(
                                        FlussPaths.remoteLogDir(conf),
                                        remoteLogSegment.physicalTablePath(),
                                        remoteLogSegment.tableBucket()),
                                remoteLogSegment.remoteLogSegmentId())
                        .toString());
    }

    private File getLocalFileByName(String fileName, File[] allFilesForLocalLog) {
        for (File localFile : allFilesForLocalLog) {
            if (localFile.getName().equals(fileName)) {
                return localFile;
            }
        }
        throw new IllegalStateException("Can not find file: " + fileName);
    }
}
