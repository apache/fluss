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

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test class for {@link org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader}. */
class KvSnapshotDataUploaderTest {

    @TempDir private Path temporaryFolder;
    private ExecutorService uploaderThreadPool;

    @BeforeEach
    void beforeEach() {
        uploaderThreadPool = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void afterEach() {
        if (uploaderThreadPool != null) {
            uploaderThreadPool.shutdownNow();
        }
    }

    /** Verifies file contents and byte accounting across concurrent uploads. */
    @Test
    void testMultiThreadUploadCorrectly() throws Exception {
        File snapshotSharedFolder = new File(temporaryFolder.toFile(), "shared");
        FsPath snapshotSharedDirectory = FsPath.fromLocalFile(snapshotSharedFolder);

        SnapshotLocation snapshotLocation =
                new SnapshotLocation(
                        LocalFileSystem.getSharedInstance(),
                        snapshotSharedDirectory,
                        snapshotSharedDirectory,
                        1024);

        String localFolder = "local";
        new File(temporaryFolder.toFile(), localFolder).mkdir();

        int sstFileCount = 6;
        int fileSizeThreshold = 1024;
        List<Path> sstFilePaths =
                generateRandomSstFiles(localFolder, sstFileCount, fileSizeThreshold);

        KvSnapshotDataUploader snapshotUploader = new KvSnapshotDataUploader(uploaderThreadPool);
        Counter counter = new ThreadSafeSimpleCounter();
        List<KvFileHandleAndLocalPath> sstFiles =
                snapshotUploader.uploadFilesToSnapshotLocation(
                        sstFilePaths,
                        snapshotLocation,
                        SnapshotFileScope.SHARED,
                        new CloseableRegistry(),
                        new CloseableRegistry(),
                        counter);

        long expectedBytes = 0L;
        for (Path path : sstFilePaths) {
            expectedBytes += Files.size(path);
            KvFileHandle kvFileHandle =
                    sstFiles.stream()
                            .filter(e -> e.getLocalPath().equals(path.getFileName().toString()))
                            .findFirst()
                            .get()
                            .getKvFileHandle();
            assertThat(kvFileHandle.getFilePath())
                    .startsWith(LocalFileSystem.getLocalFsURI().getScheme());
            FsPath fsPath = new FsPath(kvFileHandle.getFilePath());
            try (FSDataInputStream inputStream = fsPath.getFileSystem().open(fsPath)) {
                assertContentEqual(path, inputStream);
            }
        }
        assertThat(counter.getCount()).isEqualTo(expectedBytes);
    }

    @ParameterizedTest
    @EnumSource(SnapshotFileScope.class)
    void testSuccessfulUploadsAndReuploads(SnapshotFileScope scope) throws Exception {
        Path file = Files.write(temporaryFolder.resolve("snapshot-file"), new byte[123]);
        Counter counter = new ThreadSafeSimpleCounter();
        Counter otherTableCounter = new ThreadSafeSimpleCounter();
        KvSnapshotDataUploader uploader = new KvSnapshotDataUploader(uploaderThreadPool);
        try (CloseableRegistry registry = new CloseableRegistry();
                CloseableRegistry resources = new CloseableRegistry()) {
            for (int i = 1; i <= 2; i++) {
                uploader.uploadFilesToSnapshotLocation(
                        Collections.singletonList(file),
                        createSnapshotLocation(),
                        scope,
                        registry,
                        resources,
                        counter);
                assertThat(counter.getCount()).isEqualTo(123L * i);
            }
            uploader.uploadFilesToSnapshotLocation(
                    Collections.singletonList(file),
                    createSnapshotLocation(),
                    scope,
                    registry,
                    resources,
                    otherTableCounter);
            assertThat(otherTableCounter.getCount()).isEqualTo(123L);
            assertThat(counter.getCount()).isEqualTo(246L);
            uploader.uploadFilesToSnapshotLocation(
                    Collections.emptyList(),
                    createSnapshotLocation(),
                    scope,
                    registry,
                    resources,
                    counter);
            assertThat(counter.getCount()).isEqualTo(246L);
        }
        // Cleanup of an aborted snapshot must not roll back successful upload traffic.
        assertThat(counter.getCount()).isEqualTo(246L);
    }

    @Test
    void testSuccessfulFileCountedWhenAnotherUploadFails() throws Exception {
        Path file = Files.write(temporaryFolder.resolve("success.sst"), new byte[79]);
        Counter counter = new ThreadSafeSimpleCounter();
        KvSnapshotDataUploader uploader = new KvSnapshotDataUploader(uploaderThreadPool);
        try (CloseableRegistry registry = new CloseableRegistry();
                CloseableRegistry resources = new CloseableRegistry()) {
            assertThatThrownBy(
                            () ->
                                    uploader.uploadFilesToSnapshotLocation(
                                            Arrays.asList(
                                                    file, temporaryFolder.resolve("missing.sst")),
                                            createSnapshotLocation(),
                                            SnapshotFileScope.SHARED,
                                            registry,
                                            resources,
                                            counter))
                    .isInstanceOf(IOException.class);
            // The aggregate future fails fast; the other upload can still be running.
            retry(Duration.ofMinutes(1), () -> assertThat(counter.getCount()).isEqualTo(79L));
        }
        assertThat(counter.getCount()).isEqualTo(79L);
    }

    @ParameterizedTest
    @EnumSource(UploadFailure.class)
    void testFailedUploadsNotCounted(UploadFailure failure) throws Exception {
        Path file = Files.write(temporaryFolder.resolve("failed.sst"), new byte[123]);
        Counter counter = new ThreadSafeSimpleCounter();
        SnapshotLocation location = mock(SnapshotLocation.class);
        SnapshotLocation.FsSnapshotOutputStream stream =
                mock(SnapshotLocation.FsSnapshotOutputStream.class);
        when(location.createSnapshotOutputStream(SnapshotFileScope.SHARED)).thenReturn(stream);
        if (failure == UploadFailure.WRITE) {
            doThrow(new IOException("write failed"))
                    .when(stream)
                    .write(any(byte[].class), anyInt(), anyInt());
        } else {
            when(stream.closeAndGetHandle()).thenThrow(new IOException("close failed"));
        }
        KvSnapshotDataUploader uploader = new KvSnapshotDataUploader(uploaderThreadPool);
        try (CloseableRegistry registry = new CloseableRegistry();
                CloseableRegistry resources = new CloseableRegistry()) {
            assertThatThrownBy(
                            () ->
                                    uploader.uploadFilesToSnapshotLocation(
                                            Collections.singletonList(file),
                                            location,
                                            SnapshotFileScope.SHARED,
                                            registry,
                                            resources,
                                            counter))
                    .isInstanceOf(IOException.class);
            assertThat(counter.getCount()).isZero();
        }
    }

    @Test
    void testSuccessfulUploadCountedWhenCleanupRegistrationFails() throws Exception {
        Path file = Files.write(temporaryFolder.resolve("success.sst"), new byte[91]);
        Counter counter = new ThreadSafeSimpleCounter();
        KvSnapshotDataUploader uploader = new KvSnapshotDataUploader(uploaderThreadPool);
        try (CloseableRegistry registry = new CloseableRegistry();
                CloseableRegistry resources = new CloseableRegistry()) {
            resources.close();
            assertThatThrownBy(
                            () ->
                                    uploader.uploadFilesToSnapshotLocation(
                                            Collections.singletonList(file),
                                            createSnapshotLocation(),
                                            SnapshotFileScope.SHARED,
                                            registry,
                                            resources,
                                            counter))
                    .isInstanceOf(IOException.class);
            assertThat(counter.getCount()).isEqualTo(91L);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, 9999L})
    void testCountsCopiedBytesInsteadOfReportedHandleSize(long reportedSize) throws Exception {
        Path file = Files.write(temporaryFolder.resolve("snapshot.sst"), new byte[123]);
        Counter counter = new ThreadSafeSimpleCounter();
        SnapshotLocation location = mock(SnapshotLocation.class);
        SnapshotLocation.FsSnapshotOutputStream stream =
                mock(SnapshotLocation.FsSnapshotOutputStream.class);
        when(location.createSnapshotOutputStream(SnapshotFileScope.SHARED)).thenReturn(stream);
        when(stream.closeAndGetHandle())
                .thenReturn(
                        new KvFileHandle(
                                FsPath.fromLocalFile(
                                                temporaryFolder.resolve("remote-file").toFile())
                                        .toString(),
                                reportedSize));
        KvSnapshotDataUploader uploader = new KvSnapshotDataUploader(uploaderThreadPool);
        try (CloseableRegistry registry = new CloseableRegistry();
                CloseableRegistry resources = new CloseableRegistry()) {
            uploader.uploadFilesToSnapshotLocation(
                    Collections.singletonList(file),
                    location,
                    SnapshotFileScope.SHARED,
                    registry,
                    resources,
                    counter);
            assertThat(counter.getCount()).isEqualTo(123L);
        }
    }

    private SnapshotLocation createSnapshotLocation() {
        FsPath remote = FsPath.fromLocalFile(temporaryFolder.resolve("remote").toFile());
        return new SnapshotLocation(LocalFileSystem.getSharedInstance(), remote, remote, 1024);
    }

    private enum UploadFailure {
        WRITE,
        CLOSE
    }

    private void assertContentEqual(Path stateFilePath, FSDataInputStream inputStream)
            throws IOException {
        byte[] expected = Files.readAllBytes(stateFilePath);
        byte[] actual = new byte[expected.length];
        IOUtils.readFully(inputStream, actual, 0, actual.length);
        assertThat(inputStream.read()).isEqualTo(-1);
        assertThat(actual).isEqualTo(expected);
    }

    private List<Path> generateRandomSstFiles(
            String localFolder, int sstFileCount, int fileSizeThreshold) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        List<Path> sstFilePaths = new ArrayList<>(sstFileCount);
        for (int i = 0; i < sstFileCount; ++i) {
            File file =
                    new File(temporaryFolder.toFile(), String.format("%s/%d.sst", localFolder, i));
            generateRandomFileContent(
                    file.getPath(), random.nextInt(1_000_000) + fileSizeThreshold);
            sstFilePaths.add(file.toPath());
        }
        return sstFilePaths;
    }

    private void generateRandomFileContent(String filePath, int fileLength) throws IOException {
        FileOutputStream fileStream = new FileOutputStream(filePath);
        byte[] contents = new byte[fileLength];
        ThreadLocalRandom.current().nextBytes(contents);
        fileStream.write(contents);
        fileStream.close();
    }
}
