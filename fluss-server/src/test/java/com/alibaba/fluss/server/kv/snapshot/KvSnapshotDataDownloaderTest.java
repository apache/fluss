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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.utils.CloseableRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link com.alibaba.fluss.server.kv.snapshot.KvSnapshotDataDownloader}. */
class KvSnapshotDataDownloaderTest {

    private static ExecutorService downLoaderThreadPool;

    @BeforeAll
    static void beforeAll() {
        downLoaderThreadPool = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    static void afterAll() {
        if (downLoaderThreadPool != null) {
            downLoaderThreadPool.shutdownNow();
        }
    }

    /** Tests that download files with multi-thread correctly. */
    @Test
    void testMultiThreadRestoreCorrectly(@TempDir Path destDir, @TempDir Path srcDir)
            throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<KvSnapshotDownloadSpec> downloadRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            downloadRequests.add(createDownloadRequestForContent(destDir, srcDir, contents[i], i));
        }

        KvSnapshotDataDownloader kvSnapshotDataDownloader =
                new KvSnapshotDataDownloader(downLoaderThreadPool);
        kvSnapshotDataDownloader.transferAllDataToDirectory(
                downloadRequests, new CloseableRegistry());

        for (int i = 0; i < numRemoteHandles; ++i) {
            KvSnapshotDownloadSpec downloadRequest = downloadRequests.get(i);
            Path dstPath = downloadRequest.getDownloadDestination();
            assertThat(dstPath.toFile()).exists();
            for (int j = 0; j < numSubHandles; ++j) {
                assertStateContentEqual(
                        contents[i][j], dstPath.resolve(String.format("shared-%d-%d", i, j)));
            }
        }
    }

    /** Tests cleanup on download failures. */
    @Test
    public void testMultiThreadCleanupOnFailure(@TempDir Path destDir, @TempDir Path srcDir)
            throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<KvSnapshotDownloadSpec> downloadRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            downloadRequests.add(createDownloadRequestForContent(destDir, srcDir, contents[i], i));
        }

        KvSnapshotHandle kvSnapshotHandle =
                downloadRequests.get(downloadRequests.size() - 1).getKvSnapshotHandle();

        // Add a state handle that induces an exception
        kvSnapshotHandle
                .getSharedKvFileHandles()
                .add(
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle(
                                        // a file that doesn't exist should then throw IOException
                                        "file-non-exist", 0),
                                "error-handle"));

        CloseableRegistry closeableRegistry = new CloseableRegistry();
        KvSnapshotDataDownloader kvSnapshotDownloader =
                new KvSnapshotDataDownloader(downLoaderThreadPool);
        assertThatThrownBy(
                        () ->
                                kvSnapshotDownloader.transferAllDataToDirectory(
                                        downloadRequests, closeableRegistry))
                .isInstanceOf(IOException.class);

        // Check that all download directories have been deleted
        for (KvSnapshotDownloadSpec downloadRequest : downloadRequests) {
            assertThat(downloadRequest.getDownloadDestination().toFile()).doesNotExist();
        }
        // The passed in closable registry should not be closed by us on failure.
        assertThat(closeableRegistry.isClosed()).isFalse();
    }

    private void assertStateContentEqual(byte[] expected, Path path) throws IOException {
        byte[] actual = Files.readAllBytes(Paths.get(path.toUri()));
        assertThat(actual).isEqualTo(expected);
    }

    private byte[][][] createContents(int numRemoteHandles, int numSubHandles) {
        Random random = new Random();
        byte[][][] contents = new byte[numRemoteHandles][numSubHandles][];
        for (int i = 0; i < numRemoteHandles; ++i) {
            for (int j = 0; j < numSubHandles; ++j) {
                contents[i][j] = new byte[random.nextInt(100000) + 1];
                random.nextBytes(contents[i][j]);
            }
        }
        return contents;
    }

    private KvSnapshotDownloadSpec createDownloadRequestForContent(
            Path dstPath, Path srcPath, byte[][] content, int remoteHandleId) throws IOException {
        int numSubHandles = content.length;
        List<KvFileHandle> handles = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            Path path = srcPath.resolve(String.format("state-%d-%d", remoteHandleId, i));
            Files.write(path, content[i]);
            handles.add(new KvFileHandle(path.toString(), content.length));
        }

        List<KvFileHandleAndLocalPath> sharedStates = new ArrayList<>(numSubHandles);
        List<KvFileHandleAndLocalPath> privateStates = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            sharedStates.add(
                    KvFileHandleAndLocalPath.of(
                            handles.get(i), String.format("shared-%d-%d", remoteHandleId, i)));
            privateStates.add(
                    KvFileHandleAndLocalPath.of(
                            handles.get(i), String.format("private-%d-%d", remoteHandleId, i)));
        }

        KvSnapshotHandle kvSnapshotHandle = new KvSnapshotHandle(sharedStates, privateStates, -1);

        return new KvSnapshotDownloadSpec(kvSnapshotHandle, dstPath);
    }
}
