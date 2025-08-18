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

import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.local.LocalFileSystem;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link com.alibaba.fluss.server.kv.snapshot.KvSnapshotDataUploader}. */
class KvSnapshotDataUploaderTest {

    @TempDir private Path temporaryFolder;
    private ExecutorService downLoaderThreadPool;

    @BeforeEach
    void beforeEach() {
        downLoaderThreadPool = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void afterEach() {
        if (downLoaderThreadPool != null) {
            downLoaderThreadPool.shutdownNow();
        }
    }

    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
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

        KvSnapshotDataUploader snapshotUploader = new KvSnapshotDataUploader(downLoaderThreadPool);
        List<KvFileHandleAndLocalPath> sstFiles =
                snapshotUploader.uploadFilesToSnapshotLocation(
                        sstFilePaths,
                        snapshotLocation,
                        SnapshotFileScope.SHARED,
                        new CloseableRegistry(),
                        new CloseableRegistry());

        for (Path path : sstFilePaths) {
            KvFileHandle kvFileHandle =
                    sstFiles.stream()
                            .filter(e -> e.getLocalPath().equals(path.getFileName().toString()))
                            .findFirst()
                            .get()
                            .getKvFileHandle();
            assertThat(kvFileHandle.getFilePath())
                    .startsWith(LocalFileSystem.getLocalFsURI().getScheme());
            FsPath fsPath = new FsPath(kvFileHandle.getFilePath());
            FSDataInputStream inputStream = fsPath.getFileSystem().open(fsPath);
            assertContentEqual(path, inputStream);
        }
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
