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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot} . */
class CompletedSnapshotTest {

    @Test
    void testCleanup(@TempDir Path tempDir) throws Exception {
        // create base directory for snapshot
        TableBucket tableBucket = new TableBucket(1, 1);
        Path localFileDir = makeDir(tempDir, "local");
        Path snapshotBaseLocation = makeDir(tempDir, "snapshot");
        // create a share directory
        Path shareDir = makeDir(snapshotBaseLocation, "share");

        // snapshot 1
        long snapshotId = 1;
        Path snapshotPath = makeDir(snapshotBaseLocation, "snapshot-" + snapshotId);
        KvSnapshotHandle kvSnapshotHandle =
                makeSnapshotHandle(localFileDir, snapshotPath, shareDir, 100);
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        tableBucket,
                        snapshotId,
                        FsPath.fromLocalFile(snapshotPath.toFile()),
                        kvSnapshotHandle);

        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();
        // register the snapshot to a registry
        snapshot.registerSharedKvFilesAfterRestored(sharedKvFileRegistry);
        Executor ioExecutor = Executors.directExecutor();
        snapshot.discardAsync(ioExecutor).get();

        // share files shouldn't be deleted
        checkCompletedSnapshotCleanUp(snapshotPath, kvSnapshotHandle, false);

        // snapshot 2
        snapshotId = 2;
        snapshotPath = makeDir(snapshotBaseLocation, "snapshot-" + snapshotId);
        kvSnapshotHandle = makeSnapshotHandle(localFileDir, snapshotPath, shareDir, 100);
        snapshot =
                new CompletedSnapshot(
                        tableBucket,
                        snapshotId,
                        FsPath.fromLocalFile(snapshotPath.toFile()),
                        kvSnapshotHandle);
        snapshot.discardAsync(ioExecutor).get();
        // share files should be deleted since it has not been registered
        checkCompletedSnapshotCleanUp(snapshotPath, kvSnapshotHandle, true);
    }

    @Test
    void testKvSnapshotSize(@TempDir Path tempDir) throws Exception {
        // create base directory for snapshot
        TableBucket tableBucket = new TableBucket(1, 1);
        Path localFileDir = makeDir(tempDir, "local");
        Path snapshotBaseLocation = makeDir(tempDir, "snapshot");
        // create a share directory
        Path shareDir = makeDir(snapshotBaseLocation, "share");

        // snapshot 1
        long snapshotId = 1;
        Path snapshotPath = makeDir(snapshotBaseLocation, "snapshot-" + snapshotId);
        KvSnapshotHandle kvSnapshotHandle =
                makeSnapshotHandle(localFileDir, snapshotPath, shareDir, 100);
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        tableBucket,
                        snapshotId,
                        FsPath.fromLocalFile(snapshotPath.toFile()),
                        kvSnapshotHandle);
        assertThat(snapshot.getKvSnapshotHandle().getSnapshotSize()).isEqualTo(400L);
    }

    private void checkCompletedSnapshotCleanUp(
            Path snapshotPath, KvSnapshotHandle kvSnapshotHandle, boolean isShareFileShouldDelete) {
        // private should be deleted, but the local file should still remain
        for (KvFileHandleAndLocalPath kvFileHandleAndLocalPath :
                kvSnapshotHandle.getPrivateFileHandles()) {
            assertThat(new File(kvFileHandleAndLocalPath.getKvFileHandle().getFilePath()))
                    .doesNotExist();
            assertThat(new File(kvFileHandleAndLocalPath.getLocalPath())).exists();
        }

        // check the share files is as expected, and the local file should still remain
        for (KvFileHandleAndLocalPath kvFileHandleAndLocalPath :
                kvSnapshotHandle.getSharedKvFileHandles()) {
            // share files should also be deleted, but the local file should still remain
            if (isShareFileShouldDelete) {
                assertThat(new File(kvFileHandleAndLocalPath.getKvFileHandle().getFilePath()))
                        .doesNotExist();
            } else {
                assertThat(new File(kvFileHandleAndLocalPath.getKvFileHandle().getFilePath()))
                        .exists();
            }
            assertThat(new File(kvFileHandleAndLocalPath.getLocalPath())).exists();
        }

        // check the snapshot dir for is deleted
        assertThat(snapshotPath.toFile()).doesNotExist();
    }

    KvSnapshotHandle makeSnapshotHandle(
            Path localPath, Path baseSnapshotDir, Path shareDir, int perFileSize)
            throws IOException {
        List<KvFileHandleAndLocalPath> sharedFileHandles = new ArrayList<>();

        // create share files
        // share files dir
        for (int i = 0; i < 2; i++) {
            File localFile = new File(localPath.toFile(), "local_share_" + i);
            localFile.createNewFile();
            writeRandomDataToFile(localFile, perFileSize);
            File shareFile = new File(shareDir.toFile(), "remote_" + i);
            shareFile.createNewFile();
            writeRandomDataToFile(shareFile, perFileSize);
            sharedFileHandles.add(
                    KvFileHandleAndLocalPath.of(
                            new KvFileHandle(shareFile.getPath(), shareFile.length()),
                            localFile.getPath()));
        }

        // create private files
        // private files dir
        List<KvFileHandleAndLocalPath> privateFileHandles = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            File localFile = new File(localPath.toFile(), "local_private_i");
            localFile.createNewFile();
            writeRandomDataToFile(localFile, perFileSize);
            File privateFile = new File(baseSnapshotDir.toFile(), "remote_i");
            privateFile.createNewFile();
            writeRandomDataToFile(privateFile, perFileSize);
            privateFileHandles.add(
                    KvFileHandleAndLocalPath.of(
                            new KvFileHandle(privateFile.getPath(), privateFile.length()),
                            localFile.getPath()));
        }
        return new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 10);
    }

    private Path makeDir(Path basePath, String dirName) {
        File file = new File(basePath.toFile(), dirName);
        file.mkdirs();
        return file.toPath();
    }

    private static void writeRandomDataToFile(File file, int byteCount) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] data = new byte[byteCount];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i % 256);
            }
            fos.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
