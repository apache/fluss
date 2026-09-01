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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.server.kv.rocksdb.RocksDBExtension;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.utils.TestProcessBuilder;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;

import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.testutils.KvTestUtils.checkSnapshotIncrementWithNewlyFiles;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RocksIncrementalSnapshot} . */
class RocksIncrementalSnapshotTest {

    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    private static ExecutorService dataTransferThreadPool;

    @BeforeAll
    static void beforeAll() {
        dataTransferThreadPool = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    static void afterAll() {
        if (dataTransferThreadPool != null) {
            dataTransferThreadPool.shutdownNow();
        }
    }

    @Test
    void testIncrementalSnapshot(@TempDir Path snapshotBaseDir, @TempDir Path snapshotDownDir)
            throws Exception {
        FsPath testingTabletDir = FsPath.fromLocalFile(snapshotBaseDir.toFile());
        FsPath snapshotShareDir = FlussPaths.remoteKvSharedDir(testingTabletDir);
        FsPath currentSnapshotDir = FlussPaths.remoteKvSnapshotDir(testingTabletDir, 1L);
        SnapshotLocation snapshotLocation =
                new SnapshotLocation(
                        LocalFileSystem.getSharedInstance(),
                        currentSnapshotDir,
                        snapshotShareDir,
                        1024);
        try (CloseableRegistry closeableRegistry = new CloseableRegistry();
                RocksIncrementalSnapshot incrementalSnapshot = createIncrementalSnapshot()) {
            RocksDB rocksDB = rocksDBExtension.getRocksDb();
            rocksDB.put("key1".getBytes(), "val1".getBytes());

            // make and notify snapshot with id 1
            KvSnapshotHandle kvSnapshotHandle1 =
                    snapshot(1L, incrementalSnapshot, snapshotLocation, closeableRegistry);
            incrementalSnapshot.notifySnapshotComplete(1L);

            // make and notify snapshot with id 2
            KvSnapshotHandle kvSnapshotHandle2 =
                    snapshot(2L, incrementalSnapshot, snapshotLocation, closeableRegistry);
            incrementalSnapshot.notifySnapshotComplete(2L);
            // the share kv file handles for cp2 should be equal to the handles for cp1
            verifyShareFileEqual(kvSnapshotHandle2, kvSnapshotHandle1);
            // all file handles should be PlaceHolderHandle
            for (KvFileHandleAndLocalPath kvFileHandleAndLocalPath :
                    kvSnapshotHandle2.getSharedKvFileHandles()) {
                assertThat(kvFileHandleAndLocalPath.getKvFileHandle())
                        .isInstanceOf(PlaceholderKvFileHandler.class);
            }

            // write some data again
            rocksDB.put("key2".getBytes(), "val2".getBytes());
            snapshot(3L, incrementalSnapshot, snapshotLocation, closeableRegistry);
            // assume it's fail
            incrementalSnapshot.notifySnapshotAbort(3L);

            // write some data again
            rocksDB.put("key3".getBytes(), "val3".getBytes());
            KvSnapshotHandle kvSnapshotHandle4 =
                    snapshot(4L, incrementalSnapshot, snapshotLocation, closeableRegistry);
            // make sure the uploaded files contains the files in snapshot 3 and snapshot 4
            // there're two newly uploaded files, one for cp3, one for cp4
            checkSnapshotIncrementWithNewlyFiles(kvSnapshotHandle4, kvSnapshotHandle1, 2);

            // now, let try to rebuild from cp2 and cp4
            // test restore from cp2
            Path dest1 = snapshotDownDir.resolve("restore1");
            try (RocksDBKv rocksDBKv =
                    KvTestUtils.buildFromSnapshotHandle(kvSnapshotHandle2, dest1)) {
                assertThat(rocksDBKv.get("key1".getBytes())).isEqualTo("val1".getBytes());
                assertThat(rocksDBKv.get("key2".getBytes())).isNull();
                assertThat(rocksDBKv.get("key3".getBytes())).isNull();
            }
            Path dest2 = snapshotDownDir.resolve("restore2");
            // test restore from cp4
            try (RocksDBKv rocksDBKv =
                    KvTestUtils.buildFromSnapshotHandle(kvSnapshotHandle4, dest2)) {
                assertThat(rocksDBKv.get("key1".getBytes())).isEqualTo("val1".getBytes());
                assertThat(rocksDBKv.get("key2".getBytes())).isEqualTo("val2".getBytes());
                assertThat(rocksDBKv.get("key3".getBytes())).isEqualTo("val3".getBytes());
            }

            // write some data again
            rocksDB.put("key3".getBytes(), "val3_1".getBytes());
            KvSnapshotHandle kvSnapshotHandle5 =
                    snapshot(5L, incrementalSnapshot, snapshotLocation, closeableRegistry);
            // discard the snapshot handle
            kvSnapshotHandle5.discard();

            // we can still restore from cp4
            Path dest3 = snapshotDownDir.resolve("restore3");
            try (RocksDBKv rocksDBKv =
                    KvTestUtils.buildFromSnapshotHandle(kvSnapshotHandle4, dest3)) {
                assertThat(rocksDBKv.get("key1".getBytes())).isEqualTo("val1".getBytes());
                assertThat(rocksDBKv.get("key2".getBytes())).isEqualTo("val2".getBytes());
                assertThat(rocksDBKv.get("key3".getBytes())).isEqualTo("val3".getBytes());
            }
        }
    }

    @Test
    void testSnapshotCanBeReadByFrocksDB(
            @TempDir Path snapshotBaseDir, @TempDir Path snapshotDownDir) throws Exception {
        FsPath testingTabletDir = FsPath.fromLocalFile(snapshotBaseDir.toFile());
        SnapshotLocation snapshotLocation =
                new SnapshotLocation(
                        LocalFileSystem.getSharedInstance(),
                        FlussPaths.remoteKvSnapshotDir(testingTabletDir, 1L),
                        FlussPaths.remoteKvSharedDir(testingTabletDir),
                        1024);

        try (CloseableRegistry closeableRegistry = new CloseableRegistry();
                RocksIncrementalSnapshot incrementalSnapshot = createIncrementalSnapshot()) {
            RocksDB rocksDB = rocksDBExtension.getRocksDb();
            rocksDB.put(
                    "key1".getBytes(StandardCharsets.UTF_8),
                    "val1".getBytes(StandardCharsets.UTF_8));
            rocksDB.put(
                    "key2".getBytes(StandardCharsets.UTF_8),
                    "val2".getBytes(StandardCharsets.UTF_8));

            KvSnapshotHandle snapshotHandle =
                    snapshot(1L, incrementalSnapshot, snapshotLocation, closeableRegistry);
            incrementalSnapshot.notifySnapshotComplete(1L);

            Path restoredDbPath = snapshotDownDir.resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
            KvSnapshotDataDownloader snapshotDataDownloader =
                    new KvSnapshotDataDownloader(dataTransferThreadPool);
            snapshotDataDownloader.transferAllDataToDirectory(
                    new KvSnapshotDownloadSpec(snapshotHandle, restoredDbPath), closeableRegistry);

            TestProcessBuilder.TestProcess frocksDBReader = null;
            try {
                frocksDBReader =
                        new TestProcessBuilder(FrocksDBSnapshotReader.class.getName())
                                .addMainClassArg(restoredDbPath.toString())
                                .addMainClassArg("key1")
                                .addMainClassArg("val1")
                                .addMainClassArg("key2")
                                .addMainClassArg("val2")
                                .start();

                boolean exited = frocksDBReader.getProcess().waitFor(1, TimeUnit.MINUTES);
                assertThat(exited)
                        .describedAs(
                                "FRocksDB reader process output: %s", processOutput(frocksDBReader))
                        .isTrue();
                assertThat(frocksDBReader.getProcess().exitValue())
                        .describedAs(
                                "FRocksDB reader process output: %s", processOutput(frocksDBReader))
                        .isZero();
            } finally {
                if (frocksDBReader != null && frocksDBReader.getProcess().isAlive()) {
                    frocksDBReader.destroy();
                }
            }
        }
    }

    private String processOutput(TestProcessBuilder.TestProcess process) {
        return process.getProcessOutput().toString() + process.getErrorOutput().toString();
    }

    private void verifyShareFileEqual(
            KvSnapshotHandle kvSnapshotHandle1, KvSnapshotHandle kvSnapshotHandle2) {
        List<KvFileHandleAndLocalPath> handles1 = kvSnapshotHandle1.getSharedKvFileHandles();
        List<KvFileHandleAndLocalPath> handles2 = kvSnapshotHandle2.getSharedKvFileHandles();
        assertThat(handles1.size()).isEqualTo(handles2.size());
        for (int i = 0; i < handles1.size(); i++) {
            KvFileHandleAndLocalPath handle1 = handles1.get(i);
            KvFileHandleAndLocalPath handle2 = handles2.get(i);
            assertThat(handle1.getLocalPath()).isEqualTo(handle2.getLocalPath());
            assertThat(handle1.getKvFileHandle().getFilePath())
                    .isEqualTo(handle2.getKvFileHandle().getFilePath());
        }
    }

    private RocksIncrementalSnapshot createIncrementalSnapshot() {
        long lastCompletedSnapshotId = -1L;
        Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles = new HashMap<>();
        ResourceGuard rocksDBResourceGuard = new ResourceGuard();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();

        KvSnapshotDataUploader snapshotDataUploader =
                new KvSnapshotDataUploader(dataTransferThreadPool);
        return new RocksIncrementalSnapshot(
                uploadedSstFiles,
                rocksDB,
                rocksDBResourceGuard,
                snapshotDataUploader,
                rocksDBExtension.getRockDbDir(),
                lastCompletedSnapshotId);
    }

    public KvSnapshotHandle snapshot(
            long snapshotId,
            RocksIncrementalSnapshot incrementalSnapshot,
            SnapshotLocation snapshotLocation,
            CloseableRegistry closeableRegistry)
            throws Exception {
        RocksIncrementalSnapshot.NativeRocksDBSnapshotResources nativeRocksDBSnapshotResources =
                incrementalSnapshot.syncPrepareResources(snapshotId);

        return incrementalSnapshot
                .asyncSnapshot(
                        nativeRocksDBSnapshotResources,
                        snapshotId,
                        new TabletState(0L, null, null),
                        snapshotLocation)
                .get(closeableRegistry)
                .getKvSnapshotHandle();
    }
}
