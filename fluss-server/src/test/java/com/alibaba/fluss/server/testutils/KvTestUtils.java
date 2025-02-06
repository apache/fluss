/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbValue;
import com.alibaba.fluss.rpc.messages.PbValueList;
import com.alibaba.fluss.rpc.messages.PrefixLookupResponse;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotDownloadSpec;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotHandle;
import com.alibaba.fluss.server.kv.snapshot.PlaceholderKvFileHandler;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.types.Tuple2;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils related to Kv. */
public class KvTestUtils {

    public static void checkSnapshot(
            CompletedSnapshot completedSnapshot,
            List<Tuple2<byte[], byte[]>> expectedKeyValues,
            long expectLogOffset)
            throws Exception {
        Path temRebuildPath = Files.createTempDirectory("checkSnapshotTemporaryDir");
        try {
            assertThat(completedSnapshot.getLogOffset()).isEqualTo(expectLogOffset);
            try (RocksDBKv rocksDBKv =
                    buildFromSnapshotHandle(
                            completedSnapshot.getKvSnapshotHandle(), temRebuildPath)) {
                // check the key counts
                int keyCounts = getKeyCounts(rocksDBKv.getDb());
                assertThat(keyCounts).isEqualTo(expectedKeyValues.size());
                // check each key/value pair
                for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
                    assertThat(rocksDBKv.get(keyValue.f0)).isEqualTo(keyValue.f1);
                }
            }
        } finally {
            FileUtils.deleteDirectoryQuietly(temRebuildPath.toFile());
        }
    }

    /**
     * Check the current snapshot is with incremental with newly files num based on the previous
     * snapshot.
     *
     * @param currentSnapshotHandle handle for current snapshot
     * @param previousSnapshotHandle handle for previous snapshot
     * @param expectedNewFileNum expected newly uploaded files number
     */
    public static void checkSnapshotIncrementWithNewlyFiles(
            KvSnapshotHandle currentSnapshotHandle,
            KvSnapshotHandle previousSnapshotHandle,
            int expectedNewFileNum) {
        Set<String> previousNewlyFiles =
                toNewlyLocalFiles(previousSnapshotHandle.getSharedKvFileHandles());
        // get the new upload files from the current snapshot
        int newlyUploadedFiles = 0;
        for (KvFileHandleAndLocalPath handle : currentSnapshotHandle.getSharedKvFileHandles()) {
            if (handle.getKvFileHandle() instanceof PlaceholderKvFileHandler) {
                // if it's a placeholder, it should be file in previous snapshot
                assertThat(previousNewlyFiles).contains(handle.getLocalPath());
            } else {
                newlyUploadedFiles += 1;
            }
        }
        assertThat(newlyUploadedFiles).isEqualTo(expectedNewFileNum);
    }

    private static Set<String> toNewlyLocalFiles(List<KvFileHandleAndLocalPath> handles) {
        return handles.stream()
                .filter(handle -> !(handle.getKvFileHandle() instanceof PlaceholderKvFileHandler))
                .map(KvFileHandleAndLocalPath::getLocalPath)
                .collect(Collectors.toSet());
    }

    public static CompletedSnapshot mockCompletedSnapshot(
            Path snapshotRootDir, TableBucket tableBucket, long snapshotId) {
        return new CompletedSnapshot(
                tableBucket,
                snapshotId,
                new FsPath(
                        snapshotRootDir
                                + String.format(
                                        "/tableBucket-%d-bucket-%d-snapshot-%d",
                                        tableBucket.getTableId(),
                                        tableBucket.getBucket(),
                                        snapshotId)),
                new KvSnapshotHandle(Collections.emptyList(), Collections.emptyList(), 0),
                0);
    }

    public static int getKeyCounts(RocksDB rocksDB) {
        int count = 0;
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        }
        return count;
    }

    public static RocksDBKv buildFromSnapshotHandle(
            KvSnapshotHandle kvSnapshotHandle, Path destPath) throws Exception {
        ExecutorService downloadThreadPool = Executors.newSingleThreadExecutor();
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            KvSnapshotDataDownloader dbDataDownloader =
                    new KvSnapshotDataDownloader(downloadThreadPool);
            KvSnapshotDownloadSpec downloadSpec1 =
                    new KvSnapshotDownloadSpec(
                            kvSnapshotHandle,
                            destPath.resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING));

            dbDataDownloader.transferAllDataToDirectory(downloadSpec1, closeableRegistry);
            RocksDBResourceContainer rocksDBResourceContainer =
                    new RocksDBResourceContainer(new Configuration(), destPath.toFile());
            return new RocksDBKvBuilder(
                            destPath.toFile(),
                            rocksDBResourceContainer,
                            rocksDBResourceContainer.getColumnOptions())
                    .build();
        } finally {
            downloadThreadPool.shutdownNow();
        }
    }

    public static void assertLookupResponse(
            LookupResponse lookupResponse, @Nullable byte[] expectedValue) {
        assertThat(lookupResponse.getBucketsRespsCount()).isEqualTo(1);
        PbLookupRespForBucket pbLookupRespForBucket = lookupResponse.getBucketsRespAt(0);
        assertThat(pbLookupRespForBucket.getValuesCount()).isEqualTo(1);
        PbValue pbValue = pbLookupRespForBucket.getValueAt(0);
        byte[] lookupValue = pbValue.hasValues() ? pbValue.getValues() : null;
        assertThat(lookupValue).isEqualTo(expectedValue);
    }

    public static void assertPrefixLookupResponse(
            PrefixLookupResponse prefixLookupResponse, List<List<byte[]>> expectedValues) {
        assertThat(prefixLookupResponse.getBucketsRespsCount()).isEqualTo(1);
        PbPrefixLookupRespForBucket pbPrefixLookupRespForBucket =
                prefixLookupResponse.getBucketsRespAt(0);
        assertThat(pbPrefixLookupRespForBucket.getValueListsCount())
                .isEqualTo(expectedValues.size());
        for (int i = 0; i < expectedValues.size(); i++) {
            PbValueList pbValueList = pbPrefixLookupRespForBucket.getValueListAt(i);
            List<byte[]> bytesResultForOnePrefixKey = expectedValues.get(i);
            assertThat(pbValueList.getValuesCount()).isEqualTo(bytesResultForOnePrefixKey.size());
            for (int j = 0; j < bytesResultForOnePrefixKey.size(); j++) {
                assertThat(pbValueList.getValueAt(j)).isEqualTo(bytesResultForOnePrefixKey.get(j));
            }
        }
    }
}
