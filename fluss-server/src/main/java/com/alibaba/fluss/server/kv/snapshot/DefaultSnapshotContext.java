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

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.kv.KvSnapshotResource;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.function.FunctionWithException;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/** A default implementation for {@link SnapshotContext}. */
public class DefaultSnapshotContext implements SnapshotContext {

    private final ZooKeeperClient zooKeeperClient;
    private final CompletedKvSnapshotCommitter completedKvSnapshotCommitter;

    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService asyncOperationsThreadPool;
    private final KvSnapshotDataUploader kvSnapshotDataUploader;
    private final KvSnapshotDataDownloader kvSnapshotDataDownloader;

    private final long kvSnapshotIntervalMs;

    /** The write buffer size for writing the kv snapshot file to remote filesystem. */
    private final int writeBufferSizeInBytes;

    private final CompletedSnapshotHandleStore completedSnapshotHandleStore;

    private final int maxFetchLogSizeInRecoverKv;

    private final FsPath remoteKvDir;

    private DefaultSnapshotContext(
            ZooKeeperClient zooKeeperClient,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            ScheduledExecutorService snapshotScheduler,
            ExecutorService asyncOperationsThreadPool,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            KvSnapshotDataDownloader kvSnapshotDataDownloader,
            long kvSnapshotIntervalMs,
            int writeBufferSizeInBytes,
            FsPath remoteKvDir,
            CompletedSnapshotHandleStore completedSnapshotHandleStore,
            int maxFetchLogSizeInRecoverKv) {
        this.zooKeeperClient = zooKeeperClient;
        this.completedKvSnapshotCommitter = completedKvSnapshotCommitter;
        this.snapshotScheduler = snapshotScheduler;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;
        this.kvSnapshotDataUploader = kvSnapshotDataUploader;
        this.kvSnapshotDataDownloader = kvSnapshotDataDownloader;
        this.kvSnapshotIntervalMs = kvSnapshotIntervalMs;
        this.writeBufferSizeInBytes = writeBufferSizeInBytes;
        this.remoteKvDir = remoteKvDir;

        this.completedSnapshotHandleStore = completedSnapshotHandleStore;
        this.maxFetchLogSizeInRecoverKv = maxFetchLogSizeInRecoverKv;
    }

    public static DefaultSnapshotContext create(
            ZooKeeperClient zkClient,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            KvSnapshotResource kvSnapshotResource,
            Configuration conf) {
        return new DefaultSnapshotContext(
                zkClient,
                completedKvSnapshotCommitter,
                kvSnapshotResource.getKvSnapshotScheduler(),
                kvSnapshotResource.getAsyncOperationsThreadPool(),
                kvSnapshotResource.getKvSnapshotDataUploader(),
                kvSnapshotResource.getKvSnapshotDataDownloader(),
                conf.get(ConfigOptions.KV_SNAPSHOT_INTERVAL).toMillis(),
                (int) conf.get(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE).getBytes(),
                FlussPaths.remoteKvDir(conf),
                new ZooKeeperCompletedSnapshotHandleStore(zkClient),
                (int) conf.get(ConfigOptions.KV_RECOVER_LOG_RECORD_BATCH_MAX_SIZE).getBytes());
    }

    public ZooKeeperClient getZooKeeperClient() {
        return zooKeeperClient;
    }

    public ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    public KvSnapshotDataUploader getSnapshotDataUploader() {
        return kvSnapshotDataUploader;
    }

    @Override
    public KvSnapshotDataDownloader getSnapshotDataDownloader() {
        return kvSnapshotDataDownloader;
    }

    public ScheduledExecutorService getSnapshotScheduler() {
        return snapshotScheduler;
    }

    @Override
    public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
        return completedKvSnapshotCommitter;
    }

    @Override
    public long getSnapshotIntervalMs() {
        return kvSnapshotIntervalMs;
    }

    @Override
    public int getSnapshotFsWriteBufferSize() {
        return writeBufferSizeInBytes;
    }

    public FsPath getRemoteKvDir() {
        return remoteKvDir;
    }

    @Override
    public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
            getLatestCompletedSnapshotProvider() {
        return (tableBucket) -> {
            Optional<CompletedSnapshotHandle> optSnapshotHandle =
                    completedSnapshotHandleStore.getLatestCompletedSnapshotHandle(tableBucket);
            if (optSnapshotHandle.isPresent()) {
                return optSnapshotHandle.get().retrieveCompleteSnapshot();
            } else {
                return null;
            }
        };
    }

    @Override
    public int maxFetchLogSizeInRecoverKv() {
        return maxFetchLogSizeInRecoverKv;
    }
}
