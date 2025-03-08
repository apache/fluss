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
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.utils.function.FunctionWithException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A context to provide necessary objects/parameters used by kv snapshot and recover from snapshot.
 */
public interface SnapshotContext {

    /** Get the zookeeper client. */
    ZooKeeperClient getZooKeeperClient();

    /** Get the thread pool to execute tasks running in the async phase of kv snapshot . */
    ExecutorService getAsyncOperationsThreadPool();

    /** Get the uploader for uploading the kv snapshot data. */
    KvSnapshotDataUploader getSnapshotDataUploader();

    /** Get the downloader for downloading the kv snapshot data. */
    KvSnapshotDataDownloader getSnapshotDataDownloader();

    /** Get the scheduler to schedule kv snapshot. */
    ScheduledExecutorService getSnapshotScheduler();

    /** Get a reporter to report completed snapshot. */
    CompletedKvSnapshotCommitter getCompletedSnapshotReporter();

    /** Get the interval of kv snapshot. */
    long getSnapshotIntervalMs();

    /** Get the size of the write buffer for writing the kv snapshot file to remote filesystem. */
    int getSnapshotFsWriteBufferSize();

    /** Get the remote root path to store kv snapshot files. */
    FsPath getRemoteKvDir();

    /**
     * Get the provider of latest CompletedSnapshot for a table bucket. When no completed snapshot
     * exists, the CompletedSnapshot provided will be null.
     */
    FunctionWithException<TableBucket, CompletedSnapshot, Exception>
            getLatestCompletedSnapshotProvider();

    /**
     * Get the max fetch size for fetching log to apply kv during recovering kv. The kv may apply
     * log during recovering.
     */
    int maxFetchLogSizeInRecoverKv();
}
