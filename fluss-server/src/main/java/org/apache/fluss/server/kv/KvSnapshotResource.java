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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Containing resources needed to do kv snapshot. It contains:
 *
 * <ul>
 *   <li>A scheduler to schedule snapshot for kv periodically
 *   <li>A thread pool for the async part of kv snapshot
 *   <li>A uploader to upload snapshot data in the async part of kv snapshot
 * </ul>
 */
public class KvSnapshotResource {

    /** A scheduler to schedule kv snapshot. */
    private final ScheduledExecutorService kvSnapshotScheduler;
    /** Thread pool for async snapshot workers. */
    private final ExecutorService asyncOperationsThreadPool;

    /**
     * The executor service that the snapshot data uploader/downloader to upload and download data.
     */
    private final ExecutorService snapshotDataTransferService;

    /** A uploader to upload snapshot data in the async phase of kv snapshot. */
    private final KvSnapshotDataUploader kvSnapshotDataUploader;

    /** A downloader to download snapshot data. */
    private final KvSnapshotDataDownloader kvSnapshotDataDownloader;

    private KvSnapshotResource(
            ScheduledExecutorService kvSnapshotScheduler,
            ExecutorService snapshotDataTransferService,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            KvSnapshotDataDownloader kvSnapshotDataDownloader,
            ExecutorService asyncOperationsThreadPool) {
        this.kvSnapshotScheduler = kvSnapshotScheduler;
        this.snapshotDataTransferService = snapshotDataTransferService;
        this.kvSnapshotDataUploader = kvSnapshotDataUploader;
        this.kvSnapshotDataDownloader = kvSnapshotDataDownloader;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;
    }

    public ScheduledExecutorService getKvSnapshotScheduler() {
        return kvSnapshotScheduler;
    }

    public ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    public KvSnapshotDataUploader getKvSnapshotDataUploader() {
        return kvSnapshotDataUploader;
    }

    public KvSnapshotDataDownloader getKvSnapshotDataDownloader() {
        return kvSnapshotDataDownloader;
    }

    public static KvSnapshotResource create(int serverId, Configuration conf) {
        ExecutorService dataTransferThreadPool =
                Executors.newFixedThreadPool(
                        conf.getInt(ConfigOptions.KV_SNAPSHOT_TRANSFER_THREAD_NUM),
                        new ExecutorThreadFactory("fluss-kv-snapshot-data-transfer"));

        KvSnapshotDataUploader kvSnapshotDataUploader =
                new KvSnapshotDataUploader(dataTransferThreadPool);

        KvSnapshotDataDownloader kvSnapshotDataDownloader =
                new KvSnapshotDataDownloader(dataTransferThreadPool);

        ScheduledExecutorService kvSnapshotScheduler =
                Executors.newScheduledThreadPool(
                        conf.getInt(ConfigOptions.KV_SNAPSHOT_SCHEDULER_THREAD_NUM),
                        new ExecutorThreadFactory("periodic-snapshot-scheduler-" + serverId));

        // the parameter to create thread pool is from Flink. todo: may adjust according Fluss's
        // workload
        // create a thread pool for the async part of kv snapshot
        ExecutorService asyncOperationsThreadPool =
                new ThreadPoolExecutor(
                        0,
                        3,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory("fluss-kv-snapshot-async-operations"));
        return new KvSnapshotResource(
                kvSnapshotScheduler,
                dataTransferThreadPool,
                kvSnapshotDataUploader,
                kvSnapshotDataDownloader,
                asyncOperationsThreadPool);
    }

    public void close() {
        // both kvSnapshotDataUploader and kvSnapshotDataDownloader use snapshotDataTransferService
        // so, we only need to close snapshotDataTransferService
        snapshotDataTransferService.shutdownNow();

        // shutdown asyncOperationsThreadPool now
        asyncOperationsThreadPool.shutdownNow();
        // close kvSnapshotScheduler, also stop any actively executing task immediately
        // otherwise, a snapshot will still be take although it's closed, which will case exception
        kvSnapshotScheduler.shutdownNow();
    }
}
