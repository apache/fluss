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

import com.alibaba.fluss.utils.CloseableRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.RunnableFuture;

/**
 * A class to execute snapshot. It can execute a strategy either synchronously or asynchronously. It
 * takes care of common logging and resource cleaning.
 */
public class SnapshotRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotRunner.class);

    private static final String LOG_SYNC_COMPLETED_TEMPLATE =
            "Asynchronous incremental RocksDB snapshot (synchronous part) in thread {} took {} ms.";
    private static final String LOG_ASYNC_COMPLETED_TEMPLATE =
            "Asynchronous incremental RocksDB snapshot (asynchronous part) in thread {} took {} ms.";

    @Nonnull private final RocksIncrementalSnapshot rocksIncrementalSnapshot;
    @Nonnull private final CloseableRegistry cancelStreamRegistry;

    public SnapshotRunner(
            RocksIncrementalSnapshot rocksIncrementalSnapshot,
            @Nonnull CloseableRegistry cancelStreamRegistry) {
        this.rocksIncrementalSnapshot = rocksIncrementalSnapshot;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }

    public RunnableFuture<SnapshotResult> snapshot(
            long snapshotId, long logOffset, @Nonnull SnapshotLocation snapshotLocation)
            throws Exception {
        long startTime = System.currentTimeMillis();
        RocksIncrementalSnapshot.NativeRocksDBSnapshotResources snapshotResources =
                rocksIncrementalSnapshot.syncPrepareResources(snapshotId);
        logCompletedInternal(LOG_SYNC_COMPLETED_TEMPLATE, startTime);

        SnapshotResultSupplier asyncSnapshot =
                rocksIncrementalSnapshot.asyncSnapshot(
                        snapshotResources, snapshotId, logOffset, snapshotLocation);

        return new AsyncSnapshotCallable<SnapshotResult>() {
            @Override
            protected SnapshotResult callInternal() throws Exception {
                return asyncSnapshot.get(snapshotCloseableRegistry);
            }

            @Override
            protected void cleanupProvidedResources() {
                snapshotResources.release();
            }

            @Override
            protected void logAsyncSnapshotComplete(long startTime1) {
                logCompletedInternal(LOG_ASYNC_COMPLETED_TEMPLATE, startTime1);
            }
        }.toAsyncSnapshotFutureTask(cancelStreamRegistry);
    }

    private void logCompletedInternal(@Nonnull String template, long startTime) {
        long duration = (System.currentTimeMillis() - startTime);
        LOG.debug(template, Thread.currentThread(), duration);
    }
}
