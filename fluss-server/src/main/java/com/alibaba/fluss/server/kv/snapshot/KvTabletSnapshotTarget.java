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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.SequenceIDCounter;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A {@link PeriodicSnapshotManager.SnapshotTarget} for a kv tablet. It'll first initiate a
 * snapshot, then handle the snapshot result or failure.
 *
 * <p>Note: it's not thread safe, {@link #initSnapshot()}}, {@link #handleSnapshotResult(long, int,
 * int, SnapshotLocation, SnapshotResult)}, {@link #handleSnapshotFailure(long, SnapshotLocation,
 * Throwable)} may be called by different threads.
 */
@NotThreadSafe
public class KvTabletSnapshotTarget implements PeriodicSnapshotManager.SnapshotTarget {

    private static final Logger LOG = LoggerFactory.getLogger(KvTabletSnapshotTarget.class);

    private final TableBucket tableBucket;

    private final CompletedKvSnapshotCommitter completedKvSnapshotCommitter;

    private final RocksIncrementalSnapshot rocksIncrementalSnapshot;
    private final FsPath remoteKvTabletDir;
    private final FsPath remoteSnapshotSharedDir;
    private final int snapshotWriteBufferSize;
    private final FileSystem remoteFileSystem;
    private final SequenceIDCounter snapshotIdCounter;
    private final Supplier<Long> logOffsetSupplier;
    private final Consumer<Long> updateMinRetainOffset;
    private final Supplier<Integer> bucketLeaderEpochSupplier;
    private final Supplier<Integer> coordinatorEpochSupplier;

    private final SnapshotRunner snapshotRunner;

    /** A cleaner to clean snapshots when fail to persistent snapshot. */
    private final SnapshotsCleaner snapshotsCleaner;

    /** The executor used for asynchronous calls, like potentially blocking I/O. */
    private final Executor ioExecutor;

    private volatile long logOffsetOfLatestSnapshot;

    private volatile long snapshotSize;

    @VisibleForTesting
    KvTabletSnapshotTarget(
            TableBucket tableBucket,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            RocksIncrementalSnapshot rocksIncrementalSnapshot,
            FsPath remoteKvTabletDir,
            Executor ioExecutor,
            CloseableRegistry cancelStreamRegistry,
            SequenceIDCounter snapshotIdCounter,
            Supplier<Long> logOffsetSupplier,
            Consumer<Long> updateMinRetainOffset,
            Supplier<Integer> bucketLeaderEpochSupplier,
            Supplier<Integer> coordinatorEpochSupplier,
            long logOffsetOfLatestSnapshot,
            long snapshotSize)
            throws IOException {
        this(
                tableBucket,
                completedKvSnapshotCommitter,
                rocksIncrementalSnapshot,
                remoteKvTabletDir,
                (int) ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE.defaultValue().getBytes(),
                ioExecutor,
                cancelStreamRegistry,
                snapshotIdCounter,
                logOffsetSupplier,
                updateMinRetainOffset,
                bucketLeaderEpochSupplier,
                coordinatorEpochSupplier,
                logOffsetOfLatestSnapshot,
                snapshotSize);
    }

    public KvTabletSnapshotTarget(
            TableBucket tableBucket,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            RocksIncrementalSnapshot rocksIncrementalSnapshot,
            FsPath remoteKvTabletDir,
            int snapshotWriteBufferSize,
            Executor ioExecutor,
            CloseableRegistry cancelStreamRegistry,
            SequenceIDCounter snapshotIdCounter,
            Supplier<Long> logOffsetSupplier,
            Consumer<Long> updateMinRetainOffset,
            Supplier<Integer> bucketLeaderEpochSupplier,
            Supplier<Integer> coordinatorEpochSupplier,
            long logOffsetOfLatestSnapshot,
            long snapshotSize)
            throws IOException {
        this.tableBucket = tableBucket;
        this.completedKvSnapshotCommitter = completedKvSnapshotCommitter;
        this.rocksIncrementalSnapshot = rocksIncrementalSnapshot;
        this.remoteKvTabletDir = remoteKvTabletDir;
        this.remoteSnapshotSharedDir = FlussPaths.remoteKvSharedDir(remoteKvTabletDir);
        this.snapshotWriteBufferSize = snapshotWriteBufferSize;
        this.remoteFileSystem = remoteKvTabletDir.getFileSystem();
        this.snapshotIdCounter = snapshotIdCounter;
        this.logOffsetSupplier = logOffsetSupplier;
        this.updateMinRetainOffset = updateMinRetainOffset;
        this.bucketLeaderEpochSupplier = bucketLeaderEpochSupplier;
        this.coordinatorEpochSupplier = coordinatorEpochSupplier;
        this.logOffsetOfLatestSnapshot = logOffsetOfLatestSnapshot;
        this.snapshotSize = snapshotSize;
        this.ioExecutor = ioExecutor;
        this.snapshotRunner = createSnapshotRunner(cancelStreamRegistry);
        this.snapshotsCleaner = new SnapshotsCleaner();
    }

    @Override
    public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() throws Exception {
        long logOffset = logOffsetSupplier.get();
        if (logOffset <= logOffsetOfLatestSnapshot) {
            LOG.debug(
                    "The current offset for the log whose kv data is flushed is {}, "
                            + "which is not greater than the log offset in the latest snapshot {}, "
                            + "so skip one snapshot for it.",
                    logOffset,
                    logOffsetOfLatestSnapshot);
            return Optional.empty();
        }
        // init snapshot stream factory for this snapshot
        long currentSnapshotId = snapshotIdCounter.getAndIncrement();
        // get the bucket leader and coordinator epoch when the snapshot is triggered
        int bucketLeaderEpoch = bucketLeaderEpochSupplier.get();
        int coordinatorEpoch = coordinatorEpochSupplier.get();
        SnapshotLocation snapshotLocation = initSnapshotLocation(currentSnapshotId);
        try {
            PeriodicSnapshotManager.SnapshotRunnable snapshotRunnable =
                    new PeriodicSnapshotManager.SnapshotRunnable(
                            snapshotRunner.snapshot(currentSnapshotId, logOffset, snapshotLocation),
                            currentSnapshotId,
                            coordinatorEpoch,
                            bucketLeaderEpoch,
                            snapshotLocation);
            return Optional.of(snapshotRunnable);
        } catch (Exception t) {
            // dispose the snapshot location
            snapshotLocation.disposeOnFailure();
            throw t;
        }
    }

    private SnapshotLocation initSnapshotLocation(long snapshotId) throws IOException {
        final FsPath currentSnapshotDir =
                FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId);
        // create the snapshot exclusive directory
        remoteFileSystem.mkdirs(currentSnapshotDir);
        return new SnapshotLocation(
                remoteFileSystem,
                currentSnapshotDir,
                remoteSnapshotSharedDir,
                snapshotWriteBufferSize);
    }

    @Override
    public void handleSnapshotResult(
            long snapshotId,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            SnapshotLocation snapshotLocation,
            SnapshotResult snapshotResult)
            throws Throwable {
        CompletedSnapshot completedSnapshot =
                new CompletedSnapshot(
                        tableBucket,
                        snapshotId,
                        snapshotResult.getSnapshotPath(),
                        snapshotResult.getKvSnapshotHandle(),
                        snapshotResult.getLogOffset());
        try {
            // commit the completed snapshot
            completedKvSnapshotCommitter.commitKvSnapshot(
                    completedSnapshot, coordinatorEpoch, bucketLeaderEpoch);
            // notify the snapshot complete
            rocksIncrementalSnapshot.notifySnapshotComplete(snapshotId);
            logOffsetOfLatestSnapshot = snapshotResult.getLogOffset();
            snapshotSize = snapshotResult.getSnapshotSize();
            // update LogTablet to notify the lowest offset that should be retained
            updateMinRetainOffset.accept(snapshotResult.getLogOffset());
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            snapshotsCleaner.cleanSnapshot(completedSnapshot, () -> {}, ioExecutor);
            handleSnapshotFailure(snapshotId, snapshotLocation, t);
            // throw the exception to make PeriodicSnapshotManager can catch the exception
            throw t;
        }
    }

    @Override
    public void handleSnapshotFailure(
            long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {
        LOG.warn(
                "Snapshot {} failure or cancellation for TableBucket {}.",
                snapshotId,
                tableBucket,
                cause);
        rocksIncrementalSnapshot.notifySnapshotAbort(snapshotId);
        // cleanup the target snapshot location at the end
        snapshotLocation.disposeOnFailure();
    }

    @Override
    public long getSnapshotSize() {
        return snapshotSize;
    }

    @VisibleForTesting
    protected RocksIncrementalSnapshot getRocksIncrementalSnapshot() {
        return rocksIncrementalSnapshot;
    }

    private SnapshotRunner createSnapshotRunner(CloseableRegistry cancelStreamRegistry) {
        return new SnapshotRunner(rocksIncrementalSnapshot, cancelStreamRegistry);
    }
}
