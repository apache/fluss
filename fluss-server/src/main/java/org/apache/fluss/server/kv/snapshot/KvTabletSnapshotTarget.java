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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
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

    private final ZooKeeperClient zooKeeperClient;

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
            ZooKeeperClient zooKeeperClient,
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
                zooKeeperClient,
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
            @Nonnull ZooKeeperClient zooKeeperClient,
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
        this.zooKeeperClient = zooKeeperClient;
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
            // update local state after successful commit
            updateStateOnCommitSuccess(snapshotId, snapshotResult);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            // handle the exception with idempotent check
            handleSnapshotCommitException(
                    snapshotId, snapshotResult, completedSnapshot, snapshotLocation, t);
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

    /**
     * Update local state after successful snapshot completion. This includes notifying RocksDB
     * about completion, updating latest snapshot offset/size, and notifying LogTablet about the
     * minimum offset to retain.
     */
    private void updateStateOnCommitSuccess(long snapshotId, SnapshotResult snapshotResult) {
        // notify the snapshot complete
        rocksIncrementalSnapshot.notifySnapshotComplete(snapshotId);
        logOffsetOfLatestSnapshot = snapshotResult.getLogOffset();
        snapshotSize = snapshotResult.getSnapshotSize();
        // update LogTablet to notify the lowest offset that should be retained
        updateMinRetainOffset.accept(snapshotResult.getLogOffset());
    }

    /**
     * Handle snapshot commit exception with idempotent check. This method implements the fix for
     * issue #1304 by double-checking ZooKeeper to verify if the snapshot actually exists before
     * cleanup.
     */
    private void handleSnapshotCommitException(
            long snapshotId,
            SnapshotResult snapshotResult,
            CompletedSnapshot completedSnapshot,
            SnapshotLocation snapshotLocation,
            Throwable t)
            throws Throwable {

        // Fix for issue: https://github.com/apache/fluss/issues/1304
        // Tablet server try to commit kv snapshot to coordinator server,
        // coordinator server commit the kv snapshot to zk, then failover.
        // Tablet server will got exception from coordinator server, but mistake it as a fail
        // commit although coordinator server has committed to zk, then discard the commited kv
        // snapshot.
        //
        // Idempotent check: Double check ZK to verify if the snapshot actually exists before
        // cleanup
        try {
            Optional<BucketSnapshot> zkSnapshot =
                    zooKeeperClient.getTableBucketSnapshot(tableBucket, snapshotId);
            if (zkSnapshot.isPresent()) {
                // Snapshot exists in ZK, indicating the commit was actually successful,
                // just response was lost
                LOG.warn(
                        "Snapshot {} for TableBucket {} already exists in ZK. "
                                + "The commit was successful but response was lost due to coordinator failover. "
                                + "Skipping cleanup and treating as successful.",
                        snapshotId,
                        tableBucket);

                // Update local state as if the commit was successful
                updateStateOnCommitSuccess(snapshotId, snapshotResult);
                return; // Snapshot commit succeeded, return directly
            } else {
                // Snapshot does not exist in ZK, indicating the commit truly failed
                LOG.warn(
                        "Snapshot {} for TableBucket {} does not exist in ZK. "
                                + "The commit truly failed, proceeding with cleanup.",
                        snapshotId,
                        tableBucket);
                snapshotsCleaner.cleanSnapshot(completedSnapshot, () -> {}, ioExecutor);
                handleSnapshotFailure(snapshotId, snapshotLocation, t);
            }
        } catch (Exception zkException) {
            LOG.warn(
                    "Failed to query ZK for snapshot {} of TableBucket {}. "
                            + "Cannot determine actual snapshot status, keeping snapshot in current state "
                            + "to avoid potential data loss.",
                    snapshotId,
                    tableBucket,
                    zkException);
            // When ZK query fails, we cannot determine the actual status.
            // The snapshot might have succeeded or failed on the ZK side.
            // Therefore, we must not clean up the snapshot files and not update local state.
            // This avoids the risk of discarding a successfully committed snapshot that
            // connectors may already be reading, which would cause data loss or job failure.
        }

        // throw the exception to make PeriodicSnapshotManager can catch the exception
        throw t;
    }

    private SnapshotRunner createSnapshotRunner(CloseableRegistry cancelStreamRegistry) {
        return new SnapshotRunner(rocksIncrementalSnapshot, cancelStreamRegistry);
    }
}
