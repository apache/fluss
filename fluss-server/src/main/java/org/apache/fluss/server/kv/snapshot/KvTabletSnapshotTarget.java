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
import org.apache.fluss.exception.FlussRuntimeException;
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
import java.util.Set;
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
    private final Supplier<TabletState> tabletStateSupplier;
    private final Consumer<Long> updateMinRetainOffset;
    private final int bucketLeaderEpoch;
    private final Supplier<Integer> coordinatorEpochSupplier;

    private final SnapshotRunner snapshotRunner;
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
            Supplier<TabletState> tabletStateSupplier,
            Consumer<Long> updateMinRetainOffset,
            int bucketLeaderEpoch,
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
                tabletStateSupplier,
                updateMinRetainOffset,
                bucketLeaderEpoch,
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
            Supplier<TabletState> tabletStateSupplier,
            Consumer<Long> updateMinRetainOffset,
            int bucketLeaderEpoch,
            Supplier<Integer> coordinatorEpochSupplier,
            long logOffsetOfLatestSnapshot,
            long snapshotSize)
            throws IOException {
        this.tableBucket = tableBucket;
        this.ioExecutor = ioExecutor;
        this.completedKvSnapshotCommitter = completedKvSnapshotCommitter;
        this.zooKeeperClient = zooKeeperClient;
        this.rocksIncrementalSnapshot = rocksIncrementalSnapshot;
        this.remoteKvTabletDir = remoteKvTabletDir;
        this.remoteSnapshotSharedDir = FlussPaths.remoteKvSharedDir(remoteKvTabletDir);
        this.snapshotWriteBufferSize = snapshotWriteBufferSize;
        this.remoteFileSystem = remoteKvTabletDir.getFileSystem();
        this.snapshotIdCounter = snapshotIdCounter;
        this.tabletStateSupplier = tabletStateSupplier;
        this.updateMinRetainOffset = updateMinRetainOffset;
        this.bucketLeaderEpoch = bucketLeaderEpoch;
        this.coordinatorEpochSupplier = coordinatorEpochSupplier;
        this.logOffsetOfLatestSnapshot = logOffsetOfLatestSnapshot;
        this.snapshotSize = snapshotSize;
        this.snapshotRunner = createSnapshotRunner(cancelStreamRegistry);
    }

    @Override
    public long currentSnapshotId() {
        try {
            return snapshotIdCounter.getCurrent();
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to get current snapshot ID for TableBucket " + tableBucket + ".", e);
        }
    }

    @Override
    public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() throws Exception {
        TabletState tabletState = tabletStateSupplier.get();
        long logOffset = tabletState.getFlushedLogOffset();
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
        int coordinatorEpoch = coordinatorEpochSupplier.get();
        SnapshotLocation snapshotLocation = initSnapshotLocation(currentSnapshotId);
        return Optional.of(
                new PeriodicSnapshotManager.SnapshotRunnable(
                        snapshotRunner.snapshot(currentSnapshotId, tabletState, snapshotLocation),
                        currentSnapshotId,
                        coordinatorEpoch,
                        bucketLeaderEpoch,
                        snapshotLocation));
    }

    private long reconcileUnconfirmedSnapshot(long snapshotId, long previousSnapshotId)
            throws Exception {
        Set<Long> unconfirmed = rocksIncrementalSnapshot.getUnconfirmedSnapshotIds();
        unconfirmed.remove(snapshotId);
        if (unconfirmed.isEmpty()) {
            return previousSnapshotId;
        }
        Optional<BucketSnapshot> selected =
                zooKeeperClient.getTableBucketLatestSnapshot(tableBucket);
        if (selected.isPresent() && unconfirmed.contains(selected.get().getSnapshotId())) {
            CompletedSnapshot snapshot =
                    selected.get().toCompletedSnapshotHandle().retrieveCompleteSnapshot();
            updateStateOnCommitSuccess(
                    snapshot.getSnapshotID(),
                    snapshot.getLogOffset(),
                    snapshot.getKvSnapshotHandle().getSnapshotSize());
            return snapshot.getSnapshotID();
        }
        return previousSnapshotId;
    }

    private SnapshotLocation initSnapshotLocation(long snapshotId) {
        final FsPath currentSnapshotDir =
                FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId);
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
        TabletState tabletState = snapshotResult.getTabletState();
        CompletedSnapshot completedSnapshot =
                new CompletedSnapshot(
                        tableBucket,
                        snapshotId,
                        snapshotResult.getSnapshotPath(),
                        snapshotResult.getKvSnapshotHandle(),
                        tabletState.getFlushedLogOffset(),
                        tabletState.getRowCount(),
                        tabletState.getAutoIncIDRanges());
        try {
            completedKvSnapshotCommitter.commitKvSnapshot(
                    completedSnapshot,
                    coordinatorEpoch,
                    bucketLeaderEpoch,
                    reconcileUnconfirmedSnapshot(
                            snapshotId, snapshotResult.getPreviousSnapshotId()));
            updateStateOnCommitSuccess(
                    snapshotId,
                    tabletState.getFlushedLogOffset(),
                    snapshotResult.getSnapshotSize());
        } catch (Exception e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);
            try {
                Optional<BucketSnapshot> registered =
                        zooKeeperClient.getTableBucketSnapshot(tableBucket, snapshotId);
                if (!registered.isPresent()) {
                    rocksIncrementalSnapshot.notifySnapshotAbort(snapshotId);
                    completedSnapshot
                            .discardAsync(ioExecutor)
                            .whenComplete(
                                    (ignored, cleanupFailure) -> {
                                        if (cleanupFailure != null) {
                                            LOG.warn(
                                                    "Failed to clean snapshot {} for {} after commit failure.",
                                                    snapshotId,
                                                    tableBucket,
                                                    cleanupFailure);
                                        }
                                    });
                } else {
                    Optional<BucketSnapshot> selected =
                            zooKeeperClient.getTableBucketLatestSnapshot(tableBucket);
                    if (selected.isPresent() && selected.get().getSnapshotId() == snapshotId) {
                        updateStateOnCommitSuccess(
                                snapshotId,
                                tabletState.getFlushedLogOffset(),
                                snapshotResult.getSnapshotSize());
                        return;
                    }
                }
            } catch (Exception handlingFailure) {
                cause.addSuppressed(handlingFailure);
            }
            throw cause;
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
     * Update local state after successful snapshot completion. This includes updating latest
     * snapshot offset/size, notifying LogTablet about the minimum offset to retain, and finally
     * notifying RocksDB about completion.
     *
     * <p>Order matters: outward-visible state (offset/size + {@code updateMinRetainOffset}
     * callback) MUST be updated before {@link RocksIncrementalSnapshot#notifySnapshotComplete},
     * otherwise observers waiting on {@code lastCompletedSnapshotId} may see the completion signal
     * before the outward state catches up — see FLUSS-2624. The {@code finally} block also
     * guarantees the internal SST bookkeeping cleanup runs even if any outward update throws.
     */
    private void updateStateOnCommitSuccess(long snapshotId, long flushedLogOffset, long size) {
        try {
            logOffsetOfLatestSnapshot = flushedLogOffset;
            snapshotSize = size;
            updateMinRetainOffset.accept(flushedLogOffset);
        } finally {
            rocksIncrementalSnapshot.notifySnapshotComplete(snapshotId);
        }
    }

    private SnapshotRunner createSnapshotRunner(CloseableRegistry cancelStreamRegistry) {
        return new SnapshotRunner(rocksIncrementalSnapshot, cancelStreamRegistry);
    }
}
