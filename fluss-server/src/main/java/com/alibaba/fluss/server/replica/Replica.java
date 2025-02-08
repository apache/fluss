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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FencedLeaderEpochException;
import com.alibaba.fluss.exception.InvalidColumnProjectionException;
import com.alibaba.fluss.exception.InvalidTimestampException;
import com.alibaba.fluss.exception.InvalidUpdateVersionException;
import com.alibaba.fluss.exception.KvStorageException;
import com.alibaba.fluss.exception.LogStorageException;
import com.alibaba.fluss.exception.NonPrimaryKeyTableException;
import com.alibaba.fluss.exception.NotEnoughReplicasException;
import com.alibaba.fluss.exception.NotLeaderOrFollowerException;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.SimpleCounter;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.SequenceIDCounter;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.kv.KvManager;
import com.alibaba.fluss.server.kv.KvRecoverHelper;
import com.alibaba.fluss.server.kv.KvTablet;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import com.alibaba.fluss.server.kv.snapshot.CompletedKvSnapshotCommitter;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotDownloadSpec;
import com.alibaba.fluss.server.kv.snapshot.KvTabletSnapshotTarget;
import com.alibaba.fluss.server.kv.snapshot.PeriodicSnapshotManager;
import com.alibaba.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import com.alibaba.fluss.server.kv.snapshot.SnapshotContext;
import com.alibaba.fluss.server.log.FetchDataInfo;
import com.alibaba.fluss.server.log.FetchIsolation;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.ListOffsetsParam;
import com.alibaba.fluss.server.log.LogAppendInfo;
import com.alibaba.fluss.server.log.LogManager;
import com.alibaba.fluss.server.log.LogOffsetMetadata;
import com.alibaba.fluss.server.log.LogOffsetSnapshot;
import com.alibaba.fluss.server.log.LogReadInfo;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.log.checkpoint.OffsetCheckpointFile;
import com.alibaba.fluss.server.log.remote.RemoteLogManager;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;
import com.alibaba.fluss.server.metrics.group.PhysicalTableMetricGroup;
import com.alibaba.fluss.server.replica.delay.DelayedFetchLog;
import com.alibaba.fluss.server.replica.delay.DelayedOperationManager;
import com.alibaba.fluss.server.replica.delay.DelayedTableBucketKey;
import com.alibaba.fluss.server.replica.delay.DelayedWrite;
import com.alibaba.fluss.server.utils.FatalErrorHandler;
import com.alibaba.fluss.server.zk.ZkSequenceIDCounter;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.ZkData;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inReadLock;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * Physical data structure that represents a replica of a {@link TableBucket}
 *
 * <p>This class has two functions: one is to perform the operations of making a leader or making a
 * follower on one {@link TableBucket}, and the other is to serve as the entry for {@link LogTablet}
 * or {@link KvTablet} to manipulate the underlying data.
 *
 * <p>For table with pk, it contains a {@link LogTablet} and a {@link KvTablet} if the replica is
 * the leader of the table bucket. For table without pk, it contains only a {@link LogTablet}.
 */
@ThreadSafe
public final class Replica {

    private static final Logger LOG = LoggerFactory.getLogger(Replica.class);

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;

    private final LogManager logManager;
    private final LogTablet logTablet;
    private final long replicaMaxLagTime;
    /** A closeable registry to register all registered {@link Closeable}s. */
    private final CloseableRegistry closeableRegistry;

    private final int minInSyncReplicas;
    private final ServerMetadataCache metadataCache;
    private final FatalErrorHandler fatalErrorHandler;
    private final BucketMetricGroup bucketMetricGroup;

    private final SnapshotContext snapshotContext;
    // null if table without pk
    private final @Nullable KvManager kvManager;

    private final int localTabletServerId;
    private final DelayedOperationManager<DelayedWrite<?>> delayedWriteManager;
    private final DelayedOperationManager<DelayedFetchLog> delayedFetchLogManager;
    /** The manger to manger the isr expand and shrink. */
    private final AdjustIsrManager adjustIsrManager;

    private final List<String> partitionKeys;
    private final Schema schema;
    private final LogFormat logFormat;
    private final ArrowCompressionInfo arrowCompressionInfo;
    private final KvFormat kvFormat;
    private final long logTTLMs;
    private final boolean dataLakeEnabled;
    private final int tieredLogLocalSegments;
    // TODO: merge above config fields into a table config
    private final Configuration tableConfig;
    private final AtomicReference<Integer> leaderReplicaIdOpt = new AtomicReference<>();
    private final ReadWriteLock leaderIsrUpdateLock = new ReentrantReadWriteLock();
    private final Clock clock;

    /**
     * storing the remote follower replicas' state, used to update leader's highWatermark and
     * replica ISR.
     *
     * <p>followerId -> {@link FollowerReplica}.
     */
    private final Map<Integer, FollowerReplica> followerReplicasMap = new ConcurrentHashMap<>();

    private volatile IsrState isrState = new IsrState.CommittedIsrState(Collections.emptyList());
    private volatile int leaderEpoch = LeaderAndIsr.INITIAL_LEADER_EPOCH - 1;
    private volatile int bucketEpoch = LeaderAndIsr.INITIAL_BUCKET_EPOCH;
    private volatile int coordinatorEpoch = CoordinatorContext.INITIAL_COORDINATOR_EPOCH;

    // null if table without pk or haven't become leader
    private volatile @Nullable KvTablet kvTablet;
    private volatile @Nullable CloseableRegistry closeableRegistryForKv;

    // ------- metrics
    private Counter isrShrinks;
    private Counter isrExpands;
    private Counter failedIsrUpdates;

    public Replica(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogManager logManager,
            @Nullable KvManager kvManager,
            long replicaMaxLagTime,
            int minInSyncReplicas,
            int localTabletServerId,
            OffsetCheckpointFile.LazyOffsetCheckpoints lazyHighWatermarkCheckpoint,
            DelayedOperationManager<DelayedWrite<?>> delayedWriteManager,
            DelayedOperationManager<DelayedFetchLog> delayedFetchLogManager,
            AdjustIsrManager adjustIsrManager,
            SnapshotContext snapshotContext,
            ServerMetadataCache metadataCache,
            FatalErrorHandler fatalErrorHandler,
            BucketMetricGroup bucketMetricGroup,
            TableDescriptor tableDescriptor,
            Clock clock)
            throws Exception {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logManager = logManager;
        this.kvManager = kvManager;
        this.metadataCache = metadataCache;
        this.replicaMaxLagTime = replicaMaxLagTime;
        this.minInSyncReplicas = minInSyncReplicas;
        this.localTabletServerId = localTabletServerId;
        this.delayedWriteManager = delayedWriteManager;
        this.delayedFetchLogManager = delayedFetchLogManager;
        this.adjustIsrManager = adjustIsrManager;
        this.fatalErrorHandler = fatalErrorHandler;
        this.bucketMetricGroup = bucketMetricGroup;
        this.schema = tableDescriptor.getSchema();
        this.logFormat = tableDescriptor.getLogFormat();
        this.arrowCompressionInfo = tableDescriptor.getArrowCompressionInfo();
        this.kvFormat = tableDescriptor.getKvFormat();
        this.logTTLMs = tableDescriptor.getLogTTLMs();
        this.dataLakeEnabled = tableDescriptor.isDataLakeEnabled();
        this.tieredLogLocalSegments = tableDescriptor.getTieredLogLocalSegments();
        this.tableConfig = Configuration.fromMap(tableDescriptor.getProperties());
        this.partitionKeys = tableDescriptor.getPartitionKeys();
        this.snapshotContext = snapshotContext;
        // create a closeable registry for the replica
        this.closeableRegistry = new CloseableRegistry();

        this.logTablet = createLog(lazyHighWatermarkCheckpoint);
        this.clock = clock;
        registerMetrics();
    }

    private void registerMetrics() {
        bucketMetricGroup.gauge(
                MetricNames.IN_SYNC_REPLICAS, () -> isLeader() ? isrState.isr().size() : 0);
        bucketMetricGroup.gauge(MetricNames.UNDER_MIN_ISR, () -> isUnderMinIsr() ? 1 : 0);
        bucketMetricGroup.gauge(MetricNames.AT_MIN_ISR, () -> isAtMinIsr() ? 1 : 0);

        isrExpands = new SimpleCounter();
        bucketMetricGroup.meter(MetricNames.ISR_EXPANDS_RATE, new MeterView(isrExpands));
        isrShrinks = new SimpleCounter();
        bucketMetricGroup.meter(MetricNames.ISR_SHRINKS_RATE, new MeterView(isrShrinks));
        failedIsrUpdates = new SimpleCounter();
        bucketMetricGroup.meter(
                MetricNames.FAILED_ISR_UPDATES_RATE, new MeterView(failedIsrUpdates));
    }

    public boolean isKvTable() {
        return kvManager != null;
    }

    public RowType getRowType() {
        return schema.toRowType();
    }

    public ArrowCompressionInfo getArrowCompressionInfo() {
        return arrowCompressionInfo;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public @Nullable Integer getLeaderId() {
        return leaderReplicaIdOpt.get();
    }

    public LogTablet getLogTablet() {
        return logTablet;
    }

    public long getLocalLogEndOffset() {
        return logTablet.localLogEndOffset();
    }

    public long getLakeLogEndOffset() {
        return logTablet.getLakeLogEndOffset();
    }

    public boolean isDataLakeEnabled() {
        return dataLakeEnabled;
    }

    public long getLocalLogStartOffset() {
        return logTablet.localLogStartOffset();
    }

    public long getLogStartOffset() {
        return logTablet.logStartOffset();
    }

    public long getLogHighWatermark() {
        return logTablet.getHighWatermark();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getLogTTLMs() {
        return logTTLMs;
    }

    public int writerIdCount() {
        return logTablet.getWriterIdCount();
    }

    public Path getTabletParentDir() {
        return logManager.getTabletParentDir(physicalPath, tableBucket);
    }

    public @Nullable KvTablet getKvTablet() {
        return kvTablet;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalPath;
    }

    public boolean isAtMinIsr() {
        return isLeader() && isrState.isr().size() < minInSyncReplicas;
    }

    public BucketMetricGroup bucketMetrics() {
        return bucketMetricGroup;
    }

    public PhysicalTableMetricGroup tableMetrics() {
        return bucketMetricGroup.getPhysicalTableMetricGroup();
    }

    public void makeLeader(NotifyLeaderAndIsrData data) throws IOException {
        boolean leaderHWIncremented =
                inWriteLock(
                        leaderIsrUpdateLock,
                        () -> {
                            int requestBucketEpoch = data.getBucketEpoch();
                            validateBucketEpoch(requestBucketEpoch);

                            coordinatorEpoch = data.getCoordinatorEpoch();

                            long currentTimeMs = clock.milliseconds();
                            // Updating the assignment and ISR state is safe if the bucket epoch is
                            // larger or equal to the current bucket epoch.
                            updateAssignmentAndIsr(data.getReplicas(), true, data.getIsr());

                            int requestLeaderEpoch = data.getLeaderEpoch();
                            if (requestLeaderEpoch > leaderEpoch) {
                                onBecomeNewLeader();
                                leaderEpoch = requestLeaderEpoch;
                                leaderReplicaIdOpt.set(localTabletServerId);
                                LOG.info(
                                        "TabletServer {} becomes leader for bucket {}",
                                        localTabletServerId,
                                        tableBucket);
                            } else if (requestLeaderEpoch == leaderEpoch) {
                                LOG.info(
                                        "Skipped the become-leader state change for bucket {} since "
                                                + "it's already the leader with leader epoch {}",
                                        tableBucket,
                                        leaderEpoch);
                            } else {
                                String errorMessage =
                                        String.format(
                                                "the leader epoch %s in notify leader and isr data is smaller than the "
                                                        + "current leader epoch %s for table bucket %s",
                                                requestLeaderEpoch, leaderEpoch, tableBucket);
                                LOG.warn("Ignore make leader because {}", errorMessage);
                                throw new FencedLeaderEpochException(errorMessage);
                            }

                            bucketEpoch = requestBucketEpoch;

                            // We may need to increment high watermark since ISR could be down to 1.
                            return maybeIncrementLeaderHW(logTablet, currentTimeMs);
                        });

        // Some delayed operations may be unblocked after HW changed.
        if (leaderHWIncremented) {
            tryCompleteDelayedOperations();
        }
    }

    public boolean makeFollower(NotifyLeaderAndIsrData data) {
        return inWriteLock(
                leaderIsrUpdateLock,
                () -> {
                    int requestBucketEpoch = data.getBucketEpoch();
                    validateBucketEpoch(requestBucketEpoch);

                    coordinatorEpoch = data.getCoordinatorEpoch();

                    updateAssignmentAndIsr(Collections.emptyList(), false, Collections.emptyList());

                    int requestLeaderEpoch = data.getLeaderEpoch();
                    boolean isNewLeaderEpoch = requestLeaderEpoch > leaderEpoch;
                    if (isNewLeaderEpoch) {
                        LOG.info(
                                "Follower {} starts at leader epoch {} from end offset {}",
                                tableBucket,
                                requestLeaderEpoch,
                                logTablet.localLogEndOffset());
                        onBecomeNewFollower();
                    } else if (requestLeaderEpoch == leaderEpoch) {
                        LOG.info(
                                "Skipped the become-follower state change for bucket {} since "
                                        + "it's already the follower with leader epoch {}",
                                tableBucket,
                                leaderEpoch);
                    } else {
                        String errorMessage =
                                String.format(
                                        "the leader epoch %s in notify leader and isr data is smaller than the "
                                                + "current leader epoch %s for table bucket %s",
                                        requestLeaderEpoch, leaderEpoch, tableBucket);
                        LOG.warn("Ignore make follower because {}", errorMessage);
                        throw new FencedLeaderEpochException(errorMessage);
                    }

                    leaderReplicaIdOpt.set(data.getLeader());
                    leaderEpoch = requestLeaderEpoch;
                    bucketEpoch = data.getBucketEpoch();

                    // We must restart the fetchers when the leader epoch changed regardless of
                    // whether the leader changed as well.
                    return isNewLeaderEpoch;
                });
    }

    /** Delete the replica including drop the kv and log. */
    public void delete() {
        // need to hold the lock to prevent appendLog, putKv from hitting I/O exceptions due
        // to log/kv being deleted
        inWriteLock(
                leaderIsrUpdateLock,
                () -> {
                    if (isKvTable()) {
                        dropKv();
                    }
                    // drop log then
                    logManager.dropLog(tableBucket);
                    // close the closeable registry
                    IOUtils.closeQuietly(closeableRegistry);
                });
    }

    public void checkProjection(@Nullable int[] projectedFields) {
        if (projectedFields != null) {
            if (logFormat != LogFormat.ARROW) {
                throw new InvalidColumnProjectionException(
                        String.format(
                                "Column projection is only supported for ARROW format, but the table %s is %s format.",
                                physicalPath.getTablePath(), logFormat));
            }
            int fieldCount = schema.getColumns().size();
            int prev = -1;
            for (int i : projectedFields) {
                if (i <= prev) {
                    throw new InvalidColumnProjectionException(
                            "The projection indexes should be in field order, but is "
                                    + Arrays.toString(projectedFields));
                }
                if (i >= fieldCount) {
                    throw new InvalidColumnProjectionException(
                            "Projected fields "
                                    + Arrays.toString(projectedFields)
                                    + " is out of bound for schema with "
                                    + fieldCount
                                    + " fields.");
                }
                prev = i;
            }
        }
    }

    public LogOffsetSnapshot fetchOffsetSnapshot(boolean fetchOnlyFromLeader) throws IOException {
        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    LogTablet logTablet = localLogOrThrow(fetchOnlyFromLeader);
                    return logTablet.fetchOffsetSnapshot();
                });
    }

    // -------------------------------------------------------------------------------------------

    private void onBecomeNewLeader() {
        if (isKvTable()) {
            // if it's become new leader, we must
            // first destroy the old kv tablet
            // if exist. Otherwise, it'll use still the old kv tablet which will cause data loss
            dropKv();
            // now, we can create a new kv tablet
            createKv();
        }
    }

    private void onBecomeNewFollower() {
        if (isKvTable()) {
            // it should be from leader to follower, we need to destroy the kv tablet
            dropKv();
        }
    }

    private void createKv() {
        try {
            // create a closeable registry for the closable related to kv
            closeableRegistryForKv = new CloseableRegistry();
            // resister the closeable registry for kv
            closeableRegistry.registerCloseable(closeableRegistryForKv);
        } catch (IOException e) {
            LOG.warn("Fail to registry closeable registry for kv, it may cause resource leak.", e);
        }

        // init kv tablet and get the snapshot it uses to init if have any
        Optional<CompletedSnapshot> snapshotUsed = initKvTablet();
        // start periodic kv snapshot
        startPeriodicKvSnapshot(snapshotUsed.orElse(null));
    }

    private void dropKv() {
        // close any closeable registry for kv
        if (closeableRegistry.unregisterCloseable(closeableRegistryForKv)) {
            IOUtils.closeQuietly(closeableRegistryForKv);
        }
        if (kvTablet != null) {
            // drop the kv tablet
            checkNotNull(kvManager);
            kvManager.dropKv(tableBucket);
            kvTablet = null;
        }
    }

    private void mayFlushKv(long newHighWatermark) {
        KvTablet kvTablet = this.kvTablet;
        if (kvTablet != null) {
            kvTablet.flush(newHighWatermark, fatalErrorHandler);
        }
    }

    /**
     * Init kv tablet from snapshot if any or just from log.
     *
     * @return the snapshot used to init kv tablet, empty if no any snapshot.
     */
    private Optional<CompletedSnapshot> initKvTablet() {
        checkNotNull(kvManager);
        long startTime = clock.milliseconds();
        LOG.info("Start to init kv tablet for {} of table {}.", tableBucket, physicalPath);

        // todo: we may need to handle the following cases:
        // case1: no kv files in local, restore from remote snapshot; and apply
        // the log;
        // case2: kv files in local
        //       - if no remote snapshot, restore from local and apply the log known to the local
        // files.
        //       - have snapshot, if the known offset to the local files is much less than(maybe
        // some value configured)
        //         the remote snapshot; restore from remote snapshot;

        // currently for simplicity, we'll always download the snapshot files and restore from
        // the snapshots as kv files won't exist in our current implementation for
        // when replica become follower, we'll always delete the kv files.

        // get the offset from which, we should restore from. default is 0
        long restoreStartOffset = 0;
        Optional<CompletedSnapshot> optCompletedSnapshot = getLatestSnapshot(tableBucket);
        try {
            if (optCompletedSnapshot.isPresent()) {
                LOG.info(
                        "Use snapshot {} to restore kv tablet for {} of table {}.",
                        optCompletedSnapshot.get(),
                        tableBucket,
                        physicalPath);
                CompletedSnapshot completedSnapshot = optCompletedSnapshot.get();
                // always create a new dir for the kv tablet
                File tabletDir = kvManager.createTabletDir(physicalPath, tableBucket);
                // down the snapshot to target tablet dir
                downloadKvSnapshots(completedSnapshot, tabletDir.toPath());

                // as we have downloaded kv files into the tablet dir, now, we can load it
                kvTablet = kvManager.loadKv(tabletDir);

                checkNotNull(kvTablet, "kv tablet should not be null.");
                restoreStartOffset = completedSnapshot.getLogOffset();
            } else {
                LOG.info("No snapshot found, restore from log.");
                // actually, kv manager always create a kv tablet since we will drop the kv
                // if it exists before init kv tablet
                kvTablet =
                        kvManager.getOrCreateKv(
                                physicalPath,
                                tableBucket,
                                logTablet,
                                kvFormat,
                                schema,
                                tableConfig,
                                arrowCompressionInfo);
            }

            kvTablet.registerMetrics(bucketMetricGroup);

            logTablet.updateMinRetainOffset(restoreStartOffset);
            recoverKvTablet(restoreStartOffset);
        } catch (Exception e) {
            throw new KvStorageException(
                    String.format(
                            "Fail to init kv tablet for %s of table %s.",
                            tableBucket, physicalPath),
                    e);
        }
        long endTime = clock.milliseconds();
        LOG.info(
                "Init kv tablet for {} of {} finish, cost {} ms.",
                physicalPath,
                tableBucket,
                endTime - startTime);
        return optCompletedSnapshot;
    }

    private void downloadKvSnapshots(CompletedSnapshot completedSnapshot, Path kvTabletDir)
            throws IOException {
        Path kvDbPath = kvTabletDir.resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        KvSnapshotDownloadSpec downloadSpec =
                new KvSnapshotDownloadSpec(completedSnapshot.getKvSnapshotHandle(), kvDbPath);
        long start = clock.milliseconds();
        LOG.info("Start to download kv snapshot {} to directory {}.", completedSnapshot, kvDbPath);
        KvSnapshotDataDownloader kvSnapshotDataDownloader =
                snapshotContext.getSnapshotDataDownloader();
        try {
            kvSnapshotDataDownloader.transferAllDataToDirectory(downloadSpec, closeableRegistry);
        } catch (Exception e) {
            throw new IOException("Fail to download kv snapshot.", e);
        }
        long end = clock.milliseconds();
        LOG.info(
                "Download kv snapshot {} to directory {} finish, cost {} ms.",
                completedSnapshot,
                kvDbPath,
                end - start);
    }

    private Optional<CompletedSnapshot> getLatestSnapshot(TableBucket tableBucket) {
        try {
            return Optional.ofNullable(
                    snapshotContext.getLatestCompletedSnapshotProvider().apply(tableBucket));
        } catch (Exception e) {
            LOG.warn(
                    "Get latest completed snapshot for {} of table {} failed.",
                    tableBucket,
                    physicalPath,
                    e);
        }
        return Optional.empty();
    }

    private void recoverKvTablet(long startRecoverLogOffset) {
        long start = clock.milliseconds();
        checkNotNull(kvTablet, "kv tablet should not be null.");
        try {
            KvRecoverHelper.KvRecoverContext recoverContext =
                    new KvRecoverHelper.KvRecoverContext(
                            getTablePath(),
                            tableBucket,
                            snapshotContext.getZooKeeperClient(),
                            snapshotContext.maxFetchLogSizeInRecoverKv());
            KvRecoverHelper kvRecoverHelper =
                    new KvRecoverHelper(
                            kvTablet,
                            logTablet,
                            startRecoverLogOffset,
                            recoverContext,
                            kvFormat,
                            partitionKeys);
            kvRecoverHelper.recover();
        } catch (Exception e) {
            throw new KvStorageException(
                    String.format(
                            "Fail to recover kv tablet %s of table %s from log offset.",
                            tableBucket, physicalPath),
                    e);
        }
        long end = clock.milliseconds();
        LOG.info(
                "Recover kv tablet for {} of table {} from log offset {} finish, cost {} ms.",
                tableBucket,
                physicalPath,
                startRecoverLogOffset,
                end - start);
    }

    private void startPeriodicKvSnapshot(@Nullable CompletedSnapshot completedSnapshot) {
        checkNotNull(kvTablet);
        KvTabletSnapshotTarget kvTabletSnapshotTarget;
        try {
            // get the snapshot reporter to report the completed snapshot
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter =
                    snapshotContext.getCompletedSnapshotReporter();

            // get latest completed snapshot
            ZooKeeperClient zkClient = snapshotContext.getZooKeeperClient();
            RocksIncrementalSnapshot rocksIncrementalSnapshot;
            long lastCompletedSnapshotId = -1;
            long lastCompletedSnapshotLogOffset = 0;
            long snapshotSize = 0L;
            Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles = new HashMap<>();
            if (completedSnapshot != null) {
                lastCompletedSnapshotId = completedSnapshot.getSnapshotID();
                lastCompletedSnapshotLogOffset = completedSnapshot.getLogOffset();
                snapshotSize = completedSnapshot.getSnapshotSize();
                uploadedSstFiles.put(
                        completedSnapshot.getSnapshotID(),
                        completedSnapshot.getKvSnapshotHandle().getSharedKvFileHandles());
            }
            rocksIncrementalSnapshot =
                    kvTablet.createIncrementalSnapshot(
                            uploadedSstFiles,
                            snapshotContext.getSnapshotDataUploader(),
                            lastCompletedSnapshotId);

            // create snapshot ID counter
            SequenceIDCounter snapshotIDCounter =
                    new ZkSequenceIDCounter(
                            zkClient.getCuratorClient(),
                            ZkData.BucketSnapshotSequenceIdZNode.path(tableBucket));

            // todo: it's hack logic for snapshot target
            // to get bucket/coordinator leader epoch.
            // and it will happen when the snapshot target
            // is doing snapshot(leader epoch = 0), and then leader switch to another(leader epoch =
            // 1),
            // and again switch to this(leader epoch = 2). The snapshot target will
            // get the leader epoch = 2, and use the epoch 2 to submit snapshot
            // which will cause older snapshot overwrite the newer snapshot

            // refactor the snapshot logic in FLUSS-56282058, may should prepare the
            // bucket/coordinator leader epoch, kv snapshot data in replica
            // instead of a separate class
            Supplier<Integer> bucketLeaderEpochSupplier = () -> leaderEpoch;
            Supplier<Integer> coordinatorEpochSupplier = () -> coordinatorEpoch;
            FsPath remoteKvTabletDir =
                    FlussPaths.remoteKvTabletDir(
                            snapshotContext.getRemoteKvDir(), physicalPath, tableBucket);

            kvTabletSnapshotTarget =
                    new KvTabletSnapshotTarget(
                            tableBucket,
                            completedKvSnapshotCommitter,
                            rocksIncrementalSnapshot,
                            remoteKvTabletDir,
                            snapshotContext.getSnapshotFsWriteBufferSize(),
                            snapshotContext.getAsyncOperationsThreadPool(),
                            closeableRegistryForKv,
                            snapshotIDCounter,
                            kvTablet::getFlushedLogOffset,
                            logTablet::updateMinRetainOffset,
                            bucketLeaderEpochSupplier,
                            coordinatorEpochSupplier,
                            lastCompletedSnapshotLogOffset,
                            snapshotSize);
            PeriodicSnapshotManager kvSnapshotManager =
                    PeriodicSnapshotManager.create(
                            tableBucket,
                            kvTabletSnapshotTarget,
                            snapshotContext,
                            kvTablet.getGuardedExecutor(),
                            bucketMetricGroup);
            kvSnapshotManager.start();
            closeableRegistryForKv.registerCloseable(kvSnapshotManager);
        } catch (Exception e) {
            LOG.error("init kv periodic snapshot failed.", e);
        }
    }

    public LogAppendInfo appendRecordsToLeader(MemoryLogRecords memoryLogRecords, int requiredAcks)
            throws Exception {
        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    if (!isLeader()) {
                        throw new NotLeaderOrFollowerException(
                                String.format(
                                        "Leader not local for bucket %s on tabletServer %d",
                                        tableBucket, localTabletServerId));
                    }

                    validateInSyncReplicaSize(requiredAcks);

                    // TODO WRITE a leader epoch.
                    LogAppendInfo appendInfo = logTablet.appendAsLeader(memoryLogRecords);

                    // we may need to increment high watermark if isr could be down to 1 or the
                    // replica count is 1.
                    boolean hwIncreased = maybeIncrementLeaderHW(logTablet, clock.milliseconds());

                    if (hwIncreased) {
                        tryCompleteDelayedOperations();
                    }

                    return appendInfo;
                });
    }

    public LogAppendInfo appendRecordsToFollower(MemoryLogRecords memoryLogRecords)
            throws Exception {
        return logTablet.appendAsFollower(memoryLogRecords);
    }

    public LogAppendInfo putRecordsToLeader(
            KvRecordBatch kvRecords, @Nullable int[] targetColumns, int requiredAcks)
            throws Exception {
        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    if (!isLeader()) {
                        throw new NotLeaderOrFollowerException(
                                String.format(
                                        "Leader not local for bucket %s on tabletServer %d",
                                        tableBucket, localTabletServerId));
                    }

                    validateInSyncReplicaSize(requiredAcks);
                    KvTablet kv = this.kvTablet;
                    checkNotNull(
                            kv, "KvTablet for the replica to put kv records shouldn't be null.");
                    LogAppendInfo logAppendInfo = kv.putAsLeader(kvRecords, targetColumns);
                    // we may need to increment high watermark.
                    maybeIncrementLeaderHW(logTablet, clock.milliseconds());
                    return logAppendInfo;
                });
    }

    public LogReadInfo fetchRecords(FetchParams fetchParams) throws IOException {
        if (fetchParams.projection() != null && logFormat != LogFormat.ARROW) {
            throw new InvalidColumnProjectionException(
                    String.format(
                            "Table '%s' is not in ARROW log format and doesn't support column projection.",
                            physicalPath));
        }
        if (fetchParams.isFromFollower()) {
            long followerFetchTimeMs = clock.milliseconds();
            LogReadInfo logReadInfo =
                    inReadLock(
                            leaderIsrUpdateLock,
                            () -> {
                                LogTablet localLog = localLogOrThrow(fetchParams.fetchOnlyLeader());
                                return readRecords(fetchParams, localLog);
                            });

            FollowerReplica followerReplica = getFollowerReplicaOrThrown(fetchParams.replicaId());
            updateFollowerFetchState(
                    followerReplica,
                    logReadInfo.getFetchedData().getFetchOffsetMetadata(),
                    followerFetchTimeMs,
                    logReadInfo.getLogEndOffset());
            return logReadInfo;
        } else {
            return inReadLock(
                    leaderIsrUpdateLock,
                    () -> {
                        LogTablet localLog = localLogOrThrow(fetchParams.fetchOnlyLeader());
                        return readRecords(fetchParams, localLog);
                    });
        }
    }

    /**
     * Check and maybe increment the high watermark of the replica (leader). this function can be
     * triggered when:
     *
     * <pre>
     *     1. bucket ISR changed.
     *     2. any follower replica's LEO changed.
     * </pre>
     *
     * <p>The HW is determined by the smallest log end offset among all follower replicas that are
     * in sync. This way, if a replica is considered caught-up, but its log end offset is smaller
     * than HW, we will wait for this replica to catch up to the HW before advancing the HW.
     *
     * <p>Note There is no need to acquire the leaderIsrUpdate lock here since all callers of this
     * private API acquires that lock.
     *
     * @return true if the high watermark is incremented, and false otherwise.
     */
    private boolean maybeIncrementLeaderHW(LogTablet leaderLog, long currentTimeMs)
            throws IOException {
        if (isUnderMinIsr()) {
            LOG.trace(
                    "Not increasing HighWatermark because bucket {} is under min ISR(ISR={})",
                    tableBucket,
                    isrState.isr());
            return false;
        }

        // maybeIncrementLeaderHW is in the hot path, the following code is written to
        // avoid unnecessary collection generation.
        LogOffsetMetadata leaderLogEndOffset = leaderLog.getLocalEndOffsetMetadata();
        LogOffsetMetadata newHighWatermark = leaderLogEndOffset;

        for (FollowerReplica remoteFollowerReplica : followerReplicasMap.values()) {
            // Note here we are using the "maximal", see explanation above.
            FollowerReplica.FollowerReplicaState replicaState =
                    remoteFollowerReplica.stateSnapshot();
            int followerId = remoteFollowerReplica.getFollowerId();
            if (replicaState.getLogEndOffsetMetadata().getMessageOffset()
                            < newHighWatermark.getMessageOffset()
                    && (isrState.maximalIsr().contains(followerId)
                            || shouldWaitForReplicaToJoinIsr(
                                    replicaState, leaderLogEndOffset, currentTimeMs, followerId))) {
                newHighWatermark = replicaState.getLogEndOffsetMetadata();
            }
        }

        Optional<LogOffsetMetadata> oldWatermark =
                leaderLog.maybeIncrementHighWatermark(newHighWatermark);
        if (oldWatermark.isPresent()) {
            LOG.debug("High watermark update from {} to {}.", oldWatermark.get(), newHighWatermark);
            // when watermark advanced, we may need to flush kv if it's kv replica
            mayFlushKv(newHighWatermark.getMessageOffset());
            return true;
        } else {
            return false;
        }
    }

    private boolean shouldWaitForReplicaToJoinIsr(
            FollowerReplica.FollowerReplicaState replicaState,
            LogOffsetMetadata leaderLogEndOffset,
            long currentTimeMs,
            int followerId) {
        return replicaState.isCaughtUp(
                        leaderLogEndOffset.getMessageOffset(), currentTimeMs, replicaMaxLagTime)
                && isReplicaIsrEligible(followerId);
    }

    private void updateAssignmentAndIsr(
            List<Integer> replicas, boolean isLeader, List<Integer> isr) {
        if (isLeader) {
            List<Integer> followers =
                    replicas.stream()
                            .filter(id -> id != localTabletServerId)
                            .collect(Collectors.toList());
            List<Integer> removedReplicas =
                    followerReplicasMap.keySet().stream()
                            .filter(id -> !followers.contains(id))
                            .collect(Collectors.toList());
            // Due to code paths accessing followerReplicasMap without a lock, first add the new
            // replicas and then remove the old ones.
            for (Integer replica : followers) {
                followerReplicasMap.put(replica, new FollowerReplica(replica, tableBucket));
            }
            for (Integer replica : removedReplicas) {
                followerReplicasMap.remove(replica);
            }
        } else {
            followerReplicasMap.clear();
        }

        // update isr info.
        isrState = new IsrState.CommittedIsrState(isr);
    }

    private void updateFollowerFetchState(
            FollowerReplica followerReplica,
            LogOffsetMetadata followerFetchOffsetMetadata,
            long followerFetchTimeMs,
            long leaderLogEndOffset)
            throws IOException {
        long prevFollowerEndOffset = followerReplica.stateSnapshot().getLogEndOffset();

        // Apply read lock here to avoid the race between ISR updates and the fetch requests from
        // rebooted follower. It could break the tablet server epoch checks in the ISR expansion.
        inReadLock(
                leaderIsrUpdateLock,
                () ->
                        followerReplica.updateFetchState(
                                followerFetchOffsetMetadata,
                                followerFetchTimeMs,
                                leaderLogEndOffset));

        // Check if this in-sync replica needs to be added to the ISR.
        maybeExpandISr(followerReplica);

        // check if the HW of the replica can now be incremented since the replica may already be in
        // the ISR and its LEO has just incremented
        boolean leaderHWIncremented = false;
        if (prevFollowerEndOffset != followerReplica.stateSnapshot().getLogEndOffset()) {
            leaderHWIncremented = maybeIncrementLeaderHW(logTablet, followerFetchTimeMs);
        }

        if (leaderHWIncremented) {
            tryCompleteDelayedOperations();
        }

        LOG.debug(
                "Recorded replica {} log end offset (LEO) position {}.",
                localTabletServerId,
                followerFetchOffsetMetadata.getMessageOffset());
    }

    private FollowerReplica getFollowerReplicaOrThrown(int followerId) {
        FollowerReplica followerReplica = followerReplicasMap.get(followerId);
        if (followerReplica == null) {
            LOG.debug(
                    "Leader replica {} failed to record follower replica {}'s state for table bucket {}",
                    localTabletServerId,
                    followerId,
                    tableBucket);
            throw new NotLeaderOrFollowerException(
                    String.format(
                            "Replica %d is not recognized as a valid replica of %s",
                            followerId, tableBucket));
        }
        return followerReplica;
    }

    public List<byte[]> lookups(List<byte[]> keys) {
        if (!isKvTable()) {
            throw new NonPrimaryKeyTableException(
                    "the primary key table not exists for " + tableBucket);
        }
        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    try {
                        if (!isLeader()) {
                            throw new NotLeaderOrFollowerException(
                                    String.format(
                                            "Leader not local for bucket %s on tabletServer %d",
                                            tableBucket, localTabletServerId));
                        }
                        checkNotNull(
                                kvTablet, "KvTablet for the replica to get key shouldn't be null.");
                        return kvTablet.multiGet(keys);
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Failed to lookup from local kv for table bucket %s, the cause is: %s",
                                        tableBucket, e.getMessage());
                        LOG.error(errorMsg, e);
                        throw new KvStorageException(errorMsg, e);
                    }
                });
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) {
        if (!isKvTable()) {
            throw new NonPrimaryKeyTableException(
                    "Try to do prefix lookup on a non primary key table: " + getTablePath());
        }

        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    try {
                        if (!isLeader()) {
                            throw new NotLeaderOrFollowerException(
                                    String.format(
                                            "Leader not local for bucket %s on tabletServer %d",
                                            tableBucket, localTabletServerId));
                        }
                        checkNotNull(
                                kvTablet, "KvTablet for the replica to get key shouldn't be null.");
                        return kvTablet.prefixLookup(prefixKey);
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Failed to do prefix lookup from local kv for table bucket %s, the cause is: %s",
                                        tableBucket, e.getMessage());
                        LOG.error(errorMsg, e);
                        throw new KvStorageException(errorMsg, e);
                    }
                });
    }

    public DefaultValueRecordBatch limitKvScan(int limit) {
        if (!isKvTable()) {
            throw new NonPrimaryKeyTableException(
                    "the primary key table not exists for " + tableBucket);
        }

        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    try {
                        if (!isLeader()) {
                            throw new NotLeaderOrFollowerException(
                                    String.format(
                                            "Leader not local for bucket %s on tabletServer %d",
                                            tableBucket, localTabletServerId));
                        }
                        checkNotNull(
                                kvTablet,
                                "KvTablet for the replica to limit scan shouldn't be null.");
                        List<byte[]> bytes = kvTablet.limitScan(limit);
                        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
                        for (byte[] key : bytes) {
                            builder.append(key);
                        }
                        return builder.build();
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Failed to limit scan from local kv for table bucket %s, the cause is: %s",
                                        tableBucket, e.getMessage());
                        LOG.error(errorMsg, e);
                        throw new KvStorageException(errorMsg, e);
                    }
                });
    }

    public LogRecords limitLogScan(int limit) {
        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    try {
                        if (!isLeader()) {
                            throw new NotLeaderOrFollowerException(
                                    String.format(
                                            "Leader not local for bucket %s on tabletServer %d",
                                            tableBucket, localTabletServerId));
                        }

                        checkNotNull(
                                logTablet,
                                "LogTablet for the replica to limit scan shouldn't be null.");
                        long highWatermark = logTablet.getHighWatermark();
                        // client can only read under high watermark.
                        long readOffset =
                                Math.max(logTablet.logStartOffset(), highWatermark - limit);
                        FetchDataInfo dataInfo =
                                logTablet.read(
                                        readOffset,
                                        Integer.MAX_VALUE,
                                        FetchIsolation.HIGH_WATERMARK,
                                        true,
                                        null);
                        return dataInfo.getRecords();
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Failed to limit scan from local log for table bucket %s, the cause is: %s",
                                        tableBucket, e.getMessage());
                        LOG.error(errorMsg, e);
                        throw new LogStorageException(errorMsg, e);
                    }
                });
    }

    /**
     * Returns a tuple where the first element is a boolean indicating whether enough replicas
     * reached `requiredOffset` and the second element is an error (which would be `Errors.NONE` for
     * no error).
     *
     * <p>Note that this method will only be called if requiredAcks = -1, and we are waiting for all
     * replicas to be fully caught up to the (local) leader's offset corresponding to this
     * produceLog/PutKv request before we acknowledge the request.
     */
    public Tuple2<Boolean, Errors> checkEnoughReplicasReachOffset(long requiredOffset) {
        if (isLeader()) {
            // Keep the current immutable replica list reference.
            List<Integer> curMaximalIsr = isrState.maximalIsr();
            if (LOG.isTraceEnabled()) {
                traceAckInfo(curMaximalIsr, requiredOffset);
            }

            if (logTablet.getHighWatermark() >= requiredOffset) {
                if (minInSyncReplicas <= curMaximalIsr.size()) {
                    return Tuple2.of(true, Errors.NONE);
                } else {
                    return Tuple2.of(true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION);
                }
            } else {
                return Tuple2.of(false, Errors.NONE);
            }
        } else {
            return Tuple2.of(false, Errors.NOT_LEADER_OR_FOLLOWER);
        }
    }

    public long getOffset(RemoteLogManager remoteLogManager, ListOffsetsParam listOffsetsParam)
            throws IOException {
        return inReadLock(
                leaderIsrUpdateLock,
                () -> {
                    int offsetType = listOffsetsParam.getOffsetType();
                    if (offsetType == ListOffsetsParam.TIMESTAMP_OFFSET_TYPE) {
                        return getOffsetByTimestamp(remoteLogManager, listOffsetsParam);
                    } else if (offsetType == ListOffsetsParam.EARLIEST_OFFSET_TYPE) {
                        if (listOffsetsParam.getFollowerServerId() < 0) {
                            // the request is come from client, return available start offset
                            return logTablet.logStartOffset();
                        } else {
                            return logTablet.localLogStartOffset();
                        }
                    } else if (offsetType == ListOffsetsParam.LATEST_OFFSET_TYPE) {
                        return getLatestOffset(listOffsetsParam.getFollowerServerId());
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid list offset type: " + offsetType);
                    }
                });
    }

    private long getOffsetByTimestamp(
            RemoteLogManager remoteLogManager, ListOffsetsParam listOffsetsParam)
            throws IOException {
        OptionalLong startTimestampOpt = listOffsetsParam.startTimestamp();
        if (!startTimestampOpt.isPresent()) {
            throw new IllegalArgumentException(
                    "startTimestamp is not present while list offset by timestamp.");
        }

        long fetchTimestamp = startTimestampOpt.getAsLong();
        // 1. If the fetch timestamp is larger than current timestamp, we will throw an
        // invalidTimestamp exception. If the fetch timestamp is larger than the local max timestamp
        // but no larger than current timestamp, we will latest offset.
        long localMaxTimestamp = logTablet.localMaxTimestamp();
        long currentTimestamp = clock.milliseconds();
        if (fetchTimestamp > localMaxTimestamp && fetchTimestamp <= currentTimestamp) {
            return getLatestOffset(listOffsetsParam.getFollowerServerId());
        } else if (fetchTimestamp > currentTimestamp) {
            throw new InvalidTimestampException(
                    String.format(
                            "Get offset error for table bucket %s, "
                                    + "the fetch timestamp %s is larger than the current timestamp %s",
                            tableBucket, fetchTimestamp, currentTimestamp));
        }

        // 2.  we will try to find offset from remote storage.
        long remoteOffset = remoteLogManager.lookupOffsetForTimestamp(tableBucket, fetchTimestamp);
        if (remoteOffset != -1L) {
            return remoteOffset;
        }

        // 2. if not found, we will find offset from local storage.
        return logTablet.lookupOffsetForTimestamp(fetchTimestamp);
    }

    private long getLatestOffset(int followerServerId) {
        // the request is come from client.
        if (followerServerId < 0) {
            return logTablet.getHighWatermark();
        } else {
            return logTablet.localLogEndOffset();
        }
    }

    /**
     * Truncate the local log of this bucket to the specified offset and checkpoint the recovery
     * point to this offset.
     *
     * @param offset offset to be used for truncation.
     */
    public void truncateTo(long offset) throws LogStorageException {
        inReadLock(leaderIsrUpdateLock, () -> logManager.truncateTo(tableBucket, offset));
    }

    /** Delete all data in the local log of this bucket and start the log at the new offset. */
    public void truncateFullyAndStartAt(long newOffset) {
        inReadLock(
                leaderIsrUpdateLock,
                () -> logManager.truncateFullyAndStartAt(tableBucket, newOffset));
    }

    private LogReadInfo readRecords(FetchParams fetchParams, LogTablet logTablet)
            throws IOException {
        // Note we use the log end offset prior to the read. This ensures that any appends following
        // the fetch do not prevent a follower from coming into sync.
        long initialHighWatermark = logTablet.getHighWatermark();
        long initialLogEndOffset = logTablet.localLogEndOffset();
        long readOffset =
                fetchParams.fetchOffset() == FetchParams.FETCH_FROM_EARLIEST_OFFSET
                        ? logTablet.logStartOffset()
                        : fetchParams.fetchOffset();

        // todo validate fetched epoch.

        FetchDataInfo fetchDataInfo =
                logTablet.read(
                        readOffset,
                        fetchParams.maxFetchBytes(),
                        fetchParams.isolation(),
                        fetchParams.minOneMessage(),
                        fetchParams.projection());
        return new LogReadInfo(fetchDataInfo, initialHighWatermark, initialLogEndOffset);
    }

    private void tryCompleteDelayedOperations() {
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tableBucket);
        delayedWriteManager.checkAndComplete(delayedTableBucketKey);
        delayedFetchLogManager.checkAndComplete(delayedTableBucketKey);
    }

    private void validateBucketEpoch(int requestBucketEpoch) {
        if (requestBucketEpoch < bucketEpoch) {
            String message =
                    String.format(
                            "Skipped the become-leader state change for %s with a lower bucket epoch %s"
                                    + " since the leader is already at a newer bucket epoch %s",
                            tableBucket, requestBucketEpoch, bucketEpoch);
            LOG.info(message);
            throw new InvalidUpdateVersionException(message);
        }
    }

    /**
     * Check and maybe expand the ISR of this table bucket.
     *
     * <p>A replica will be added to ISR if its LEO >= current hw of this bucket.
     *
     * <p>Technically, a replica shouldn't be in ISR if it hasn't caught up for longer that {@link
     * ConfigOptions#LOG_REPLICA_MAX_LAG_TIME}, even if its log end offset is >= HW. However, to be
     * consistent with how the follower determines whether a replica is in-sync, we only check HW.
     *
     * <p>This function can be triggered when a replica's LEO has incremented.
     */
    private void maybeExpandISr(FollowerReplica followerReplica) {
        IsrState currentIsrState = isrState;
        boolean needsIsrUpdate =
                !currentIsrState.isInflight()
                        && inReadLock(leaderIsrUpdateLock, () -> needsExpandIsr(followerReplica));

        if (needsIsrUpdate) {
            Optional<IsrState.PendingExpandIsrState> adjustIsrUpdateOpt =
                    inWriteLock(
                            leaderIsrUpdateLock,
                            () -> {
                                // check if this replica needs to be added to the ISR.
                                if (currentIsrState instanceof IsrState.CommittedIsrState) {
                                    if (needsExpandIsr(followerReplica)) {
                                        return Optional.of(
                                                prepareIsrExpand(
                                                        (IsrState.CommittedIsrState)
                                                                currentIsrState,
                                                        followerReplica.getFollowerId()));
                                    }
                                }

                                return Optional.empty();
                            });

            // Send adjust isr request outside the leaderIsrUpdateLock since the completion
            // logic may increment the high watermark (and consequently complete delayed
            // operations).
            adjustIsrUpdateOpt.map(this::submitAdjustIsr);
        }
    }

    void maybeShrinkIsr() {
        IsrState currentIstState = isrState;
        boolean needsIsrUpdate =
                !currentIstState.isInflight()
                        && inReadLock(leaderIsrUpdateLock, this::needsShrinkIsr);

        if (needsIsrUpdate) {
            Optional<IsrState.PendingShrinkIsrState> adjustIsrUpdateOpt =
                    inWriteLock(
                            leaderIsrUpdateLock,
                            () -> {
                                if (isLeader()) {
                                    List<Integer> outOfSyncFollowerReplicas =
                                            getOutOfSyncFollowerReplicas(replicaMaxLagTime);
                                    if (currentIstState instanceof IsrState.CommittedIsrState
                                            && !outOfSyncFollowerReplicas.isEmpty()) {
                                        List<Integer> newIsr =
                                                new ArrayList<>(currentIstState.isr());
                                        newIsr.removeAll(outOfSyncFollowerReplicas);
                                        LOG.info(
                                                "Shrink ISR From {} to {}. Leader: (high watermark: {}, "
                                                        + "end offset: {}, out of sync replicas: {})",
                                                currentIstState.isr(),
                                                newIsr,
                                                logTablet.getHighWatermark(),
                                                logTablet.localLogEndOffset(),
                                                outOfSyncFollowerReplicas);
                                        return Optional.of(
                                                prepareIsrShrink(
                                                        (IsrState.CommittedIsrState)
                                                                currentIstState,
                                                        newIsr,
                                                        outOfSyncFollowerReplicas));
                                    }
                                }

                                return Optional.empty();
                            });

            // Send adjust isr request outside the leaderIsrUpdateLock since the completion
            // logic may increment the high watermark (and consequently complete delayed
            // operations).
            adjustIsrUpdateOpt.map(this::submitAdjustIsr);
        }
    }

    private IsrState.PendingExpandIsrState prepareIsrExpand(
            IsrState.CommittedIsrState currentState, int newInSyncReplicaId) {
        // When expanding the ISR, we assume that the new replica will make it into the ISR
        // before we receive confirmation that it has. This ensures that the HW will already
        // reflect the updated ISR even if there is a delay before we receive the confirmation.
        // Alternatively, if the update fails, no harm is done since the expanded ISR puts
        // a stricter requirement for advancement of the HW.
        List<Integer> isrToSend = new ArrayList<>(isrState.isr());
        isrToSend.add(newInSyncReplicaId);

        // TODO add server epoch to isr.

        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        localTabletServerId, leaderEpoch, isrToSend, coordinatorEpoch, bucketEpoch);

        IsrState.PendingExpandIsrState updatedState =
                new IsrState.PendingExpandIsrState(
                        newInSyncReplicaId, newLeaderAndIsr, currentState);
        isrState = updatedState;
        return updatedState;
    }

    private IsrState.PendingShrinkIsrState prepareIsrShrink(
            IsrState.CommittedIsrState currentState,
            List<Integer> isrToSend,
            List<Integer> outOfSyncFollowerReplicas) {
        // When shrinking the ISR, we cannot assume that the update will succeed as this could
        // erroneously advance the HW if the `AdjustIsr` were to fail. Hence, the "maximal ISR"
        // for `PendingShrinkIsr` is the current ISR.

        // TODO add server epoch to isr.

        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        localTabletServerId, leaderEpoch, isrToSend, coordinatorEpoch, bucketEpoch);
        IsrState.PendingShrinkIsrState updatedState =
                new IsrState.PendingShrinkIsrState(
                        outOfSyncFollowerReplicas, newLeaderAndIsr, currentState);
        isrState = updatedState;
        return updatedState;
    }

    private CompletableFuture<LeaderAndIsr> submitAdjustIsr(
            IsrState.PendingIsrState proposedIsrState) {
        LOG.debug("Submitting ISR state change {}.", proposedIsrState);
        CompletableFuture<LeaderAndIsr> future =
                adjustIsrManager.submit(tableBucket, proposedIsrState.sentLeaderAndIsr());

        return future.whenComplete(
                (leaderAndIsr, exception) -> {
                    AtomicBoolean hwIncremented = new AtomicBoolean(false);
                    AtomicBoolean shouldRetry = new AtomicBoolean(false);

                    inWriteLock(
                            leaderIsrUpdateLock,
                            () -> {
                                if (isrState != proposedIsrState) {
                                    // This means replicaState was updated through leader election
                                    // or some other mechanism before we got the AdjustIsr response.
                                    // We don't know what happened on the coordinator server
                                    // exactly, but we do know this response is out of date, so we
                                    // ignore it.
                                    LOG.debug(
                                            "Ignoring failed ISR update to {} since we have already updated state to {}",
                                            proposedIsrState,
                                            isrState);
                                } else if (leaderAndIsr != null) {
                                    hwIncremented.set(
                                            handleAdjustIsrUpdate(proposedIsrState, leaderAndIsr));
                                } else {
                                    shouldRetry.set(
                                            handleAdjustIsrError(
                                                    proposedIsrState,
                                                    Errors.forException(exception)));
                                }
                            });

                    if (hwIncremented.get()) {
                        tryCompleteDelayedOperations();
                    }

                    // Send the AdjustIsr request outside the leaderIsrUpdateLock since the
                    // completion logic may increment the high watermark (and consequently complete
                    // delayed operations).
                    if (shouldRetry.get()) {
                        submitAdjustIsr(proposedIsrState);
                    }
                });
    }

    /**
     * Handle a successful 'AdjustIsr' response.
     *
     * @param proposedIsrState The ISR state change that was requested
     * @param leaderAndIsr The updated LeaderAndIsr state
     * @return true if the high watermark was successfully incremented following, false otherwise
     */
    private boolean handleAdjustIsrUpdate(
            IsrState.PendingIsrState proposedIsrState, LeaderAndIsr leaderAndIsr) {
        // Success from coordinator, still need to check a few things.
        if (leaderAndIsr.bucketEpoch() < bucketEpoch) {
            LOG.debug(
                    "Ignoring new ISR {} since we have a newer replica epoch {}",
                    leaderAndIsr,
                    bucketEpoch);
            return false;
        } else {
            // This is one of two states:
            //   1) leaderAndIsr.bucketEpoch > bucketEpoch: Coordinator updated to new version
            // with proposedIsrState.
            //   2) leaderAndIsr.bucketEpoch == bucketEpoch: No update was performed since
            // proposed and actual state are the same.
            // In both cases, we want to move from Pending to Committed state to ensure new updates
            // are processed.
            isrState = new IsrState.CommittedIsrState(leaderAndIsr.isr());
            bucketEpoch = leaderAndIsr.bucketEpoch();
            LOG.info(
                    "ISR updated to {} and bucket epoch updated to {} for bucket {}",
                    isrState.isr(),
                    bucketEpoch,
                    tableBucket);

            if (proposedIsrState instanceof IsrState.PendingShrinkIsrState) {
                isrShrinks.inc();
            } else if (proposedIsrState instanceof IsrState.PendingExpandIsrState) {
                isrExpands.inc();
            }

            // We may need to increment high watermark since ISR could be down to 1.
            try {
                return maybeIncrementLeaderHW(logTablet, clock.milliseconds());
            } catch (IOException e) {
                LOG.error("Failed to increment leader HW", e);
                return false;
            }
        }
    }

    /**
     * Handle a failed `AdjustIsr` request. For errors which are non-retrievable, we simply give up.
     * This leaves replica state in a pending state. Since the error was non-retrievable, we are
     * okay staying in this state until we see new metadata from LeaderAndIsr.
     *
     * @param proposedIsrState The ISR state change that was requested
     * @param error The error returned from {@link AdjustIsrManager}
     * @return true if the `AdjustIsr` request should be retried, false otherwise
     */
    private boolean handleAdjustIsrError(IsrState.PendingIsrState proposedIsrState, Errors error) {
        failedIsrUpdates.inc();
        switch (error) {
            case OPERATION_NOT_ATTEMPTED_EXCEPTION:
                // Care must be taken when resetting to the last committed state since we may not
                // know in general whether the request was applied or not taking into account
                // retries and controller changes which might have occurred before we received the
                // response.
                isrState = proposedIsrState.lastCommittedState();
                LOG.info(
                        "Failed to adjust isr to {} since the adjust isr manager rejected the request with error {}. "
                                + "Replica state has been reset to the latest committed state {}",
                        proposedIsrState,
                        error,
                        isrState);
                return false;
            case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                LOG.debug(
                        "Failed to adjust isr to {} since the coordinator doesn't know about this table or bucket. "
                                + "Replica state may be out of sync, awaiting new the latest metadata.",
                        proposedIsrState);
                return false;
            case INVALID_UPDATE_VERSION_EXCEPTION:
                LOG.debug(
                        "Failed to adjust isr to {} because the request is invalid. Replica state may be out of sync, "
                                + "awaiting new the latest metadata.",
                        proposedIsrState);
                return false;
            default:
                LOG.warn(
                        "Failed to adjust isr to {} due to unexpected error {}. Retrying.",
                        proposedIsrState,
                        error);
                return true;
        }
    }

    private boolean canAddFollowerReplicaToIsr(int followerId) {
        IsrState currentIsrState = isrState;
        return !currentIsrState.isInflight()
                && !currentIsrState.isr().contains(followerId)
                && isReplicaIsrEligible(followerId);
    }

    private boolean isReplicaIsrEligible(int followerId) {
        return metadataCache.isAliveTabletServer(followerId);
    }

    private boolean needsExpandIsr(FollowerReplica followerReplica) {
        return canAddFollowerReplicaToIsr(followerReplica.getFollowerId())
                && isFollowerInSync(followerReplica);
    }

    private boolean needsShrinkIsr() {
        return isLeader() && !getOutOfSyncFollowerReplicas(replicaMaxLagTime).isEmpty();
    }

    /**
     * If the follower already has the same LEO as the leader, it will not be considered as
     * out-of-sync, otherwise there are two cases that will be handled here
     *
     * <pre>
     *    1. Stuck followers: If the LEO of the replica hasn't been updated for maxLagMs ms,
     *    the follower is stuck and should  be removed from the ISR.
     *
     *    2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *    then the follower is lagging and should be removed from the ISR Both these cases are
     *    handled by checking the lastCaughtUpTimeMs which represents the last time when the
     *    replica was fully caught up.
     * </pre>
     *
     * <p>If either of the above conditions is violated, that replica is considered to be out of
     * sync
     *
     * <p>If an ISR update is in-flight, we will return an empty set here
     */
    private List<Integer> getOutOfSyncFollowerReplicas(long maxLagTime) {
        IsrState currentState = isrState;
        List<Integer> outOfSyncReplicas = new ArrayList<>();
        if (!currentState.isInflight()) {
            Set<Integer> candidateReplicas = new HashSet<>(currentState.isr());
            candidateReplicas.remove(localTabletServerId);
            long currentTimeMillis = clock.milliseconds();
            long leaderEndOffset = logTablet.localLogEndOffset();
            for (int replicaId : candidateReplicas) {
                if (isFollowerOutOfSync(
                        replicaId, leaderEndOffset, currentTimeMillis, maxLagTime)) {
                    outOfSyncReplicas.add(replicaId);
                }
            }

            return outOfSyncReplicas;
        }

        return outOfSyncReplicas;
    }

    private boolean isFollowerInSync(FollowerReplica followerReplica) {
        long followerEndOffset = followerReplica.stateSnapshot().getLogEndOffset();
        return followerEndOffset >= logTablet.getHighWatermark();
    }

    private boolean isFollowerOutOfSync(
            int replicaId, long leaderEndOffset, long currentTimeMs, long replicaMaxLagTime) {
        FollowerReplica followerReplica = followerReplicasMap.get(replicaId);
        if (followerReplica == null) {
            return true;
        }
        return !followerReplica
                .stateSnapshot()
                .isCaughtUp(leaderEndOffset, currentTimeMs, replicaMaxLagTime);
    }

    private void validateInSyncReplicaSize(int requiredAcks) {
        int inSyncSize = isrState.isr().size();
        if (inSyncSize < minInSyncReplicas && requiredAcks == -1) {
            throw new NotEnoughReplicasException(
                    String.format(
                            "The size of the current ISR %s is insufficient to satisfy "
                                    + "the required acks %s for table bucket %s.",
                            isrState.isr(), requiredAcks, tableBucket));
        }
    }

    private boolean isUnderMinIsr() {
        return isLeader() && isrState.isr().size() < minInSyncReplicas;
    }

    private LogTablet localLogOrThrow(boolean requireLeader) {
        // TODO check leader epoch.
        if (requireLeader && !isLeader()) {
            throw new NotLeaderOrFollowerException(
                    String.format(
                            "Leader not local for bucket %s on tabletServer %d",
                            tableBucket, localTabletServerId));
        }

        return logTablet;
    }

    @VisibleForTesting
    public boolean isLeader() {
        Integer leaderReplicaId = leaderReplicaIdOpt.get();
        return leaderReplicaId != null && leaderReplicaId.equals(localTabletServerId);
    }

    private LogTablet createLog(
            OffsetCheckpointFile.LazyOffsetCheckpoints lazyHighWatermarkCheckpoint)
            throws Exception {
        LogTablet log =
                logManager.getOrCreateLog(
                        physicalPath, tableBucket, logFormat, tieredLogLocalSegments, isKvTable());
        // update high watermark.
        Optional<Long> watermarkOpt = lazyHighWatermarkCheckpoint.fetch(tableBucket);
        long watermark =
                watermarkOpt.orElseGet(
                        () -> {
                            LOG.info(
                                    "No local checkpoint high watermark found for table bucket {}",
                                    tableBucket);
                            return 0L;
                        });
        log.updateHighWatermark(watermark);
        log.registerMetrics(bucketMetricGroup);
        LOG.info("Log loaded for bucket {} with initial high watermark {}", tableBucket, watermark);
        return log;
    }

    private void traceAckInfo(List<Integer> curMaximalIsr, long requiredOffset) {
        List<Tuple2<Integer, Long>> followerReplicaInfo = new ArrayList<>();
        curMaximalIsr.forEach(
                replica -> {
                    if (replica != localTabletServerId
                            && followerReplicasMap.containsKey(replica)) {
                        FollowerReplica rp = followerReplicasMap.get(replica);
                        followerReplicaInfo.add(
                                Tuple2.of(
                                        rp.getFollowerId(), rp.stateSnapshot().getLogEndOffset()));
                    }
                });

        List<Tuple2<Integer, Long>> ackedReplicas = new ArrayList<>();
        List<Tuple2<Integer, Long>> awaitingReplicas = new ArrayList<>();

        Tuple2<Integer, Long> localLogInfo =
                Tuple2.of(localTabletServerId, logTablet.localLogEndOffset());
        if (logTablet.localLogEndOffset() >= requiredOffset) {
            ackedReplicas.add(localLogInfo);
        } else {
            awaitingReplicas.add(localLogInfo);
        }

        followerReplicaInfo.forEach(
                replicaInfo -> {
                    if (replicaInfo.f1 >= requiredOffset) {
                        ackedReplicas.add(replicaInfo);
                    } else {
                        awaitingReplicas.add(replicaInfo);
                    }
                });

        LOG.trace(
                "Progress awaiting ISR acks for offset {}, acked replicas: {}, awaiting replicas: {}",
                requiredOffset,
                ackedReplicas.stream()
                        .map(tuple -> "server-" + tuple.f0 + ":" + tuple.f1)
                        .collect(Collectors.toList()),
                awaitingReplicas.stream()
                        .map(tuple -> "server-" + tuple.f0 + ":" + tuple.f1)
                        .collect(Collectors.toList()));
    }

    @VisibleForTesting
    public int getBucketEpoch() {
        return bucketEpoch;
    }

    @VisibleForTesting
    public List<Integer> getIsr() {
        return isrState.isr();
    }
}
