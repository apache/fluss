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

package org.apache.fluss.server.replica;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.exception.InvalidRequiredAcksException;
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.StorageException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ProjectionPushdownCache;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.LimitScanResultForBucket;
import org.apache.fluss.rpc.entity.ListOffsetsResultForBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PrefixLookupResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.entity.WriteResultForBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.LakeBucketOffset;
import org.apache.fluss.server.entity.NotifyKvSnapshotOffsetData;
import org.apache.fluss.server.entity.NotifyLakeTableOffsetData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.entity.NotifyRemoteLogOffsetsData;
import org.apache.fluss.server.entity.StopReplicaData;
import org.apache.fluss.server.entity.StopReplicaResultForBucket;
import org.apache.fluss.server.entity.UserContext;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvSnapshotResource;
import org.apache.fluss.server.kv.snapshot.CompletedKvSnapshotCommitter;
import org.apache.fluss.server.kv.snapshot.DefaultSnapshotContext;
import org.apache.fluss.server.kv.snapshot.SnapshotContext;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.ListOffsetsParam;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogOffsetMetadata;
import org.apache.fluss.server.log.LogReadInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.UserMetrics;
import org.apache.fluss.server.metrics.group.BucketMetricGroup;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.delay.DelayedFetchLog;
import org.apache.fluss.server.replica.delay.DelayedFetchLog.FetchBucketStatus;
import org.apache.fluss.server.replica.delay.DelayedOperationManager;
import org.apache.fluss.server.replica.delay.DelayedTableBucketKey;
import org.apache.fluss.server.replica.delay.DelayedWrite;
import org.apache.fluss.server.replica.fetcher.InitialFetchStatus;
import org.apache.fluss.server.replica.fetcher.ReplicaFetcherManager;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.server.TabletManagerBase.getTableInfo;
import static org.apache.fluss.utils.FileUtils.isDirectoryEmpty;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** A manager for replica. */
public class ReplicaManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaManager.class);

    public static final String HIGH_WATERMARK_CHECKPOINT_FILE_NAME = "high-watermark-checkpoint";
    private final Configuration conf;
    private final Scheduler scheduler;
    private final LogManager logManager;
    private final KvManager kvManager;
    private final ZooKeeperClient zkClient;
    protected final int serverId;
    private final AtomicBoolean highWatermarkCheckPointThreadStarted = new AtomicBoolean(false);
    private final OffsetCheckpointFile highWatermarkCheckpoint;

    @GuardedBy("replicaStateChangeLock")
    private final Map<TableBucket, HostedReplica> allReplicas = MapUtils.newConcurrentHashMap();

    private final TabletServerMetadataCache metadataCache;
    private final ExecutorService ioExecutor;
    private final ProjectionPushdownCache projectionsCache = new ProjectionPushdownCache();
    private final Lock replicaStateChangeLock = new ReentrantLock();

    /**
     * delayed write operation manager is used to manage the delayed write operation, which is
     * waited for other follower replicas ack.
     */
    private final DelayedOperationManager<DelayedWrite<?>> delayedWriteManager;

    /**
     * delayed fetch log operation manager is used to manage the delayed fetch log operation, which
     * is waited for the available fetch log size bigger than the minLogFetchSize or the wait time
     * is up.
     */
    private final DelayedOperationManager<DelayedFetchLog> delayedFetchLogManager;

    private final ReplicaFetcherManager replicaFetcherManager;
    // The manager used to manager the replica alter, especially the isr expand and shrink.
    private final AdjustIsrManager adjustIsrManager;
    private final FatalErrorHandler fatalErrorHandler;

    /** epoch of the coordinator that last changed the leader. */
    @GuardedBy("replicaStateChangeLock")
    private volatile int coordinatorEpoch = CoordinatorContext.INITIAL_COORDINATOR_EPOCH;

    // for kv snapshot
    private final KvSnapshotResource kvSnapshotResource;
    private final SnapshotContext kvSnapshotContext;

    // remote log manager for remote log storage.
    private final RemoteLogManager remoteLogManager;

    // for metrics
    private final TabletServerMetricGroup serverMetricGroup;
    private final UserMetrics userMetrics;
    private final String internalListenerName;

    private final Clock clock;

    public ReplicaManager(
            Configuration conf,
            Scheduler scheduler,
            LogManager logManager,
            KvManager kvManager,
            ZooKeeperClient zkClient,
            int serverId,
            TabletServerMetadataCache metadataCache,
            RpcClient rpcClient,
            CoordinatorGateway coordinatorGateway,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            FatalErrorHandler fatalErrorHandler,
            TabletServerMetricGroup serverMetricGroup,
            UserMetrics userMetrics,
            Clock clock,
            ExecutorService ioExecutor)
            throws IOException {
        this(
                conf,
                scheduler,
                logManager,
                kvManager,
                zkClient,
                serverId,
                metadataCache,
                rpcClient,
                coordinatorGateway,
                completedKvSnapshotCommitter,
                fatalErrorHandler,
                serverMetricGroup,
                userMetrics,
                new RemoteLogManager(conf, zkClient, coordinatorGateway, clock, ioExecutor),
                clock,
                ioExecutor);
    }

    @VisibleForTesting
    ReplicaManager(
            Configuration conf,
            Scheduler scheduler,
            LogManager logManager,
            KvManager kvManager,
            ZooKeeperClient zkClient,
            int serverId,
            TabletServerMetadataCache metadataCache,
            RpcClient rpcClient,
            CoordinatorGateway coordinatorGateway,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            FatalErrorHandler fatalErrorHandler,
            TabletServerMetricGroup serverMetricGroup,
            UserMetrics userMetrics,
            RemoteLogManager remoteLogManager,
            Clock clock,
            ExecutorService ioExecutor)
            throws IOException {
        this.conf = conf;
        this.zkClient = zkClient;
        this.scheduler = scheduler;
        this.logManager = logManager;
        this.kvManager = kvManager;
        this.serverId = serverId;
        this.metadataCache = metadataCache;

        this.highWatermarkCheckpoint =
                new OffsetCheckpointFile(
                        new File(
                                logManager.getDataDir().getAbsolutePath(),
                                HIGH_WATERMARK_CHECKPOINT_FILE_NAME));
        this.delayedWriteManager =
                new DelayedOperationManager<>(
                        "delay write",
                        serverId,
                        conf.getInt(ConfigOptions.LOG_REPLICA_WRITE_OPERATION_PURGE_NUMBER));
        this.delayedFetchLogManager =
                new DelayedOperationManager<>(
                        "delay fetch log",
                        serverId,
                        conf.getInt(ConfigOptions.LOG_REPLICA_FETCH_OPERATION_PURGE_NUMBER));
        this.internalListenerName = conf.get(ConfigOptions.INTERNAL_LISTENER_NAME);

        this.replicaFetcherManager =
                new ReplicaFetcherManager(
                        conf,
                        rpcClient,
                        serverId,
                        this,
                        (nodeId) -> metadataCache.getTabletServer(nodeId, internalListenerName));
        this.adjustIsrManager = new AdjustIsrManager(scheduler, coordinatorGateway, serverId);
        this.fatalErrorHandler = fatalErrorHandler;

        // for kv snapshot
        this.kvSnapshotResource = KvSnapshotResource.create(serverId, conf, ioExecutor);
        this.kvSnapshotContext =
                DefaultSnapshotContext.create(
                        zkClient, completedKvSnapshotCommitter, kvSnapshotResource, conf);
        this.remoteLogManager = remoteLogManager;
        this.serverMetricGroup = serverMetricGroup;
        this.userMetrics = userMetrics;
        this.clock = clock;
        this.ioExecutor = ioExecutor;
        registerMetrics();
    }

    public void startup() {
        // start up ISR expiration thread.
        // A follower can log behind leader for up tp configOptions#LOG_REPLICA_MAX_LAG_TIME x 1.5
        // before it is removed from ISR.
        scheduler.schedule(
                "isr-expiration",
                this::maybeShrinkIsr,
                0L,
                conf.get(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME).toMillis() / 2);
    }

    public RemoteLogManager getRemoteLogManager() {
        return remoteLogManager;
    }

    private void registerMetrics() {
        serverMetricGroup.gauge(
                MetricNames.REPLICA_LEADER_COUNT,
                () -> onlineReplicas().filter(Replica::isLeader).count());
        serverMetricGroup.gauge(MetricNames.REPLICA_COUNT, allReplicas::size);
        serverMetricGroup.gauge(MetricNames.WRITE_ID_COUNT, this::writerIdCount);
        serverMetricGroup.gauge(MetricNames.DELAYED_WRITE_COUNT, delayedWriteManager::numDelayed);
        serverMetricGroup.gauge(
                MetricNames.DELAYED_FETCH_COUNT, delayedFetchLogManager::numDelayed);

        serverMetricGroup.gauge(MetricNames.UNDER_REPLICATED, this::underReplicatedCount);
        serverMetricGroup.gauge(MetricNames.UNDER_MIN_ISR, this::underMinIsrCount);
        serverMetricGroup.gauge(MetricNames.AT_MIN_ISR, this::atMinIsrCount);

        MetricGroup logicalStorage = serverMetricGroup.addGroup("logicalStorage");
        logicalStorage.gauge(
                MetricNames.SERVER_LOGICAL_STORAGE_LOG_SIZE, this::logicalStorageLogSize);
        logicalStorage.gauge(
                MetricNames.SERVER_LOGICAL_STORAGE_KV_SIZE, this::logicalStorageKvSize);

        MetricGroup physicalStorage = serverMetricGroup.addGroup("physicalStorage");
        physicalStorage.gauge(
                MetricNames.SERVER_PHYSICAL_STORAGE_LOCAL_SIZE, this::physicalStorageLocalSize);
        physicalStorage.gauge(
                MetricNames.SERVER_PHYSICAL_STORAGE_REMOTE_LOG_SIZE,
                this::physicalStorageRemoteLogSize);
    }

    @VisibleForTesting
    public Stream<Replica> onlineReplicas() {
        return allReplicas.values().stream()
                .map(
                        t -> {
                            if (t instanceof OnlineReplica) {
                                return Optional.of(((OnlineReplica) t).getReplica());
                            } else {
                                return Optional.empty();
                            }
                        })
                .filter(Optional::isPresent)
                .map(t -> (Replica) t.get());
    }

    private long underReplicatedCount() {
        return onlineReplicas().filter(Replica::isUnderReplicated).count();
    }

    private long underMinIsrCount() {
        return onlineReplicas().filter(Replica::isUnderMinIsr).count();
    }

    private long atMinIsrCount() {
        return onlineReplicas().filter(Replica::isAtMinIsr).count();
    }

    @VisibleForTesting
    public long leaderCount() {
        return onlineReplicas().filter(Replica::isLeader).count();
    }

    private int writerIdCount() {
        return onlineReplicas().map(Replica::writerIdCount).reduce(0, Integer::sum);
    }

    private long logicalStorageLogSize() {
        return onlineReplicas().map(Replica::logicalStorageLogSize).reduce(0L, Long::sum);
    }

    private long logicalStorageKvSize() {
        return onlineReplicas().map(Replica::logicalStorageKvSize).reduce(0L, Long::sum);
    }

    private long physicalStorageLocalSize() {
        return onlineReplicas()
                .mapToLong(
                        replica -> {
                            long size = replica.getLogTablet().logSize();
                            if (replica.isKvTable()) {
                                size += replica.getLatestKvSnapshotSize();
                            }
                            return size;
                        })
                .reduce(0L, Long::sum);
    }

    private long physicalStorageRemoteLogSize() {
        return remoteLogManager.getRemoteLogSize();
    }

    /**
     * Receive a request to make these replicas to become leader or follower, if the replica doesn't
     * exit, we will create it.
     */
    public void becomeLeaderOrFollower(
            int requestCoordinatorEpoch,
            List<NotifyLeaderAndIsrData> notifyLeaderAndIsrDataList,
            Consumer<List<NotifyLeaderAndIsrResultForBucket>> responseCallback) {
        Map<TableBucket, NotifyLeaderAndIsrResultForBucket> result = new HashMap<>();
        inLock(
                replicaStateChangeLock,
                () -> {
                    // check or apply coordinator epoch.
                    validateAndApplyCoordinatorEpoch(requestCoordinatorEpoch, "notifyLeaderAndIsr");

                    List<NotifyLeaderAndIsrData> replicasToBeLeader = new ArrayList<>();
                    List<NotifyLeaderAndIsrData> replicasToBeFollower = new ArrayList<>();
                    for (NotifyLeaderAndIsrData data : notifyLeaderAndIsrDataList) {
                        LOG.info(
                                "Try to become leaderAndFollower for {} with isr {}, replicas: {}",
                                data.getTableBucket(),
                                data.getLeaderAndIsr(),
                                data.getReplicas());
                        TableBucket tb = data.getTableBucket();
                        try {
                            boolean becomeLeader = validateAndGetIsBecomeLeader(data);
                            if (becomeLeader) {
                                replicasToBeLeader.add(data);
                            } else {
                                replicasToBeFollower.add(data);
                            }
                        } catch (Exception e) {
                            result.put(
                                    tb,
                                    new NotifyLeaderAndIsrResultForBucket(
                                            tb, ApiError.fromThrowable(e)));
                        }
                    }

                    makeLeaders(replicasToBeLeader, result);
                    makeFollowers(replicasToBeFollower, result);

                    // We initialize highWatermark thread after the first LeaderAndIsr request. This
                    // ensures that all the replicas have been completely populated before starting
                    // the checkpointing there by avoiding weird race conditions
                    startHighWatermarkCheckPointThread();
                    replicaFetcherManager.shutdownIdleFetcherThreads();
                });

        responseCallback.accept(new ArrayList<>(result.values()));
    }

    public void maybeUpdateMetadataCache(int coordinatorEpoch, ClusterMetadata clusterMetadata) {
        inLock(
                replicaStateChangeLock,
                () -> {
                    // check or apply coordinator epoch.
                    validateAndApplyCoordinatorEpoch(coordinatorEpoch, "updateMetadataCache");
                    metadataCache.updateClusterMetadata(clusterMetadata);
                });
    }

    /**
     * Append log records to leader replicas of the buckets, and wait for them to be replicated to
     * other replicas.
     *
     * <p>The callback function will be triggered when the required acks are satisfied; if the
     * callback function itself is already synchronized on some object then pass this object to
     * avoid deadlock.
     */
    public void appendRecordsToLog(
            int timeoutMs,
            int requiredAcks,
            Map<TableBucket, MemoryLogRecords> entriesPerBucket,
            @Nullable UserContext userContext,
            Consumer<List<ProduceLogResultForBucket>> responseCallback) {
        if (isRequiredAcksInvalid(requiredAcks)) {
            throw new InvalidRequiredAcksException("Invalid required acks: " + requiredAcks);
        }

        long startTime = System.currentTimeMillis();
        Map<TableBucket, ProduceLogResultForBucket> appendResult =
                appendToLocalLog(entriesPerBucket, requiredAcks, userContext);
        LOG.debug("Append records to local log in {} ms", System.currentTimeMillis() - startTime);

        // maybe do delay write operation.
        maybeAddDelayedWrite(
                timeoutMs, requiredAcks, entriesPerBucket.size(), appendResult, responseCallback);
    }

    /**
     * Fetch records from a replica. Currently, we will return the fetched records immediately.
     *
     * <p>The callback function will be triggered when required fetch info is satisfied. Both client
     * scanner and followers can only fetch from leader replica.
     */
    public void fetchLogRecords(
            FetchParams params,
            Map<TableBucket, FetchReqInfo> bucketFetchInfo,
            @Nullable UserContext userContext,
            Consumer<Map<TableBucket, FetchLogResultForBucket>> responseCallback) {
        long startTime = System.currentTimeMillis();
        Map<TableBucket, LogReadResult> logReadResults =
                readFromLog(params, bucketFetchInfo, userContext);
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Fetch log records from local log in {} ms",
                    System.currentTimeMillis() - startTime);
        }

        // maybe do delay fetch log operation.
        maybeAddDelayedFetchLog(
                params, bucketFetchInfo, logReadResults, userContext, responseCallback);
    }

    /**
     * Put kv records to leader replicas of the buckets, the kv data will write to kv tablet and the
     * response callback need to wait for the cdc log to be replicated to other replicas if needed.
     */
    public void putRecordsToKv(
            int timeoutMs,
            int requiredAcks,
            Map<TableBucket, KvRecordBatch> entriesPerBucket,
            @Nullable int[] targetColumns,
            Consumer<List<PutKvResultForBucket>> responseCallback) {
        if (isRequiredAcksInvalid(requiredAcks)) {
            throw new InvalidRequiredAcksException("Invalid required acks: " + requiredAcks);
        }

        long startTime = System.currentTimeMillis();
        Map<TableBucket, PutKvResultForBucket> kvPutResult =
                putToLocalKv(entriesPerBucket, targetColumns, requiredAcks);
        LOG.debug(
                "Put records to local kv storage and wait generate cdc log in {} ms",
                System.currentTimeMillis() - startTime);

        // maybe do delay write operation to write cdc log to be replicated to other follower
        // replicas.
        maybeAddDelayedWrite(
                timeoutMs, requiredAcks, entriesPerBucket.size(), kvPutResult, responseCallback);
    }

    /** Lookup a single key value. */
    @VisibleForTesting
    protected void lookup(TableBucket tableBucket, byte[] key, Consumer<byte[]> responseCallback) {
        lookups(
                Collections.singletonMap(tableBucket, Collections.singletonList(key)),
                multiLookupResponseCallBack -> {
                    LookupResultForBucket result = multiLookupResponseCallBack.get(tableBucket);
                    List<byte[]> values = result.lookupValues();
                    checkState(
                            values.size() == 1,
                            "The result value for single lookup should be with size 1, "
                                    + "but the result size is {}",
                            values.size());
                    responseCallback.accept(values.get(0));
                });
    }

    /** Lookup with multi key from leader replica of the buckets. */
    public void lookups(
            Map<TableBucket, List<byte[]>> entriesPerBucket,
            Consumer<Map<TableBucket, LookupResultForBucket>> responseCallback) {
        Map<TableBucket, LookupResultForBucket> lookupResultForBucketMap = new HashMap<>();
        long startTime = System.currentTimeMillis();
        TableMetricGroup tableMetrics = null;
        for (Map.Entry<TableBucket, List<byte[]>> entry : entriesPerBucket.entrySet()) {
            TableBucket tb = entry.getKey();
            try {
                Replica replica = getReplicaOrException(tb);
                tableMetrics = replica.tableMetrics();
                tableMetrics.totalLookupRequests().inc();
                lookupResultForBucketMap.put(
                        tb, new LookupResultForBucket(tb, replica.lookups(entry.getValue())));
            } catch (Exception e) {
                if (isUnexpectedException(e)) {
                    LOG.error("Error lookup from local kv on replica {}", tb, e);
                    // NOTE: Failed lookup requests metric is not incremented for known exceptions
                    // since it is supposed to indicate un-expected failure of a server in handling
                    // a lookup request.
                    if (tableMetrics != null) {
                        tableMetrics.failedLookupRequests().inc();
                    }
                }
                lookupResultForBucketMap.put(
                        tb, new LookupResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }
        LOG.debug("Lookup from local kv in {}ms", System.currentTimeMillis() - startTime);
        responseCallback.accept(lookupResultForBucketMap);
    }

    /** Lookup multi prefixKeys by prefix scan on kv store. */
    public void prefixLookups(
            Map<TableBucket, List<byte[]>> entriesPerBucket,
            Consumer<Map<TableBucket, PrefixLookupResultForBucket>> responseCallback) {
        TableMetricGroup tableMetrics = null;
        Map<TableBucket, PrefixLookupResultForBucket> result = new HashMap<>();
        for (Map.Entry<TableBucket, List<byte[]>> entry : entriesPerBucket.entrySet()) {
            TableBucket tb = entry.getKey();
            List<List<byte[]>> resultForBucket = new ArrayList<>();
            try {
                Replica replica = getReplicaOrException(tb);
                tableMetrics = replica.tableMetrics();
                tableMetrics.totalPrefixLookupRequests().inc();
                for (byte[] prefixKey : entry.getValue()) {
                    List<byte[]> resultForPerKey = replica.prefixLookup(prefixKey);
                    resultForBucket.add(resultForPerKey);
                }
                result.put(tb, new PrefixLookupResultForBucket(tb, resultForBucket));
            } catch (Exception e) {
                if (isUnexpectedException(e)) {
                    LOG.error("Error processing prefix lookup operation on replica {}", tb, e);
                    if (tableMetrics != null) {
                        tableMetrics.failedPrefixLookupRequests().inc();
                    }
                }
                result.put(tb, new PrefixLookupResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }
        responseCallback.accept(result);
    }

    public void listOffsets(
            ListOffsetsParam listOffsetsParam,
            Set<TableBucket> tableBuckets,
            Consumer<List<ListOffsetsResultForBucket>> responseCallBack) {
        List<ListOffsetsResultForBucket> result = new ArrayList<>();
        for (TableBucket tb : tableBuckets) {
            try {
                Replica replica = getReplicaOrException(tb);
                result.add(
                        new ListOffsetsResultForBucket(
                                tb, replica.getOffset(remoteLogManager, listOffsetsParam)));
            } catch (Exception e) {
                LOG.error("Error processing list offsets operation on replica {}", tb, e);
                result.add(new ListOffsetsResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }
        responseCallBack.accept(result);
    }

    public void stopReplicas(
            int requestCoordinatorEpoch,
            List<StopReplicaData> stopReplicaDataList,
            Consumer<List<StopReplicaResultForBucket>> responseCallback) {
        List<StopReplicaResultForBucket> result = new ArrayList<>();
        inLock(
                replicaStateChangeLock,
                () -> {
                    // check or apply coordinator epoch.
                    validateAndApplyCoordinatorEpoch(requestCoordinatorEpoch, "stopReplicas");

                    // store the deleted table id and the table dir path to delete the table dir
                    // after delete all the buckets of this table.
                    Map<Long, Path> deletedTableIds = new HashMap<>();
                    // the same to partition id and partition dir path
                    Map<Long, Path> deletedPartitionIds = new HashMap<>();

                    for (StopReplicaData data : stopReplicaDataList) {
                        TableBucket tb = data.getTableBucket();
                        HostedReplica hostedReplica = getReplica(tb);
                        if (hostedReplica instanceof NoneReplica) {
                            // do nothing fort this case.
                            result.add(new StopReplicaResultForBucket(tb));
                        } else if (hostedReplica instanceof OfflineReplica) {
                            LOG.warn(
                                    "Ignoring stopReplica request for table bucket {} as the local replica is offline",
                                    tb);
                            result.add(
                                    new StopReplicaResultForBucket(
                                            tb,
                                            Errors.LOG_STORAGE_EXCEPTION,
                                            "local replica is offline"));
                        } else if (hostedReplica instanceof OnlineReplica) {
                            Replica replica = ((OnlineReplica) hostedReplica).getReplica();
                            int requestLeaderEpoch = data.getLeaderEpoch();
                            int currentLeaderEpoch = replica.getLeaderEpoch();
                            if (requestLeaderEpoch < currentLeaderEpoch) {
                                String errorMessage =
                                        String.format(
                                                "invalid leader epoch %s in stop replica request, "
                                                        + "The latest known leader epoch is %s for table bucket %s.",
                                                requestLeaderEpoch, currentLeaderEpoch, tb);
                                LOG.warn(
                                        "Ignore the stop replica request for bucket {} because {}",
                                        tb,
                                        errorMessage);
                                result.add(
                                        new StopReplicaResultForBucket(
                                                tb,
                                                Errors.FENCED_LEADER_EPOCH_EXCEPTION,
                                                errorMessage));
                            } else {
                                try {
                                    result.add(
                                            stopReplica(
                                                    tb,
                                                    data.isDelete(),
                                                    deletedTableIds,
                                                    deletedPartitionIds));
                                } catch (Exception e) {
                                    LOG.error(
                                            "Error processing stopReplica operation on hostedReplica {}",
                                            tb,
                                            e);
                                    result.add(
                                            new StopReplicaResultForBucket(
                                                    tb, ApiError.fromThrowable(e)));
                                }
                            }
                        }
                    }

                    // must delete partition dir first, then table dir
                    deletedPartitionIds.forEach(
                            (id, dir) -> dropEmptyTableOrPartitionDir(dir, id, "partition"));
                    deletedTableIds.forEach(
                            (id, dir) -> dropEmptyTableOrPartitionDir(dir, id, "table"));
                });

        responseCallback.accept(result);
    }

    public void notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsData notifyRemoteLogOffsetsData,
            Consumer<NotifyRemoteLogOffsetsResponse> responseCallback) {
        inLock(
                replicaStateChangeLock,
                () -> {
                    // check or apply coordinator epoch.
                    validateAndApplyCoordinatorEpoch(
                            notifyRemoteLogOffsetsData.getCoordinatorEpoch(),
                            "notifyRemoteLogOffsets");
                    // update the remote log offsets and delete local segments already copied to
                    // remote.
                    TableBucket tb = notifyRemoteLogOffsetsData.getTableBucket();
                    LogTablet logTablet = getReplicaOrException(tb).getLogTablet();
                    logTablet.updateRemoteLogStartOffset(
                            notifyRemoteLogOffsetsData.getRemoteLogStartOffset());
                    logTablet.updateRemoteLogEndOffset(
                            notifyRemoteLogOffsetsData.getRemoteLogEndOffset());
                    responseCallback.accept(new NotifyRemoteLogOffsetsResponse());
                });
    }

    public void notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetData notifyKvSnapshotOffsetData,
            Consumer<NotifyKvSnapshotOffsetResponse> responseCallback) {
        inLock(
                replicaStateChangeLock,
                () -> {
                    // check or apply coordinator epoch.
                    validateAndApplyCoordinatorEpoch(
                            notifyKvSnapshotOffsetData.getCoordinatorEpoch(),
                            "notifyKvSnapshotOffset");
                    // update the snapshot offset.
                    TableBucket tb = notifyKvSnapshotOffsetData.getTableBucket();
                    LogTablet logTablet = getReplicaOrException(tb).getLogTablet();
                    logTablet.updateMinRetainOffset(
                            notifyKvSnapshotOffsetData.getMinRetainOffset());
                    responseCallback.accept(new NotifyKvSnapshotOffsetResponse());
                });
    }

    public void notifyLakeTableOffset(
            NotifyLakeTableOffsetData notifyLakeTableOffsetData,
            Consumer<NotifyLakeTableOffsetResponse> responseCallback) {
        inLock(
                replicaStateChangeLock,
                () -> {
                    // check or apply coordinator epoch.
                    validateAndApplyCoordinatorEpoch(
                            notifyLakeTableOffsetData.getCoordinatorEpoch(),
                            "notifyLakeTableOffset");

                    Map<TableBucket, LakeBucketOffset> lakeBucketOffsets =
                            notifyLakeTableOffsetData.getLakeBucketOffsets();
                    for (Map.Entry<TableBucket, LakeBucketOffset> lakeBucketOffsetEntry :
                            lakeBucketOffsets.entrySet()) {
                        TableBucket tb = lakeBucketOffsetEntry.getKey();
                        LakeBucketOffset lakeBucketOffset = lakeBucketOffsetEntry.getValue();
                        LogTablet logTablet = getReplicaOrException(tb).getLogTablet();
                        logTablet.updateLakeTableSnapshotId(lakeBucketOffset.getSnapshotId());

                        lakeBucketOffset
                                .getLogStartOffset()
                                .ifPresent(logTablet::updateLakeLogStartOffset);

                        lakeBucketOffset
                                .getLogEndOffset()
                                .ifPresent(logTablet::updateLakeLogEndOffset);

                        lakeBucketOffset
                                .getMaxTimestamp()
                                .ifPresent(logTablet::updateLakeMaxTimestamp);

                        responseCallback.accept(new NotifyLakeTableOffsetResponse());
                    }
                });
    }

    /**
     * Make the current server to become leader for a given set of replicas by:
     *
     * <pre>
     *     1. Stop fetchers for these replicas
     *     2. Make these replicas to the leader
     * </pre>
     */
    private void makeLeaders(
            List<NotifyLeaderAndIsrData> replicasToBeLeader,
            Map<TableBucket, NotifyLeaderAndIsrResultForBucket> result) {
        if (replicasToBeLeader.isEmpty()) {
            return;
        }
        replicaFetcherManager.removeFetcherForBuckets(
                replicasToBeLeader.stream()
                        .map(NotifyLeaderAndIsrData::getTableBucket)
                        .collect(Collectors.toSet()));

        for (NotifyLeaderAndIsrData data : replicasToBeLeader) {
            TableBucket tb = data.getTableBucket();
            try {
                Replica replica = getReplicaOrException(tb);
                replica.makeLeader(data);
                if (replica.isDataLakeEnabled()) {
                    updateWithLakeTableSnapshot(replica);
                }
                // start the remote log tiering tasks for leaders
                remoteLogManager.startLogTiering(replica);
                result.put(tb, new NotifyLeaderAndIsrResultForBucket(tb));
            } catch (Exception e) {
                LOG.error("Error make replica {} to leader", tb, e);
                result.put(
                        tb, new NotifyLeaderAndIsrResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }
    }

    // NOTE: This method can be removed when fetchFromLake is deprecated
    private void updateWithLakeTableSnapshot(Replica replica) throws Exception {
        TableBucket tb = replica.getTableBucket();
        Optional<LakeTableSnapshot> optLakeTableSnapshot =
                zkClient.getLakeTableSnapshot(replica.getTableBucket().getTableId());
        if (optLakeTableSnapshot.isPresent()) {
            LakeTableSnapshot lakeTableSnapshot = optLakeTableSnapshot.get();
            long snapshotId = optLakeTableSnapshot.get().getSnapshotId();
            replica.getLogTablet().updateLakeTableSnapshotId(snapshotId);
            lakeTableSnapshot
                    .getLogEndOffset(tb)
                    .ifPresent(replica.getLogTablet()::updateLakeLogEndOffset);
        }
    }

    /**
     * Make the current server to become follower for a given set of replicas by:
     *
     * <pre>
     *      1. Mark the replicas as followers so that no more data can be added from the producer clients.
     *      2. Stop fetchers for these replicas so that no more data can be added by the replica fetcher threads.
     *      3. Truncate the log and checkpoint offsets for these replicas.
     *      4. Clear the delayed produce in the purgatory.
     *      5. If the server is not shutting down,add the fetcher to the new leaders.
     * </pre>
     */
    private void makeFollowers(
            List<NotifyLeaderAndIsrData> replicasToBeFollower,
            Map<TableBucket, NotifyLeaderAndIsrResultForBucket> result) {
        if (replicasToBeFollower.isEmpty()) {
            return;
        }
        List<Replica> replicasBecomeFollower = new ArrayList<>();
        for (NotifyLeaderAndIsrData data : replicasToBeFollower) {
            TableBucket tb = data.getTableBucket();
            try {
                Replica replica = getReplicaOrException(data.getTableBucket());
                if (replica.makeFollower(data)) {
                    replicasBecomeFollower.add(replica);
                }
                // stop the remote log tiering tasks for followers
                remoteLogManager.stopLogTiering(replica);
                result.put(tb, new NotifyLeaderAndIsrResultForBucket(tb));
            } catch (Exception e) {
                LOG.error("Error make replica {} to follower", tb, e);
                result.put(
                        tb, new NotifyLeaderAndIsrResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }

        // Stopping the fetchers must be done first in order to initialize the fetch position
        // correctly.
        replicaFetcherManager.removeFetcherForBuckets(
                replicasBecomeFollower.stream()
                        .map(Replica::getTableBucket)
                        .collect(Collectors.toSet()));

        replicasBecomeFollower.forEach(
                replica -> completeDelayedOperations(replica.getTableBucket()));

        LOG.info(
                "Stopped fetchers as part of become follower request for {} replicas",
                replicasToBeFollower.size());

        // Truncate the follower replicas LEO to highWatermark.
        // TODO this logic need to be removed after we introduce leader epoch cache, and fetcher
        // manager support truncating while fetching. Trace by
        // https://github.com/apache/fluss/issues/673
        truncateToHighWatermark(replicasBecomeFollower);

        // add fetcher for those follower replicas.
        addFetcherForReplicas(replicasBecomeFollower, result);
    }

    private void addFetcherForReplicas(
            List<Replica> replicas, Map<TableBucket, NotifyLeaderAndIsrResultForBucket> result) {
        Map<TableBucket, InitialFetchStatus> bucketAndStatus = new HashMap<>();
        for (Replica replica : replicas) {
            Integer leaderId = replica.getLeaderId();
            TableBucket tb = replica.getTableBucket();
            LogTablet logTablet = replica.getLogTablet();
            if (leaderId == null) {
                result.put(
                        tb,
                        new NotifyLeaderAndIsrResultForBucket(
                                tb,
                                ApiError.fromThrowable(
                                        new StorageException(
                                                String.format(
                                                        "Could not find leader for follower replica %s while make "
                                                                + "follower for %s.",
                                                        serverId, tb)))));
            } else {
                bucketAndStatus.put(
                        tb,
                        new InitialFetchStatus(
                                tb.getTableId(),
                                replica.getTablePath(),
                                leaderId,
                                logTablet.localLogEndOffset()));
            }
        }
        replicaFetcherManager.addFetcherForBuckets(bucketAndStatus);
    }

    /** Append log records to leader replicas of the buckets. */
    private Map<TableBucket, ProduceLogResultForBucket> appendToLocalLog(
            Map<TableBucket, MemoryLogRecords> entriesPerBucket,
            int requiredAcks,
            @Nullable UserContext userContext) {
        Map<TableBucket, ProduceLogResultForBucket> resultForBucketMap = new HashMap<>();
        for (Map.Entry<TableBucket, MemoryLogRecords> entry : entriesPerBucket.entrySet()) {
            TableBucket tb = entry.getKey();
            MemoryLogRecords records = entry.getValue();
            TableMetricGroup tableMetrics = null;
            try {
                Replica replica = getReplicaOrException(tb);
                tableMetrics = replica.tableMetrics();
                tableMetrics.totalProduceLogRequests().inc();
                // record user metrics before appending to log,
                // so that if appending fails, we still account the bytes.
                userMetrics.incBytesIn(userContext, replica.getTablePath(), records.sizeInBytes());
                LOG.trace("Append records to local log tablet for table bucket {}", tb);
                LogAppendInfo appendInfo = replica.appendRecordsToLeader(records, requiredAcks);

                long baseOffset = appendInfo.firstOffset();
                LOG.trace(
                        "Append to log {} beginning at offset {} and ending at offset {}",
                        tb,
                        baseOffset,
                        appendInfo.lastOffset());

                resultForBucketMap.put(
                        tb,
                        new ProduceLogResultForBucket(tb, baseOffset, appendInfo.lastOffset() + 1));
                tableMetrics.incLogBytesIn(appendInfo.validBytes());
                tableMetrics.incLogMessageIn(appendInfo.numMessages());
            } catch (Exception e) {
                if (isUnexpectedException(e)) {
                    LOG.error("Error append records to local log on replica {}", tb, e);
                    // NOTE: Failed produce requests metric is not incremented for known exceptions
                    // since it is supposed to indicate un-expected failure of a server in
                    // handling a produce request
                    if (tableMetrics != null) {
                        tableMetrics.failedProduceLogRequests().inc();
                    }
                }
                resultForBucketMap.put(
                        tb, new ProduceLogResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }

        return resultForBucketMap;
    }

    private Map<TableBucket, PutKvResultForBucket> putToLocalKv(
            Map<TableBucket, KvRecordBatch> entriesPerBucket,
            @Nullable int[] targetColumns,
            int requiredAcks) {
        Map<TableBucket, PutKvResultForBucket> putResultForBucketMap = new HashMap<>();
        for (Map.Entry<TableBucket, KvRecordBatch> entry : entriesPerBucket.entrySet()) {
            TableBucket tb = entry.getKey();
            TableMetricGroup tableMetrics = null;
            try {
                LOG.trace("Put records to local kv tablet for table bucket {}", tb);
                Replica replica = getReplicaOrException(tb);
                tableMetrics = replica.tableMetrics();
                tableMetrics.totalPutKvRequests().inc();
                LogAppendInfo appendInfo =
                        replica.putRecordsToLeader(entry.getValue(), targetColumns, requiredAcks);
                LOG.trace(
                        "Written to local kv for {}, and the cdc log beginning at offset {} and ending at offset {}",
                        tb,
                        appendInfo.firstOffset(),
                        appendInfo.lastOffset());
                putResultForBucketMap.put(
                        tb, new PutKvResultForBucket(tb, appendInfo.lastOffset() + 1));

                // metric for kv
                tableMetrics.incKvMessageIn(entry.getValue().getRecordCount());
                tableMetrics.incKvBytesIn(entry.getValue().sizeInBytes());
                // metric for cdc log of kv
                tableMetrics.incLogBytesIn(appendInfo.validBytes());
                tableMetrics.incLogMessageIn(appendInfo.numMessages());
            } catch (Exception e) {
                if (isUnexpectedException(e)) {
                    LOG.error("Error put records to local kv on replica {}", tb, e);
                    // NOTE: Failed put requests metric is not incremented for known exceptions
                    // since it is supposed to indicate un-expected failure of a server in
                    // handling a put request
                    if (tableMetrics != null) {
                        tableMetrics.failedPutKvRequests().inc();
                    }
                }
                putResultForBucketMap.put(
                        tb, new PutKvResultForBucket(tb, ApiError.fromThrowable(e)));
            }
        }

        return putResultForBucketMap;
    }

    public void limitScan(
            TableBucket tableBucket,
            int limit,
            Consumer<LimitScanResultForBucket> responseCallback) {
        LimitScanResultForBucket limitScanResultForBucket;
        TableMetricGroup tableMetrics = null;
        try {
            Replica replica = getReplicaOrException(tableBucket);
            tableMetrics = replica.tableMetrics();
            tableMetrics.totalLimitScanRequests().inc();
            if (replica.isKvTable()) {
                limitScanResultForBucket =
                        new LimitScanResultForBucket(tableBucket, replica.limitKvScan(limit));
            } else {
                limitScanResultForBucket =
                        new LimitScanResultForBucket(tableBucket, replica.limitLogScan(limit));
            }
        } catch (Exception e) {
            if (isUnexpectedException(e)) {
                LOG.error("Error limitScan records on replica {}", tableBucket, e);
                // NOTE: Failed put requests metric is not incremented for known exceptions
                // since it is supposed to indicate un-expected failure of a server in
                // handling a put request
                if (tableMetrics != null) {
                    tableMetrics.failedLimitScanRequests().inc();
                }
            }
            limitScanResultForBucket =
                    new LimitScanResultForBucket(tableBucket, ApiError.fromThrowable(e));
        }
        responseCallback.accept(limitScanResultForBucket);
    }

    public Map<TableBucket, LogReadResult> readFromLog(
            FetchParams fetchParams,
            Map<TableBucket, FetchReqInfo> bucketFetchInfo,
            @Nullable UserContext userContext) {
        Map<TableBucket, LogReadResult> logReadResult = new HashMap<>();
        boolean isFromFollower = fetchParams.isFromFollower();
        int limitBytes = fetchParams.maxFetchBytes();
        for (Map.Entry<TableBucket, FetchReqInfo> entry : bucketFetchInfo.entrySet()) {
            TableBucket tb = entry.getKey();
            TableMetricGroup tableMetrics = null;
            Replica replica = null;
            FetchReqInfo fetchReqInfo = entry.getValue();
            long fetchOffset = fetchReqInfo.getFetchOffset();
            int adjustedMaxBytes = Math.min(limitBytes, fetchReqInfo.getMaxBytes());
            try {
                replica = getReplicaOrException(tb);
                tableMetrics = replica.tableMetrics();
                tableMetrics.totalFetchLogRequests().inc();
                LOG.trace(
                        "Fetching log record for replica {}, offset {}",
                        tb,
                        fetchReqInfo.getFetchOffset());
                // todo: change here to modified project fields.
                if (fetchReqInfo.getProjectFields() != null
                        && replica.getLogFormat() != LogFormat.ARROW) {
                    throw new InvalidColumnProjectionException(
                            String.format(
                                    "Column projection is only supported for ARROW format, but the table %s is %s format.",
                                    replica.getTablePath(), replica.getLogFormat()));
                }

                fetchParams.setCurrentFetch(
                        tb.getTableId(),
                        fetchOffset,
                        adjustedMaxBytes,
                        replica.getSchemaGetter(),
                        replica.getArrowCompressionInfo(),
                        fetchReqInfo.getProjectFields(),
                        projectionsCache);
                LogReadInfo readInfo = replica.fetchRecords(fetchParams);

                // Once we read from a non-empty bucket, we stop ignoring request and bucket
                // level size limits.
                FetchDataInfo fetchedData = readInfo.getFetchedData();
                int recordBatchSize = fetchedData.getRecords().sizeInBytes();
                if (recordBatchSize > 0) {
                    fetchParams.markReadOneMessage();
                }
                limitBytes = Math.max(0, limitBytes - recordBatchSize);

                logReadResult.put(
                        tb,
                        new LogReadResult(
                                new FetchLogResultForBucket(
                                        tb, fetchedData.getRecords(), readInfo.getHighWatermark()),
                                fetchedData.getFetchOffsetMetadata()));

                // update metrics
                if (isFromFollower) {
                    serverMetricGroup.replicationBytesOut().inc(recordBatchSize);
                } else {
                    tableMetrics.incLogBytesOut(recordBatchSize);
                    userMetrics.incBytesOut(userContext, replica.getTablePath(), recordBatchSize);
                }
            } catch (Exception e) {
                if (isUnexpectedException(e)) {
                    LOG.error("Error processing log fetch operation on replica {}", tb, e);
                    // NOTE: Failed fetch requests metric is not incremented for known exceptions
                    // since it is supposed to indicate un-expected failure of a server in
                    // handling a fetch request
                    if (tableMetrics != null) {
                        tableMetrics.failedFetchLogRequests().inc();
                    }
                }

                FetchLogResultForBucket result;
                if (replica != null && e instanceof LogOffsetOutOfRangeException) {
                    result = handleFetchOutOfRangeException(replica, fetchOffset, e);
                } else {
                    result = new FetchLogResultForBucket(tb, ApiError.fromThrowable(e));
                }
                logReadResult.put(
                        tb, new LogReadResult(result, LogOffsetMetadata.UNKNOWN_OFFSET_METADATA));
            }
        }
        return logReadResult;
    }

    private FetchLogResultForBucket handleFetchOutOfRangeException(
            Replica replica, long fetchOffset, Exception e) {
        TableBucket tb = replica.getTableBucket();
        if (fetchOffset == FetchParams.FETCH_FROM_EARLIEST_OFFSET) {
            fetchOffset = replica.getLogStartOffset();
        }

        if (canFetchFromLakeLog(replica, fetchOffset)) {
            // todo: currently, we just return empty records directly
            // need to return the info of datalake to make client can fetch
            // from datalake directly
            return new FetchLogResultForBucket(
                    tb, MemoryLogRecords.EMPTY, replica.getLogHighWatermark());
        }
        // Once we get a fetch out of range exception from local storage, we need to check whether
        // the log segment already upload to the remote storage. If uploaded, we will return a list
        // of RemoteLogSegment. For client fetcher, it will fetch the log from remote in client.
        // For follower, it can update its local metadata to adjust the next fetch offset.
        else if (canFetchFromRemoteLog(replica, fetchOffset)) {
            RemoteLogFetchInfo remoteLogFetchInfo = fetchLogFromRemote(replica, fetchOffset);
            if (remoteLogFetchInfo != null) {
                return new FetchLogResultForBucket(
                        tb, remoteLogFetchInfo, replica.getLogHighWatermark());
            } else {
                return new FetchLogResultForBucket(
                        tb,
                        ApiError.fromThrowable(
                                new LogOffsetOutOfRangeException(
                                        String.format(
                                                "The fetch offset %s is out of range for table bucket %s",
                                                fetchOffset, tb))));
            }
        } else {
            return new FetchLogResultForBucket(tb, ApiError.fromThrowable(e));
        }
    }

    private boolean canFetchFromLakeLog(Replica replica, long fetchOffset) {
        return replica.getLogTablet().canFetchFromLakeLog(fetchOffset);
    }

    private boolean canFetchFromRemoteLog(Replica replica, long fetchOffset) {
        return replica.getLogTablet().canFetchFromRemoteLog(fetchOffset);
    }

    private @Nullable RemoteLogFetchInfo fetchLogFromRemote(Replica replica, long fetchOffset) {
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(replica.getTableBucket(), fetchOffset);
        if (!remoteLogSegmentList.isEmpty()) {
            int firstStartPos =
                    remoteLogManager.lookupPositionForOffset(
                            remoteLogSegmentList.get(0), fetchOffset);
            PhysicalTablePath physicalTablePath = replica.getPhysicalTablePath();
            FsPath remoteLogTabletDir =
                    FlussPaths.remoteLogTabletDir(
                            remoteLogManager.remoteLogDir(),
                            physicalTablePath,
                            replica.getTableBucket());
            return new RemoteLogFetchInfo(
                    remoteLogTabletDir.toString(),
                    physicalTablePath.getPartitionName(),
                    remoteLogSegmentList,
                    firstStartPos);
        } else {
            return null;
        }
    }

    /**
     * We don't print or increment metrics for known exceptions, like UnknownTableOrBucketException,
     * because they happen frequently before the table is created, and the exception is expected.
     *
     * @return true if the exception is unexpected and need to print and increment metrics.
     */
    private boolean isUnexpectedException(Exception e) {
        return !(e instanceof UnknownTableOrBucketException
                || e instanceof NotLeaderOrFollowerException
                || e instanceof LogOffsetOutOfRangeException);
    }

    /**
     * Start the high watermark check point thread to periodically flush the high watermark value
     * for all buckets to the high watermark checkpoint file.
     */
    private void startHighWatermarkCheckPointThread() {
        if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true)) {
            scheduler.schedule(
                    "highWatermark-checkpoint",
                    this::checkpointHighWatermarks,
                    0L,
                    conf.get(ConfigOptions.LOG_REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL)
                            .toMillis());
        }
    }

    /** Flushes the high watermark value for all buckets to the high watermark checkpoint file. */
    @VisibleForTesting
    void checkpointHighWatermarks() {
        List<Replica> onlineReplicasList = getOnlineReplicaList();
        Map<TableBucket, Long> highWatermarks = new HashMap<>();
        for (Replica replica : onlineReplicasList) {
            LogTablet logTablet = replica.getLogTablet();
            highWatermarks.put(logTablet.getTableBucket(), logTablet.getHighWatermark());
        }

        if (!highWatermarks.isEmpty()) {
            try {
                highWatermarkCheckpoint.write(highWatermarks);
            } catch (Exception e) {
                throw new LogStorageException("Error while writing to high watermark file", e);
            }
        }
    }

    /**
     * A list over all non-offline replicas. This is a weakly consistent list. A replica made
     * offline after the iterator has been constructed could still be included in the list.
     */
    private List<Replica> getOnlineReplicaList() {
        return allReplicas.values().stream()
                .filter(b -> b instanceof OnlineReplica)
                .map(b -> ((OnlineReplica) b).getReplica())
                .collect(Collectors.toList());
    }

    private <T extends WriteResultForBucket> void maybeAddDelayedWrite(
            int timeoutMs,
            int requiredAcks,
            int requestBucketSize,
            Map<TableBucket, T> writeResults,
            Consumer<List<T>> responseCallback) {
        if (delayedWriteRequired(requiredAcks, requestBucketSize, writeResults)) {
            Map<TableBucket, DelayedWrite.DelayedBucketStatus<T>> bucketStatusMap = new HashMap<>();
            writeResults.forEach(
                    (tb, result) ->
                            bucketStatusMap.put(
                                    tb,
                                    new DelayedWrite.DelayedBucketStatus<>(
                                            result.getWriteLogEndOffset(), result)));
            DelayedWrite<T> delayedWrite =
                    new DelayedWrite<>(
                            timeoutMs,
                            new DelayedWrite.DelayedWriteMetadata<>(requiredAcks, bucketStatusMap),
                            this,
                            responseCallback,
                            serverMetricGroup);

            // try to complete the request immediately, otherwise put it into the manager.
            // This is because while the delayed write operation is being created, new
            // requests may arrive and hence make this operation completable.
            delayedWriteManager.tryCompleteElseWatch(
                    delayedWrite,
                    bucketStatusMap.keySet().stream()
                            .map(DelayedTableBucketKey::new)
                            .collect(Collectors.toList()));
        } else {
            responseCallback.accept(new ArrayList<>(writeResults.values()));
        }
    }

    private void maybeAddDelayedFetchLog(
            FetchParams params,
            Map<TableBucket, FetchReqInfo> bucketFetchInfo,
            Map<TableBucket, LogReadResult> logReadResults,
            @Nullable UserContext userContext,
            Consumer<Map<TableBucket, FetchLogResultForBucket>> responseCallback) {
        long bytesReadable = 0;
        boolean errorReadingData = false;
        boolean hasFetchFromLocal = false;
        Map<TableBucket, FetchBucketStatus> fetchBucketStatusMap = new HashMap<>();
        Map<TableBucket, FetchLogResultForBucket> expectedErrorBuckets = new HashMap<>();
        for (Map.Entry<TableBucket, LogReadResult> logReadResultEntry : logReadResults.entrySet()) {
            TableBucket tb = logReadResultEntry.getKey();
            LogReadResult logReadResult = logReadResultEntry.getValue();
            FetchLogResultForBucket fetchLogResultForBucket =
                    logReadResult.getFetchLogResultForBucket();
            if (fetchLogResultForBucket.failed()) {
                // Check if this is an expected error (like NOT_LEADER_OR_FOLLOWER,
                // UNKNOWN_TABLE_OR_BUCKET) which should not
                // short-circuit the entire fetch request.
                Errors error = fetchLogResultForBucket.getError().error();
                if (isNonCriticalFetchError(error)) {
                    // Expected errors should not prevent other buckets from being delayed.
                    // Save the error bucket to be returned later, and continue processing others.
                    expectedErrorBuckets.put(tb, fetchLogResultForBucket);
                    continue;
                } else {
                    // Severe/unexpected error - short-circuit and return immediately
                    errorReadingData = true;
                    break;
                }
            }

            if (!fetchLogResultForBucket.fetchFromRemote()) {
                hasFetchFromLocal = true;
                bytesReadable += fetchLogResultForBucket.recordsOrEmpty().sizeInBytes();
            }

            fetchBucketStatusMap.put(
                    tb,
                    new FetchBucketStatus(
                            bucketFetchInfo.get(tb),
                            logReadResult.getLogOffsetMetadata(),
                            fetchLogResultForBucket));
        }

        if (!hasFetchFromLocal
                || params.maxWaitMs() <= 0
                || bucketFetchInfo.isEmpty()
                || bytesReadable >= params.minFetchBytes()
                || errorReadingData) {
            responseCallback.accept(
                    logReadResults.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    entry.getValue()
                                                            .getFetchLogResultForBucket())));
        } else {
            // need to put the fetch log request into delayed fetch log manager.
            DelayedFetchLog delayedFetchLog =
                    new DelayedFetchLog(
                            params,
                            this,
                            fetchBucketStatusMap,
                            delayedResponse -> {
                                // Merge expected error buckets with delayed response
                                delayedResponse.putAll(expectedErrorBuckets);
                                responseCallback.accept(delayedResponse);
                            },
                            serverMetricGroup,
                            userContext);

            // try to complete the request immediately, otherwise put it into the
            // delayedFetchLogManager; this is because while the delayed fetch log operation is
            // being created, new requests may arrive and hence make this operation completable.
            delayedFetchLogManager.tryCompleteElseWatch(
                    delayedFetchLog,
                    fetchBucketStatusMap.keySet().stream()
                            .map(DelayedTableBucketKey::new)
                            .collect(Collectors.toList()));
        }
    }

    /**
     * Check if the error is an expected fetch error that should not short-circuit the entire fetch
     * request. These errors are common during normal operations (e.g., leader changes, bucket
     * migrations) and should not prevent other buckets from being delayed.
     *
     * @param error the error to check
     * @return true if the error is expected and should not short-circuit
     */
    private boolean isNonCriticalFetchError(Errors error) {
        return error == Errors.NOT_LEADER_OR_FOLLOWER
                || error == Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION;
    }

    private void completeDelayedOperations(TableBucket tableBucket) {
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tableBucket);
        delayedWriteManager.checkAndComplete(delayedTableBucketKey);
        delayedFetchLogManager.checkAndComplete(delayedTableBucketKey);
    }

    /**
     * validate notify leader and isr data, if the data is invalid, throw exception. otherwise,
     * return the data is to become leader or not.
     */
    protected boolean validateAndGetIsBecomeLeader(NotifyLeaderAndIsrData data) {
        TableBucket tb = data.getTableBucket();
        Replica replica =
                maybeCreateReplica(data)
                        .orElseThrow(
                                () ->
                                        new StorageException(
                                                String.format(
                                                        "The replica %s for table bucket %s is offline while "
                                                                + "notify leader and isr",
                                                        serverId, tb)));
        int currentLeaderEpoch = replica.getLeaderEpoch();
        int requestLeaderEpoch = data.getLeaderEpoch();
        if (requestLeaderEpoch >= currentLeaderEpoch) {
            if (data.getReplicas().contains(serverId)) {
                int leaderId = data.getLeader();
                return leaderId == serverId;
            } else {
                String errorMessage =
                        String.format(
                                "ignore the notify leader and isr request for bucket %s as itself "
                                        + "is not in assigned replica list %s",
                                tb, data.getReplicas());
                LOG.warn(errorMessage);
                throw new UnknownTableOrBucketException(errorMessage);
            }
        } else {
            String errorMessage =
                    String.format(
                            "the leader epoch %s in request is smaller than the "
                                    + "current leader epoch %s for table bucket %s",
                            requestLeaderEpoch, currentLeaderEpoch, tb);
            LOG.warn("Ignore the notify leader and isr request because {}", errorMessage);
            throw new FencedLeaderEpochException(errorMessage);
        }
    }

    /**
     * If all the following conditions are true, we need to put a delayed write operation into the
     * delayed write manager and wait for replication to complete.
     *
     * <pre>
     *     1. requiredAcks = -1.
     *     2. there is data to append.
     *     3. at least one bucket append was successful.
     * </pre>
     */
    private boolean delayedWriteRequired(
            int requiredAcks,
            int inputBucketSize,
            Map<TableBucket, ? extends WriteResultForBucket> writeResults) {
        boolean needDelayedWrite = false;
        int failedBucketSize = 0;
        for (WriteResultForBucket result : writeResults.values()) {
            if (result.failed()) {
                failedBucketSize++;
            }
        }
        if (requiredAcks == -1 && inputBucketSize > 0 && failedBucketSize < inputBucketSize) {
            needDelayedWrite = true;
        }

        return needDelayedWrite;
    }

    private void maybeShrinkIsr() {
        LOG.trace(
                "Evaluating ISR list of buckets to see which replicas can be removed from the ISR list.");
        // Shrink ISRs for non-offline replicas.
        for (Replica replica : getOnlineReplicaList()) {
            replica.maybeShrinkIsr();
        }
    }

    /** Stop the given replica. */
    private StopReplicaResultForBucket stopReplica(
            TableBucket tb,
            boolean delete,
            Map<Long, Path> deletedTableIds,
            Map<Long, Path> deletedPartitionIds) {
        // First stop fetchers for this table bucket.
        replicaFetcherManager.removeFetcherForBuckets(Collections.singleton(tb));

        HostedReplica replica = getReplica(tb);
        if (replica instanceof OnlineReplica) {
            Replica replicaToDelete = ((OnlineReplica) replica).getReplica();
            if (delete) {
                if (allReplicas.remove(tb) != null) {
                    serverMetricGroup.removeTableBucketMetricGroup(
                            replicaToDelete.getPhysicalTablePath().getTablePath(), tb);
                    replicaToDelete.delete();
                    Path tabletParentDir = replicaToDelete.getTabletParentDir();
                    if (tb.getPartitionId() != null) {
                        deletedPartitionIds.put(tb.getPartitionId(), tabletParentDir);
                        deletedTableIds.put(tb.getTableId(), tabletParentDir.getParent());
                    } else {
                        deletedTableIds.put(tb.getTableId(), tabletParentDir);
                    }
                }
            }

            remoteLogManager.stopReplica(replicaToDelete, delete && replicaToDelete.isLeader());
            if (delete && replicaToDelete.isLeader()) {
                kvManager.deleteRemoteKvSnapshot(
                        replicaToDelete.getPhysicalTablePath(), replicaToDelete.getTableBucket());
            }
        }

        // If we were the leader, we may have some operations still waiting for completion.
        // We force completion to prevent them from timing out.
        completeDelayedOperations(tb);

        return new StopReplicaResultForBucket(tb);
    }

    private void truncateToHighWatermark(List<Replica> replicas) {
        for (Replica replica : replicas) {
            long highWatermark = replica.getLogTablet().getHighWatermark();
            LOG.info(
                    "Truncating the logEndOffset for replica id {} of table bucket {} to local "
                            + "highWatermark {} as it becomes the follower",
                    serverId,
                    replica.getTableBucket(),
                    highWatermark);
            replica.truncateTo(highWatermark);
        }
    }

    private void validateAndApplyCoordinatorEpoch(int requestCoordinatorEpoch, String requestName) {
        if (requestCoordinatorEpoch < this.coordinatorEpoch) {
            String errorMessage =
                    String.format(
                            "invalid coordinator epoch %s in %s request, "
                                    + "The latest known coordinator epoch is %s.",
                            requestCoordinatorEpoch, requestName, this.coordinatorEpoch);
            LOG.warn("Ignore the {} request because {}", requestName, errorMessage);
            throw new InvalidCoordinatorException(errorMessage);
        } else {
            this.coordinatorEpoch = requestCoordinatorEpoch;
        }
    }

    private void dropEmptyTableOrPartitionDir(Path dir, long id, String dirType) {
        if (!Files.exists(dir) || !isDirectoryEmpty(dir)) {
            return;
        }

        LOG.info("Drop empty {} dir '{}' of {} id {}.", dirType, dir, dirType, id);
        try {
            FileUtils.deleteDirectory(dir.toFile());
        } catch (Exception e) {
            LOG.error("Failed to delete empty {} dir '{}' of {} id {}.", dirType, dir, dirType, e);
        }
    }

    protected Optional<Replica> maybeCreateReplica(NotifyLeaderAndIsrData data) {
        Optional<Replica> replicaOpt = Optional.empty();
        try {
            TableBucket tb = data.getTableBucket();
            HostedReplica hostedReplica = getReplica(tb);
            if (hostedReplica instanceof NoneReplica) {
                PhysicalTablePath physicalTablePath = data.getPhysicalTablePath();
                TablePath tablePath = physicalTablePath.getTablePath();
                TableInfo tableInfo = getTableInfo(zkClient, tablePath);

                boolean isKvTable = tableInfo.hasPrimaryKey();
                BucketMetricGroup bucketMetricGroup =
                        serverMetricGroup.addTableBucketMetricGroup(
                                physicalTablePath, tb, isKvTable);
                Replica replica =
                        new Replica(
                                physicalTablePath,
                                tb,
                                logManager,
                                isKvTable ? kvManager : null,
                                conf.get(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME).toMillis(),
                                conf.get(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER),
                                serverId,
                                new OffsetCheckpointFile.LazyOffsetCheckpoints(
                                        highWatermarkCheckpoint),
                                delayedWriteManager,
                                delayedFetchLogManager,
                                adjustIsrManager,
                                kvSnapshotContext,
                                metadataCache,
                                fatalErrorHandler,
                                bucketMetricGroup,
                                tableInfo,
                                clock);
                allReplicas.put(tb, new OnlineReplica(replica));
                replicaOpt = Optional.of(replica);
            } else if (hostedReplica instanceof OnlineReplica) {
                replicaOpt = Optional.of(((OnlineReplica) hostedReplica).getReplica());
            } else if (hostedReplica instanceof OfflineReplica) {
                LOG.warn("Unable to get the Replica {} while it is offline", tb);
            }
        } catch (Exception e) {
            LOG.error("Error while checking and creating replica", e);
        }

        return replicaOpt;
    }

    public Replica getReplicaOrException(TableBucket tableBucket) {
        HostedReplica replica = getReplica(tableBucket);
        if (replica instanceof OnlineReplica) {
            return ((OnlineReplica) replica).getReplica();
        } else if (replica instanceof OfflineReplica) {
            throw new StorageException(tableBucket + " is offline on server " + serverId);
        } else if ((replica instanceof NoneReplica) && metadataCache.contains(tableBucket)) {
            throw new NotLeaderOrFollowerException(
                    "server " + serverId + " is not leader or follower for " + tableBucket);
        } else {
            throw new UnknownTableOrBucketException("Unknown table or bucket: " + tableBucket);
        }
    }

    public HostedReplica getReplica(TableBucket tableBucket) {
        return allReplicas.getOrDefault(tableBucket, new NoneReplica());
    }

    private boolean isRequiredAcksInvalid(int requiredAcks) {
        return requiredAcks != 0 && requiredAcks != 1 && requiredAcks != -1;
    }

    @VisibleForTesting
    public DelayedOperationManager<DelayedWrite<?>> getDelayedWriteManager() {
        return delayedWriteManager;
    }

    @VisibleForTesting
    public DelayedOperationManager<DelayedFetchLog> getDelayedFetchLogManager() {
        return delayedFetchLogManager;
    }

    @VisibleForTesting
    public AdjustIsrManager getAdjustIsrManager() {
        return adjustIsrManager;
    }

    @VisibleForTesting
    public ReplicaFetcherManager getReplicaFetcherManager() {
        return replicaFetcherManager;
    }

    public TabletServerMetricGroup getServerMetricGroup() {
        return serverMetricGroup;
    }

    /**
     * Interface to represent the state of hosted {@link Replica}. We create a concrete (active)
     * {@link Replica} instance when the TabletServer receives a createLogLeader request or
     * createFollower request from the Coordinator server.
     */
    public interface HostedReplica {}

    /** This TabletServer does not have any state for this {@link Replica} locally. */
    public static final class NoneReplica implements HostedReplica {}

    /** This TabletServer hosts the {@link Replica} and it is online. */
    public static final class OnlineReplica implements HostedReplica {
        private final Replica replica;

        public OnlineReplica(Replica replica) {
            this.replica = replica;
        }

        public Replica getReplica() {
            return replica;
        }
    }

    /** This TabletServer hosts the {@link Replica}, but it is in an offline log directory. */
    public static final class OfflineReplica implements HostedReplica {}

    public void shutdown() throws InterruptedException {
        // Close the resources for snapshot kv
        kvSnapshotResource.close();
        replicaFetcherManager.shutdown();
        delayedWriteManager.shutdown();
        delayedFetchLogManager.shutdown();

        // Checkpoint highWatermark.
        checkpointHighWatermarks();
    }

    /** The result of reading log. */
    public static final class LogReadResult {
        private final FetchLogResultForBucket fetchLogResultForBucket;
        private final LogOffsetMetadata logOffsetMetadata;

        public LogReadResult(
                FetchLogResultForBucket fetchLogResultForBucket,
                LogOffsetMetadata logOffsetMetadata) {
            this.fetchLogResultForBucket = fetchLogResultForBucket;
            this.logOffsetMetadata = logOffsetMetadata;
        }

        public FetchLogResultForBucket getFetchLogResultForBucket() {
            return fetchLogResultForBucket;
        }

        public LogOffsetMetadata getLogOffsetMetadata() {
            return logOffsetMetadata;
        }
    }
}
