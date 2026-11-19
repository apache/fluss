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

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.client.tiering.TieringSplitGenerator;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.flink.metrics.FlinkMetricRegistry;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.TieringReachMaxDurationEvent;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.flink.tiering.source.state.TieringSourceEnumeratorState;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.TieringTableValidator;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbHeartbeatReqForTable;
import org.apache.fluss.rpc.messages.PbLakeTieringStats;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.basicHeartBeat;
import static org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.heartBeatWithRequestNewTieringTable;
import static org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.tieringTableHeartBeat;
import static org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.waitHeartbeatResponse;

/**
 * An implementation of {@link SplitEnumerator} used to request {@link TieringSplit} from Fluss
 * Cluster.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Register the Tiering Service job that the current TieringSourceEnumerator belongs to with
 *       the Fluss Cluster when the Flink Tiering job starts up.
 *   <li>Request Fluss table splits from Fluss Cluster and assigns to SourceReader to tier.
 *   <li>Un-Register the Tiering Service job that the current TieringSourceEnumerator belongs to
 *       with the Fluss Cluster when the Flink Tiering job shutdown as much as possible.
 * </ul>
 */
public class TieringSourceEnumerator
        implements SplitEnumerator<FlinkTieringSplit, TieringSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSourceEnumerator.class);

    /** Delay between claims when draining consecutive empty tables, to pace the claim RPC rate. */
    private static final long EMPTY_TABLE_POLL_DELAY_MS = 1000L;

    /**
     * KV snapshot lease duration for the whole tiering job. One lease covers the entire job
     * lifecycle; it is renewed implicitly by every {@code acquireSnapshots} call, so a relatively
     * long duration is safe and also bounds the worst-case leaked-lease lifetime if the job dies
     * abnormally. Tiering rounds are typically minute-level today so a 6-hour lease is more than
     * sufficient for most cases.
     */
    private static final long KV_SNAPSHOT_LEASE_DURATION_MS = Duration.ofHours(6).toMillis();

    private final Configuration flussConf;
    private final Configuration lakeTieringConfig;
    private final SplitEnumeratorContext<FlinkTieringSplit> context;
    private final LakeTieringFactory<?, ?> lakeTieringFactory;
    private final ScheduledExecutorService timerService;
    private final SplitEnumeratorMetricGroup enumeratorMetricGroup;
    private final AutoCloseable tieringMetrics;
    private final long pollTieringTableIntervalMs;
    // Also inspected by asynchronous heartbeat requests.
    private final Deque<TieringSplit> pendingSplits;
    private final Set<Integer> readersAwaitingSplit;

    private final Map<Long, Long> tieringTableEpochs;
    private final Map<Long, Long> failedTableEpochs;
    private final Map<Long, TieringFinishInfo> finishedTables;
    private final Set<Long> tieringReachMaxDurationsTables;

    /**
     * Buckets whose kv snapshots are currently held under the lease, grouped by tableId. Used to
     * release the correct bucket subset when a table finishes/fails or when a failover happens.
     */
    private final Map<Long, Set<TableBucket>> leasedBucketsByTable;

    /**
     * A unique lease id for this tiering job. Reused across all tables to keep a single renewal and
     * bookkeeping entry on the server side.
     */
    private final String kvSnapshotLeaseId;

    // lazily instantiated
    private RpcClient rpcClient;
    private CoordinatorGateway coordinatorGateway;
    private Connection connection;
    private Admin flussAdmin;
    private TieringSplitGenerator splitGenerator;
    private int flussCoordinatorEpoch;

    private volatile boolean isFailOvering = false;

    private volatile boolean closed = false;

    /** Tracks a pending delayed poll, but not an in-flight request. */
    private boolean delayedPollScheduled = false;

    public TieringSourceEnumerator(
            Configuration flussConf,
            SplitEnumeratorContext<FlinkTieringSplit> context,
            LakeTieringFactory<?, ?> lakeTieringFactory,
            long pollTieringTableIntervalMs) {
        this(
                flussConf,
                new Configuration(),
                context,
                lakeTieringFactory,
                pollTieringTableIntervalMs);
    }

    public TieringSourceEnumerator(
            Configuration flussConf,
            Configuration lakeTieringConfig,
            SplitEnumeratorContext<FlinkTieringSplit> context,
            LakeTieringFactory<?, ?> lakeTieringFactory,
            long pollTieringTableIntervalMs) {
        this(
                flussConf,
                lakeTieringConfig,
                context,
                lakeTieringFactory,
                pollTieringTableIntervalMs,
                Executors.newSingleThreadScheduledExecutor(
                        r -> new Thread(r, "Tiering-Timer-Thread")));
    }

    @VisibleForTesting
    TieringSourceEnumerator(
            Configuration flussConf,
            Configuration lakeTieringConfig,
            SplitEnumeratorContext<FlinkTieringSplit> context,
            LakeTieringFactory<?, ?> lakeTieringFactory,
            long pollTieringTableIntervalMs,
            ScheduledExecutorService timerService) {
        this.flussConf = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
        this.context = context;
        this.lakeTieringFactory = lakeTieringFactory;
        this.timerService = timerService;
        this.enumeratorMetricGroup = context.metricGroup();
        this.pollTieringTableIntervalMs = pollTieringTableIntervalMs;
        this.pendingSplits = new ConcurrentLinkedDeque<>();
        this.readersAwaitingSplit = Collections.synchronizedSet(new TreeSet<>());
        this.tieringTableEpochs = new ConcurrentHashMap<>();
        this.finishedTables = new ConcurrentHashMap<>();
        this.failedTableEpochs = new ConcurrentHashMap<>();
        this.tieringReachMaxDurationsTables = Collections.synchronizedSet(new TreeSet<>());
        this.leasedBucketsByTable = new ConcurrentHashMap<>();
        this.kvSnapshotLeaseId = "tiering-" + UUID.randomUUID();
        this.tieringMetrics =
                TieringMetrics.registerReadersAwaitingSplitRatio(
                        enumeratorMetricGroup,
                        () -> (double) readersAwaitingSplit.size() / context.currentParallelism());
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConf);
        Admin admin = connection.getAdmin();
        FlinkMetricRegistry metricRegistry = new FlinkMetricRegistry(enumeratorMetricGroup);
        ClientMetricGroup clientMetricGroup =
                new ClientMetricGroup(metricRegistry, "LakeTieringService");
        this.rpcClient = RpcClient.create(flussConf, clientMetricGroup);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        CoordinatorGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        start(gateway, admin, new TieringSplitGenerator(admin));
    }

    @VisibleForTesting
    void start(
            CoordinatorGateway coordinatorGateway,
            Admin flussAdmin,
            TieringSplitGenerator splitGenerator) {
        this.coordinatorGateway = coordinatorGateway;
        this.flussAdmin = flussAdmin;
        this.splitGenerator = splitGenerator;

        LOG.info("Starting register Tiering Service to Fluss Coordinator...");
        try {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(basicHeartBeat()));
            this.flussCoordinatorEpoch = heartbeatResponse.getCoordinatorEpoch();
            LOG.info(
                    "Register Tiering Service to Fluss Coordinator(epoch={}) success.",
                    flussCoordinatorEpoch);

        } catch (Exception e) {
            LOG.error("Register Tiering Service failed due to ", e);
            throw new FlinkRuntimeException("Register Tiering Service failed due to ", e);
        }
        this.context.callAsync(
                this::requestTieringTableSplitsViaHeartBeat,
                this::generateAndAssignSplits,
                0,
                pollTieringTableIntervalMs);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader may be failed, skip this request.
            return;
        }
        LOG.info("TieringSourceReader {} requests split.", subtaskId);
        readersAwaitingSplit.add(subtaskId);

        // Skip the synchronous split request during failover. Otherwise we may immediately
        // re-acquire a kv snapshot lease that failover handling just released. The periodic
        // poll resumes split generation once failover completes.
        if (isFailOvering) {
            return;
        }

        // If pending splits exist, assign them directly to the requesting reader
        if (!pendingSplits.isEmpty()) {
            assignSplits();
        } else {
            // Note: Ideally, only one table should be tiering at a time.
            // Here we block to request a tiering table synchronously to avoid multiple threads
            // requesting tiering tables concurrently, which would cause the enumerator to contain
            // multiple tiering tables simultaneously. This is not optimal for tiering performance.
            Tuple3<Long, Long, TablePath> tieringTable = null;
            Throwable throwable = null;
            try {
                tieringTable = this.requestTieringTableSplitsViaHeartBeat();
            } catch (Throwable t) {
                throwable = t;
            }
            this.generateAndAssignSplits(tieringTable, throwable);
        }
    }

    @Override
    public void addSplitsBack(List<FlinkTieringSplit> splits, int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
        splits.forEach(split -> pendingSplits.addLast(split.unwrap()));
        assignSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader: {} to Tiering Source enumerator.", subtaskId);
        Map<Integer, ReaderInfo> readerByAttempt =
                context.registeredReadersOfAttempts().get(subtaskId);
        if (readerByAttempt != null && !readerByAttempt.isEmpty()) {
            readersAwaitingSplit.add(subtaskId);
            int maxAttempt = max(readerByAttempt.keySet());
            if (maxAttempt >= 1) {
                if (isFailOvering) {
                    LOG.warn(
                            "Subtask {} (max attempt {}) registered during ongoing failover.",
                            subtaskId,
                            maxAttempt);
                } else {
                    LOG.warn(
                            "Detected failover: subtask {} has max attempt {} > 0. Triggering global failover handling.",
                            subtaskId,
                            maxAttempt);
                    // should be failover
                    isFailOvering = true;
                    handleSourceReaderFailOver();
                }

                // if registered readers equal to current parallelism, check whether all registered
                // readers have same max attempt
                if (context.registeredReadersOfAttempts().size() == context.currentParallelism()) {
                    // Check if all readers have the same max attempt number
                    Set<Integer> maxAttempts =
                            context.registeredReadersOfAttempts().values().stream()
                                    .map(_readerByAttempt -> max(_readerByAttempt.keySet()))
                                    .collect(Collectors.toSet());
                    int globalMaxAttempt = max(maxAttempts);
                    if (maxAttempts.size() == 1 && globalMaxAttempt >= 1) {
                        LOG.info(
                                "Failover completed. All {} subtasks reached the same attempt number {}. Current registered readers are {}",
                                context.currentParallelism(),
                                globalMaxAttempt,
                                context.registeredReadersOfAttempts());
                        // Defer clearing isFailOvering until the coordinator thread runs, so a
                        // synchronous handleSplitRequest right after addReader still skips the
                        // fetch path and does not re-acquire the lease just released.
                        context.runInCoordinatorThread(
                                () -> {
                                    isFailOvering = false;
                                });
                    }
                }
            }
        }
    }

    private int max(Set<Integer> integers) {
        return integers.stream().max(Integer::compareTo).orElse(-1);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedTieringEvent) {
            FinishedTieringEvent finishedTieringEvent = (FinishedTieringEvent) sourceEvent;
            long finishedTableId = finishedTieringEvent.getTableId();
            Long tieringEpoch = tieringTableEpochs.remove(finishedTableId);
            LOG.info("Got FinishedTieringEvent for tiering table {}. ", finishedTableId);
            if (tieringEpoch == null) {
                // shouldn't happen, warn it
                LOG.warn(
                        "The finished table {} is not in tiering table, won't report it to Fluss to mark as finished.",
                        finishedTableId);
            } else {
                boolean isForceFinished = tieringReachMaxDurationsTables.remove(finishedTableId);
                finishedTables.put(
                        finishedTableId,
                        TieringFinishInfo.from(
                                tieringEpoch, isForceFinished, finishedTieringEvent.getStats()));
            }
            // release the kv snapshot lease held for this table (if any).
            maybeReleaseKvSnapshotLease(finishedTableId);
        }

        if (sourceEvent instanceof FailedTieringEvent) {
            FailedTieringEvent failedEvent = (FailedTieringEvent) sourceEvent;
            long failedTableId = failedEvent.getTableId();
            Long tieringEpoch = tieringTableEpochs.remove(failedTableId);
            LOG.info(
                    "Tiering table {} is failed, fail reason is {}.",
                    failedTableId,
                    failedEvent.failReason());
            if (tieringEpoch == null) {
                // shouldn't happen, warn it
                LOG.warn(
                        "The failed table {} is not in tiering table, won't report it to Fluss to mark as failed.",
                        failedTableId);
            } else {
                failedTableEpochs.put(failedTableId, tieringEpoch);
            }
            // release the kv snapshot lease held for this table (if any).
            maybeReleaseKvSnapshotLease(failedTableId);
        }

        if (!finishedTables.isEmpty() || !failedTableEpochs.isEmpty()) {
            // call one round of heartbeat to notify table has been finished or failed
            LOG.info("Finished tiering table {}.", finishedTables);
            requestTableAndAssign(0);
        }
    }

    private void handleSourceReaderFailOver() {
        LOG.info(
                "Handling source reader fail over, mark current tiering table epoch {} as failed.",
                tieringTableEpochs);
        // we need to make all as failed
        failedTableEpochs.putAll(new HashMap<>(tieringTableEpochs));
        tieringTableEpochs.clear();
        tieringReachMaxDurationsTables.clear();
        // also clean all pending splits since we mark all as failed
        pendingSplits.clear();
        // Release leases for tables marked failed during failover so server-side snapshot
        // references are not held until the lease expires.
        for (Long tableId : new HashSet<>(leasedBucketsByTable.keySet())) {
            maybeReleaseKvSnapshotLease(tableId);
        }
        if (!failedTableEpochs.isEmpty()) {
            // call one round of heartbeat to notify table has been finished or failed
            requestTableAndAssign(0);
        }
    }

    @VisibleForTesting
    protected void handleTableTieringReachMaxDuration(
            TablePath tablePath, long tableId, long tieringEpoch) {
        Long currentEpoch = tieringTableEpochs.get(tableId);
        if (currentEpoch != null && currentEpoch.equals(tieringEpoch)) {
            LOG.info("Table {}-{} reached max duration. Force completing.", tablePath, tableId);
            tieringReachMaxDurationsTables.add(tableId);

            for (TieringSplit tieringSplit : pendingSplits) {
                if (tieringSplit.getTableBucket().getTableId() == tableId) {
                    // mark this tiering split to skip the current round since the tiering for
                    // this table has timed out, so the tiering source reader can skip them directly
                    tieringSplit.skipCurrentRound();
                }
            }

            // broadcast the tiering reach max duration event to all readers,
            // we broadcast all for simplicity
            Set<Integer> readers = new HashSet<>(context.registeredReaders().keySet());
            for (int reader : readers) {
                TieringReachMaxDurationEvent tieringReachMaxDurationEvent =
                        new TieringReachMaxDurationEvent(tableId);
                LOG.info("Send {} to reader {}", tieringReachMaxDurationEvent, reader);
                context.sendEventToSourceReader(reader, tieringReachMaxDurationEvent);
            }
        }
    }

    @VisibleForTesting
    void generateAndAssignSplits(
            @Nullable Tuple3<Long, Long, TablePath> tieringTable, Throwable throwable) {
        if (throwable != null) {
            ExceptionUtils.rethrow(throwable);
        }
        if (tieringTable != null) {
            generateTieringSplits(tieringTable);
        }
        assignSplits();
    }

    private void assignSplits() {
        // we don't assign splits during failover
        if (isFailOvering) {
            return;
        }
        if (!readersAwaitingSplit.isEmpty()) {
            final Integer[] readers = readersAwaitingSplit.toArray(new Integer[0]);
            for (Integer nextAwaitingReader : readers) {
                if (!context.registeredReaders().containsKey(nextAwaitingReader)) {
                    readersAwaitingSplit.remove(nextAwaitingReader);
                    continue;
                }
                TieringSplit tieringSplit = pendingSplits.pollFirst();
                if (tieringSplit != null) {
                    context.assignSplit(new FlinkTieringSplit(tieringSplit), nextAwaitingReader);
                    LOG.info("Assigning split {} to readers {}", tieringSplit, nextAwaitingReader);
                    readersAwaitingSplit.remove(nextAwaitingReader);
                }
            }
        }
    }

    private @Nullable Tuple3<Long, Long, TablePath> requestTieringTableSplitsViaHeartBeat() {
        if (closed) {
            return null;
        }
        Map<Long, TieringFinishInfo> currentFinishedTables = new HashMap<>(this.finishedTables);
        Map<Long, Long> currentFailedTableEpochs = new HashMap<>(this.failedTableEpochs);
        LakeTieringHeartbeatRequest tieringHeartbeatRequest =
                tieringTableHeartBeat(
                        basicHeartBeat(),
                        this.tieringTableEpochs,
                        currentFinishedTables,
                        currentFailedTableEpochs,
                        this.flussCoordinatorEpoch);

        Tuple3<Long, Long, TablePath> lakeTieringInfo = null;
        // report heartbeat with request table to fluss coordinator
        LOG.info(
                "currentFinishedTables: {}, currentFailedTableEpochs: {}, tieringTableEpochs: {}",
                currentFinishedTables,
                currentFailedTableEpochs,
                tieringTableEpochs);

        if (pendingSplits.isEmpty() && !readersAwaitingSplit.isEmpty()) {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    heartBeatWithRequestNewTieringTable(tieringHeartbeatRequest)));
            if (heartbeatResponse.hasTieringTable()) {
                PbLakeTieringTableInfo tieringTable = heartbeatResponse.getTieringTable();
                lakeTieringInfo =
                        Tuple3.of(
                                tieringTable.getTableId(),
                                tieringTable.getTieringEpoch(),
                                TablePath.of(
                                        tieringTable.getTablePath().getDatabaseName(),
                                        tieringTable.getTablePath().getTableName()));
                tieringTableEpochs.put(lakeTieringInfo.f0, lakeTieringInfo.f1);
                LOG.info("Tiering table {} has been requested.", lakeTieringInfo);
            } else {
                LOG.info("No available Tiering table found, will poll later.");
            }
        } else {
            // report heartbeat to fluss coordinator
            waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(tieringHeartbeatRequest));
        }

        // if come to here, we can remove currentFinishedTables/failedTableEpochs to avoid send
        // in next round
        currentFinishedTables.forEach(finishedTables::remove);
        currentFailedTableEpochs.forEach(failedTableEpochs::remove);
        return lakeTieringInfo;
    }

    private void generateTieringSplits(Tuple3<Long, Long, TablePath> tieringTable)
            throws FlinkRuntimeException {
        if (tieringTable == null) {
            return;
        }
        long start = System.currentTimeMillis();
        LOG.info("Generate Tiering splits for table {}.", tieringTable.f2);
        try {
            TablePath tablePath = tieringTable.f2;
            final TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
            if (lakeTieringFactory instanceof TieringTableValidator) {
                ((TieringTableValidator) lakeTieringFactory).validateTable(tableInfo);
            }
            List<TieringSplit> tieringSplits = splitGenerator.generateTableSplits(tableInfo);
            if (tieringSplits.isEmpty()
                    && lakeTieringConfig.get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                    && lakeTieringFactory.supportsPartitionMarkDone()
                    && tableInfo.isPartitioned()) {
                // Let the committer check the lake table options without lake I/O here.
                tieringSplits = new ArrayList<>();
                tieringSplits.add(
                        new TieringLogSplit(
                                tablePath,
                                new TableBucket(tableInfo.getTableId(), 0),
                                null,
                                0L,
                                0L,
                                1,
                                true));
            }
            // shuffle tiering split to avoid splits tiering skew
            // after introduce tiering max duration
            Collections.shuffle(tieringSplits);
            List<TieringSplit> splitsWithMetadata = populateTieringRoundMetadata(tieringSplits);
            LOG.info(
                    "Generate Tiering {} splits for table {} with cost {}ms.",
                    splitsWithMetadata.size(),
                    tieringTable.f2,
                    System.currentTimeMillis() - start);
            if (splitsWithMetadata.isEmpty()) {
                LOG.info(
                        "Generate Tiering splits for table {} is empty, no need to tier data.",
                        tieringTable.f2.getTableName());
                tieringTableEpochs.remove(tieringTable.f0);
                finishedTables.put(tieringTable.f0, TieringFinishInfo.from(tieringTable.f1));
                // An empty round has no split or completion event to drive the next table.
                requestTableAndAssign(EMPTY_TABLE_POLL_DELAY_MS);
            } else {
                // Acquire a kv snapshot lease before the splits are assigned, so the server does
                // not garbage-collect snapshots the tiering job still needs.
                maybeAcquireKvSnapshotLease(tieringTable.f0, splitsWithMetadata);
                pendingSplits.addAll(splitsWithMetadata);

                timerService.schedule(
                        () ->
                                context.runInCoordinatorThread(
                                        () ->
                                                handleTableTieringReachMaxDuration(
                                                        tablePath,
                                                        tieringTable.f0,
                                                        tieringTable.f1)),

                        // for simplicity, we use the freshness as
                        tableInfo.getTableConfig().getDataLakeFreshness().toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOG.warn("Fail to generate Tiering splits for table {}.", tieringTable.f2, e);
            failedTableEpochs.put(tieringTable.f0, tieringTable.f1);
            tieringTableEpochs.remove(tieringTable.f0);
            // Release any lease acquired before the failure so snapshots are not held until expiry.
            maybeReleaseKvSnapshotLease(tieringTable.f0);
        }
    }

    /** Requests a tiering table and assigns its splits, immediately or after a delay. */
    private void requestTableAndAssign(long delayMs) {
        if (closed) {
            return;
        }
        if (delayMs == 0) {
            context.callAsync(
                    this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
            return;
        }
        if (delayedPollScheduled) {
            return;
        }
        delayedPollScheduled = true;
        timerService.schedule(
                () ->
                        context.runInCoordinatorThread(
                                () -> {
                                    delayedPollScheduled = false;
                                    if (!isFailOvering) {
                                        requestTableAndAssign(0);
                                    }
                                }),
                delayMs,
                TimeUnit.MILLISECONDS);
    }

    private List<TieringSplit> populateTieringRoundMetadata(List<TieringSplit> tieringSplits) {
        int numberOfSplits = tieringSplits.size();
        if (numberOfSplits == 0) {
            return Collections.emptyList();
        }
        long tieringRoundTimestamp = System.currentTimeMillis();
        List<TieringSplit> splitsWithMetadata = new ArrayList<>(numberOfSplits);
        for (int splitIndex = 0; splitIndex < numberOfSplits; splitIndex++) {
            splitsWithMetadata.add(
                    tieringSplits
                            .get(splitIndex)
                            .copy(numberOfSplits, splitIndex, tieringRoundTimestamp));
        }
        return splitsWithMetadata;
    }

    @Override
    public TieringSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        // do nothing, the downstream lake committer will snapshot the state to Fluss Cluster
        return new TieringSourceEnumeratorState();
    }

    @Override
    public void close() throws IOException {
        // Repeated lifecycle calls are a no-op: the final report is attempted once and the
        // resources are released once.
        if (closed) {
            return;
        }
        closed = true;
        timerService.shutdownNow();
        try {
            if (coordinatorGateway != null) {
                // The finished tables have to be flushed here: a table that produced no splits is
                // removed from the in-flight epochs as soon as it is marked as finished, so it is
                // neither part of tieringTableEpochs nor of failedTableEpochs at close time.
                // Without flushing it explicitly, its completion would never reach the coordinator
                // and the table remains in the Tiering state until the tiering service timeout
                // expires. Tables that are still in flight are reported as failed so that the
                // coordinator reschedules them.
                Map<Long, TieringFinishInfo> currentFinishedTables = new HashMap<>(finishedTables);
                Map<Long, Long> currentFailedTableEpochs = new HashMap<>(failedTableEpochs);
                currentFailedTableEpochs.putAll(tieringTableEpochs);
                if (!currentFinishedTables.isEmpty() || !currentFailedTableEpochs.isEmpty()) {
                    waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    tieringTableHeartBeat(
                                            basicHeartBeat(),
                                            Collections.emptyMap(),
                                            currentFinishedTables,
                                            currentFailedTableEpochs,
                                            flussCoordinatorEpoch)));
                }
            }
        } finally {
            for (Long tableId : new HashSet<>(leasedBucketsByTable.keySet())) {
                try {
                    maybeReleaseKvSnapshotLease(tableId);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to release kv snapshot lease for table {} on close.",
                            tableId,
                            e);
                }
            }
            IOUtils.closeQuietly(tieringMetrics, "tiering metrics");
            IOUtils.closeQuietly(rpcClient, "tiering RPC client");
            IOUtils.closeQuietly(flussAdmin, "Fluss admin client");
            IOUtils.closeQuietly(connection, "Fluss connection");
        }
    }

    /**
     * Acquire kv snapshot lease for all {@link TieringSnapshotSplit}s of the given table so that
     * snapshots referenced by these splits will not be cleaned up by the Fluss server during
     * tiering. Bucket-snapshot mappings are remembered in {@link #leasedBucketsByTable} so they can
     * be released on finish/fail.
     *
     * <p>Falls back to a warning (no exception thrown) when the server does not support the kv
     * snapshot lease API, to preserve compatibility with older Fluss clusters.
     */
    private void maybeAcquireKvSnapshotLease(long tableId, List<TieringSplit> tieringSplits) {
        Map<TableBucket, Long> bucketsToLease = new HashMap<>();
        for (TieringSplit split : tieringSplits) {
            if (split.isTieringSnapshotSplit()) {
                TieringSnapshotSplit snapshotSplit = split.asTieringSnapshotSplit();
                bucketsToLease.put(snapshotSplit.getTableBucket(), snapshotSplit.getSnapshotId());
            }
        }
        if (bucketsToLease.isEmpty()) {
            return;
        }
        LOG.info(
                "Try to acquire kv snapshot lease {} for tiering table {} with {} buckets.",
                kvSnapshotLeaseId,
                tableId,
                bucketsToLease.size());
        try {
            Set<TableBucket> unavailableBuckets =
                    flussAdmin
                            .createKvSnapshotLease(kvSnapshotLeaseId, KV_SNAPSHOT_LEASE_DURATION_MS)
                            .acquireSnapshots(bucketsToLease)
                            .get()
                            .getUnavailableTableBucketSet();
            // Only record successfully leased buckets so we don't later try to release
            // buckets that were never actually acquired.
            Set<TableBucket> acquiredBuckets = new HashSet<>(bucketsToLease.keySet());
            if (!unavailableBuckets.isEmpty()) {
                LOG.warn(
                        "Failed to acquire kv snapshot lease for {} of {} buckets of tiering "
                                + "table {}: {}. The corresponding snapshots may have already "
                                + "been garbage-collected; tiering for those buckets may fail "
                                + "later when the snapshots are accessed.",
                        unavailableBuckets.size(),
                        bucketsToLease.size(),
                        tableId,
                        unavailableBuckets);
                acquiredBuckets.removeAll(unavailableBuckets);
            }
            if (!acquiredBuckets.isEmpty()) {
                leasedBucketsByTable
                        .computeIfAbsent(tableId, k -> ConcurrentHashMap.newKeySet())
                        .addAll(acquiredBuckets);
            }
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class).isPresent()) {
                LOG.warn(
                        "Failed to acquire kv snapshot lease for tiering table {} because the "
                                + "server does not support kv snapshot lease API. Snapshots may "
                                + "be cleaned up earlier than expected. Please upgrade the Fluss "
                                + "server to version 0.9 or later.",
                        tableId,
                        e);
            } else {
                LOG.error(
                        "Failed to acquire kv snapshot lease for tiering table {}. "
                                + "Tiering will proceed without snapshot protection; the "
                                + "snapshot may be garbage-collected while tiering is in progress.",
                        tableId,
                        e);
            }
        }
    }

    /**
     * Release the kv snapshot lease held for a specific table. Called when a table finishes
     * tiering, fails, or is abandoned due to failover. Missing leases (log-only tables, or tables
     * for which acquire failed) are handled as no-ops.
     */
    private void maybeReleaseKvSnapshotLease(long tableId) {
        Set<TableBucket> bucketsToRelease = leasedBucketsByTable.remove(tableId);
        if (bucketsToRelease == null || bucketsToRelease.isEmpty()) {
            return;
        }
        LOG.info(
                "Try to release kv snapshot lease {} for tiering table {} with {} buckets.",
                kvSnapshotLeaseId,
                tableId,
                bucketsToRelease.size());
        try {
            flussAdmin
                    .createKvSnapshotLease(kvSnapshotLeaseId, KV_SNAPSHOT_LEASE_DURATION_MS)
                    .releaseSnapshots(bucketsToRelease)
                    .get();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class).isPresent()) {
                LOG.warn(
                        "Failed to release kv snapshot lease for tiering table {} because the "
                                + "server does not support kv snapshot lease API.",
                        tableId,
                        e);
            } else {
                LOG.error(
                        "Failed to release kv snapshot lease for tiering table {}. The lease "
                                + "will expire naturally on the server side.",
                        tableId,
                        e);
            }
        }
    }

    @VisibleForTesting
    String getKvSnapshotLeaseId() {
        return kvSnapshotLeaseId;
    }

    @VisibleForTesting
    Map<Long, Set<TableBucket>> getLeasedBucketsByTable() {
        return leasedBucketsByTable;
    }

    /** A helper class to build heartbeat request. */
    @VisibleForTesting
    static class HeartBeatHelper {

        static LakeTieringHeartbeatRequest basicHeartBeat() {
            return new LakeTieringHeartbeatRequest();
        }

        static LakeTieringHeartbeatRequest heartBeatWithRequestNewTieringTable(
                LakeTieringHeartbeatRequest heartbeatRequest) {
            heartbeatRequest.setRequestTable(true);
            return heartbeatRequest;
        }

        static LakeTieringHeartbeatRequest tieringTableHeartBeat(
                LakeTieringHeartbeatRequest heartbeatRequest,
                Map<Long, Long> tieringTableEpochs,
                Map<Long, TieringFinishInfo> finishedTables,
                Map<Long, Long> failedTableEpochs,
                int coordinatorEpoch) {
            if (!tieringTableEpochs.isEmpty()) {
                heartbeatRequest.addAllTieringTables(
                        toPbHeartbeatReqForTable(tieringTableEpochs, coordinatorEpoch));
            }
            if (!finishedTables.isEmpty()) {
                Set<Long> forceFinishedTables = new HashSet<>();
                List<PbHeartbeatReqForTable> finishedTableReqs = new ArrayList<>();
                finishedTables.forEach(
                        (tableId, tieringFinishInfo) -> {
                            if (tieringFinishInfo.isForceFinished) {
                                forceFinishedTables.add(tableId);
                            }
                            PbHeartbeatReqForTable pbHeartbeatReqForTable =
                                    new PbHeartbeatReqForTable()
                                            .setTableId(tableId)
                                            .setCoordinatorEpoch(coordinatorEpoch)
                                            .setTieringEpoch(tieringFinishInfo.tieringEpoch);
                            TieringStats stats = tieringFinishInfo.stats;
                            if (stats.isAvailableStats()) {
                                PbLakeTieringStats pbLakeTieringStats = new PbLakeTieringStats();
                                if (stats.getFileSize() != null) {
                                    pbLakeTieringStats.setFileSize(stats.getFileSize());
                                }
                                if (stats.getRecordCount() != null) {
                                    pbLakeTieringStats.setRecordCount(stats.getRecordCount());
                                }
                                pbHeartbeatReqForTable.setLakeTieringStats(pbLakeTieringStats);
                            }
                            finishedTableReqs.add(pbHeartbeatReqForTable);
                        });
                heartbeatRequest.addAllFinishedTables(finishedTableReqs);
                for (long forceFinishedTableId : forceFinishedTables) {
                    heartbeatRequest.addForceFinishedTable(forceFinishedTableId);
                }
            }
            // add failed tiering table to heart beat request
            return failedTableHeartBeat(heartbeatRequest, failedTableEpochs, coordinatorEpoch);
        }

        private static Set<PbHeartbeatReqForTable> toPbHeartbeatReqForTable(
                Map<Long, Long> tableEpochs, int coordinatorEpoch) {
            return tableEpochs.entrySet().stream()
                    .map(
                            tieringTableEpoch ->
                                    new PbHeartbeatReqForTable()
                                            .setTableId(tieringTableEpoch.getKey())
                                            .setCoordinatorEpoch(coordinatorEpoch)
                                            .setTieringEpoch(tieringTableEpoch.getValue()))
                    .collect(Collectors.toSet());
        }

        /**
         * Report failed table to Fluss coordinator via HeartBeat, this method should be called when
         * {@link TieringSourceEnumerator} is closed or receives failed table from downstream lake
         * committer.
         */
        static LakeTieringHeartbeatRequest failedTableHeartBeat(
                LakeTieringHeartbeatRequest lakeTieringHeartbeatRequest,
                Map<Long, Long> failedTieringTableEpochs,
                int coordinatorEpoch) {
            if (!failedTieringTableEpochs.isEmpty()) {
                lakeTieringHeartbeatRequest.addAllFailedTables(
                        toPbHeartbeatReqForTable(failedTieringTableEpochs, coordinatorEpoch));
            }
            return lakeTieringHeartbeatRequest;
        }

        static LakeTieringHeartbeatResponse waitHeartbeatResponse(
                CompletableFuture<LakeTieringHeartbeatResponse> responseCompletableFuture) {
            try {
                return responseCompletableFuture.get(3, TimeUnit.MINUTES);
            } catch (Exception e) {
                LOG.error("Failed to wait heartbeat response due to ", e);
                throw new FlinkRuntimeException("Failed to wait heartbeat response due to ", e);
            }
        }
    }

    private static class TieringFinishInfo {
        /** The epoch of the tiering operation for this table. */
        final long tieringEpoch;

        /**
         * Whether this table was force finished due to reaching the maximum tiering duration. When
         * a table's tiering operation exceeds the max duration (data lake freshness), it will be
         * force finished to prevent it from blocking other tables' tiering operations.
         */
        final boolean isForceFinished;

        /** Stats collected during this tiering round. */
        final TieringStats stats;

        public static TieringFinishInfo from(long tieringEpoch) {
            return new TieringFinishInfo(tieringEpoch, false, null);
        }

        public static TieringFinishInfo from(
                long tieringEpoch, boolean isForceFinished, @Nullable TieringStats stats) {
            return new TieringFinishInfo(tieringEpoch, isForceFinished, stats);
        }

        private TieringFinishInfo(
                long tieringEpoch, boolean isForceFinished, @Nullable TieringStats stats) {
            this.tieringEpoch = tieringEpoch;
            this.isForceFinished = isForceFinished;
            this.stats = stats != null ? stats : TieringStats.UNKNOWN;
        }
    }
}
