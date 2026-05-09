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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.flink.metrics.FlinkMetricRegistry;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.TieringReachMaxDurationEvent;
import org.apache.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplitGenerator;
import org.apache.fluss.flink.tiering.source.state.TieringSourceEnumeratorState;
import org.apache.fluss.lake.committer.TieringStats;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.basicHeartBeat;
import static org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.failedTableHeartBeat;
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
        implements SplitEnumerator<TieringSplit, TieringSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSourceEnumerator.class);

    /**
     * KV snapshot lease duration for the whole tiering job. One lease covers the entire job
     * lifecycle; it is renewed implicitly by every {@code acquireSnapshots} call, so a relatively
     * long duration is safe and also bounds the worst-case leaked-lease lifetime if the job dies
     * abnormally.
     *
     * <p>TODO: introduce an explicit periodic lease-renewal mechanism so that a single tiering
     * round that exceeds {@link #KV_SNAPSHOT_LEASE_DURATION_MS} (e.g. for very large tables) will
     * not see its snapshots garbage-collected mid-flight. Tracked as a follow-up issue; tiering
     * rounds are typically minute-level today so a 1-day lease is sufficient in practice.
     */
    private static final long KV_SNAPSHOT_LEASE_DURATION_MS = Duration.ofDays(1).toMillis();

    private final Configuration flussConf;
    private final SplitEnumeratorContext<TieringSplit> context;
    private final ScheduledExecutorService timerService;
    private final SplitEnumeratorMetricGroup enumeratorMetricGroup;
    private final long pollTieringTableIntervalMs;
    private final List<TieringSplit> pendingSplits;
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
     * bookkeeping entry on the server side, aligned with the normal Flink Source design.
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

    public TieringSourceEnumerator(
            Configuration flussConf,
            SplitEnumeratorContext<TieringSplit> context,
            long pollTieringTableIntervalMs) {
        this(flussConf, context, pollTieringTableIntervalMs, null);
    }

    /**
     * Creates a new enumerator, optionally restoring from a previously persisted lease id.
     *
     * @param restoredLeaseId the lease id from a previous checkpoint, or null for fresh start
     */
    public TieringSourceEnumerator(
            Configuration flussConf,
            SplitEnumeratorContext<TieringSplit> context,
            long pollTieringTableIntervalMs,
            @Nullable String restoredLeaseId) {
        this.flussConf = flussConf;
        this.context = context;
        this.timerService =
                Executors.newSingleThreadScheduledExecutor(
                        r -> new Thread(r, "Tiering-Timer-Thread"));
        this.enumeratorMetricGroup = context.metricGroup();
        this.pollTieringTableIntervalMs = pollTieringTableIntervalMs;
        this.pendingSplits = Collections.synchronizedList(new ArrayList<>());
        this.readersAwaitingSplit = Collections.synchronizedSet(new TreeSet<>());
        this.tieringTableEpochs = new ConcurrentHashMap<>();
        this.finishedTables = new ConcurrentHashMap<>();
        this.failedTableEpochs = new ConcurrentHashMap<>();
        this.tieringReachMaxDurationsTables = Collections.synchronizedSet(new TreeSet<>());
        // Thread safety: outer map is ConcurrentHashMap, values are ConcurrentHashMap-backed
        // Sets. Reads/writes are safe across the coordinator thread and the timer thread.
        this.leasedBucketsByTable = new ConcurrentHashMap<>();
        this.kvSnapshotLeaseId =
                restoredLeaseId != null ? restoredLeaseId : "tiering-" + UUID.randomUUID();
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        FlinkMetricRegistry metricRegistry = new FlinkMetricRegistry(enumeratorMetricGroup);
        ClientMetricGroup clientMetricGroup =
                new ClientMetricGroup(metricRegistry, "LakeTieringService");
        this.rpcClient = RpcClient.create(flussConf, clientMetricGroup);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        this.splitGenerator = new TieringSplitGenerator(flussAdmin);

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

        // During failover we must not request a new tiering table or assign any splits.
        // Otherwise we might re-acquire leases for a table whose leases were just released by
        // handleSourceReaderFailOver, or assign splits to subtasks whose readers from the new
        // attempt have not been fully registered yet. The pending failed-table report and the
        // subsequent split request will be driven by the periodic callAsync once failover is
        // marked complete.
        if (isFailOvering) {
            LOG.info(
                    "Skip handling split request from subtask {} because the enumerator is in failover.",
                    subtaskId);
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
    public void addSplitsBack(List<TieringSplit> splits, int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
        pendingSplits.addAll(splits);
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
                                "All {} subtasks reached the same attempt number {}. Current registered readers are {}. Waiting for failed-table report to complete before clearing failover state.",
                                context.currentParallelism(),
                                globalMaxAttempt,
                                context.registeredReadersOfAttempts());
                        // Drive a heartbeat round to report the failed tables and request a
                        // fresh tiering table; clear the failover flag only after that round
                        // completes, so that no split is assigned and no lease is re-acquired
                        // for a table whose leases were just released during failover.
                        this.context.callAsync(
                                this::requestTieringTableSplitsViaHeartBeat,
                                (tieringTable, throwable) -> {
                                    isFailOvering = false;
                                    LOG.info(
                                            "Failover completed for attempt {}. Cleared failover flag.",
                                            globalMaxAttempt);
                                    generateAndAssignSplits(tieringTable, throwable);
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
            this.context.callAsync(
                    this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
        }
    }

    private void handleSourceReaderFailOver() {
        LOG.info(
                "Handling source reader fail over, mark current tiering table epoch {} as failed.",
                tieringTableEpochs);
        // we need to make all as failed
        failedTableEpochs.putAll(new HashMap<>(tieringTableEpochs));
        // release all currently leased buckets for tables that are being marked failed;
        // take a snapshot of the keys first to avoid concurrent modification.
        Set<Long> tableIdsToRelease = new HashSet<>(tieringTableEpochs.keySet());
        tieringTableEpochs.clear();
        tieringReachMaxDurationsTables.clear();
        // also clean all pending splits since we mark all as failed
        pendingSplits.clear();
        // Release leases asynchronously to avoid blocking the coordinator thread when
        // multiple tables are involved and RPC calls may time out.
        for (Long tableId : tableIdsToRelease) {
            maybeReleaseKvSnapshotLeaseAsync(tableId);
        }
        if (!failedTableEpochs.isEmpty()) {
            // call one round of heartbeat to notify table has been finished or failed
            this.context.callAsync(
                    this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
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

    private void generateAndAssignSplits(
            @Nullable Tuple3<Long, Long, TablePath> tieringTable, Throwable throwable) {
        if (throwable != null) {
            LOG.warn("Failed to request tiering table, will retry later.", throwable);
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
                if (!pendingSplits.isEmpty()) {
                    TieringSplit tieringSplit = pendingSplits.remove(0);
                    context.assignSplit(tieringSplit, nextAwaitingReader);
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
            List<TieringSplit> tieringSplits =
                    populateNumberOfTieringSplits(splitGenerator.generateTableSplits(tableInfo));
            // shuffle tiering split to avoid splits tiering skew
            // after introduce tiering max duration
            Collections.shuffle(tieringSplits);
            LOG.info(
                    "Generate Tiering {} splits for table {} with cost {}ms.",
                    tieringSplits.size(),
                    tieringTable.f2,
                    System.currentTimeMillis() - start);
            if (tieringSplits.isEmpty()) {
                LOG.info(
                        "Generate Tiering splits for table {} is empty, no need to tier data.",
                        tieringTable.f2.getTableName());
                finishedTables.put(tieringTable.f0, TieringFinishInfo.from(tieringTable.f1));
            } else {
                tieringTableEpochs.put(tieringTable.f0, tieringTable.f1);
                // Acquire kv snapshot lease for all snapshot splits of this table before they
                // are assigned to readers, so that snapshots referenced by these splits will not
                // be garbage-collected by the Fluss server while tiering is in progress.
                maybeAcquireKvSnapshotLease(tieringTable.f0, tieringSplits);
                pendingSplits.addAll(tieringSplits);

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
            // Remove from tieringTableEpochs in case it was already added before the failure.
            tieringTableEpochs.remove(tieringTable.f0);
            failedTableEpochs.put(tieringTable.f0, tieringTable.f1);
            // Release any lease that was partially acquired before the failure so the
            // server-side snapshot references are not held unnecessarily until lease expiry.
            maybeReleaseKvSnapshotLease(tieringTable.f0);
        }
    }

    private List<TieringSplit> populateNumberOfTieringSplits(List<TieringSplit> tieringSplits) {
        int numberOfSplits = tieringSplits.size();
        return tieringSplits.stream()
                .map(split -> split.copy(numberOfSplits))
                .collect(Collectors.toList());
    }

    @Override
    public TieringSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        // Persist the lease id so that on restore we can reuse the same id instead of leaking
        // an orphaned lease on the server.
        return new TieringSourceEnumeratorState(kvSnapshotLeaseId);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        timerService.shutdownNow();
        if (rpcClient != null) {
            failedTableEpochs.putAll(tieringTableEpochs);
            tieringTableEpochs.clear();
            if (!failedTableEpochs.isEmpty()) {
                reportFailedTable(basicHeartBeat(), failedTableEpochs);
            }
            try {
                LOG.info("Closing Tiering Source Enumerator of at {}.", System.currentTimeMillis());
                rpcClient.close();
            } catch (Exception e) {
                LOG.error("Failed to close Tiering Source enumerator.", e);
            }
        }
        // NOTE: we intentionally do NOT drop the kv snapshot lease here. The lease id is
        // persisted into the enumerator checkpoint state and will be reused by the restored
        // enumerator after a JM failover. Dropping it on close would destroy the lease that
        // the restored enumerator expects to reuse, potentially causing the referenced
        // snapshots to be garbage-collected before tiering finishes. The lease will expire
        // naturally on the server side (see KV_SNAPSHOT_LEASE_DURATION_MS).
        //
        // TODO: if the job is cancelled by the user (rather than restarted), the lease will only
        // be reclaimed on the server side when it expires (up to KV_SNAPSHOT_LEASE_DURATION_MS).
        // We cannot currently distinguish "user cancel" from "failover" at the SplitEnumerator
        // layer; consider wiring a cancel hook (or using Flink's close(reason) when available)
        // so that user-initiated cancellations can drop the lease eagerly.
        try {
            if (flussAdmin != null) {
                LOG.info("Closing Fluss Admin client...");
                flussAdmin.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Fluss Admin client.", e);
        }
        try {
            if (connection != null) {
                LOG.info("Closing Fluss connection...");
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Fluss connection.", e);
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
        if (flussAdmin == null) {
            return;
        }
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
            // buckets that were never actually acquired (e.g. snapshots already GC'ed or
            // missing on the server).
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
        // Peek the buckets without removing so that, if the release RPC fails, we can keep
        // the entry in leasedBucketsByTable for a later retry instead of permanently
        // forgetting which buckets are still leased on the server side.
        Set<TableBucket> buckets = leasedBucketsByTable.get(tableId);
        if (flussAdmin == null || buckets == null || buckets.isEmpty()) {
            // Nothing to release; clean up any empty entry.
            leasedBucketsByTable.remove(tableId);
            return;
        }
        LOG.info(
                "Try to release kv snapshot lease {} for tiering table {} with {} buckets.",
                kvSnapshotLeaseId,
                tableId,
                buckets.size());
        // Take a defensive copy of the buckets to release so concurrent updates to the
        // tracked set do not affect the in-flight RPC payload.
        Set<TableBucket> bucketsToRelease = new HashSet<>(buckets);
        try {
            flussAdmin
                    .createKvSnapshotLease(kvSnapshotLeaseId, KV_SNAPSHOT_LEASE_DURATION_MS)
                    .releaseSnapshots(bucketsToRelease)
                    .get();
            // Only drop the bookkeeping entry after the server confirms the release.
            Set<TableBucket> tracked = leasedBucketsByTable.get(tableId);
            if (tracked != null) {
                tracked.removeAll(bucketsToRelease);
                if (tracked.isEmpty()) {
                    leasedBucketsByTable.remove(tableId);
                }
            }
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class).isPresent()) {
                // Server does not support the lease API; drop tracking since release is a
                // no-op and there is no point retrying.
                leasedBucketsByTable.remove(tableId);
                LOG.warn(
                        "Failed to release kv snapshot lease for tiering table {} because the "
                                + "server does not support kv snapshot lease API.",
                        tableId,
                        e);
            } else {
                // Keep the buckets tracked so we (or the next failover/close) can retry.
                LOG.error(
                        "Failed to release kv snapshot lease for tiering table {}; the buckets "
                                + "remain tracked and will be retried on next release attempt.",
                        tableId,
                        e);
            }
        }
    }

    /**
     * Asynchronous variant of {@link #maybeReleaseKvSnapshotLease(long)} used during failover to
     * avoid blocking the coordinator thread when multiple tables need to be released.
     */
    private void maybeReleaseKvSnapshotLeaseAsync(long tableId) {
        // Peek the buckets without removing them yet; only drop the tracking entry once the
        // server confirms the release. This way a transient failure (e.g. RPC timeout) does
        // not silently leak the lease until expiry.
        Set<TableBucket> tracked = leasedBucketsByTable.get(tableId);
        if (flussAdmin == null || tracked == null || tracked.isEmpty()) {
            leasedBucketsByTable.remove(tableId);
            return;
        }
        Set<TableBucket> bucketsToRelease = new HashSet<>(tracked);
        LOG.info(
                "Asynchronously releasing kv snapshot lease {} for tiering table {} with {} buckets.",
                kvSnapshotLeaseId,
                tableId,
                bucketsToRelease.size());
        flussAdmin
                .createKvSnapshotLease(kvSnapshotLeaseId, KV_SNAPSHOT_LEASE_DURATION_MS)
                .releaseSnapshots(bucketsToRelease)
                .whenComplete(
                        (ignored, e) -> {
                            if (e == null) {
                                Set<TableBucket> remaining = leasedBucketsByTable.get(tableId);
                                if (remaining != null) {
                                    remaining.removeAll(bucketsToRelease);
                                    if (remaining.isEmpty()) {
                                        leasedBucketsByTable.remove(tableId);
                                    }
                                }
                                return;
                            }
                            if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class)
                                    .isPresent()) {
                                // Server does not support the lease API; tracking is useless,
                                // drop it so we don't keep retrying.
                                leasedBucketsByTable.remove(tableId);
                                LOG.warn(
                                        "Failed to release kv snapshot lease for tiering "
                                                + "table {} because the server does not "
                                                + "support kv snapshot lease API.",
                                        tableId,
                                        e);
                            } else {
                                // Keep the buckets tracked so a later failover/close can
                                // retry the release instead of leaking the lease until
                                // expiry.
                                LOG.error(
                                        "Failed to release kv snapshot lease for tiering "
                                                + "table {} during failover; the buckets "
                                                + "remain tracked and will be retried later.",
                                        tableId,
                                        e);
                            }
                        });
    }

    @VisibleForTesting
    String getKvSnapshotLeaseId() {
        return kvSnapshotLeaseId;
    }

    @VisibleForTesting
    Map<Long, Set<TableBucket>> getLeasedBucketsByTable() {
        return leasedBucketsByTable;
    }

    /**
     * Report failed table to Fluss coordinator via HeartBeat, this method should be called when
     * {@link TieringSourceEnumerator} is closed or receives failed table from downstream lake
     * committer.
     */
    private void reportFailedTable(
            LakeTieringHeartbeatRequest heartbeatRequest, Map<Long, Long> failedTableEpochs)
            throws FlinkRuntimeException {
        try {
            waitHeartbeatResponse(
                    coordinatorGateway.lakeTieringHeartbeat(
                            failedTableHeartBeat(
                                    heartbeatRequest, failedTableEpochs, flussCoordinatorEpoch)));
            LOG.info("Report failed table to Fluss Coordinator success");

        } catch (Exception e) {
            LOG.error("Errors happens when report failed table to Fluss cluster.", e);
            throw new FlinkRuntimeException(
                    "Errors happens when report failed table to Fluss cluster.", e);
        }
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
