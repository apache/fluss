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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;

/** Retries independent cache adoption and leader recovery from physical bucket records. */
public final class BulkLoadRecoveryManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadRecoveryManager.class);
    private static final int MAX_IN_FLIGHT_RECOVERIES = 16;
    private static final int MAX_BUCKETS_PER_SCAN = 128;
    private static final long RETRY_INTERVAL_MS = 1_000L;
    private static final Object RECONCILE_AFTER_TAKEOVER = new Object();

    private final ZooKeeperClient client;
    private final CoordinatorEventManager events;
    private final CoordinatorChannelManager channels;
    private final CompletedSnapshotStoreManager snapshots;
    private final Map<TableBucket, Object> active = new ConcurrentHashMap<>();
    private final Map<TableBucket, Object> newCommitRecoveries = new ConcurrentHashMap<>();
    private final Set<TableBucket> inFlight = ConcurrentHashMap.newKeySet();
    private final Executor executor;
    private final Scheduler scheduler;
    private final ZkEpoch epoch;
    private Iterator<Map.Entry<TableBucket, Object>> recoveryRound = Collections.emptyIterator();
    private boolean takeoverScanned;
    private ScheduledFuture<?> scheduled;
    private boolean closed = true;
    private boolean running;

    public BulkLoadRecoveryManager(
            ZooKeeperClient client,
            CoordinatorEventManager events,
            CoordinatorChannelManager channels,
            CompletedSnapshotStoreManager snapshots,
            Executor executor,
            Scheduler scheduler,
            ZkEpoch epoch) {
        this.client = client;
        this.events = events;
        this.channels = channels;
        this.snapshots = snapshots;
        this.executor = executor;
        this.scheduler = scheduler;
        this.epoch = epoch;
    }

    /** Schedules discovery of incomplete recoveries when this Coordinator takes over. */
    public synchronized void start() {
        if (epoch.getCoordinatorEpochZkVersion() < 0) {
            return;
        }
        closed = false;
        scheduleScan(0);
    }

    /** Schedules reconciliation of the currently selected snapshot for the bucket. */
    public synchronized void request(TableBucket bucket) {
        if (!closed) {
            Object request = new Object();
            // Expose the priority entry before this request can enter the regular sweep.
            newCommitRecoveries.put(bucket, request);
            active.put(bucket, request);
            scheduleScan(0);
        }
    }

    private void scheduleScan(long delayMs) {
        if (scheduled == null && !running) {
            scheduled = scheduler.scheduleOnce("bulk-load-recovery", this::submitScan, delayMs);
        }
    }

    private synchronized void submitScan() {
        scheduled = null;
        if (closed) {
            return;
        }
        running = true;
        try {
            executor.execute(this::runScan);
        } catch (RuntimeException e) {
            finishScan(e);
        }
    }

    private void runScan() {
        Exception failure = null;
        try {
            synchronized (this) {
                if (closed) {
                    return;
                }
            }
            recoverImports();
        } catch (Exception e) {
            failure = e;
        } finally {
            finishScan(failure);
        }
    }

    private synchronized void finishScan(Exception failure) {
        running = false;
        if (closed) {
            return;
        }
        if (failure != null) {
            LOG.warn("BulkLoad recovery scan failed; the next pass will retry.", failure);
        }
        if (failure != null || !takeoverScanned || !active.isEmpty()) {
            scheduleScan(RETRY_INTERVAL_MS);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }

    private void recoverImports() throws Exception {
        if (!takeoverScanned) {
            AccessContextEvent<Set<TableBucket>> event =
                    new AccessContextEvent<>(
                            ctx ->
                                    ctx.getKvBuckets().stream()
                                            .filter(
                                                    bucket -> {
                                                        TableInfo table =
                                                                ctx.getTableInfoById(
                                                                        bucket.getTableId());
                                                        if (table == null
                                                                || !table.hasPrimaryKey()) {
                                                            return false;
                                                        }
                                                        Long partitionId = bucket.getPartitionId();
                                                        return partitionId == null
                                                                || ctx.getPhysicalTablePath(
                                                                                partitionId)
                                                                        .map(
                                                                                PhysicalTablePath
                                                                                        ::getPartitionName)
                                                                        .filter(
                                                                                name ->
                                                                                        !HISTORICAL_PARTITION_VALUE
                                                                                                .equals(
                                                                                                        name))
                                                                        .isPresent();
                                                    })
                                            .collect(Collectors.toSet()));
            events.put(event);
            for (TableBucket bucket : event.getResultFuture().get(30, TimeUnit.SECONDS)) {
                active.putIfAbsent(bucket, RECONCILE_AFTER_TAKEOVER);
            }
            takeoverScanned = true;
        }
        if (!recoveryRound.hasNext()) {
            recoveryRound = active.entrySet().iterator();
        }
        Iterator<Map.Entry<TableBucket, Object>> newCommits =
                newCommitRecoveries.entrySet().iterator();
        List<Map.Entry<TableBucket, Object>> firstAttempts = new ArrayList<>();
        // Give new commits the first available slots. Their retries join the regular sweep.
        for (int scanned = 0;
                scanned < MAX_BUCKETS_PER_SCAN
                        && inFlight.size() < MAX_IN_FLIGHT_RECOVERIES
                        && (newCommits.hasNext() || recoveryRound.hasNext());
                scanned++) {
            boolean firstCommitAttempt = newCommits.hasNext();
            Map.Entry<TableBucket, Object> pending =
                    firstCommitAttempt ? newCommits.next() : recoveryRound.next();
            TableBucket bucket = pending.getKey();
            Object request = pending.getValue();
            if (active.get(bucket) != request
                    || (!firstCommitAttempt && newCommitRecoveries.containsKey(bucket))
                    || !inFlight.add(bucket)) {
                continue;
            }
            if (firstCommitAttempt) {
                firstAttempts.add(pending);
            }
            try {
                Optional<Long> selected = client.getSelectedRecoverySnapshotId(bucket);
                if (!selected.isPresent()) {
                    finish(bucket, request, false, null);
                    continue;
                }
                recover(bucket, selected.get(), epoch, request != RECONCILE_AFTER_TAKEOVER)
                        .whenComplete((retry, error) -> finish(bucket, request, retry, error));
            } catch (Exception e) {
                finish(bucket, request, null, e);
            }
        }
        // Keep first attempts out of the regular sweep until the next scan.
        for (Map.Entry<TableBucket, Object> attempt : firstAttempts) {
            newCommitRecoveries.remove(attempt.getKey(), attempt.getValue());
        }
    }

    private void finish(TableBucket bucket, Object request, Boolean retry, Throwable error) {
        if (error != null) {
            if (request == RECONCILE_AFTER_TAKEOVER
                    && stripCompletionException(error) instanceof UnsupportedVersionException) {
                active.remove(bucket, request);
            } else {
                LOG.warn(
                        "Cannot reconcile BulkLoad snapshot for {}; the next pass will retry.",
                        bucket,
                        error);
            }
        } else if (!retry) {
            active.remove(bucket, request);
        }
        inFlight.remove(bucket);
    }

    private CompletableFuture<Boolean> recover(
            TableBucket bucket, long selectedSnapshotId, ZkEpoch epoch, boolean retainAfterImport) {
        AccessContextEvent<NotifyLeaderAndIsrData> context =
                new AccessContextEvent<>(
                        ctx -> {
                            if (ctx.getCoordinatorZkVersion()
                                    != epoch.getCoordinatorEpochZkVersion()) {
                                return null;
                            }
                            TableInfo table = ctx.getTableInfoById(bucket.getTableId());
                            Optional<LeaderAndIsr> role = ctx.getBucketLeaderAndIsr(bucket);
                            if (table == null || !role.isPresent()) {
                                return null;
                            }
                            PhysicalTablePath path;
                            if (bucket.getPartitionId() == null) {
                                path = PhysicalTablePath.of(table.getTablePath());
                            } else {
                                Optional<PhysicalTablePath> partition =
                                        ctx.getPhysicalTablePath(bucket.getPartitionId());
                                if (!partition.isPresent()
                                        || !partition
                                                .get()
                                                .getTablePath()
                                                .equals(table.getTablePath())) {
                                    return null;
                                }
                                path = partition.get();
                            }
                            List<Integer> assignment = ctx.getAssignment(bucket);
                            if (assignment == null || assignment.isEmpty()) {
                                return null;
                            }
                            return NotifyLeaderAndIsrData.forBulkLoadRecovery(
                                    path, bucket, assignment, role.get(), selectedSnapshotId);
                        });
        events.put(context);
        return FutureUtils.orTimeout(
                        context.getResultFuture(),
                        30,
                        TimeUnit.SECONDS,
                        "BulkLoad recovery context timed out.")
                .thenCompose(
                        data -> {
                            if (data == null) {
                                return CompletableFuture.completedFuture(true);
                            }
                            NotifyLeaderAndIsrRequest request =
                                    ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest(
                                            epoch.getCoordinatorEpoch(),
                                            Collections.singletonList(
                                                    ServerRpcMessageUtils
                                                            .makeNotifyBucketLeaderAndIsr(data)));
                            return channels.notifyBulkLoadRecovery(data.getLeader(), request)
                                    .thenApplyAsync(
                                            response -> {
                                                try {
                                                    return handleRecoveryResponse(
                                                            data,
                                                            selectedSnapshotId,
                                                            retainAfterImport,
                                                            response.getNotifyBucketsLeaderRespAt(
                                                                    0));
                                                } catch (Exception e) {
                                                    throw new CompletionException(e);
                                                }
                                            },
                                            executor);
                        });
    }

    private boolean handleRecoveryResponse(
            NotifyLeaderAndIsrData data,
            long selectedSnapshotId,
            boolean retainAfterImport,
            PbNotifyLeaderAndIsrRespForBucket response)
            throws Exception {
        TableBucket bucket = data.getTableBucket();
        NotifyLeaderAndIsrResultForBucket result =
                ServerRpcMessageUtils.toNotifyLeaderAndIsrResult(response);
        if (result.failed()) {
            throw result.getError().error().exception(result.getErrorMessage());
        }
        Long recoveredSnapshotId = result.getRecoveredSnapshotId();
        if (recoveredSnapshotId != null
                && recoveredSnapshotId == selectedSnapshotId
                && result.getTableBucket().equals(bucket)) {
            Optional<Long> current = client.getSelectedRecoverySnapshotId(bucket);
            if (!current.isPresent() || current.get() != selectedSnapshotId) {
                return true;
            }
            if (retainAfterImport || snapshots.isSnapshotRetentionExceeded(bucket)) {
                snapshots
                        .getOrCreateCompletedSnapshotStore(
                                data.getPhysicalTablePath().getTablePath(), bucket)
                        .retainSnapshotsAfterImport();
            }
            return false;
        }
        return true;
    }
}
