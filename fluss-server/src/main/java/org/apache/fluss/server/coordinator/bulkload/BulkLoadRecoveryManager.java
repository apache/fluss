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
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;

/** Retries independent cache adoption and leader recovery from physical bucket records. */
public final class BulkLoadRecoveryManager {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadRecoveryManager.class);
    private static final int MAX_IN_FLIGHT_RECOVERIES = 16;
    private static final Object RECONCILE_AFTER_TAKEOVER = new Object();

    private final ZooKeeperClient client;
    private final CoordinatorEventManager events;
    private final CoordinatorChannelManager channels;
    private final CompletedSnapshotStoreManager snapshots;
    private final Map<TableBucket, Object> active = new ConcurrentHashMap<>();
    private final Set<TableBucket> inFlight = ConcurrentHashMap.newKeySet();
    private final Executor executor;
    private Iterator<Map.Entry<TableBucket, Object>> recoveryRound = Collections.emptyIterator();
    private int scannedEpoch = -1;

    public BulkLoadRecoveryManager(
            ZooKeeperClient client,
            CoordinatorEventManager events,
            CoordinatorChannelManager channels,
            CompletedSnapshotStoreManager snapshots,
            Executor executor) {
        this.client = client;
        this.events = events;
        this.channels = channels;
        this.snapshots = snapshots;
        this.executor = executor;
    }

    public void request(TableBucket bucket) {
        active.put(bucket, new Object());
    }

    boolean hasPendingWork() {
        return !active.isEmpty();
    }

    void recoverImports(ZkEpoch epoch) throws Exception {
        int version = epoch.getCoordinatorEpochZkVersion();
        if (scannedEpoch != version) {
            AccessContextEvent<Set<TableBucket>> event =
                    new AccessContextEvent<>(
                            ctx ->
                                    ctx.getAllBuckets().stream()
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
            scannedEpoch = version;
        }
        if (!recoveryRound.hasNext()) {
            recoveryRound = active.entrySet().iterator();
        }
        // Resume the same sweep when all slots are busy so retries cannot starve later buckets.
        while (inFlight.size() < MAX_IN_FLIGHT_RECOVERIES && recoveryRound.hasNext()) {
            Map.Entry<TableBucket, Object> pending = recoveryRound.next();
            TableBucket bucket = pending.getKey();
            Object request = pending.getValue();
            if (active.get(bucket) != request || !inFlight.add(bucket)) {
                continue;
            }
            try {
                Optional<ZooKeeperClient.VersionedData<Long>> selected =
                        client.getRecoverySnapshotSelection(bucket);
                if (!selected.isPresent()) {
                    finish(bucket, request, false, null);
                    continue;
                }
                recover(bucket, selected.get().data(), epoch, request != RECONCILE_AFTER_TAKEOVER)
                        .whenComplete((retry, error) -> finish(bucket, request, retry, error));
            } catch (Exception e) {
                finish(bucket, request, null, e);
            }
        }
    }

    private void finish(TableBucket bucket, Object request, Boolean retry, Throwable error) {
        if (error != null) {
            LOG.warn(
                    "Cannot reconcile BulkLoad snapshot for {}; the next pass will retry.",
                    bucket,
                    error);
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
                            return new NotifyLeaderAndIsrData(
                                    path,
                                    bucket,
                                    assignment,
                                    role.get(),
                                    null,
                                    null,
                                    selectedSnapshotId);
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
                            return channels.bulkLoadRestore(data.getLeader(), request)
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
        if (response.hasErrorCode() && response.getErrorCode() != 0) {
            throw org.apache.fluss.rpc.protocol.Errors.forCode(response.getErrorCode())
                    .exception(response.hasErrorMessage() ? response.getErrorMessage() : null);
        }
        if (response.hasRecoveredSnapshotId()
                && response.getRecoveredSnapshotId() == selectedSnapshotId
                && ServerRpcMessageUtils.toTableBucket(response.getTableBucket()).equals(bucket)) {
            Optional<ZooKeeperClient.VersionedData<Long>> current =
                    client.getRecoverySnapshotSelection(bucket);
            if (!current.isPresent() || current.get().data() != selectedSnapshotId) {
                return true;
            }
            if (retainAfterImport) {
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
