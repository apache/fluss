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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.messages.FreezePartitionRequest;
import org.apache.fluss.rpc.messages.FreezePartitionResponse;
import org.apache.fluss.rpc.messages.PbFreezePartitionRespForBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.PartitionRegistration.FrozenBucket;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFreezePartitionRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;

/** Coordinates freeze, lake offset verification, and deletion for safe partition retention. */
public class LakeAwarePartitionRetentionManager implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(LakeAwarePartitionRetentionManager.class);

    private final MetadataManager metadataManager;
    private final ZooKeeperClient zooKeeperClient;
    private final CoordinatorChannelManager coordinatorChannelManager;
    private final int coordinatorEpoch;
    private final Executor executor;
    private final ConcurrentMap<PhysicalTablePath, RetentionStatus> retainingPartitions =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public LakeAwarePartitionRetentionManager(
            MetadataManager metadataManager,
            ZooKeeperClient zooKeeperClient,
            CoordinatorChannelManager coordinatorChannelManager,
            int coordinatorEpoch,
            Executor executor) {
        this.metadataManager = metadataManager;
        this.zooKeeperClient = zooKeeperClient;
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.coordinatorEpoch = coordinatorEpoch;
        this.executor = executor;
    }

    /** Start or continue lake-aware retention for an expired partition. */
    public void startRetention(TableInfo tableInfo, String partitionName) {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(tableInfo.getTablePath(), partitionName);
        if (closed.get()) {
            return;
        }

        RetentionStatus newStatus = new RetentionStatus();
        RetentionStatus retention = retainingPartitions.putIfAbsent(physicalTablePath, newStatus);
        if (retention == null) {
            submitFreeze(tableInfo, partitionName, physicalTablePath, newStatus);
        } else {
            toNext(tableInfo, partitionName, physicalTablePath, retention);
        }
    }

    private void toNext(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        synchronized (retention) {
            if (closed.get()
                    || retainingPartitions.get(physicalTablePath) != retention
                    || retention.operationRunning) {
                return;
            }
            retention.operationRunning = true;
            if (retention.stage == RetentionStage.FROZEN) {
                submitLakeCheck(tableInfo, partitionName, physicalTablePath, retention);
            } else if (retention.stage == RetentionStage.LAKE_TIERED) {
                submitDrop(tableInfo, partitionName, physicalTablePath, retention);
            }
        }
    }

    private void submitFreeze(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        if (closed.get()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }
        try {
            executor.execute(
                    () -> freezePartition(tableInfo, partitionName, physicalTablePath, retention));
        } catch (RuntimeException e) {
            retainingPartitions.remove(physicalTablePath, retention);
            LOG.warn(
                    "Failed to submit lake-aware retention for partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void freezePartition(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        Optional<PartitionRegistration> registration;
        try {
            registration =
                    metadataManager.getPartitionRegistration(
                            tableInfo.getTablePath(), partitionName);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to load partition {} of table {} for lake-aware retention.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }
        if (!registration.isPresent()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }
        if (registration.get().getTableId() != tableInfo.getTableId()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }
        freezePartition(tableInfo, partitionName, registration.get(), physicalTablePath, retention);
    }

    private void freezePartition(
            TableInfo tableInfo,
            String partitionName,
            PartitionRegistration partitionRegistration,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        if (closed.get()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }

        try {
            PartitionAssignment partitionAssignment =
                    zooKeeperClient
                            .getPartitionAssignment(partitionRegistration.getPartitionId())
                            .orElse(null);
            if (partitionAssignment == null) {
                LOG.info(
                        "Waiting to retain partition {} of table {} because its assignment is unavailable.",
                        partitionName,
                        tableInfo.getTablePath());
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }

            List<TableBucket> tableBuckets = new ArrayList<>();
            for (Integer bucketId : partitionAssignment.getBucketAssignments().keySet()) {
                tableBuckets.add(
                        new TableBucket(
                                tableInfo.getTableId(),
                                partitionRegistration.getPartitionId(),
                                bucketId));
            }
            Map<TableBucket, LeaderAndIsr> leaders = zooKeeperClient.getLeaderAndIsrs(tableBuckets);
            if (leaders.size() != tableBuckets.size()
                    || leaders.values().stream()
                            .anyMatch(
                                    leaderAndIsr ->
                                            leaderAndIsr.leader() == LeaderAndIsr.NO_LEADER)) {
                LOG.info(
                        "Waiting to retain partition {} of table {} because not all bucket leaders are available.",
                        partitionName,
                        tableInfo.getTablePath());
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }

            Map<Integer, Map<TableBucket, Integer>> leaderEpochsByServer = new HashMap<>();
            leaders.forEach(
                    (tableBucket, leaderAndIsr) ->
                            leaderEpochsByServer
                                    .computeIfAbsent(
                                            leaderAndIsr.leader(), ignored -> new HashMap<>())
                                    .put(tableBucket, leaderAndIsr.leaderEpoch()));

            List<CompletableFuture<FreezePartitionResponse>> freezeFutures = new ArrayList<>();
            for (Map.Entry<Integer, Map<TableBucket, Integer>> entry :
                    leaderEpochsByServer.entrySet()) {
                FreezePartitionRequest request =
                        makeFreezePartitionRequest(coordinatorEpoch, entry.getValue());
                freezeFutures.add(
                        coordinatorChannelManager.sendFreezePartitionRequest(
                                entry.getKey(), request));
            }

            CompletableFuture.allOf(
                            freezeFutures.toArray(new CompletableFuture[freezeFutures.size()]))
                    .whenCompleteAsync(
                            (ignored, error) -> {
                                try {
                                    if (closed.get()) {
                                        retainingPartitions.remove(physicalTablePath, retention);
                                        return;
                                    }
                                    if (error != null) {
                                        LOG.warn(
                                                "Failed to freeze partition {} of table {} for retention.",
                                                partitionName,
                                                tableInfo.getTablePath(),
                                                error);
                                        retainingPartitions.remove(physicalTablePath, retention);
                                        return;
                                    }
                                    finishFreeze(
                                            tableInfo,
                                            partitionName,
                                            partitionRegistration,
                                            leaders,
                                            freezeFutures,
                                            physicalTablePath,
                                            retention);
                                } catch (Exception e) {
                                    LOG.warn(
                                            "Failed to finish freezing partition {} of table {} for retention.",
                                            partitionName,
                                            tableInfo.getTablePath(),
                                            e);
                                    retainingPartitions.remove(physicalTablePath, retention);
                                }
                            },
                            executor);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to start lake-aware retention for partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
            retainingPartitions.remove(physicalTablePath, retention);
        }
    }

    private void finishFreeze(
            TableInfo tableInfo,
            String partitionName,
            PartitionRegistration initialRegistration,
            Map<TableBucket, LeaderAndIsr> expectedLeaders,
            List<CompletableFuture<FreezePartitionResponse>> freezeFutures,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention)
            throws Exception {
        Map<Integer, FrozenBucket> frozenBuckets =
                collectFrozenBuckets(expectedLeaders, freezeFutures);
        if (!leadersUnchanged(expectedLeaders)) {
            LOG.info(
                    "Waiting to retry retention for partition {} of table {} because a bucket leader changed while freezing.",
                    partitionName,
                    tableInfo.getTablePath());
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }

        Optional<PartitionRegistration> currentRegistration =
                metadataManager.getPartitionRegistration(tableInfo.getTablePath(), partitionName);
        if (!currentRegistration.isPresent()
                || currentRegistration.get().getPartitionId()
                        != initialRegistration.getPartitionId()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }

        frozenBuckets = mergeFrozenOffsets(currentRegistration.get(), frozenBuckets);
        if (!currentRegistration.get().getFrozenBuckets().equals(frozenBuckets)) {
            PartitionRegistration updatedRegistration =
                    currentRegistration.get().withFrozenBuckets(frozenBuckets);
            metadataManager.updatePartitionRegistration(
                    tableInfo.getTablePath(), partitionName, updatedRegistration);
        }
        completeOperation(physicalTablePath, retention, RetentionStage.FROZEN);
        LOG.info(
                "Partition {} of table {} is frozen and ready for the lake tiering check.",
                partitionName,
                tableInfo.getTablePath());
    }

    private void submitLakeCheck(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        if (closed.get()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }
        try {
            executor.execute(
                    () -> checkLakeTiered(tableInfo, partitionName, physicalTablePath, retention));
        } catch (RuntimeException e) {
            completeOperation(physicalTablePath, retention, RetentionStage.FROZEN);
            LOG.warn(
                    "Failed to submit lake tiering check for partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void checkLakeTiered(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        try {
            Optional<PartitionRegistration> registration =
                    metadataManager.getPartitionRegistration(
                            tableInfo.getTablePath(), partitionName);
            if (!registration.isPresent()
                    || registration.get().getTableId() != tableInfo.getTableId()) {
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }

            Map<Integer, FrozenBucket> frozenBuckets = registration.get().getFrozenBuckets();
            TablePartition tablePartition = registration.get().toTablePartition();
            if (frozenBuckets.isEmpty() || !leadersUnchanged(tablePartition, frozenBuckets)) {
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }

            if (!isFullyTiered(tablePartition, frozenBuckets)) {
                completeOperation(physicalTablePath, retention, RetentionStage.FROZEN);
                LOG.info(
                        "Partition {} of table {} is frozen and waiting for lake tiering.",
                        partitionName,
                        tableInfo.getTablePath());
                return;
            }
            if (!leadersUnchanged(tablePartition, frozenBuckets)) {
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }

            synchronized (retention) {
                if (retainingPartitions.get(physicalTablePath) != retention) {
                    return;
                }
                retention.stage = RetentionStage.LAKE_TIERED;
            }
            dropPartition(tableInfo, partitionName, physicalTablePath, retention);
        } catch (Exception e) {
            completeOperation(physicalTablePath, retention, RetentionStage.FROZEN);
            LOG.warn(
                    "Failed to check lake tiering for partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void submitDrop(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        if (closed.get()) {
            retainingPartitions.remove(physicalTablePath, retention);
            return;
        }
        try {
            executor.execute(
                    () -> dropPartition(tableInfo, partitionName, physicalTablePath, retention));
        } catch (RuntimeException e) {
            completeOperation(physicalTablePath, retention, RetentionStage.LAKE_TIERED);
            LOG.warn(
                    "Failed to submit drop for retained partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void dropPartition(
            TableInfo tableInfo,
            String partitionName,
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention) {
        try {
            Optional<PartitionRegistration> registration =
                    metadataManager.getPartitionRegistration(
                            tableInfo.getTablePath(), partitionName);
            if (!registration.isPresent()
                    || registration.get().getTableId() != tableInfo.getTableId()) {
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }

            Map<Integer, FrozenBucket> frozenBuckets = registration.get().getFrozenBuckets();
            TablePartition tablePartition = registration.get().toTablePartition();
            if (!leadersUnchanged(tablePartition, frozenBuckets)) {
                retainingPartitions.remove(physicalTablePath, retention);
                return;
            }
            if (!isFullyTiered(tablePartition, frozenBuckets)) {
                completeOperation(physicalTablePath, retention, RetentionStage.FROZEN);
                return;
            }

            metadataManager.dropPartition(
                    tableInfo.getTablePath(),
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), partitionName),
                    false);
            retainingPartitions.remove(physicalTablePath, retention);
            LOG.info(
                    "Deleted partition {} of table {} after all frozen offsets were committed to the lake.",
                    partitionName,
                    tableInfo.getTablePath());
        } catch (Exception e) {
            completeOperation(physicalTablePath, retention, RetentionStage.LAKE_TIERED);
            LOG.warn(
                    "Failed to drop retained partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void completeOperation(
            PhysicalTablePath physicalTablePath,
            RetentionStatus retention,
            RetentionStage nextStage) {
        synchronized (retention) {
            if (retainingPartitions.get(physicalTablePath) == retention) {
                retention.stage = nextStage;
                retention.operationRunning = false;
            }
        }
    }

    private Map<Integer, FrozenBucket> collectFrozenBuckets(
            Map<TableBucket, LeaderAndIsr> expectedLeaders,
            List<CompletableFuture<FreezePartitionResponse>> freezeFutures) {
        Map<Integer, FrozenBucket> frozenBuckets = new HashMap<>();
        for (CompletableFuture<FreezePartitionResponse> freezeFuture : freezeFutures) {
            FreezePartitionResponse response = freezeFuture.join();
            for (PbFreezePartitionRespForBucket bucketResponse : response.getBucketsRespsList()) {
                TableBucket tableBucket = toTableBucket(bucketResponse.getTableBucket());
                LeaderAndIsr leaderAndIsr = expectedLeaders.get(tableBucket);
                if (leaderAndIsr == null
                        || bucketResponse.hasErrorCode()
                        || !bucketResponse.hasHighWatermark()
                        || !bucketResponse.hasLogEndOffset()) {
                    throw new FlussRuntimeException(
                            "Failed to freeze bucket "
                                    + tableBucket
                                    + ": "
                                    + (bucketResponse.hasErrorMessage()
                                            ? bucketResponse.getErrorMessage()
                                            : "invalid response"));
                }
                frozenBuckets.put(
                        tableBucket.getBucket(),
                        new FrozenBucket(
                                leaderAndIsr.leader(),
                                leaderAndIsr.leaderEpoch(),
                                bucketResponse.getHighWatermark(),
                                bucketResponse.getLogEndOffset()));
            }
        }
        if (frozenBuckets.size() != expectedLeaders.size()) {
            throw new FlussRuntimeException(
                    String.format(
                            "Only %s of %s partition buckets were frozen.",
                            frozenBuckets.size(), expectedLeaders.size()));
        }
        return frozenBuckets;
    }

    private Map<Integer, FrozenBucket> mergeFrozenOffsets(
            PartitionRegistration registration, Map<Integer, FrozenBucket> frozenBuckets) {
        Map<Integer, FrozenBucket> merged = new HashMap<>(frozenBuckets);
        registration
                .getFrozenBuckets()
                .forEach(
                        (bucketId, previous) -> {
                            FrozenBucket current = merged.get(bucketId);
                            if (current != null) {
                                merged.put(
                                        bucketId,
                                        new FrozenBucket(
                                                current.getLeaderId(),
                                                current.getLeaderEpoch(),
                                                Math.max(
                                                        current.getHighWatermark(),
                                                        previous.getHighWatermark()),
                                                Math.max(
                                                        current.getLogEndOffset(),
                                                        previous.getLogEndOffset())));
                            }
                        });
        return merged;
    }

    private boolean leadersUnchanged(Map<TableBucket, LeaderAndIsr> expectedLeaders)
            throws Exception {
        Map<TableBucket, LeaderAndIsr> currentLeaders =
                zooKeeperClient.getLeaderAndIsrs(expectedLeaders.keySet());
        if (currentLeaders.size() != expectedLeaders.size()) {
            return false;
        }
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : expectedLeaders.entrySet()) {
            LeaderAndIsr current = currentLeaders.get(entry.getKey());
            LeaderAndIsr expected = entry.getValue();
            if (current == null
                    || current.leader() != expected.leader()
                    || current.leaderEpoch() != expected.leaderEpoch()) {
                return false;
            }
        }
        return true;
    }

    private boolean leadersUnchanged(
            TablePartition tablePartition, Map<Integer, FrozenBucket> frozenBuckets)
            throws Exception {
        Map<TableBucket, FrozenBucket> expectedLeaders = new HashMap<>();
        frozenBuckets.forEach(
                (bucketId, frozenBucket) ->
                        expectedLeaders.put(
                                new TableBucket(
                                        tablePartition.getTableId(),
                                        tablePartition.getPartitionId(),
                                        bucketId),
                                frozenBucket));

        Map<TableBucket, LeaderAndIsr> currentLeaders =
                zooKeeperClient.getLeaderAndIsrs(expectedLeaders.keySet());
        if (currentLeaders.size() != expectedLeaders.size()) {
            return false;
        }
        for (Map.Entry<TableBucket, FrozenBucket> entry : expectedLeaders.entrySet()) {
            LeaderAndIsr current = currentLeaders.get(entry.getKey());
            FrozenBucket expected = entry.getValue();
            if (current == null
                    || current.leader() != expected.getLeaderId()
                    || current.leaderEpoch() != expected.getLeaderEpoch()) {
                return false;
            }
        }
        return true;
    }

    private boolean isFullyTiered(
            TablePartition tablePartition, Map<Integer, FrozenBucket> frozenBuckets)
            throws Exception {
        Optional<LakeTableSnapshot> lakeSnapshot =
                zooKeeperClient.getLakeTableSnapshot(tablePartition.getTableId(), null);
        if (!lakeSnapshot.isPresent()) {
            return false;
        }
        for (Map.Entry<Integer, FrozenBucket> entry : frozenBuckets.entrySet()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tablePartition.getTableId(),
                            tablePartition.getPartitionId(),
                            entry.getKey());
            long tieredOffset = lakeSnapshot.get().getLogEndOffset(tableBucket).orElse(0L);
            if (tieredOffset < entry.getValue().getLogEndOffset()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            retainingPartitions.clear();
        }
    }

    private enum RetentionStage {
        FREEZING,
        FROZEN,
        LAKE_TIERED
    }

    private static class RetentionStatus {
        private RetentionStage stage = RetentionStage.FREEZING;
        private boolean operationRunning = true;
    }
}
