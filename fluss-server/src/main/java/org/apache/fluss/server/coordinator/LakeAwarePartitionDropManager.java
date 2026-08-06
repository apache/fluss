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
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFreezePartitionRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;

/** Coordinates freezing, lake tiering verification, and dropping expired partitions. */
public class LakeAwarePartitionDropManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeAwarePartitionDropManager.class);

    private final MetadataManager metadataManager;
    private final ZooKeeperClient zooKeeperClient;
    private final CoordinatorChannelManager coordinatorChannelManager;
    private final Executor executor;
    private final ConcurrentMap<PhysicalTablePath, PartitionDropState> partitionDropStates =
            new ConcurrentHashMap<>();

    public LakeAwarePartitionDropManager(
            MetadataManager metadataManager,
            ZooKeeperClient zooKeeperClient,
            CoordinatorChannelManager coordinatorChannelManager,
            Executor executor) {
        this.metadataManager = metadataManager;
        this.zooKeeperClient = zooKeeperClient;
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.executor = executor;
    }

    /** Try to drop an expired partition after its data has been tiered to the lake. */
    public void tryDrop(TableInfo tableInfo, String partitionName) {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(tableInfo.getTablePath(), partitionName);
        PartitionDropState dropState =
                partitionDropStates.computeIfAbsent(
                        physicalTablePath, ignored -> new PartitionDropState(physicalTablePath));
        submitAsync(dropState);
    }

    private void submitAsync(PartitionDropState dropState) {
        try {
            executor.execute(() -> runInternal(dropState));
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to submit lake-aware drop for partition {} of table {}.",
                    dropState.physicalTablePath.getPartitionName(),
                    dropState.physicalTablePath.getTablePath(),
                    e);
        }
    }

    private void runInternal(PartitionDropState dropState) {
        if (!dropState.lock.tryLock()) {
            LOG.debug(
                    "Skip dropping partition {} of table {} because another operation is running.",
                    dropState.physicalTablePath.getPartitionName(),
                    dropState.physicalTablePath.getTablePath());
            return;
        }

        try {
            // Ignore a queued task after its drop state has been removed or replaced.
            if (partitionDropStates.get(dropState.physicalTablePath) != dropState) {
                return;
            }

            // Check the partition existence from metadata.
            Optional<PartitionRegistration> registration = getPartitionRegistration(dropState);
            if (!registration.isPresent()) {
                removeDropState(dropState);
                return;
            }

            toNext(dropState, registration.get());
        } finally {
            dropState.lock.unlock();
        }
    }

    private void toNext(PartitionDropState dropState, PartitionRegistration partitionRegistration) {
        PartitionStatus status = dropState.status;
        switch (status) {
            case FREEZING:
                freezePartition(dropState, partitionRegistration);
                break;
            case FROZEN:
                checkLakeTiered(dropState, partitionRegistration);
                break;
            case LAKE_TIERED:
                dropPartition(dropState, partitionRegistration);
                break;
            default:
                throw new FlussRuntimeException("Unknown partition status " + status);
        }
    }

    private void freezePartition(
            PartitionDropState dropState, PartitionRegistration partitionRegistration) {
        try {
            // Update the partition registration in ZooKeeper before fencing replica writes.
            if (!partitionRegistration.isFrozen()) {
                Optional<PartitionRegistration> frozenRegistration =
                        metadataManager.markPartitionFrozen(
                                dropState.physicalTablePath.getTablePath(),
                                dropState.physicalTablePath.getPartitionName(),
                                partitionRegistration.getTableId(),
                                partitionRegistration.getPartitionId());
                if (!frozenRegistration.isPresent()) {
                    LOG.info(
                            "Stop dropping partition {} because its registration no longer matches.",
                            dropState);
                    removeDropState(dropState);
                    return;
                }
                partitionRegistration = frozenRegistration.get();
            }

            // Send freeze requests to the partition's replica leaders.
            freezeReplicaLeaders(dropState, partitionRegistration);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to prepare partition {} of table {} for lake-aware drop.",
                    dropState.physicalTablePath.getPartitionName(),
                    dropState.physicalTablePath.getTablePath(),
                    e);
        }
    }

    private void freezeReplicaLeaders(
            PartitionDropState dropState, PartitionRegistration partitionRegistration) {
        try {
            PartitionAssignment partitionAssignment =
                    zooKeeperClient
                            .getPartitionAssignment(partitionRegistration.getPartitionId())
                            .orElse(null);
            if (partitionAssignment == null) {
                LOG.info(
                        "Waiting to drop partition {} of table {} because its assignment is unavailable.",
                        dropState.physicalTablePath.getPartitionName(),
                        dropState.physicalTablePath.getTablePath());
                return;
            }

            List<TableBucket> tableBuckets = new ArrayList<>();
            for (Integer bucketId : partitionAssignment.getBucketAssignments().keySet()) {
                tableBuckets.add(
                        new TableBucket(
                                partitionRegistration.getTableId(),
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
                        "Waiting to drop partition {} of table {} because not all bucket leaders are available.",
                        dropState.physicalTablePath.getPartitionName(),
                        dropState.physicalTablePath.getTablePath());
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
                FreezePartitionRequest request = makeFreezePartitionRequest(entry.getValue());
                freezeFutures.add(
                        coordinatorChannelManager.sendFreezePartitionRequest(
                                entry.getKey(), request));
            }

            // TODO: Add timeout.
            CompletableFuture.allOf(
                            freezeFutures.toArray(new CompletableFuture[freezeFutures.size()]))
                    .join();
            finishFreeze(dropState, partitionRegistration, leaders, freezeFutures);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to freeze partition {} of table {} before dropping it.",
                    dropState.physicalTablePath.getPartitionName(),
                    dropState.physicalTablePath.getTablePath(),
                    e);
        }
    }

    private void finishFreeze(
            PartitionDropState dropState,
            PartitionRegistration partitionRegistration,
            Map<TableBucket, LeaderAndIsr> expectedLeaders,
            List<CompletableFuture<FreezePartitionResponse>> freezeFutures) {
        Optional<Map<Integer, Long>> frozenOffsets =
                collectStableFrozenOffsets(expectedLeaders, freezeFutures);
        if (!frozenOffsets.isPresent()) {
            LOG.info(
                    "Partition {} of table {} is frozen and waiting for all bucket writes to become committed.",
                    dropState.physicalTablePath.getPartitionName(),
                    dropState.physicalTablePath.getTablePath());
            return;
        }

        dropState.frozenOffsets = frozenOffsets.get();
        dropState.status = PartitionStatus.FROZEN;
        LOG.info(
                "Partition {} of table {} is frozen and ready for the lake tiering check.",
                dropState.physicalTablePath.getPartitionName(),
                dropState.physicalTablePath.getTablePath());
        toNext(dropState, partitionRegistration);
    }

    private void checkLakeTiered(PartitionDropState dropState, PartitionRegistration registration) {
        try {
            if (!registration.isFrozen()) {
                dropState.status = PartitionStatus.FREEZING;
                toNext(dropState, registration);
                return;
            }

            Map<Integer, Long> frozenOffsets = dropState.frozenOffsets;
            TablePartition tablePartition = registration.toTablePartition();
            if (frozenOffsets.isEmpty()) {
                removeDropState(dropState);
                return;
            }

            if (!isFullyTiered(tablePartition, frozenOffsets)) {
                LOG.info(
                        "Partition {} of table {} is frozen and waiting for lake tiering.",
                        dropState.physicalTablePath.getPartitionName(),
                        dropState.physicalTablePath.getTablePath());
                return;
            }

            dropState.status = PartitionStatus.LAKE_TIERED;
            toNext(dropState, registration);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to check lake tiering for partition {} of table {}.",
                    dropState.physicalTablePath.getPartitionName(),
                    dropState.physicalTablePath.getTablePath(),
                    e);
        }
    }

    private void dropPartition(PartitionDropState dropState, PartitionRegistration registration) {
        String partitionName = dropState.physicalTablePath.getPartitionName();

        try {
            if (!registration.isFrozen()) {
                dropState.status = PartitionStatus.FREEZING;
                toNext(dropState, registration);
                return;
            }
            boolean dropped =
                    metadataManager.dropFrozenPartition(
                            dropState.physicalTablePath.getTablePath(),
                            dropState.physicalTablePath.getPartitionName(),
                            registration.getTableId(),
                            registration.getPartitionId());
            if (dropped) {
                LOG.info(
                        "Deleted partition {} of table {} after all frozen offsets were committed to the lake.",
                        partitionName,
                        dropState.physicalTablePath.getTablePath());
                removeDropState(dropState);
            } else {
                LOG.warn(
                        "Failed to drop partition {} of table {}.",
                        partitionName,
                        dropState.physicalTablePath.getTablePath());
            }
        } catch (Exception e) {
            LOG.error(
                    "Failed to drop partition {} of table {}.",
                    partitionName,
                    dropState.physicalTablePath.getTablePath(),
                    e);
        }
    }

    private Optional<PartitionRegistration> getPartitionRegistration(PartitionDropState dropState) {
        return metadataManager.getPartitionRegistration(
                dropState.physicalTablePath.getTablePath(),
                dropState.physicalTablePath.getPartitionName());
    }

    private void removeDropState(PartitionDropState dropState) {
        partitionDropStates.remove(dropState.physicalTablePath, dropState);
    }

    private Optional<Map<Integer, Long>> collectStableFrozenOffsets(
            Map<TableBucket, LeaderAndIsr> expectedLeaders,
            List<CompletableFuture<FreezePartitionResponse>> freezeFutures) {
        Map<Integer, Long> frozenOffsets = new HashMap<>();
        boolean allWritesCommitted = true;
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
                if (bucketResponse.getHighWatermark() != bucketResponse.getLogEndOffset()) {
                    allWritesCommitted = false;
                }
                frozenOffsets.put(tableBucket.getBucket(), bucketResponse.getLogEndOffset());
            }
        }
        if (frozenOffsets.size() != expectedLeaders.size()) {
            throw new FlussRuntimeException(
                    String.format(
                            "Only %s of %s partition buckets were frozen.",
                            frozenOffsets.size(), expectedLeaders.size()));
        }
        return allWritesCommitted
                ? Optional.of(Collections.unmodifiableMap(frozenOffsets))
                : Optional.empty();
    }

    private boolean isFullyTiered(TablePartition tablePartition, Map<Integer, Long> frozenOffsets)
            throws Exception {
        Optional<LakeTableSnapshot> lakeSnapshot =
                zooKeeperClient.getLakeTableSnapshot(tablePartition.getTableId(), null);
        if (!lakeSnapshot.isPresent()) {
            return false;
        }
        for (Map.Entry<Integer, Long> entry : frozenOffsets.entrySet()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tablePartition.getTableId(),
                            tablePartition.getPartitionId(),
                            entry.getKey());
            long tieredOffset = lakeSnapshot.get().getLogEndOffset(tableBucket).orElse(0L);
            if (tieredOffset < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        List<PartitionDropState> dropStates = new ArrayList<>(partitionDropStates.values());
        partitionDropStates.clear();
        for (PartitionDropState dropState : dropStates) {
            // Wait until any task that passed the map identity check has finished.
            dropState.lock.lock();
            dropState.lock.unlock();
        }
    }

    private enum PartitionStatus {
        FREEZING,
        FROZEN,
        LAKE_TIERED
    }

    private static final class PartitionDropState {
        private final ReentrantLock lock = new ReentrantLock();
        private final PhysicalTablePath physicalTablePath;
        private PartitionStatus status = PartitionStatus.FREEZING;
        private Map<Integer, Long> frozenOffsets = Collections.emptyMap();

        private PartitionDropState(PhysicalTablePath physicalTablePath) {
            this.physicalTablePath = physicalTablePath;
        }

        @Override
        public String toString() {
            return physicalTablePath.toString();
        }
    }
}
