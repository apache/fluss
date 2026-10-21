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

import org.apache.fluss.exception.CoordinatorEpochFencedException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.messages.FreezePartitionResponse;
import org.apache.fluss.rpc.messages.PbFrozenBucketTiered;
import org.apache.fluss.rpc.messages.ReportFrozenBucketsTieredRequest;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.utils.concurrent.FutureUtils;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFreezePartitionRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;

/**
 * Freezes expired partitions and deletes them after all bucket leaders report completed lake
 * tiering.
 */
public class LakeAwarePartitionDropManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeAwarePartitionDropManager.class);

    private final MetadataManager metadataManager;
    private final ZooKeeperClient zooKeeperClient;
    private final CoordinatorChannelManager coordinatorChannelManager;
    private final Executor executor;
    private final long freezeTimeoutMs;
    private final ConcurrentMap<TablePartition, PartitionDropState> partitionDropStates =
            new ConcurrentHashMap<>();
    private volatile boolean closed;

    public LakeAwarePartitionDropManager(
            MetadataManager metadataManager,
            ZooKeeperClient zooKeeperClient,
            CoordinatorChannelManager coordinatorChannelManager,
            Executor executor,
            long freezeTimeoutMs) {
        this.metadataManager = metadataManager;
        this.zooKeeperClient = zooKeeperClient;
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.executor = executor;
        this.freezeTimeoutMs = freezeTimeoutMs;
    }

    /**
     * Initiates an idempotent freeze; tablet servers subsequently report completed lake tiering.
     */
    public void tryDrop(TableInfo tableInfo, String partitionName) {
        if (closed) {
            return;
        }
        try {
            executor.execute(
                    () -> {
                        try {
                            PhysicalTablePath path =
                                    PhysicalTablePath.of(tableInfo.getTablePath(), partitionName);
                            Optional<PartitionRegistration> registration =
                                    metadataManager.getOptionalPartitionRegistration(
                                            path.getTablePath(), partitionName);
                            if (!registration.isPresent()
                                    || registration.get().getTableId() != tableInfo.getTableId()) {
                                return;
                            }
                            PartitionDropState state = getOrCreateState(registration.get());
                            if (!state.lock.tryLock()) {
                                return;
                            }
                            try {
                                if (!closed) {
                                    freezePartition(path, registration.get());
                                }
                            } finally {
                                state.lock.unlock();
                            }
                        } catch (Exception e) {
                            LOG.warn(
                                    "Failed to freeze partition {} of table {}.",
                                    partitionName,
                                    tableInfo.getTablePath(),
                                    e);
                        }
                    });
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to submit freeze for partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void freezePartition(PhysicalTablePath path, PartitionRegistration registration)
            throws Exception {
        if (!registration.isFrozen()) {
            Optional<PartitionRegistration> frozenRegistration =
                    metadataManager.markPartitionFrozen(
                            path.getTablePath(),
                            path.getPartitionName(),
                            registration.getTableId(),
                            registration.getPartitionId());
            if (!frozenRegistration.isPresent()) {
                return;
            }
        }
        Optional<PartitionAssignment> assignment =
                zooKeeperClient.getPartitionAssignment(registration.getPartitionId());
        if (!assignment.isPresent()) {
            return;
        }
        List<TableBucket> buckets = new ArrayList<>();
        for (Integer bucketId : assignment.get().getBucketAssignments().keySet()) {
            buckets.add(
                    new TableBucket(
                            registration.getTableId(), registration.getPartitionId(), bucketId));
        }
        Map<Integer, Map<TableBucket, Integer>> requests = new HashMap<>();
        zooKeeperClient
                .getLeaderAndIsrs(buckets)
                .forEach(
                        (bucket, leader) -> {
                            if (leader.leader() != LeaderAndIsr.NO_LEADER) {
                                requests.computeIfAbsent(
                                                leader.leader(), ignored -> new HashMap<>())
                                        .put(bucket, leader.leaderEpoch());
                            }
                        });
        for (Map.Entry<Integer, Map<TableBucket, Integer>> entry : requests.entrySet()) {
            CompletableFuture<FreezePartitionResponse> response =
                    coordinatorChannelManager.sendFreezePartitionRequest(
                            entry.getKey(), makeFreezePartitionRequest(entry.getValue()));
            FutureUtils.orTimeout(
                            response,
                            freezeTimeoutMs,
                            TimeUnit.MILLISECONDS,
                            "Freeze partition request timed out")
                    .whenComplete(
                            (result, error) -> {
                                if (error != null) {
                                    LOG.warn(
                                            "Failed to freeze partition {} on server {}.",
                                            path,
                                            entry.getKey(),
                                            error);
                                } else {
                                    result.getBucketsRespsList()
                                            .forEach(
                                                    bucket -> {
                                                        if (bucket.hasErrorCode()) {
                                                            LOG.warn(
                                                                    "Failed to freeze bucket {}: {}.",
                                                                    toTableBucket(
                                                                            bucket
                                                                                    .getTableBucket()),
                                                                    bucket.getErrorMessage());
                                                        }
                                                    });
                                }
                            });
        }
    }

    /**
     * Aggregates reports on the coordinator event thread, which serializes leader changes with
     * validation and deletion. Reports are repeated until the partition disappears.
     */
    void reportTiered(ReportFrozenBucketsTieredRequest request, CoordinatorContext context) {
        if (closed) {
            return;
        }
        if (request.getCoordinatorEpoch() != context.getCoordinatorEpoch()) {
            throw new CoordinatorEpochFencedException(
                    "Stale coordinator epoch in frozen bucket report.");
        }
        // Forget reports for partitions removed independently of auto retention.
        partitionDropStates
                .keySet()
                .removeIf(partition -> !context.containsPartitionId(partition.getPartitionId()));
        Map<TablePartition, Map<Integer, Integer>> reportsByPartition = new HashMap<>();
        for (PbFrozenBucketTiered report : request.getBucketsList()) {
            TableBucket bucket = toTableBucket(report.getTableBucket());
            Optional<LeaderAndIsr> leader = context.getBucketLeaderAndIsr(bucket);
            if (bucket.getPartitionId() == null
                    || !leader.isPresent()
                    || context.isToBeDeleted(bucket)) {
                continue;
            }
            if (leader.get().leader() != request.getServerId()
                    || leader.get().leaderEpoch() != report.getLeaderEpoch()) {
                // A stale item must not prevent other buckets in the batch from progressing.
                LOG.debug(
                        "Ignoring stale tiering report for bucket {} from server {}.",
                        bucket,
                        request.getServerId());
                continue;
            }
            reportsByPartition
                    .computeIfAbsent(
                            new TablePartition(bucket.getTableId(), bucket.getPartitionId()),
                            ignored -> new HashMap<>())
                    .put(bucket.getBucket(), report.getLeaderEpoch());
        }
        for (Map.Entry<TablePartition, Map<Integer, Integer>> entry :
                reportsByPartition.entrySet()) {
            TablePartition partition = entry.getKey();
            Optional<PhysicalTablePath> path =
                    context.getPhysicalTablePath(partition.getPartitionId());
            if (!path.isPresent()) {
                continue;
            }
            Optional<PartitionRegistration> registration =
                    metadataManager.getOptionalPartitionRegistration(
                            path.get().getTablePath(), path.get().getPartitionName());
            if (!registration.isPresent()
                    || !registration.get().isFrozen()
                    || registration.get().getTableId() != partition.getTableId()
                    || registration.get().getPartitionId() != partition.getPartitionId()) {
                continue;
            }
            PartitionDropState state = getOrCreateState(registration.get());
            state.lock.lock();
            try {
                if (closed) {
                    return;
                }
                state.tieredLeaderEpochs.putAll(entry.getValue());
                Map<Integer, List<Integer>> assignment = context.getPartitionAssignment(partition);
                if (assignment.isEmpty()) {
                    continue;
                }
                boolean allTiered = true;
                for (Integer bucketId : assignment.keySet()) {
                    Optional<LeaderAndIsr> currentLeader =
                            context.getBucketLeaderAndIsr(
                                    new TableBucket(
                                            partition.getTableId(),
                                            partition.getPartitionId(),
                                            bucketId));
                    if (!currentLeader.isPresent()
                            || currentLeader.get().leader() == LeaderAndIsr.NO_LEADER
                            || !Integer.valueOf(currentLeader.get().leaderEpoch())
                                    .equals(state.tieredLeaderEpochs.get(bucketId))) {
                        state.tieredLeaderEpochs.remove(bucketId);
                        allTiered = false;
                    }
                }
                if (allTiered
                        && metadataManager.dropFrozenPartition(
                                path.get().getTablePath(), path.get().getPartitionName(),
                                partition.getTableId(), partition.getPartitionId())) {
                    partitionDropStates.remove(partition, state);
                    LOG.info(
                            "Deleted frozen partition {} after all bucket leaders reported completed lake tiering.",
                            path.get());
                }
            } finally {
                state.lock.unlock();
            }
        }
    }

    private PartitionDropState getOrCreateState(PartitionRegistration registration) {
        return partitionDropStates.computeIfAbsent(
                registration.toTablePartition(), ignored -> new PartitionDropState());
    }

    @Override
    public void close() {
        closed = true;
        for (PartitionDropState state : partitionDropStates.values()) {
            state.lock.lock();
            state.lock.unlock();
        }
        partitionDropStates.clear();
    }

    private static final class PartitionDropState {
        private final ReentrantLock lock = new ReentrantLock();
        private final Map<Integer, Integer> tieredLeaderEpochs = new HashMap<>();
    }
}
