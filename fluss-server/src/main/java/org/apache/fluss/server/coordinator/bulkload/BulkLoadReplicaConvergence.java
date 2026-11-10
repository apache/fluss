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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Confirms imported snapshots on the exact current leaders captured for a shared transaction. */
final class BulkLoadReplicaConvergence {
    private final CoordinatorChannelManager channelManager;

    BulkLoadReplicaConvergence(CoordinatorChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    CompletableFuture<Void> apply(
            BulkLoadTransaction transaction, ReplicaConvergenceContext round, int epoch) {
        Map<Integer, List<PbNotifyLeaderAndIsrReqForBucket>> requests = new HashMap<>();
        for (BulkLoadTarget target : transaction.getTargets()) {
            TableAssignment assignment =
                    round.assignmentFor(transaction.getTableId(), target).getValue();
            long[] snapshots = target.getSnapshotIds();
            for (int id = 0; id < snapshots.length; id++) {
                TableBucket bucket =
                        new TableBucket(transaction.getTableId(), target.getPartitionId(), id);
                LeaderAndIsr role = round.leaders.get(bucket).getValue();
                checkState(
                        round.holders.containsKey(role.leader()), "Current leader is unavailable.");
                requests.computeIfAbsent(role.leader(), ignored -> new ArrayList<>())
                        .add(
                                makeNotifyBucketLeaderAndIsr(
                                        new NotifyLeaderAndIsrData(
                                                target.getTarget(),
                                                bucket,
                                                assignment.getBucketAssignment(id).getReplicas(),
                                                role,
                                                snapshots.length,
                                                round.bucketCountEpoch,
                                                snapshots[id])));
            }
        }
        List<CompletableFuture<Void>> results = new ArrayList<>();
        for (Map.Entry<Integer, List<PbNotifyLeaderAndIsrReqForBucket>> entry :
                requests.entrySet()) {
            CompletableFuture<NotifyLeaderAndIsrResponse> response = new CompletableFuture<>();
            channelManager.sendBucketLeaderAndIsrRequest(
                    entry.getKey(),
                    makeNotifyLeaderAndIsrRequest(epoch, entry.getValue()),
                    (value, failure) -> {
                        if (failure == null) {
                            response.complete(value);
                        } else {
                            response.completeExceptionally(failure);
                        }
                    });
            Set<TableBucket> expected = new HashSet<>();
            for (PbNotifyLeaderAndIsrReqForBucket bucket : entry.getValue()) {
                expected.add(toTableBucket(bucket.getTableBucket()));
            }
            long serverSessionId = round.holders.get(entry.getKey()).getEphemeralOwner();
            results.add(
                    response.thenAccept(
                            value -> {
                                if (!value.hasServerSessionId()
                                        || value.getServerSessionId() != serverSessionId) {
                                    throw new TimeoutException(
                                            "Restoration acknowledgement does not match the current TabletServer session.");
                                }
                                checkState(
                                        value.getNotifyBucketsLeaderRespsCount() == expected.size(),
                                        "Incomplete restoration acknowledgement.");
                                for (PbNotifyLeaderAndIsrRespForBucket item :
                                        value.getNotifyBucketsLeaderRespsList()) {
                                    TableBucket bucket = toTableBucket(item.getTableBucket());
                                    if (item.hasErrorCode() || !expected.remove(bucket)) {
                                        throw notifyFailure(bucket, item);
                                    }
                                }
                            }));
        }
        return CompletableFuture.allOf(results.toArray(new CompletableFuture<?>[0]));
    }

    static FlussRuntimeException notifyFailure(
            TableBucket bucket, PbNotifyLeaderAndIsrRespForBucket item) {
        String message = "Snapshot restoration failed for " + bucket + '.';
        return item.hasErrorCode()
                ? new FlussRuntimeException(
                        message,
                        new ApiError(
                                        Errors.forCode((short) item.getErrorCode()),
                                        item.hasErrorMessage() ? item.getErrorMessage() : null)
                                .exception())
                : new FlussRuntimeException(message);
    }

    /** Immutable ZooKeeper observations authorizing this round and its terminal write. */
    static final class ReplicaConvergenceContext {
        final Map<String, Versioned<? extends TableAssignment>> assignments;
        final Map<TableBucket, Versioned<LeaderAndIsr>> leaders;
        final Map<Integer, Versioned<TabletServerRegistration>> holders;
        final long bucketCountEpoch;

        ReplicaConvergenceContext(
                List<Versioned<? extends TableAssignment>> assignments,
                Map<TableBucket, Versioned<LeaderAndIsr>> leaders,
                Map<Integer, Versioned<TabletServerRegistration>> holders,
                long bucketCountEpoch) {
            Map<String, Versioned<? extends TableAssignment>> byPath = new HashMap<>();
            for (Versioned<? extends TableAssignment> assignment : assignments) {
                byPath.put(assignment.getPath(), assignment);
            }
            this.assignments = Collections.unmodifiableMap(byPath);
            this.leaders = Collections.unmodifiableMap(new HashMap<>(leaders));
            this.holders = Collections.unmodifiableMap(new HashMap<>(holders));
            this.bucketCountEpoch = bucketCountEpoch;
        }

        Versioned<? extends TableAssignment> assignmentFor(long tableId, BulkLoadTarget target) {
            String path =
                    target.getPartitionId() == null
                            ? org.apache.fluss.server.zk.data.ZkData.TableIdZNode.path(tableId)
                            : org.apache.fluss.server.zk.data.ZkData.PartitionIdZNode.path(
                                    target.getPartitionId());
            return assignments.get(path);
        }
    }
}
