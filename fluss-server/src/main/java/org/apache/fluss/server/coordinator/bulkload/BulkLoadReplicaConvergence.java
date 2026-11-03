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
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaRespForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.metadata.RegisteredTabletServer;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.tablet.bulkload.BulkLoadTargetMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeStopBucketReplica;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Converges one BulkLoad target using only ordinary TabletServer metadata and replica RPCs. */
final class BulkLoadReplicaConvergence {

    private final CoordinatorContext context;
    private final CoordinatorChannelManager channelManager;

    BulkLoadReplicaConvergence(
            CoordinatorContext context, CoordinatorChannelManager channelManager) {
        this.context = context;
        this.channelManager = channelManager;
    }

    /** Applies one BulkLoad replica action using the current durable transaction and assignment. */
    CompletableFuture<Void> apply(
            BulkLoadTransaction transaction,
            ReplicaConvergenceContext round,
            BulkLoadReplicaAction action) {
        captureBucketLeaderAndIsr(transaction, round);
        Versioned<? extends TableAssignment> assignment = round.assignment;
        List<RegisteredTabletServer> registrations = round.recipients;
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        for (RegisteredTabletServer registration : registrations) {
            List<PbNotifyLeaderAndIsrReqForBucket> notifications = new ArrayList<>();
            List<PbStopReplicaReqForBucket> localOnlyDeletes = new ArrayList<>();
            for (Map.Entry<TableBucket, LeaderAndIsr> entry : round.bucketLeaderAndIsr.entrySet()) {
                TableBucket bucket = entry.getKey();
                List<Integer> replicas =
                        assignment.getValue().getBucketAssignment(bucket.getBucket()).getReplicas();
                if (replicas.contains(registration.getServerId())) {
                    notifications.add(
                            makeNotifyBucketLeaderAndIsr(
                                    new NotifyLeaderAndIsrData(
                                            transaction.getHandle().getTarget(),
                                            bucket,
                                            replicas,
                                            entry.getValue(),
                                            assignment.getValue().getBuckets().size(),
                                            context.getTableInfoById(transaction.getTableId())
                                                    .getBucketCountEpoch(),
                                            target(transaction, bucket, action.targetState()),
                                            action.requiresEmptyTarget())));
                } else if (action.shouldCleanUnassignedReplicas()) {
                    // Nodes outside the assignment must stop serving stale local replicas.
                    // Keep remote data available to the assigned replicas during recovery.
                    localOnlyDeletes.add(
                            makeStopBucketReplica(
                                    bucket, true, false, entry.getValue().leaderEpoch()));
                }
            }
            if (!notifications.isEmpty()) {
                acknowledgements.add(sendNotify(registration.getServerId(), notifications));
            }
            if (!localOnlyDeletes.isEmpty()) {
                acknowledgements.add(sendStop(registration.getServerId(), localOnlyDeletes));
            }
        }
        return complete(acknowledgements);
    }

    /**
     * Builds the failure for a rejected bucket, keeping the recipient's error type so that an
     * empty-target rejection still maps to a TARGET_NOT_EMPTY abort decision.
     */
    static FlussRuntimeException notifyFailure(
            TableBucket bucket, PbNotifyLeaderAndIsrRespForBucket item) {
        String message =
                "NotifyLeaderAndIsr failed for "
                        + bucket
                        + (item.hasErrorMessage() ? ": " + item.getErrorMessage() : ".");
        if (!item.hasErrorCode()) {
            return new FlussRuntimeException(message);
        }
        ApiError error =
                new ApiError(
                        Errors.forCode((short) item.getErrorCode()),
                        item.hasErrorMessage() ? item.getErrorMessage() : null);
        return new FlussRuntimeException(message, error.exception());
    }

    private CompletableFuture<Void> sendNotify(
            int serverId, List<PbNotifyLeaderAndIsrReqForBucket> notifications) {
        NotifyLeaderAndIsrRequest request =
                makeNotifyLeaderAndIsrRequest(context.getCoordinatorEpoch(), notifications);
        return BulkLoadReplicaConvergence.<NotifyLeaderAndIsrResponse>request(
                        callback ->
                                channelManager.sendBucketLeaderAndIsrRequest(
                                        serverId, request, callback))
                .thenAccept(
                        response -> {
                            checkState(
                                    response.getNotifyBucketsLeaderRespsCount()
                                            == notifications.size(),
                                    "Incomplete NotifyLeaderAndIsr acknowledgement.");
                            Set<TableBucket> expected = new HashSet<>();
                            for (PbNotifyLeaderAndIsrReqForBucket item : notifications) {
                                expected.add(toTableBucket(item.getTableBucket()));
                            }
                            for (PbNotifyLeaderAndIsrRespForBucket item :
                                    response.getNotifyBucketsLeaderRespsList()) {
                                TableBucket bucket = toTableBucket(item.getTableBucket());
                                if (item.hasErrorCode() || !expected.remove(bucket)) {
                                    throw notifyFailure(bucket, item);
                                }
                            }
                        });
    }

    private CompletableFuture<Void> sendStop(
            int serverId, List<PbStopReplicaReqForBucket> buckets) {
        StopReplicaRequest request =
                new StopReplicaRequest().setCoordinatorEpoch(context.getCoordinatorEpoch());
        request.addAllStopReplicasReqs(buckets);
        return BulkLoadReplicaConvergence.<StopReplicaResponse>request(
                        callback ->
                                channelManager.sendStopBucketReplicaRequest(
                                        serverId, request, callback))
                .thenAccept(
                        response -> {
                            checkState(
                                    response.getStopReplicasRespsCount() == buckets.size(),
                                    "Incomplete StopReplica acknowledgement.");
                            Set<TableBucket> expected = new HashSet<>();
                            for (PbStopReplicaReqForBucket item : buckets) {
                                expected.add(toTableBucket(item.getTableBucket()));
                            }
                            for (PbStopReplicaRespForBucket item :
                                    response.getStopReplicasRespsList()) {
                                TableBucket bucket = toTableBucket(item.getTableBucket());
                                checkState(
                                        !item.hasErrorCode() && expected.remove(bucket),
                                        "StopReplica failed for %s.",
                                        bucket);
                            }
                        });
    }

    private static <T> CompletableFuture<T> request(Consumer<BiConsumer<T, Throwable>> sender) {
        CompletableFuture<T> result = new CompletableFuture<>();
        sender.accept(
                (response, failure) -> {
                    if (failure == null) {
                        result.complete(response);
                    } else {
                        result.completeExceptionally(failure);
                    }
                });
        return result;
    }

    private CompletableFuture<Void> complete(
            List<? extends CompletableFuture<?>> acknowledgements) {
        return CompletableFuture.allOf(acknowledgements.toArray(new CompletableFuture<?>[0]));
    }

    void captureBucketLeaderAndIsr(
            BulkLoadTransaction transaction, ReplicaConvergenceContext round) {
        for (Integer bucketId : round.assignment.getValue().getBuckets()) {
            TableBucket bucket =
                    new TableBucket(
                            transaction.getTableId(), transaction.getPartitionId(), bucketId);
            LeaderAndIsr role =
                    context.getBucketLeaderAndIsr(bucket)
                            .orElseThrow(
                                    () ->
                                            new TimeoutException(
                                                    "Missing current role for " + bucket + '.'));
            round.bucketLeaderAndIsr.put(bucket, role);
        }
    }

    static List<RegisteredTabletServer> liveRecipients(
            CoordinatorContext context, List<RegisteredTabletServer> registrations) {
        List<RegisteredTabletServer> recipients = new ArrayList<>();
        for (RegisteredTabletServer registration : registrations) {
            ServerInfo live = context.getLiveTabletServers().get(registration.getServerId());
            RegisteredTabletServer bound =
                    context.getBoundTabletServerRegistrations().get(registration.getServerId());
            if (live != null
                    && bound != null
                    && registration.matchesLive(live)
                    && registration.matchesRegistration(bound)) {
                recipients.add(registration);
            }
        }
        return Collections.unmodifiableList(recipients);
    }

    private static BulkLoadTargetMetadata target(
            BulkLoadTransaction transaction, TableBucket bucket, BulkLoadDataState state) {
        return new BulkLoadTargetMetadata(bucket, transaction.getMetadataVersion(), state);
    }

    /**
     * Replica operations for a BulkLoad target, derived without changing persisted target state.
     */
    enum BulkLoadReplicaAction {
        /** Fences assigned replicas in LOADING and requires empty-target validation. */
        FENCE_EMPTY_TARGET,

        /** Restores replicas after snapshot installation while the target remains LOADING. */
        RECOVER_LOADING_TARGET,

        /** Confirms replicas against the target's already persisted ACTIVE state. */
        CONFIRM_ACTIVE_TARGET;

        BulkLoadDataState targetState() {
            return this == CONFIRM_ACTIVE_TARGET
                    ? BulkLoadDataState.ACTIVE
                    : BulkLoadDataState.LOADING;
        }

        boolean requiresEmptyTarget() {
            return this == FENCE_EMPTY_TARGET;
        }

        boolean shouldCleanUnassignedReplicas() {
            return this != FENCE_EMPTY_TARGET;
        }
    }

    /** Assignment, holder sessions and bucket LeaderAndIsr captured for replica convergence. */
    static final class ReplicaConvergenceContext {
        final Versioned<? extends TableAssignment> assignment;
        final List<RegisteredTabletServer> recipients;
        final Map<Integer, RegisteredTabletServer> holders = new HashMap<>();
        private final Map<TableBucket, LeaderAndIsr> bucketLeaderAndIsr;

        ReplicaConvergenceContext(
                Versioned<? extends TableAssignment> assignment,
                List<RegisteredTabletServer> recipients,
                Map<TableBucket, LeaderAndIsr> bucketLeaderAndIsr) {
            this.assignment = assignment;
            this.recipients = recipients;
            this.bucketLeaderAndIsr = new HashMap<>(bucketLeaderAndIsr);
            Map<Integer, RegisteredTabletServer> byId = new HashMap<>();
            for (RegisteredTabletServer recipient : recipients) {
                byId.put(recipient.getServerId(), recipient);
            }
            for (int bucket : assignment.getValue().getBuckets()) {
                for (int id : assignment.getValue().getBucketAssignment(bucket).getReplicas()) {
                    holders.put(id, byId.get(id));
                }
            }
        }

        boolean matchesRoundFacts(
                Versioned<? extends TableAssignment> assignment,
                CoordinatorContext context,
                List<RegisteredTabletServer> registrations) {
            if (this.assignment.getVersion() != assignment.getVersion()) {
                return false;
            }
            for (Map.Entry<TableBucket, LeaderAndIsr> role : bucketLeaderAndIsr.entrySet()) {
                Optional<LeaderAndIsr> current = context.getBucketLeaderAndIsr(role.getKey());
                if (!current.isPresent() || !current.get().equals(role.getValue())) {
                    return false;
                }
            }
            Map<Integer, RegisteredTabletServer> currentById = new HashMap<>();
            for (RegisteredTabletServer registration : registrations) {
                currentById.put(registration.getServerId(), registration);
            }
            for (Map.Entry<Integer, RegisteredTabletServer> entry : this.holders.entrySet()) {
                RegisteredTabletServer expected = entry.getValue();
                RegisteredTabletServer current = currentById.get(entry.getKey());
                if (expected == null
                        ? current != null
                        : current == null || !expected.matchesRegistration(current)) {
                    return false;
                }
            }
            for (RegisteredTabletServer expected : recipients) {
                RegisteredTabletServer current = currentById.get(expected.getServerId());
                RegisteredTabletServer bound =
                        context.getBoundTabletServerRegistrations().get(expected.getServerId());
                ServerInfo live = context.getLiveTabletServers().get(expected.getServerId());
                if (current == null
                        || bound == null
                        || live == null
                        || !expected.matchesRegistration(current)
                        || !expected.matchesRegistration(bound)
                        || !current.matchesLive(live)) {
                    return false;
                }
            }
            return true;
        }
    }
}
