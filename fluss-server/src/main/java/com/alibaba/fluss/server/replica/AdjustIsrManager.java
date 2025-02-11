/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.OperationNotAttemptedException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.AdjustIsrRequest;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.entity.AdjustIsrResultForBucket;
import com.alibaba.fluss.server.utils.RpcMessageUtils;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.utils.MapUtils;
import com.alibaba.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Handles updating the ISR by sending {@link AdjustIsrRequest} to the coordinator server. Updating
 * the ISR is an asynchronous operation, so replica will learn about the result of their request
 * through a callback.
 *
 * <p>Note that ISR state changes can still be initiated by the coordinator server and sent to the
 * replicas via NotifyLeaderAndIsr requests.
 */
public class AdjustIsrManager {
    private static final Logger LOG = LoggerFactory.getLogger(AdjustIsrManager.class);

    private final CoordinatorGateway coordinatorGateway;
    private final Scheduler scheduler;
    private final int serverId;

    /** Used to allow only one pending adjust Isr request per bucket (visible for testing). */
    protected final Map<TableBucket, AdjustIsrItem> unsentAdjustIsrMap =
            MapUtils.newConcurrentHashMap();

    /** Used to allow only one in-flight request at a time. */
    private final AtomicBoolean inflightRequest = new AtomicBoolean(false);

    public AdjustIsrManager(
            Scheduler scheduler, CoordinatorGateway coordinatorGateway, int serverId) {
        this.coordinatorGateway = coordinatorGateway;
        this.scheduler = scheduler;
        this.serverId = serverId;
    }

    public CompletableFuture<LeaderAndIsr> submit(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr) {
        // TODO add coordinatorEpoch.
        CompletableFuture<LeaderAndIsr> future = new CompletableFuture<>();
        AdjustIsrItem adjustIsrItem = new AdjustIsrItem(tableBucket, leaderAndIsr, future);
        boolean enqueued = unsentAdjustIsrMap.putIfAbsent(tableBucket, adjustIsrItem) == null;
        if (enqueued) {
            // start send adjust isr request to coordinator.
            maybePropagateIsrAdjust();
        } else {
            future.completeExceptionally(
                    new OperationNotAttemptedException(
                            String.format(
                                    "Failed to enqueue ISR change state %s for bucket %s because there is already a "
                                            + "pending change isr request for that bucket.",
                                    leaderAndIsr, tableBucket)));
        }

        return future;
    }

    private void maybePropagateIsrAdjust() {
        // Send all pending items if there is not already a request in-flight.
        if (!unsentAdjustIsrMap.isEmpty() && inflightRequest.compareAndSet(false, true)) {
            // Copy current unsent ISRs but don't remove from the map, they get cleared in the
            // response callback.
            List<AdjustIsrItem> adjustIsrItemList = new ArrayList<>(unsentAdjustIsrMap.values());
            sendAdjustIsrRequest(adjustIsrItemList);
        }
    }

    protected void sendAdjustIsrRequest(List<AdjustIsrItem> adjustIsrItemList) {
        Map<TableBucket, LeaderAndIsr> isrMap = new HashMap<>();
        adjustIsrItemList.forEach(
                adjustIsrItem -> isrMap.put(adjustIsrItem.tableBucket, adjustIsrItem.leaderAndIsr));
        AdjustIsrRequest adjustIsrRequest = RpcMessageUtils.makeAdjustIsrRequest(serverId, isrMap);
        LOG.debug(
                "Sending adjust isr request {} to coordinator server from tablet server {}",
                adjustIsrRequest,
                serverId);

        // We will not time out AdjustIsrRequest, instead letting it retry indefinitely until a
        // response is received, or a new LeaderAndIsr overwrites the existing isrState which causes
        // the response for those replicas to be ignored.
        coordinatorGateway
                .adjustIsr(adjustIsrRequest)
                .whenComplete(
                        (response, exception) -> {
                            Errors errors;
                            try {
                                if (exception != null) {
                                    errors = Errors.forException(exception);
                                } else {
                                    handleAdjustIsrResponse(response, adjustIsrItemList);
                                    errors = Errors.NONE;
                                }
                            } finally {
                                // clear the flag so future requests can proceed.
                                clearInFlightRequest();
                            }

                            if (errors == Errors.NONE) {
                                maybePropagateIsrAdjust();
                            } else {
                                // If we received a top-level error from the coordinator, retry
                                // the request in near future.
                                scheduler.scheduleOnce(
                                        "send-adjust-isr", this::maybePropagateIsrAdjust, 50);
                            }
                        });
    }

    private void handleAdjustIsrResponse(
            AdjustIsrResponse response, List<AdjustIsrItem> adjustIsrItemList) {
        Map<TableBucket, AdjustIsrResultForBucket> resultForBucketMap =
                RpcMessageUtils.getAdjustIsrResponseData(response);
        // Iterate across the items we sent rather than what we received to ensure we run the
        // callback even if a replica was somehow erroneously excluded from the response. Note that
        // these callbacks are run from the leaderIsrUpdateLock write lock in
        // Replica#sendAdjustIsrRequest.
        adjustIsrItemList.forEach(
                adjustIsrItem -> {
                    TableBucket tableBucket = adjustIsrItem.tableBucket;
                    if (resultForBucketMap.containsKey(tableBucket)) {
                        unsentAdjustIsrMap.remove(tableBucket);
                        AdjustIsrResultForBucket resultForBucket =
                                resultForBucketMap.get(tableBucket);
                        if (resultForBucket.failed()) {
                            adjustIsrItem.future.completeExceptionally(
                                    resultForBucket.getError().exception());
                        } else {
                            adjustIsrItem.future.complete(resultForBucket.leaderAndIsr());
                        }
                    } else {
                        // Don't remove this replica from the update map, so it will get re-sent.
                        LOG.warn(
                                "Replica {} was sent in adjust isr request, but not included in the response",
                                tableBucket);
                    }
                });
    }

    protected void clearInFlightRequest() {
        if (!inflightRequest.compareAndSet(true, false)) {
            LOG.warn(
                    "Attempting to clear adjust isr in-flight flag when no apparent request is in-flight.");
        }
    }

    /** per bucket leader and isr data with future result callback. */
    @VisibleForTesting
    protected static class AdjustIsrItem {
        protected final TableBucket tableBucket;
        protected final LeaderAndIsr leaderAndIsr;
        protected final CompletableFuture<LeaderAndIsr> future;

        public AdjustIsrItem(
                TableBucket tableBucket,
                LeaderAndIsr leaderAndIsr,
                CompletableFuture<LeaderAndIsr> future) {
            this.tableBucket = tableBucket;
            this.leaderAndIsr = leaderAndIsr;
            this.future = future;
        }
    }
}
