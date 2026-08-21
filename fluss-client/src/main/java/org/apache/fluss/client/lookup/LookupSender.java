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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbValueList;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeLookupRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makePrefixLookupRequest;

/**
 * This background thread pool lookup operations from {@link #lookupQueue}, and send lookup requests
 * to the tablet server.
 */
@Internal
class LookupSender implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LookupSender.class);

    private volatile boolean running;

    /** true when the caller wants to ignore all unsent/inflight messages and force close. */
    private volatile boolean forceClose;

    private final MetadataUpdater metadataUpdater;

    private final LookupQueue lookupQueue;

    private final Semaphore maxInFlightRequestsSemaphore;

    private final int maxRetries;

    private final int maxRequestTimeoutMs;

    private final short acks;

    private final ExponentialBackoff historicalThrottleBackoff;

    LookupSender(
            MetadataUpdater metadataUpdater,
            LookupQueue lookupQueue,
            int maxFlightRequests,
            int maxRetries,
            short acks,
            int maxRequestTimeoutMs) {
        this.metadataUpdater = metadataUpdater;
        this.lookupQueue = lookupQueue;
        this.maxInFlightRequestsSemaphore = new Semaphore(maxFlightRequests);
        this.maxRetries = maxRetries;
        this.running = true;
        this.acks = acks;
        this.maxRequestTimeoutMs = maxRequestTimeoutMs;
        this.historicalThrottleBackoff = new ExponentialBackoff(100L, 2, 5000L, 0.2);
    }

    @Override
    public void run() {
        LOG.debug("Starting Fluss lookup sender thread.");

        // main loop, runs until close is called.
        while (running) {
            try {
                runOnce(false);
            } catch (Throwable t) {
                LOG.error("Uncaught error in Fluss lookup sender thread: ", t);
            }
        }

        LOG.debug("Beginning shutdown of Fluss lookup I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be requests in the accumulator or
        // waiting for acknowledgment, wait until these are completed.
        // TODO Check the in flight request count in the accumulator.
        while (!forceClose && lookupQueue.hasUnDrained()) {
            try {
                runOnce(true);
            } catch (Exception e) {
                LOG.error("Uncaught error in Fluss lookup sender thread: ", e);
            }
        }

        // TODO if force close failed, add logic to abort incomplete lookup requests.
        LOG.debug("Shutdown of Fluss lookup sender I/O thread has completed.");
    }

    /** Run a single iteration of sending. */
    private void runOnce(boolean drainAll) throws Exception {
        List<LookupBatch> lookupBatches = drainAll ? lookupQueue.drainAll() : lookupQueue.drain();
        Map<Tuple2<Integer, LookupType>, List<LookupBatch>> batchesByLeaderAndType =
                groupByLeaderAndType(lookupBatches);
        for (Map.Entry<Tuple2<Integer, LookupType>, List<LookupBatch>> entry :
                batchesByLeaderAndType.entrySet()) {
            sendLookupBatches(entry.getKey().f0, entry.getKey().f1, entry.getValue());
        }
    }

    private Map<Tuple2<Integer, LookupType>, List<LookupBatch>> groupByLeaderAndType(
            List<LookupBatch> lookupBatches) {
        Map<Tuple2<Integer, LookupType>, List<LookupBatch>> batchesByLeaderAndType =
                new LinkedHashMap<>();
        for (LookupBatch lookupBatch : lookupBatches) {
            AbstractLookupQuery<?> firstLookup = lookupBatch.lookups().get(0);
            int destination;
            try {
                destination =
                        metadataUpdater.leaderFor(
                                firstLookup.tablePath(), lookupBatch.tableBucket());
            } catch (PartitionNotExistException e) {
                lookupQueue.releaseBatchCapacity(lookupBatch);
                for (AbstractLookupQuery<?> lookup : lookupBatch.lookups()) {
                    lookup.future().completeExceptionally(e);
                }
                continue;
            } catch (Exception e) {
                LOG.warn(
                        "Failed to lookup the leader for {} when lookup",
                        lookupBatch.tableBucket(),
                        e);
                for (AbstractLookupQuery<?> lookup : lookupBatch.lookups()) {
                    reEnqueueLookup(lookup);
                }
                lookupQueue.releaseBatchCapacity(lookupBatch);
                continue;
            }

            batchesByLeaderAndType
                    .computeIfAbsent(
                            Tuple2.of(destination, lookupBatch.lookupType()),
                            ignored -> new ArrayList<>())
                    .add(lookupBatch);
        }
        return batchesByLeaderAndType;
    }

    private void sendLookupBatches(
            int destination, LookupType lookupType, List<LookupBatch> lookupBatches) {
        Map<Tuple2<Long, Boolean>, List<LookupBatch>> batchesByTableAndKind = new LinkedHashMap<>();
        for (LookupBatch lookupBatch : lookupBatches) {
            batchesByTableAndKind
                    .computeIfAbsent(
                            Tuple2.of(
                                    lookupBatch.tableBucket().getTableId(),
                                    lookupBatch.historical()),
                            ignored -> new ArrayList<>())
                    .add(lookupBatch);
        }

        for (List<LookupBatch> batchesForTableAndKind : batchesByTableAndKind.values()) {
            for (List<LookupBatch> requestBatches : packLookupBatches(batchesForTableAndKind)) {
                List<AbstractLookupQuery<?>> lookups = new ArrayList<>();
                for (LookupBatch requestBatch : requestBatches) {
                    lookups.addAll(requestBatch.lookups());
                }
                sendLookups(
                        destination,
                        lookupType,
                        lookups,
                        () -> {
                            for (LookupBatch requestBatch : requestBatches) {
                                lookupQueue.releaseBatchCapacity(requestBatch);
                            }
                        });
            }
        }
    }

    @VisibleForTesting
    List<List<LookupBatch>> packLookupBatches(List<LookupBatch> lookupBatches) {
        List<List<LookupBatch>> requestGroups = new ArrayList<>();
        List<LookupBatch> currentGroup = new ArrayList<>();
        int currentSize = 0;
        for (LookupBatch lookupBatch : lookupBatches) {
            if (!currentGroup.isEmpty()
                    && currentSize + lookupBatch.size() > lookupQueue.maxBatchSize()) {
                requestGroups.add(currentGroup);
                currentGroup = new ArrayList<>();
                currentSize = 0;
            }
            currentGroup.add(lookupBatch);
            currentSize += lookupBatch.size();
        }
        if (!currentGroup.isEmpty()) {
            requestGroups.add(currentGroup);
        }
        return requestGroups;
    }

    @VisibleForTesting
    void sendLookups(int destination, LookupType lookupType, List<AbstractLookupQuery<?>> lookups) {
        sendLookups(destination, lookupType, lookups, null);
    }

    private void sendLookups(
            int destination,
            LookupType lookupType,
            List<AbstractLookupQuery<?>> lookups,
            @Nullable Runnable releaseOriginalBatchCapacity) {
        if (lookupType == LookupType.LOOKUP) {
            sendLookupRequest(destination, lookups, false, releaseOriginalBatchCapacity);
        } else if (lookupType == LookupType.LOOKUP_WITH_INSERT_IF_NOT_EXISTS) {
            sendLookupRequest(destination, lookups, true, releaseOriginalBatchCapacity);
        } else if (lookupType == LookupType.PREFIX_LOOKUP) {
            sendPrefixLookupRequest(destination, lookups, releaseOriginalBatchCapacity);
        } else {
            throw new IllegalArgumentException("Unsupported lookup type: " + lookupType);
        }
    }

    private void sendLookupRequest(
            int destination,
            List<AbstractLookupQuery<?>> lookups,
            boolean insertIfNotExists,
            @Nullable Runnable releaseOriginalBatchCapacity) {
        // table id -> (bucket and original partition name -> lookups)
        Map<Long, Map<LookupBatchKey, LookupRequestBatch>> lookupByTableId = new LinkedHashMap<>();
        for (AbstractLookupQuery<?> abstractLookupQuery : lookups) {
            LookupQuery lookup = (LookupQuery) abstractLookupQuery;
            TableBucket tb = lookup.tableBucket();
            long tableId = tb.getTableId();
            LookupBatchKey batchKey = new LookupBatchKey(tb, lookup.originalPartitionName());
            lookupByTableId
                    .computeIfAbsent(tableId, k -> new LinkedHashMap<>())
                    .computeIfAbsent(batchKey, k -> new LookupRequestBatch(batchKey))
                    .addLookup(lookup);
        }

        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            List<Runnable> futureCompletions = new ArrayList<>();
            lookupByTableId.forEach(
                    (tableId, lookupsByBatchKey) ->
                            handleLookupRequestException(
                                    new LeaderNotAvailableException(
                                            "Server "
                                                    + destination
                                                    + " is not found in metadata cache."),
                                    destination,
                                    lookupsByBatchKey.values(),
                                    futureCompletions));
            runFutureCompletionsAfterReleasingOriginalBatchCapacity(
                    releaseOriginalBatchCapacity, futureCompletions);
            return;
        }

        lookupByTableId.forEach(
                (tableId, lookupsByBatchKey) -> {
                    List<Map<LookupBatchKey, LookupRequestBatch>> lookupRequestGroups =
                            packLookupRequestGroups(lookupsByBatchKey.values());
                    for (Map<LookupBatchKey, LookupRequestBatch> lookupsByBatchKeyInRequest :
                            lookupRequestGroups) {
                        sendLookupRequestAndHandleResponse(
                                destination,
                                gateway,
                                makeLookupRequest(
                                        tableId,
                                        lookupsByBatchKeyInRequest.values(),
                                        insertIfNotExists,
                                        acks,
                                        maxRequestTimeoutMs),
                                tableId,
                                lookupsByBatchKeyInRequest,
                                releaseOriginalBatchCapacity);
                    }
                });
    }

    /**
     * Packs lookup batches into RPC request groups so each request has one lookup kind.
     *
     * <p>Normal and historical lookups are sent in separate RPCs. Within a historical request,
     * multiple original partitions may target the same {@link TableBucket}; responses carry the
     * original partition name so they can be dispatched using {@link LookupBatchKey}.
     */
    private List<Map<LookupBatchKey, LookupRequestBatch>> packLookupRequestGroups(
            Collection<LookupRequestBatch> lookupBatches) {
        Map<LookupBatchKey, LookupRequestBatch> normalLookups = new LinkedHashMap<>();
        Map<LookupBatchKey, LookupRequestBatch> historicalLookups = new LinkedHashMap<>();
        for (LookupRequestBatch lookupBatch : lookupBatches) {
            if (lookupBatch.originalPartitionName() == null) {
                normalLookups.put(lookupBatch.lookupBatchKey(), lookupBatch);
            } else {
                historicalLookups.put(lookupBatch.lookupBatchKey(), lookupBatch);
            }
        }

        List<Map<LookupBatchKey, LookupRequestBatch>> lookupRequestGroups = new ArrayList<>(2);
        if (!normalLookups.isEmpty()) {
            lookupRequestGroups.add(normalLookups);
        }
        if (!historicalLookups.isEmpty()) {
            lookupRequestGroups.add(historicalLookups);
        }
        return lookupRequestGroups;
    }

    private void sendPrefixLookupRequest(
            int destination,
            List<AbstractLookupQuery<?>> prefixLookups,
            @Nullable Runnable releaseOriginalBatchCapacity) {
        // table id -> (bucket -> lookups)
        Map<Long, Map<TableBucket, PrefixLookupBatch>> lookupByTableId = new HashMap<>();
        for (AbstractLookupQuery<?> abstractLookupQuery : prefixLookups) {
            PrefixLookupQuery prefixLookup = (PrefixLookupQuery) abstractLookupQuery;
            TableBucket tb = prefixLookup.tableBucket();
            long tableId = tb.getTableId();
            lookupByTableId
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .computeIfAbsent(tb, k -> new PrefixLookupBatch(tb))
                    .addLookup(prefixLookup);
        }

        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            List<Runnable> futureCompletions = new ArrayList<>();
            lookupByTableId.forEach(
                    (tableId, lookupsByBucket) ->
                            handlePrefixLookupException(
                                    new LeaderNotAvailableException(
                                            "Server "
                                                    + destination
                                                    + " is not found in metadata cache."),
                                    destination,
                                    lookupsByBucket,
                                    futureCompletions));
            runFutureCompletionsAfterReleasingOriginalBatchCapacity(
                    releaseOriginalBatchCapacity, futureCompletions);
            return;
        }

        lookupByTableId.forEach(
                (tableId, prefixLookupBatch) ->
                        sendPrefixLookupRequestAndHandleResponse(
                                destination,
                                gateway,
                                makePrefixLookupRequest(tableId, prefixLookupBatch.values()),
                                tableId,
                                prefixLookupBatch,
                                releaseOriginalBatchCapacity));
    }

    private void sendLookupRequestAndHandleResponse(
            int destination,
            TabletServerGateway gateway,
            LookupRequest lookupRequest,
            long tableId,
            Map<LookupBatchKey, LookupRequestBatch> lookupsByBatchKey,
            @Nullable Runnable releaseOriginalBatchCapacity) {
        Set<TableBucket> tableBuckets =
                lookupsByBatchKey.keySet().stream()
                        .map(LookupBatchKey::tableBucket)
                        .collect(Collectors.toSet());
        boolean acquired = false;

        try {
            acquireInFlightRequest(tableBuckets);
            acquired = true;
            gateway.lookup(lookupRequest)
                    .whenComplete(
                            (lookupResponse, e) -> {
                                List<Runnable> futureCompletions = new ArrayList<>();
                                try {
                                    if (e != null) {
                                        handleLookupRequestException(
                                                e,
                                                destination,
                                                lookupsByBatchKey.values(),
                                                futureCompletions);
                                    } else {
                                        try {
                                            handleLookupResponse(
                                                    tableId,
                                                    destination,
                                                    lookupResponse,
                                                    lookupsByBatchKey,
                                                    futureCompletions);
                                        } catch (Throwable t) {
                                            handleLookupRequestException(
                                                    t,
                                                    destination,
                                                    lookupsByBatchKey.values(),
                                                    futureCompletions);
                                        }
                                    }
                                } finally {
                                    try {
                                        releaseInFlightRequest(tableBuckets);
                                    } finally {
                                        runFutureCompletionsAfterReleasingOriginalBatchCapacity(
                                                releaseOriginalBatchCapacity, futureCompletions);
                                    }
                                }
                            });
        } catch (Throwable t) {
            List<Runnable> futureCompletions = new ArrayList<>();
            try {
                handleLookupRequestException(
                        t, destination, lookupsByBatchKey.values(), futureCompletions);
            } finally {
                try {
                    if (acquired) {
                        releaseInFlightRequest(tableBuckets);
                    }
                } finally {
                    runFutureCompletionsAfterReleasingOriginalBatchCapacity(
                            releaseOriginalBatchCapacity, futureCompletions);
                }
            }
        }
    }

    private void sendPrefixLookupRequestAndHandleResponse(
            int destination,
            TabletServerGateway gateway,
            PrefixLookupRequest prefixLookupRequest,
            long tableId,
            Map<TableBucket, PrefixLookupBatch> lookupsByBucket,
            @Nullable Runnable releaseOriginalBatchCapacity) {
        Set<TableBucket> tableBuckets = lookupsByBucket.keySet();
        boolean acquired = false;

        try {
            acquireInFlightRequest(tableBuckets);
            acquired = true;
            gateway.prefixLookup(prefixLookupRequest)
                    .whenComplete(
                            (prefixLookupResponse, e) -> {
                                List<Runnable> futureCompletions = new ArrayList<>();
                                try {
                                    if (e != null) {
                                        handlePrefixLookupException(
                                                e, destination, lookupsByBucket, futureCompletions);
                                    } else {
                                        try {
                                            handlePrefixLookupResponse(
                                                    tableId,
                                                    destination,
                                                    prefixLookupResponse,
                                                    lookupsByBucket,
                                                    futureCompletions);
                                        } catch (Throwable t) {
                                            handlePrefixLookupException(
                                                    t,
                                                    destination,
                                                    lookupsByBucket,
                                                    futureCompletions);
                                        }
                                    }
                                } finally {
                                    try {
                                        releaseInFlightRequest(tableBuckets);
                                    } finally {
                                        runFutureCompletionsAfterReleasingOriginalBatchCapacity(
                                                releaseOriginalBatchCapacity, futureCompletions);
                                    }
                                }
                            });
        } catch (Throwable t) {
            List<Runnable> futureCompletions = new ArrayList<>();
            try {
                handlePrefixLookupException(t, destination, lookupsByBucket, futureCompletions);
            } finally {
                try {
                    if (acquired) {
                        releaseInFlightRequest(tableBuckets);
                    }
                } finally {
                    runFutureCompletionsAfterReleasingOriginalBatchCapacity(
                            releaseOriginalBatchCapacity, futureCompletions);
                }
            }
        }
    }

    private static void runFutureCompletionsAfterReleasingOriginalBatchCapacity(
            @Nullable Runnable releaseOriginalBatchCapacity, List<Runnable> futureCompletions) {
        try {
            if (releaseOriginalBatchCapacity != null) {
                releaseOriginalBatchCapacity.run();
            }
        } finally {
            // Complete user futures only after releasing original batch capacity, because
            // CompletableFuture continuations run synchronously and may append new lookups.
            for (Runnable futureCompletion : futureCompletions) {
                futureCompletion.run();
            }
        }
    }

    private void acquireInFlightRequest(Set<TableBucket> tableBuckets) {
        try {
            maxInFlightRequestsSemaphore.acquire();
            lookupQueue.addInFlightRequests(tableBuckets);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while sending lookup request.", e);
        }
    }

    private void releaseInFlightRequest(Set<TableBucket> tableBuckets) {
        lookupQueue.completeInFlightRequests(tableBuckets);
        maxInFlightRequestsSemaphore.release();
    }

    private void handleLookupResponse(
            long tableId,
            int destination,
            LookupResponse lookupResponse,
            Map<LookupBatchKey, LookupRequestBatch> lookupsByBatchKey,
            List<Runnable> futureCompletions) {
        for (PbLookupRespForBucket pbLookupRespForBucket : lookupResponse.getBucketsRespsList()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tableId,
                            pbLookupRespForBucket.hasPartitionId()
                                    ? pbLookupRespForBucket.getPartitionId()
                                    : null,
                            pbLookupRespForBucket.getBucketId());
            LookupBatchKey lookupBatchKey =
                    new LookupBatchKey(
                            tableBucket,
                            pbLookupRespForBucket.hasOriginalPartitionName()
                                    ? pbLookupRespForBucket.getOriginalPartitionName()
                                    : null);
            LookupRequestBatch lookupBatch = lookupsByBatchKey.get(lookupBatchKey);
            if (pbLookupRespForBucket.hasErrorCode()) {
                ApiError error = ApiError.fromErrorMessage(pbLookupRespForBucket);
                handleLookupError(
                        tableBucket,
                        destination,
                        error,
                        lookupBatch.lookups(),
                        "lookup",
                        futureCompletions);
            } else {
                List<byte[]> byteValues =
                        pbLookupRespForBucket.getValuesList().stream()
                                .map(
                                        pbValue -> {
                                            if (pbValue.hasValues()) {
                                                return pbValue.getValues();
                                            } else {
                                                return null;
                                            }
                                        })
                                .collect(Collectors.toList());
                futureCompletions.add(() -> lookupBatch.complete(byteValues));
            }
        }
    }

    private void handlePrefixLookupResponse(
            long tableId,
            int destination,
            PrefixLookupResponse prefixLookupResponse,
            Map<TableBucket, PrefixLookupBatch> prefixLookupsByBucket,
            List<Runnable> futureCompletions) {
        for (PbPrefixLookupRespForBucket pbRespForBucket :
                prefixLookupResponse.getBucketsRespsList()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tableId,
                            pbRespForBucket.hasPartitionId()
                                    ? pbRespForBucket.getPartitionId()
                                    : null,
                            pbRespForBucket.getBucketId());

            PrefixLookupBatch prefixLookupBatch = prefixLookupsByBucket.get(tableBucket);
            if (pbRespForBucket.hasErrorCode()) {
                ApiError error = ApiError.fromErrorMessage(pbRespForBucket);
                handleLookupError(
                        tableBucket,
                        destination,
                        error,
                        prefixLookupBatch.lookups(),
                        "prefix lookup",
                        futureCompletions);
            } else {
                List<List<byte[]>> result = new ArrayList<>(pbRespForBucket.getValueListsCount());
                for (int i = 0; i < pbRespForBucket.getValueListsCount(); i++) {
                    PbValueList pbValueList = pbRespForBucket.getValueListAt(i);
                    List<byte[]> keyResult = new ArrayList<>(pbValueList.getValuesCount());
                    for (int j = 0; j < pbValueList.getValuesCount(); j++) {
                        keyResult.add(pbValueList.getValueAt(j));
                    }
                    result.add(keyResult);
                }
                futureCompletions.add(() -> prefixLookupBatch.complete(result));
            }
        }
    }

    private void handleLookupRequestException(
            Throwable t,
            int destination,
            Collection<LookupRequestBatch> lookupBatches,
            List<Runnable> futureCompletions) {
        ApiError error = ApiError.fromThrowable(t);
        for (LookupRequestBatch lookupBatch : lookupBatches) {
            handleLookupError(
                    lookupBatch.tableBucket(),
                    destination,
                    error,
                    lookupBatch.lookups(),
                    "lookup",
                    futureCompletions);
        }
    }

    private void handlePrefixLookupException(
            Throwable t,
            int destination,
            Map<TableBucket, PrefixLookupBatch> lookupsByBucket,
            List<Runnable> futureCompletions) {
        ApiError error = ApiError.fromThrowable(t);
        for (PrefixLookupBatch lookupBatch : lookupsByBucket.values()) {
            handleLookupError(
                    lookupBatch.tableBucket(),
                    destination,
                    error,
                    lookupBatch.lookups(),
                    "prefix lookup",
                    futureCompletions);
        }
    }

    private void reEnqueueLookup(AbstractLookupQuery<?> lookup) {
        lookupQueue.reEnqueue(lookup);
    }

    // TODO: Retrying lookup queries can change their submission order. See
    // https://github.com/apache/fluss/issues/3765.
    private long prepareRetry(AbstractLookupQuery<?> lookup, Exception exception) {
        long retryDelayMs = 0;
        if (exception instanceof HistoricalPartitionThrottledException) {
            retryDelayMs = historicalThrottleBackoff.backoff(lookup.retries());
            lookup.setNextRetryTimeMs(System.currentTimeMillis() + retryDelayMs);
        } else {
            lookup.setNextRetryTimeMs(0);
        }
        lookup.incrementRetries();
        return retryDelayMs;
    }

    private boolean canRetry(AbstractLookupQuery<?> lookup, Exception exception) {
        return lookup.retries() < maxRetries
                && !lookup.future().isDone()
                && exception instanceof RetriableException;
    }

    /**
     * Handle lookup error with retry logic. For each lookup in the list, check if it can be
     * retried. If yes, re-enqueue it; otherwise, complete it exceptionally.
     *
     * @param tableBucket the table bucket
     * @param error the error from server response
     * @param lookups the list of lookups to handle
     * @param futureCompletions deferred successful or terminal future completions
     */
    private void handleLookupError(
            TableBucket tableBucket,
            int destination,
            ApiError error,
            List<? extends AbstractLookupQuery<?>> lookups,
            String lookupType,
            List<Runnable> futureCompletions) {
        ApiException exception = error.exception();
        LOG.error(
                "Failed to {} from node {} for bucket {}",
                lookupType,
                destination,
                tableBucket,
                exception);
        if (exception instanceof InvalidMetadataException) {
            LOG.warn(
                    "Invalid metadata error in {} request. Going to request metadata update.",
                    lookupType,
                    exception);
            long tableId = tableBucket.getTableId();
            TableOrPartitions tableOrPartitions;
            if (tableBucket.getPartitionId() == null) {
                tableOrPartitions = new TableOrPartitions(Collections.singleton(tableId), null);
            } else {
                tableOrPartitions =
                        new TableOrPartitions(
                                null,
                                Collections.singleton(
                                        new TablePartition(tableId, tableBucket.getPartitionId())));
            }
            invalidTableOrPartitions(tableOrPartitions);
        }

        for (AbstractLookupQuery<?> lookup : lookups) {
            String originalPartitionNameMsg =
                    lookup.originalPartitionName() == null
                            ? ""
                            : " for historical partition " + lookup.originalPartitionName();
            if (canRetry(lookup, exception)) {
                long retryDelayMs = prepareRetry(lookup, exception);
                LOG.warn(
                        "Get error {} response on table bucket {}{}, retrying after {} ms ({} attempts left). Error: {}",
                        lookupType,
                        tableBucket,
                        originalPartitionNameMsg,
                        retryDelayMs,
                        maxRetries - lookup.retries(),
                        error.formatErrMsg());
                reEnqueueLookup(lookup);
            } else {
                LOG.warn(
                        "Get error {} response on table bucket {}{}, fail. Error: {}",
                        lookupType,
                        tableBucket,
                        originalPartitionNameMsg,
                        error.formatErrMsg());
                futureCompletions.add(() -> lookup.future().completeExceptionally(exception));
            }
        }
    }

    void forceClose() {
        forceClose = true;
        initiateClose();
    }

    void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        lookupQueue.close();
        running = false;
    }

    /** A helper class to hold table ids or table partitions. */
    private static class TableOrPartitions {
        private final @Nullable Set<Long> tableIds;
        private final @Nullable Set<TablePartition> tablePartitions;

        TableOrPartitions(
                @Nullable Set<Long> tableIds, @Nullable Set<TablePartition> tablePartitions) {
            this.tableIds = tableIds;
            this.tablePartitions = tablePartitions;
        }
    }

    private void invalidTableOrPartitions(TableOrPartitions tableOrPartitions) {
        Set<PhysicalTablePath> physicalTablePaths =
                metadataUpdater.getPhysicalTablePathByIds(
                        tableOrPartitions.tableIds, tableOrPartitions.tablePartitions);
        metadataUpdater.invalidPhysicalTableBucketMeta(physicalTablePaths);
    }
}
