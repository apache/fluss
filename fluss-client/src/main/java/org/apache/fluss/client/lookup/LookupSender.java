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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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
            int maxInFlightRequests,
            int maxRetries,
            short acks,
            int maxRequestTimeoutMs) {
        this.metadataUpdater = metadataUpdater;
        this.lookupQueue = lookupQueue;
        this.maxInFlightRequestsSemaphore = new Semaphore(maxInFlightRequests);
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
        List<AbstractLookupQuery<?>> lookups =
                drainAll ? lookupQueue.drainAll() : lookupQueue.drain();
        if (lookups.isEmpty() || forceClose) {
            return;
        }

        sendLookups(lookups);
    }

    private void sendLookups(List<AbstractLookupQuery<?>> lookups) throws Exception {
        // group by <leader, lookup type> to lookup batches
        Map<Tuple2<Integer, LookupType>, List<AbstractLookupQuery<?>>> lookupBatches =
                groupByLeaderAndType(lookups);

        // if no lookup batches, sleep a bit to avoid busy loop. This case will happen when there is
        // no leader for all the lookup request in queue.
        if (lookupBatches.isEmpty() && !lookupQueue.hasUnDrained()) {
            // TODO: may use wait/notify mechanism to avoid active sleep, and use a dynamic sleep
            // time based on the request waited time.
            Thread.sleep(100);
        }

        // now, send the batches
        lookupBatches.forEach(
                (destAndType, batch) -> sendLookups(destAndType.f0, destAndType.f1, batch));
    }

    private Map<Tuple2<Integer, LookupType>, List<AbstractLookupQuery<?>>> groupByLeaderAndType(
            List<AbstractLookupQuery<?>> lookups) {
        Map<LookupQueueKey, List<AbstractLookupQuery<?>>> lookupsByQueueKey = new LinkedHashMap<>();
        for (AbstractLookupQuery<?> lookup : lookups) {
            lookupsByQueueKey
                    .computeIfAbsent(LookupQueueKey.fromLookup(lookup), key -> new ArrayList<>())
                    .add(lookup);
        }

        // <leader, LookupType> -> lookup batches
        Map<Tuple2<Integer, LookupType>, List<AbstractLookupQuery<?>>> lookupBatchesByLeader =
                new HashMap<>();
        for (Map.Entry<LookupQueueKey, List<AbstractLookupQuery<?>>> entry :
                lookupsByQueueKey.entrySet()) {
            LookupQueueKey lookupQueueKey = entry.getKey();
            List<AbstractLookupQuery<?>> lookupsForKey = entry.getValue();
            AbstractLookupQuery<?> representativeLookup = lookupsForKey.get(0);
            int leader;
            // lookup the leader node
            try {
                // TODO Metadata requests are being sent too frequently here. consider first
                // collecting the tables that need to be updated and then sending them together in
                // one request.
                leader =
                        metadataUpdater.leaderFor(
                                representativeLookup.tablePath(), lookupQueueKey.tableBucket());
            } catch (PartitionNotExistException e) {
                // Metadata refresh confirmed that the queued lookups carry a deleted partition id.
                // Complete them instead of repeatedly enqueueing the stale TableBucket; a primary
                // key lookuper can then reroute by partition name.
                lookupsForKey.forEach(lookup -> lookup.future().completeExceptionally(e));
                continue;
            } catch (Exception e) {
                // if leader is not found, re-enqueue the lookups to send again.
                LOG.warn(
                        "Failed to lookup the leader for {} when lookup",
                        lookupQueueKey.tableBucket(),
                        e);
                lookupsForKey.forEach(this::reEnqueueLookup);
                continue;
            }
            lookupBatchesByLeader
                    .computeIfAbsent(
                            Tuple2.of(leader, lookupQueueKey.lookupType()),
                            key -> new ArrayList<>())
                    .addAll(lookupsForKey);
        }
        return lookupBatchesByLeader;
    }

    @VisibleForTesting
    void sendLookups(
            int destination, LookupType lookupType, List<AbstractLookupQuery<?>> lookupBatches) {
        if (lookupType == LookupType.LOOKUP) {
            sendLookupRequest(destination, lookupBatches, false);
        } else if (lookupType == LookupType.LOOKUP_WITH_INSERT_IF_NOT_EXISTS) {
            sendLookupRequest(destination, lookupBatches, true);
        } else if (lookupType == LookupType.PREFIX_LOOKUP) {
            sendPrefixLookupRequest(destination, lookupBatches);
        } else {
            throw new IllegalArgumentException("Unsupported lookup type: " + lookupType);
        }
    }

    private void sendLookupRequest(
            int destination, List<AbstractLookupQuery<?>> lookups, boolean insertIfNotExists) {
        // table id -> (bucket and original partition name -> lookups)
        Map<Long, Map<LookupBatchKey, LookupBatch>> lookupByTableId = new LinkedHashMap<>();
        for (AbstractLookupQuery<?> abstractLookupQuery : lookups) {
            LookupQuery lookup = (LookupQuery) abstractLookupQuery;
            TableBucket tb = lookup.tableBucket();
            long tableId = tb.getTableId();
            LookupBatchKey batchKey = new LookupBatchKey(tb, lookup.originalPartitionName());
            lookupByTableId
                    .computeIfAbsent(tableId, k -> new LinkedHashMap<>())
                    .computeIfAbsent(batchKey, k -> new LookupBatch(batchKey))
                    .addLookup(lookup);
        }

        TabletServerGateway gateway;
        Throwable gatewayFailure;
        try {
            gateway = metadataUpdater.newTabletServerClientForNode(destination);
            gatewayFailure = null;
        } catch (Throwable t) {
            gateway = null;
            gatewayFailure = t;
        }
        if (gateway == null) {
            if (gatewayFailure == null) {
                gatewayFailure =
                        new LeaderNotAvailableException(
                                "Server " + destination + " is not found in metadata cache.");
            }
            final Throwable requestFailure = gatewayFailure;
            lookupByTableId.forEach(
                    (tableId, lookupsByBatchKey) ->
                            handleLookupRequestException(
                                    requestFailure, destination, lookupsByBatchKey.values()));
            return;
        }

        final TabletServerGateway requestGateway = gateway;
        lookupByTableId.forEach(
                (tableId, lookupsByBatchKey) -> {
                    List<Map<LookupBatchKey, LookupBatch>> lookupRequestGroups =
                            packLookupRequestGroups(lookupsByBatchKey.values());
                    for (Map<LookupBatchKey, LookupBatch> lookupsByBatchKeyInRequest :
                            lookupRequestGroups) {
                        InFlightBatch inFlightBatch =
                                new InFlightBatch(
                                        lookupQueue,
                                        lookupQueueKeysFromLookupBatches(
                                                lookupsByBatchKeyInRequest.values()));
                        sendLookupRequestAndHandleResponse(
                                destination,
                                requestGateway,
                                tableId,
                                lookupsByBatchKeyInRequest,
                                insertIfNotExists,
                                inFlightBatch);
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
    private List<Map<LookupBatchKey, LookupBatch>> packLookupRequestGroups(
            Collection<LookupBatch> lookupBatches) {
        Map<LookupBatchKey, LookupBatch> normalLookups = new LinkedHashMap<>();
        Map<LookupBatchKey, LookupBatch> historicalLookups = new LinkedHashMap<>();
        for (LookupBatch lookupBatch : lookupBatches) {
            if (lookupBatch.originalPartitionName() == null) {
                normalLookups.put(lookupBatch.lookupBatchKey(), lookupBatch);
            } else {
                historicalLookups.put(lookupBatch.lookupBatchKey(), lookupBatch);
            }
        }

        List<Map<LookupBatchKey, LookupBatch>> lookupRequestGroups = new ArrayList<>(2);
        if (!normalLookups.isEmpty()) {
            lookupRequestGroups.add(normalLookups);
        }
        if (!historicalLookups.isEmpty()) {
            lookupRequestGroups.add(historicalLookups);
        }
        return lookupRequestGroups;
    }

    private void sendPrefixLookupRequest(
            int destination, List<AbstractLookupQuery<?>> prefixLookups) {
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

        TabletServerGateway gateway;
        Throwable gatewayFailure;
        try {
            gateway = metadataUpdater.newTabletServerClientForNode(destination);
            gatewayFailure = null;
        } catch (Throwable t) {
            gateway = null;
            gatewayFailure = t;
        }
        if (gateway == null) {
            if (gatewayFailure == null) {
                gatewayFailure =
                        new LeaderNotAvailableException(
                                "Server " + destination + " is not found in metadata cache.");
            }
            final Throwable requestFailure = gatewayFailure;
            lookupByTableId.forEach(
                    (tableId, lookupsByBucket) ->
                            handlePrefixLookupException(
                                    requestFailure, destination, lookupsByBucket));
            return;
        }

        final TabletServerGateway requestGateway = gateway;
        lookupByTableId.forEach(
                (tableId, prefixLookupBatch) -> {
                    InFlightBatch inFlightBatch =
                            new InFlightBatch(
                                    lookupQueue,
                                    lookupQueueKeysFromPrefixLookupBatches(
                                            prefixLookupBatch.values()));
                    sendPrefixLookupRequestAndHandleResponse(
                            destination, requestGateway, tableId, prefixLookupBatch, inFlightBatch);
                });
    }

    private void sendLookupRequestAndHandleResponse(
            int destination,
            TabletServerGateway gateway,
            long tableId,
            Map<LookupBatchKey, LookupBatch> lookupsByBatchKey,
            boolean insertIfNotExists,
            InFlightBatch inFlightBatch) {
        try {
            acquireInFlightRequest(inFlightBatch);
            LookupRequest lookupRequest =
                    makeLookupRequest(
                            tableId,
                            lookupsByBatchKey.values(),
                            insertIfNotExists,
                            acks,
                            maxRequestTimeoutMs);
            gateway.lookup(lookupRequest)
                    .whenComplete(
                            (lookupResponse, e) -> {
                                try {
                                    if (e != null) {
                                        handleLookupRequestException(
                                                e, destination, lookupsByBatchKey.values());
                                    } else {
                                        try {
                                            handleLookupResponse(
                                                    tableId,
                                                    destination,
                                                    lookupResponse,
                                                    lookupsByBatchKey);
                                        } catch (Throwable t) {
                                            handleLookupRequestException(
                                                    t, destination, lookupsByBatchKey.values());
                                        }
                                    }
                                } finally {
                                    releaseInFlightRequest(inFlightBatch);
                                }
                            });
        } catch (Throwable t) {
            try {
                handleLookupRequestException(t, destination, lookupsByBatchKey.values());
            } finally {
                releaseInFlightRequest(inFlightBatch);
            }
        }
    }

    private void sendPrefixLookupRequestAndHandleResponse(
            int destination,
            TabletServerGateway gateway,
            long tableId,
            Map<TableBucket, PrefixLookupBatch> lookupsByBucket,
            InFlightBatch inFlightBatch) {
        try {
            acquireInFlightRequest(inFlightBatch);
            PrefixLookupRequest prefixLookupRequest =
                    makePrefixLookupRequest(tableId, lookupsByBucket.values());
            gateway.prefixLookup(prefixLookupRequest)
                    .whenComplete(
                            (prefixLookupResponse, e) -> {
                                try {
                                    if (e != null) {
                                        handlePrefixLookupException(
                                                e, destination, lookupsByBucket);
                                    } else {
                                        try {
                                            handlePrefixLookupResponse(
                                                    tableId,
                                                    destination,
                                                    prefixLookupResponse,
                                                    lookupsByBucket);
                                        } catch (Throwable t) {
                                            handlePrefixLookupException(
                                                    t, destination, lookupsByBucket);
                                        }
                                    }
                                } finally {
                                    releaseInFlightRequest(inFlightBatch);
                                }
                            });
        } catch (Throwable t) {
            try {
                handlePrefixLookupException(t, destination, lookupsByBucket);
            } finally {
                releaseInFlightRequest(inFlightBatch);
            }
        }
    }

    private void handleLookupResponse(
            long tableId,
            int destination,
            LookupResponse lookupResponse,
            Map<LookupBatchKey, LookupBatch> lookupsByBatchKey) {
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
            LookupBatch lookupBatch = lookupsByBatchKey.get(lookupBatchKey);
            if (pbLookupRespForBucket.hasErrorCode()) {
                ApiError error = ApiError.fromErrorMessage(pbLookupRespForBucket);
                handleLookupError(tableBucket, destination, error, lookupBatch.lookups(), "lookup");
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
                lookupBatch.complete(byteValues);
            }
        }
    }

    private void handlePrefixLookupResponse(
            long tableId,
            int destination,
            PrefixLookupResponse prefixLookupResponse,
            Map<TableBucket, PrefixLookupBatch> prefixLookupsByBucket) {
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
                        "prefix lookup");
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
                prefixLookupBatch.complete(result);
            }
        }
    }

    private void handleLookupRequestException(
            Throwable t, int destination, Collection<LookupBatch> lookupBatches) {
        ApiError error = ApiError.fromThrowable(t);
        for (LookupBatch lookupBatch : lookupBatches) {
            handleLookupError(
                    lookupBatch.tableBucket(), destination, error, lookupBatch.lookups(), "lookup");
        }
    }

    private void handlePrefixLookupException(
            Throwable t, int destination, Map<TableBucket, PrefixLookupBatch> lookupsByBucket) {
        ApiError error = ApiError.fromThrowable(t);
        for (PrefixLookupBatch lookupBatch : lookupsByBucket.values()) {
            handleLookupError(
                    lookupBatch.tableBucket(),
                    destination,
                    error,
                    lookupBatch.lookups(),
                    "prefix lookup");
        }
    }

    private static Set<LookupQueueKey> lookupQueueKeysFromLookupBatches(
            Collection<LookupBatch> lookupBatches) {
        Set<LookupQueueKey> lookupQueueKeys = new HashSet<>();
        for (LookupBatch lookupBatch : lookupBatches) {
            for (LookupQuery lookup : lookupBatch.lookups()) {
                lookupQueueKeys.add(LookupQueueKey.fromLookup(lookup));
            }
        }
        return lookupQueueKeys;
    }

    private static Set<LookupQueueKey> lookupQueueKeysFromPrefixLookupBatches(
            Collection<PrefixLookupBatch> prefixLookupBatches) {
        Set<LookupQueueKey> lookupQueueKeys = new HashSet<>();
        for (PrefixLookupBatch prefixLookupBatch : prefixLookupBatches) {
            for (PrefixLookupQuery lookup : prefixLookupBatch.lookups()) {
                lookupQueueKeys.add(LookupQueueKey.fromLookup(lookup));
            }
        }
        return lookupQueueKeys;
    }

    private void acquireInFlightRequest(InFlightBatch inFlightBatch) {
        try {
            maxInFlightRequestsSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while sending lookup request.", e);
        }
        try {
            inFlightBatch.startInFlightRequests();
        } catch (Throwable t) {
            maxInFlightRequestsSemaphore.release();
            throw t;
        }
    }

    private void releaseInFlightRequest(InFlightBatch inFlightBatch) {
        if (inFlightBatch.requestCompleted()) {
            maxInFlightRequestsSemaphore.release();
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
     * @param lookupType the type of lookup ("" for regular lookup, "prefix " for prefix lookup)
     */
    private void handleLookupError(
            TableBucket tableBucket,
            int destination,
            ApiError error,
            List<? extends AbstractLookupQuery<?>> lookups,
            String lookupType) {
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
            if (canRetry(lookup, exception)) {
                long retryDelayMs = prepareRetry(lookup, exception);
                LOG.warn(
                        "Get error {} response on table bucket {}, retrying after {} ms ({} attempts left). Error: {}",
                        lookupType,
                        tableBucket,
                        retryDelayMs,
                        maxRetries - lookup.retries(),
                        error.formatErrMsg());
                reEnqueueLookup(lookup);
            } else {
                LOG.warn(
                        "Get error {} response on table bucket {}, fail. Error: {}",
                        lookupType,
                        tableBucket,
                        error.formatErrMsg());
                lookup.future().completeExceptionally(exception);
            }
        }
    }

    void forceClose() {
        forceClose = true;
        lookupQueue.forceClose();
        running = false;
    }

    void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        lookupQueue.close();
        running = false;
    }

    static class InFlightBatch {
        private final LookupQueue lookupQueue;
        private final Set<LookupQueueKey> lookupQueueKeys;
        private final AtomicBoolean started = new AtomicBoolean();

        InFlightBatch(LookupQueue lookupQueue, Set<LookupQueueKey> lookupQueueKeys) {
            this.lookupQueue = lookupQueue;
            this.lookupQueueKeys = lookupQueueKeys;
        }

        void startInFlightRequests() {
            if (started.compareAndSet(false, true)) {
                try {
                    lookupQueue.startInFlightRequests(lookupQueueKeys);
                } catch (Throwable t) {
                    started.set(false);
                    throw t;
                }
            }
        }

        boolean requestCompleted() {
            if (started.compareAndSet(true, false)) {
                lookupQueue.completeInFlightRequests(lookupQueueKeys);
                return true;
            }
            return false;
        }
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
