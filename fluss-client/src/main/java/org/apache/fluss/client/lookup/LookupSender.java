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
import org.apache.fluss.exception.InvalidBucketRoutingException;
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
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
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

    private final Semaphore maxInFlightReuqestsSemaphore;

    private final int maxRetries;

    private final int maxRequestTimeoutMs;

    private final short acks;

    private final ExponentialBackoff retryBackoff;

    private final ExponentialBackoff historicalThrottleBackoff;

    private final Clock clock;

    /**
     * Buckets whose leader could not be found, keyed to their backoff state. Only accessed by the
     * sender thread.
     */
    private final Map<TableBucket, LeaderlessBucket> leaderlessBuckets = new HashMap<>();

    LookupSender(
            MetadataUpdater metadataUpdater,
            LookupQueue lookupQueue,
            int maxFlightRequests,
            int maxRetries,
            ExponentialBackoff retryBackoff,
            short acks,
            int maxRequestTimeoutMs) {
        this(
                metadataUpdater,
                lookupQueue,
                maxFlightRequests,
                maxRetries,
                retryBackoff,
                acks,
                maxRequestTimeoutMs,
                SystemClock.getInstance());
    }

    @VisibleForTesting
    LookupSender(
            MetadataUpdater metadataUpdater,
            LookupQueue lookupQueue,
            int maxFlightRequests,
            int maxRetries,
            ExponentialBackoff retryBackoff,
            short acks,
            int maxRequestTimeoutMs,
            Clock clock) {
        this.metadataUpdater = metadataUpdater;
        this.lookupQueue = lookupQueue;
        this.maxInFlightReuqestsSemaphore = new Semaphore(maxFlightRequests);
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
        this.running = true;
        this.acks = acks;
        this.maxRequestTimeoutMs = maxRequestTimeoutMs;
        this.historicalThrottleBackoff = new ExponentialBackoff(100L, 2, 5000L, 0.2);
        this.clock = clock;
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
        if (!forceClose && lookupQueue.hasUnDrained()) {
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
        sendLookups(lookups);
    }

    @VisibleForTesting
    void sendLookups(List<AbstractLookupQuery<?>> lookups) throws Exception {
        if (lookups.isEmpty()) {
            return;
        }
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
        // <leader, LookupType> -> lookup batches
        Map<Tuple2<Integer, LookupType>, List<AbstractLookupQuery<?>>> lookupBatchesByLeader =
                new HashMap<>();
        // Resolve the leader once per bucket, as leaderFor() may block on metadata requests.
        Map<TableBucket, List<AbstractLookupQuery<?>>> lookupsByBucket = new LinkedHashMap<>();
        for (AbstractLookupQuery<?> lookup : lookups) {
            lookupsByBucket
                    .computeIfAbsent(lookup.tableBucket(), k -> new ArrayList<>())
                    .add(lookup);
        }
        long nowMs = clock.milliseconds();
        for (Map.Entry<TableBucket, List<AbstractLookupQuery<?>>> entry :
                lookupsByBucket.entrySet()) {
            TableBucket tb = entry.getKey();
            List<AbstractLookupQuery<?>> bucketLookups = entry.getValue();
            LeaderlessBucket leaderlessBucket = leaderlessBuckets.get(tb);
            if (leaderlessBucket != null && nowMs < leaderlessBucket.retryTimeMs) {
                // Lookups that arrive while the bucket backs off wait for it as well, instead of
                // blocking the sender thread on another metadata request.
                reEnqueueLookups(bucketLookups, leaderlessBucket.retryTimeMs);
                continue;
            }
            int leader;
            try {
                // TODO Metadata requests are being sent too frequently here. consider first
                // collecting the tables that need to be updated and then sending them together in
                // one request.
                leader = metadataUpdater.leaderFor(bucketLookups.get(0).tablePath(), tb);
            } catch (PartitionNotExistException e) {
                // Metadata refresh confirmed that the queued lookup carries a deleted partition
                // id. Complete it instead of repeatedly enqueueing the stale TableBucket; a
                // primary key lookuper can then reroute by partition name.
                leaderlessBuckets.remove(tb);
                bucketLookups.forEach(lookup -> lookup.future().completeExceptionally(e));
                continue;
            } catch (Exception e) {
                if (leaderlessBucket == null) {
                    leaderlessBucket = new LeaderlessBucket();
                    leaderlessBuckets.put(tb, leaderlessBucket);
                }
                reEnqueueLookupsWithoutLeader(tb, leaderlessBucket, bucketLookups, e);
                continue;
            }
            leaderlessBuckets.remove(tb);
            for (AbstractLookupQuery<?> lookup : bucketLookups) {
                lookupBatchesByLeader
                        .computeIfAbsent(
                                Tuple2.of(leader, lookup.lookupType()), k -> new ArrayList<>())
                        .add(lookup);
            }
        }
        // Lookups waiting on a bucket come back once its backoff expires. A bucket without any is
        // no longer looked up, e.g. its table was dropped, so stop tracking it.
        leaderlessBuckets
                .entrySet()
                .removeIf(
                        e ->
                                e.getValue().retryTimeMs <= nowMs
                                        && !lookupsByBucket.containsKey(e.getKey()));
        return lookupBatchesByLeader;
    }

    private void reEnqueueLookupsWithoutLeader(
            TableBucket tableBucket,
            LeaderlessBucket leaderlessBucket,
            List<AbstractLookupQuery<?>> lookups,
            Exception exception) {
        // Not counted toward max retries: a bucket can be leaderless for a while during a routine
        // rolling restart, and lookups should not fail because of that.
        long retryDelayMs = retryBackoff.backoff(leaderlessBucket.attempts++);
        leaderlessBucket.retryTimeMs = clock.milliseconds() + retryDelayMs;
        reEnqueueLookups(lookups, leaderlessBucket.retryTimeMs);
        LOG.warn(
                "Failed to find the leader for table bucket {}, retrying {} lookup(s) after {} ms. "
                        + "Error: {}",
                tableBucket,
                lookups.size(),
                retryDelayMs,
                exception.toString());
        LOG.debug("Failed to find the leader for table bucket {}.", tableBucket, exception);
    }

    private void reEnqueueLookups(List<AbstractLookupQuery<?>> lookups, long retryTimeMs) {
        for (AbstractLookupQuery<?> lookup : lookups) {
            lookup.setNextRetryTimeMs(retryTimeMs);
            reEnqueueLookup(lookup);
        }
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
                    .computeIfAbsent(batchKey, k -> new LookupBatch(batchKey, lookup.bucketCount()))
                    .addLookup(lookup);
        }

        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            lookupByTableId.forEach(
                    (tableId, lookupsByBatchKey) ->
                            handleLookupRequestException(
                                    new LeaderNotAvailableException(
                                            "Server "
                                                    + destination
                                                    + " is not found in metadata cache."),
                                    destination,
                                    lookupsByBatchKey.values()));
            return;
        }

        lookupByTableId.forEach(
                (tableId, lookupsByBatchKey) -> {
                    List<Map<LookupBatchKey, LookupBatch>> lookupRequestGroups =
                            packLookupRequestGroups(lookupsByBatchKey.values());
                    for (Map<LookupBatchKey, LookupBatch> lookupsByBatchKeyInRequest :
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
                                lookupsByBatchKeyInRequest);
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
                    .computeIfAbsent(tb, k -> new PrefixLookupBatch(tb, prefixLookup.bucketCount()))
                    .addLookup(prefixLookup);
        }

        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            lookupByTableId.forEach(
                    (tableId, lookupsByBucket) ->
                            handlePrefixLookupException(
                                    new LeaderNotAvailableException(
                                            "Server "
                                                    + destination
                                                    + " is not found in metadata cache."),
                                    destination,
                                    lookupsByBucket));
            return;
        }

        lookupByTableId.forEach(
                (tableId, prefixLookupBatch) ->
                        sendPrefixLookupRequestAndHandleResponse(
                                destination,
                                gateway,
                                makePrefixLookupRequest(tableId, prefixLookupBatch.values()),
                                tableId,
                                prefixLookupBatch));
    }

    private void sendLookupRequestAndHandleResponse(
            int destination,
            TabletServerGateway gateway,
            LookupRequest lookupRequest,
            long tableId,
            Map<LookupBatchKey, LookupBatch> lookupsByBatchKey) {
        // TODO: Normal and historical lookups share the in-flight permits for simplicity. See
        // https://github.com/apache/fluss/issues/3766.
        try {
            maxInFlightReuqestsSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("interrupted:", e);
        }
        gateway.lookup(lookupRequest)
                .thenAccept(
                        lookupResponse -> {
                            try {
                                handleLookupResponse(
                                        tableId, destination, lookupResponse, lookupsByBatchKey);
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        })
                .exceptionally(
                        e -> {
                            try {
                                handleLookupRequestException(
                                        e, destination, lookupsByBatchKey.values());
                                return null;
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        });
    }

    private void sendPrefixLookupRequestAndHandleResponse(
            int destination,
            TabletServerGateway gateway,
            PrefixLookupRequest prefixLookupRequest,
            long tableId,
            Map<TableBucket, PrefixLookupBatch> lookupsByBucket) {
        try {
            maxInFlightReuqestsSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("interrupted:", e);
        }
        gateway.prefixLookup(prefixLookupRequest)
                .thenAccept(
                        prefixLookupResponse -> {
                            try {
                                handlePrefixLookupResponse(
                                        tableId,
                                        destination,
                                        prefixLookupResponse,
                                        lookupsByBucket);
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        })
                .exceptionally(
                        e -> {
                            try {
                                handlePrefixLookupException(e, destination, lookupsByBucket);
                                return null;
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        });
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

    private void reEnqueueLookup(AbstractLookupQuery<?> lookup) {
        lookupQueue.reEnqueue(lookup);
    }

    // TODO: Retrying lookup queries can change their submission order. See
    // https://github.com/apache/fluss/issues/3765.
    private long prepareRetry(AbstractLookupQuery<?> lookup, Exception exception) {
        // Retrying immediately does not help while the server keeps rejecting the lookup (e.g. no
        // ready leader during a restart); it only floods the server and the logs.
        ExponentialBackoff backoff =
                exception instanceof HistoricalPartitionThrottledException
                        ? historicalThrottleBackoff
                        : retryBackoff;
        long retryDelayMs = backoff.backoff(lookup.retries());
        lookup.setNextRetryTimeMs(clock.milliseconds() + retryDelayMs);
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
     * retried. If yes, re-enqueue it after a backoff; otherwise, complete it exceptionally.
     *
     * @param tableBucket the table bucket
     * @param error the error from server response
     * @param lookups the list of lookups to handle, all sharing the same original partition name
     * @param lookupType the type of lookup ("lookup" or "prefix lookup")
     */
    private void handleLookupError(
            TableBucket tableBucket,
            int destination,
            ApiError error,
            List<? extends AbstractLookupQuery<?>> lookups,
            String lookupType) {
        ApiException exception = error.exception();
        if (exception instanceof InvalidMetadataException
                || exception instanceof InvalidBucketRoutingException) {
            LOG.debug(
                    "Metadata or bucket routing error in {} request for bucket {}. Going to "
                            + "request metadata update.",
                    lookupType,
                    tableBucket);
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

        int retrying = 0;
        int failed = 0;
        long maxRetryDelayMs = 0;
        int minAttemptsLeft = Integer.MAX_VALUE;
        for (AbstractLookupQuery<?> lookup : lookups) {
            if (canRetry(lookup, exception)) {
                maxRetryDelayMs = Math.max(maxRetryDelayMs, prepareRetry(lookup, exception));
                minAttemptsLeft = Math.min(minAttemptsLeft, maxRetries - lookup.retries());
                retrying++;
                reEnqueueLookup(lookup);
            } else {
                failed++;
                lookup.future().completeExceptionally(exception);
            }
        }

        String originalPartitionName = lookups.get(0).originalPartitionName();
        String originalPartitionNameMsg =
                originalPartitionName == null
                        ? ""
                        : " for historical partition " + originalPartitionName;
        if (retrying > 0) {
            LOG.warn(
                    "Get error {} response from node {} on table bucket {}{}, retrying {} "
                            + "lookup(s) after up to {} ms (at least {} attempts left). Error: {}",
                    lookupType,
                    destination,
                    tableBucket,
                    originalPartitionNameMsg,
                    retrying,
                    maxRetryDelayMs,
                    minAttemptsLeft,
                    error.formatErrMsg());
        }
        if (failed > 0) {
            LOG.warn(
                    "Get error {} response from node {} on table bucket {}{}, failing {} "
                            + "lookup(s). Error: {}",
                    lookupType,
                    destination,
                    tableBucket,
                    originalPartitionNameMsg,
                    failed,
                    error.formatErrMsg());
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

    /** Backoff state of a bucket whose leader could not be found. */
    private static class LeaderlessBucket {
        /** Failed leader lookups since the bucket last had a known leader. */
        private int attempts;

        /** The leader is not looked up again before this time. */
        private long retryTimeMs;
    }

    private void invalidTableOrPartitions(TableOrPartitions tableOrPartitions) {
        Set<PhysicalTablePath> physicalTablePaths =
                metadataUpdater.getPhysicalTablePathByIds(
                        tableOrPartitions.tableIds, tableOrPartitions.tablePartitions);
        metadataUpdater.invalidPhysicalTableBucketMeta(physicalTablePaths);
    }
}
