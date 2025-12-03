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
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LeaderNotAvailableException;
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
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

    LookupSender(
            MetadataUpdater metadataUpdater,
            LookupQueue lookupQueue,
            int maxFlightRequests,
            int maxRetries) {
        this.metadataUpdater = metadataUpdater;
        this.lookupQueue = lookupQueue;
        this.maxInFlightReuqestsSemaphore = new Semaphore(maxFlightRequests);
        this.maxRetries = maxRetries;
        this.running = true;
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

    private void sendLookups(List<AbstractLookupQuery<?>> lookups) throws Exception {
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
        for (AbstractLookupQuery<?> lookup : lookups) {
            int leader;
            // lookup the leader node
            TableBucket tb = lookup.tableBucket();
            try {
                leader = metadataUpdater.leaderFor(tb);
            } catch (Exception e) {
                // if leader is not found, re-enqueue the lookup to send again.
                reEnqueueLookup(lookup);
                continue;
            }
            lookupBatchesByLeader
                    .computeIfAbsent(Tuple2.of(leader, lookup.lookupType()), k -> new ArrayList<>())
                    .add(lookup);
        }
        return lookupBatchesByLeader;
    }

    @VisibleForTesting
    void sendLookups(
            int destination, LookupType lookupType, List<AbstractLookupQuery<?>> lookupBatches) {
        if (lookupType == LookupType.LOOKUP) {
            sendLookupRequest(destination, lookupBatches);
        } else if (lookupType == LookupType.PREFIX_LOOKUP) {
            sendPrefixLookupRequest(destination, lookupBatches);
        } else {
            throw new IllegalArgumentException("Unsupported lookup type: " + lookupType);
        }
    }

    private void sendLookupRequest(int destination, List<AbstractLookupQuery<?>> lookups) {
        // table id -> (bucket -> lookups)
        Map<Long, Map<TableBucket, LookupBatch>> lookupByTableId = new HashMap<>();
        for (AbstractLookupQuery<?> abstractLookupQuery : lookups) {
            LookupQuery lookup = (LookupQuery) abstractLookupQuery;
            TableBucket tb = lookup.tableBucket();
            long tableId = tb.getTableId();
            lookupByTableId
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .computeIfAbsent(tb, k -> new LookupBatch(tb))
                    .addLookup(lookup);
        }

        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            lookupByTableId.forEach(
                    (tableId, lookupsByBucket) ->
                            handleLookupRequestException(
                                    new LeaderNotAvailableException(
                                            "Server "
                                                    + destination
                                                    + " is not found in metadata cache."),
                                    destination,
                                    lookupsByBucket));
        }

        lookupByTableId.forEach(
                (tableId, lookupsByBucket) ->
                        sendLookupRequestAndHandleResponse(
                                destination,
                                gateway,
                                makeLookupRequest(tableId, lookupsByBucket.values()),
                                tableId,
                                lookupsByBucket));
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
            Map<TableBucket, LookupBatch> lookupsByBucket) {
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
                                        tableId, destination, lookupResponse, lookupsByBucket);
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        })
                .exceptionally(
                        e -> {
                            try {
                                handleLookupRequestException(e, destination, lookupsByBucket);
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
            Map<TableBucket, LookupBatch> lookupsByBucket) {
        for (PbLookupRespForBucket pbLookupRespForBucket : lookupResponse.getBucketsRespsList()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tableId,
                            pbLookupRespForBucket.hasPartitionId()
                                    ? pbLookupRespForBucket.getPartitionId()
                                    : null,
                            pbLookupRespForBucket.getBucketId());
            LookupBatch lookupBatch = lookupsByBucket.get(tableBucket);
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
            Throwable t, int destination, Map<TableBucket, LookupBatch> lookupsByBucket) {
        ApiError error = ApiError.fromThrowable(t);
        for (LookupBatch lookupBatch : lookupsByBucket.values()) {
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
        lookupQueue.appendLookup(lookup);
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
        ApiException exception = error.error().exception();
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
            if (canRetry(lookup, error.exception())) {
                LOG.warn(
                        "Get error {} response on table bucket {}, retrying ({} attempts left). Error: {}",
                        lookupType,
                        tableBucket,
                        maxRetries - lookup.retries(),
                        error.formatErrMsg());
                lookup.incrementRetries();
                reEnqueueLookup(lookup);
            } else {
                LOG.warn(
                        "Get error {} response on table bucket {}, fail. Error: {}",
                        lookupType,
                        tableBucket,
                        error.formatErrMsg());
                lookup.future().completeExceptionally(error.exception());
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
