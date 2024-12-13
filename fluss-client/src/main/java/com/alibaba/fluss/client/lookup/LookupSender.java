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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbValue;
import com.alibaba.fluss.rpc.protocol.ApiError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeLookupRequest;

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

    LookupSender(MetadataUpdater metadataUpdater, LookupQueue lookupQueue, int maxFlightRequests) {
        this.metadataUpdater = metadataUpdater;
        this.lookupQueue = lookupQueue;
        this.maxInFlightReuqestsSemaphore = new Semaphore(maxFlightRequests);
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
        List<Lookup> lookups = drainAll ? lookupQueue.drainAll() : lookupQueue.drain();
        sendLookups(lookups);
    }

    private void sendLookups(List<Lookup> lookups) {
        if (lookups.isEmpty()) {
            return;
        }
        // group by <leader, lookup batches> to lookup batches
        Map<Integer, List<Lookup>> lookupBatches = groupByLeader(lookups);
        // now, send the batches
        lookupBatches.forEach(this::sendLookups);
    }

    private Map<Integer, List<Lookup>> groupByLeader(List<Lookup> lookups) {
        // leader -> lookup batches
        Map<Integer, List<Lookup>> lookupBatchesByLeader = new HashMap<>();
        for (Lookup lookup : lookups) {
            // get the leader node
            TableBucket tb = lookup.tableBucket();

            int leader;
            try {
                // TODO this can be a re-triable operation. We should retry here instead of throwing
                // exception.
                leader = metadataUpdater.leaderFor(tb);
            } catch (Exception e) {
                lookup.future().completeExceptionally(e);
                continue;
            }

            lookupBatchesByLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(lookup);
        }
        return lookupBatchesByLeader;
    }

    private void sendLookups(int destination, List<Lookup> lookupBatches) {
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);

        // table id -> (bucket -> lookups)
        Map<Long, Map<TableBucket, LookupBatch>> lookupsByTableId = new HashMap<>();
        for (Lookup lookup : lookupBatches) {
            TableBucket tb = lookup.tableBucket();
            long tableId = tb.getTableId();
            lookupsByTableId
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .computeIfAbsent(tb, k -> new LookupBatch(tb))
                    .addLookup(lookup);
        }

        lookupsByTableId.forEach(
                (tableId, lookupsByBucket) ->
                        sendLookupRequestAndHandleResponse(
                                gateway,
                                makeLookupRequest(tableId, lookupsByBucket.values()),
                                tableId,
                                lookupsByBucket));
    }

    private void sendLookupRequestAndHandleResponse(
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
                                handleLookupResponse(tableId, lookupResponse, lookupsByBucket);
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        })
                .exceptionally(
                        e -> {
                            try {
                                handleLookupRequestException(e, lookupsByBucket);
                                return null;
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        });
    }

    private void handleLookupResponse(
            long tableId,
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
                // TODO for re-triable error, we should retry here instead of throwing exception.
                LOG.warn(
                        "Get error lookup response on table bucket {}, fail. Error: {}",
                        tableBucket,
                        pbLookupRespForBucket.getErrorMessage());
                lookupBatch.completeExceptionally(
                        new FlussRuntimeException(pbLookupRespForBucket.getErrorMessage()));
            } else {
                List<PbValue> pbValues = pbLookupRespForBucket.getValuesList();
                lookupBatch.complete(pbValues);
            }
        }
    }

    private void handleLookupRequestException(
            Throwable t, Map<TableBucket, LookupBatch> lookupsByBucket) {
        ApiError error = ApiError.fromThrowable(t);
        for (LookupBatch lookupBatch : lookupsByBucket.values()) {
            // TODO for re-triable error, we should retry here instead of throwing exception.
            LOG.warn(
                    "Get error lookup response on table bucket {}, fail. Error: {}",
                    lookupBatch.tableBucket(),
                    error.formatErrMsg());
            lookupBatch.completeExceptionally(error.exception());
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
}
