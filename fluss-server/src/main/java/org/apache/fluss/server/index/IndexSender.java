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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutIndexRespForBucket;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.index.IndexRequestFactory.SizeAccumulator;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * TabletServer-global send layer for index replication. A fixed pool of worker threads drains
 * pre-encoded {@link IndexBatch}es from the shared {@link IndexSendBuffer}, groups them by target
 * Index Table leader server, and dispatches consolidated multi-bucket {@code PutIndexRequest}s.
 *
 * <p>Within one {@code IndexSender} instance, each target index bucket is owned by exactly one
 * worker ({@code bucket.hashCode() % M}). A bucket with an in-flight request is also "muted", so a
 * later local batch cannot overtake it. Target-side application progress handles requests from
 * other TabletServers and late requests from a previous source leader.
 *
 * <p>Reliability follows at-least-once semantics: on RPC failure (or unresolved leader) the batch
 * releases its buffer claim for unlimited retry, and the owning window's offset is never advanced.
 * On success the batch notifies its window via {@link IndexBatch#window()}, which advances the
 * replicator's pushed offset once the whole window is acknowledged.
 */
@Internal
@ThreadSafe
public final class IndexSender implements AutoCloseable {

    @VisibleForTesting
    enum LifecyclePhase {
        OPEN,
        CLOSING,
        CLOSED
    }

    /** Resolves the leader server ID for a given Index Table bucket. */
    @FunctionalInterface
    public interface LeaderResolver {
        OptionalInt resolveLeader(long indexTableId, int indexBucket);
    }

    private static final Logger LOG = LoggerFactory.getLogger(IndexSender.class);

    private final IndexSendBuffer sendBuffer;
    private final LeaderResolver leaderResolver;
    private final Function<Integer, TabletServerGateway> gatewayFactory;
    private final TabletServerMetricGroup metrics;

    private final SenderWorker[] workers;
    private final AtomicBoolean bucketRescanRequired = new AtomicBoolean();

    /** Maximum total encoded bytes packed into a single consolidated PutIndex request. */
    private final long maxRequestBytes;

    /** Exact framed request limit enforced by the target Netty server. */
    private final long maxTransportRequestBytes;

    /** Timeout applied to each index {@code PutIndex} request. */
    private final long requestTimeoutMs;

    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final Condition lifecycleDrained = lifecycleLock.newCondition();
    private final Map<Integer, TargetContext> targetsByServer = new HashMap<>();
    private final Set<Integer> creatingTargets = new HashSet<>();
    private volatile LifecyclePhase lifecyclePhase = LifecyclePhase.OPEN;
    private int outstandingAsyncOperations;

    /**
     * Creates and starts an index sender.
     *
     * @param sendBuffer shared buffer of index batches awaiting delivery
     * @param leaderResolver resolver for target index-bucket leaders
     * @param gatewayFactory factory for tablet-server gateways
     * @param metrics tablet-server metrics updated by the sender
     * @param numWorkers number of sender workers
     * @param backoffMs idle-worker backoff in milliseconds
     * @param maxRequestBytes maximum encoded payload bytes per request
     * @param maxTransportRequestBytes maximum framed request bytes accepted by the transport
     * @param requestTimeoutMs timeout for each index write request
     */
    public IndexSender(
            IndexSendBuffer sendBuffer,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs,
            long maxRequestBytes,
            long maxTransportRequestBytes,
            long requestTimeoutMs) {
        checkArgument(numWorkers > 0, "numWorkers must be positive");
        checkArgument(backoffMs > 0, "backoffMs must be positive");
        checkArgument(maxRequestBytes > 0, "maxRequestBytes must be positive");
        checkArgument(maxTransportRequestBytes > 0, "maxTransportRequestBytes must be positive");
        checkArgument(requestTimeoutMs > 0, "requestTimeoutMs must be positive");
        this.sendBuffer = sendBuffer;
        this.leaderResolver = leaderResolver;
        this.gatewayFactory = gatewayFactory;
        this.metrics = metrics;
        this.maxRequestBytes = maxRequestBytes;
        this.maxTransportRequestBytes = maxTransportRequestBytes;
        this.requestTimeoutMs = requestTimeoutMs;
        this.workers = new SenderWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            this.workers[i] = new SenderWorker("index-sender-" + i, i, backoffMs);
        }
        sendBuffer.setReadyBucketListener(this::notifyReadyBucket);
        for (TableBucket bucket : sendBuffer.buckets()) {
            enqueueReadyBucket(bucket);
        }
        for (SenderWorker worker : workers) {
            worker.start();
        }
        LOG.info("IndexSender started with {} workers", numWorkers);
    }

    private void enqueueReadyBucket(TableBucket bucket) {
        if (lifecyclePhase != LifecyclePhase.OPEN) {
            return;
        }
        workers[ownerOf(bucket)].enqueueReadyBucket(bucket);
    }

    private void notifyReadyBucket(TableBucket bucket) {
        try {
            enqueueReadyBucket(bucket);
        } catch (Throwable failure) {
            bucketRescanRequired.set(true);
            LOG.warn(
                    "Failed to notify sender for index bucket {}; scheduling a rescan",
                    bucket,
                    failure);
        }
    }

    private int ownerOf(TableBucket bucket) {
        return Math.floorMod(bucket.hashCode(), workers.length);
    }

    @Override
    public void close() {
        LOG.info("IndexSender closing");
        lifecycleLock.lock();
        try {
            if (lifecyclePhase == LifecyclePhase.CLOSED) {
                return;
            }
            if (lifecyclePhase == LifecyclePhase.CLOSING) {
                awaitClosedLocked();
                return;
            }
            lifecyclePhase = LifecyclePhase.CLOSING;
            targetsByServer.clear();
            creatingTargets.clear();
        } finally {
            lifecycleLock.unlock();
        }
        requestWorkerShutdown();
        boolean interrupted = false;
        for (SenderWorker worker : workers) {
            boolean shutdownComplete = false;
            while (!shutdownComplete) {
                try {
                    worker.awaitShutdown();
                    shutdownComplete = true;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }

        lifecycleLock.lock();
        try {
            while (outstandingAsyncOperations > 0) {
                try {
                    lifecycleDrained.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            lifecycleLock.unlock();
        }
        sendBuffer.discardClaims();

        lifecycleLock.lock();
        try {
            lifecyclePhase = LifecyclePhase.CLOSED;
            lifecycleDrained.signalAll();
        } finally {
            lifecycleLock.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void requestWorkerShutdown() {
        for (SenderWorker worker : workers) {
            worker.initiateShutdown();
            worker.wakeup();
        }
    }

    private void awaitClosedLocked() {
        while (lifecyclePhase != LifecyclePhase.CLOSED) {
            lifecycleDrained.awaitUninterruptibly();
        }
    }

    /** A single sender worker that owns the buckets hashing to its index. */
    private final class SenderWorker extends ShutdownableThread {

        private final int workerId;
        private final long backoffMs;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private final Set<TableBucket> readyBuckets = new LinkedHashSet<>();

        SenderWorker(String name, int workerId, long backoffMs) {
            super(name, false);
            this.workerId = workerId;
            this.backoffMs = backoffMs;
        }

        void wakeup() {
            inLock(lock, condition::signalAll);
        }

        void enqueueReadyBucket(TableBucket bucket) {
            inLock(
                    lock,
                    () -> {
                        if (readyBuckets.add(bucket)) {
                            condition.signalAll();
                        }
                    });
        }

        @Nullable
        private TableBucket pollReadyBucket() {
            return inLock(
                    lock,
                    () -> {
                        Iterator<TableBucket> iterator = readyBuckets.iterator();
                        if (!iterator.hasNext()) {
                            return null;
                        }
                        TableBucket bucket = iterator.next();
                        iterator.remove();
                        return bucket;
                    });
        }

        @Override
        public void doWork() {
            boolean didWork = drainAndSend();
            if (!didWork) {
                rescanBucketsAfterNotificationFailure();
                didWork = drainAndSend();
            }
            if (!didWork) {
                inLock(
                        lock,
                        () -> {
                            try {
                                condition.await(backoffMs, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }
        }

        private void rescanBucketsAfterNotificationFailure() {
            if (lifecyclePhase != LifecyclePhase.OPEN
                    || !bucketRescanRequired.compareAndSet(true, false)) {
                return;
            }
            try {
                for (TableBucket bucket : sendBuffer.buckets()) {
                    IndexSender.this.enqueueReadyBucket(bucket);
                }
            } catch (Throwable failure) {
                bucketRescanRequired.set(true);
                LOG.warn(
                        "Failed to rescan pending index buckets; scheduling another rescan",
                        failure);
            }
        }

        /** Claims at most one batch per owned, non-muted bucket and dispatches them. */
        private boolean drainAndSend() {
            if (lifecyclePhase != LifecyclePhase.OPEN) {
                return false;
            }
            List<IndexBatch> claimed = new ArrayList<>();
            List<TableBucket> deferredBuckets = new ArrayList<>();
            TableBucket bucket;
            while ((bucket = pollReadyBucket()) != null) {
                lifecycleLock.lock();
                try {
                    if (lifecyclePhase != LifecyclePhase.OPEN) {
                        break;
                    }
                    if (ownerOf(bucket) != workerId || sendBuffer.hasClaim(bucket)) {
                        continue;
                    }
                    IndexBatch batch = sendBuffer.claimFirstReady(bucket);
                    if (batch == null) {
                        if (!sendBuffer.hasClaim(bucket) && sendBuffer.hasPending(bucket)) {
                            deferredBuckets.add(bucket);
                        }
                        continue;
                    }
                    claimed.add(batch);
                } finally {
                    lifecycleLock.unlock();
                }
            }
            for (TableBucket deferredBucket : deferredBuckets) {
                enqueueReadyBucket(deferredBucket);
            }
            if (claimed.isEmpty()) {
                return false;
            }

            claimed.removeIf(
                    batch -> {
                        if (!batch.ownerActive()) {
                            discardClaimedBatch(batch);
                            return true;
                        }
                        SizeAccumulator singleton =
                                new SizeAccumulator(
                                        batch.targetBucket().getTableId(),
                                        batch.sourceBucket().getTableId(),
                                        requestTimeoutMs);
                        singleton.addBucket(batch);
                        if (singleton.exceedsLimit(maxTransportRequestBytes)) {
                            retryOversizedBatch(batch, singleton);
                            return true;
                        }
                        return !batch.ownerActive();
                    });

            // Group claimed batches by target leader server.
            Map<Integer, List<IndexBatch>> byServer = new HashMap<>();
            for (IndexBatch batch : claimed) {
                if (!batch.ownerActive()) {
                    discardClaimedBatch(batch);
                    continue;
                }
                TableBucket tb = batch.targetBucket();
                OptionalInt leaderOpt =
                        leaderResolver.resolveLeader(tb.getTableId(), tb.getBucket());
                if (leaderOpt.isPresent()) {
                    int leader = leaderOpt.getAsInt();
                    byServer.computeIfAbsent(leader, k -> new ArrayList<>()).add(batch);
                } else if (retryClaimedBatch(batch) && batch.attempts() == 1) {
                    // Reuse the batch's retry history to avoid logging every metadata retry.
                    LOG.warn(
                            "No leader available for index target {} from source {} at source "
                                    + "end offset {}. Retrying with backoff",
                            tb,
                            batch.sourceBucket(),
                            batch.sourceEndOffset());
                }
            }

            for (Map.Entry<Integer, List<IndexBatch>> serverEntry : byServer.entrySet()) {
                sendToServer(serverEntry.getKey(), serverEntry.getValue());
            }
            return true;
        }

        private void sendToServer(int serverId, List<IndexBatch> batches) {
            removeInactiveBatches(batches);
            if (batches.isEmpty()) {
                return;
            }
            TargetContext target = null;
            boolean createTarget = false;
            boolean dispatch = false;
            List<BatchAction> actions = new ArrayList<>();
            lifecycleLock.lock();
            try {
                if (lifecyclePhase != LifecyclePhase.OPEN) {
                    addBatchActionsLocked(actions, batches, BatchDisposition.RELEASE);
                } else {
                    target = targetsByServer.get(serverId);
                    if (target == null) {
                        if (creatingTargets.add(serverId)) {
                            createTarget = true;
                        } else {
                            addBatchActionsLocked(actions, batches, BatchDisposition.REQUEUE);
                        }
                    } else {
                        dispatch = true;
                    }
                }
            } finally {
                lifecycleLock.unlock();
            }
            applyBatchActions(actions);

            if (createTarget) {
                createTargetAndDispatch(serverId, batches);
                return;
            }
            if (dispatch) {
                dispatchIfCurrent(target, batches);
            }
        }

        private void createTargetAndDispatch(int serverId, List<IndexBatch> batches) {
            TabletServerGateway gateway = null;
            Throwable failure = null;
            try {
                gateway = gatewayFactory.apply(serverId);
            } catch (Throwable t) {
                failure = t;
            }
            boolean leadersCurrent = failure == null && leadersMatch(serverId, batches);
            TargetContext target = null;
            List<BatchAction> actions = new ArrayList<>();
            lifecycleLock.lock();
            try {
                creatingTargets.remove(serverId);
                if (lifecyclePhase != LifecyclePhase.OPEN) {
                    addBatchActionsLocked(actions, batches, BatchDisposition.RELEASE);
                } else if (failure != null || gateway == null || !leadersCurrent) {
                    addBatchActionsLocked(actions, batches, BatchDisposition.REQUEUE);
                } else {
                    target = new TargetContext(serverId, gateway);
                    targetsByServer.put(serverId, target);
                }
            } finally {
                lifecycleLock.unlock();
            }
            if (failure != null) {
                LOG.warn("Failed to get gateway for server {}, re-enqueuing", serverId, failure);
            }
            applyBatchActions(actions);
            if (target != null) {
                dispatchIfCurrent(target, batches);
            }
        }

        private void dispatchIfCurrent(TargetContext target, List<IndexBatch> batches) {
            removeInactiveBatches(batches);
            if (batches.isEmpty()) {
                return;
            }
            // Group by tableId for consolidated multi-bucket requests.
            Map<Long, List<IndexBatch>> byTable = new HashMap<>();
            for (IndexBatch batch : batches) {
                byTable.computeIfAbsent(batch.targetBucket().getTableId(), k -> new ArrayList<>())
                        .add(batch);
            }

            for (Map.Entry<Long, List<IndexBatch>> tableEntry : byTable.entrySet()) {
                long tableId = tableEntry.getKey();
                for (RequestChunk chunk : splitByRequestBytes(tableId, tableEntry.getValue())) {
                    sendOneRequest(target, tableId, chunk);
                }
            }
        }

        private void sendOneRequest(TargetContext target, long tableId, RequestChunk chunk) {
            RequestChunk requestChunk = activeChunk(tableId, chunk);
            if (requestChunk == null) {
                return;
            }
            if (requestChunk.size.exceedsLimit(maxTransportRequestBytes)) {
                for (IndexBatch batch : requestChunk.batches) {
                    retryOversizedBatch(batch, requestChunk.size);
                }
                return;
            }
            requestChunk = activeChunk(tableId, requestChunk);
            if (requestChunk == null) {
                return;
            }
            List<IndexBatch> requestBatches = requestChunk.batches;
            PutIndexRequest request =
                    IndexRequestFactory.build(tableId, requestBatches, requestTimeoutMs);
            boolean leadersCurrent = leadersMatch(target.serverId, requestBatches);
            List<BatchAction> actions = new ArrayList<>();
            boolean registered = false;
            lifecycleLock.lock();
            try {
                if (!targetIsCurrentLocked(target) || !leadersCurrent) {
                    invalidateTargetLocked(target);
                    addBatchActionsLocked(actions, requestBatches, BatchDisposition.REQUEUE);
                } else if (!batchesActiveAndClaimed(requestBatches)) {
                    addBatchActionsLocked(actions, requestBatches, BatchDisposition.REQUEUE);
                } else {
                    registerAsyncOperationLocked();
                    registered = true;
                }
            } finally {
                lifecycleLock.unlock();
            }
            applyBatchActions(actions);
            if (!registered) {
                return;
            }
            long requestStartNs = System.nanoTime();
            CompletableFuture<PutIndexResponse> future;
            try {
                future = target.gateway.putIndex(request);
                FutureUtils.orTimeout(
                                future,
                                requestTimeoutMs,
                                TimeUnit.MILLISECONDS,
                                "Index push PutIndex timed out for tableId=" + tableId)
                        .whenComplete(
                                (resp, err) -> {
                                    completePutIndexRequest(
                                            target,
                                            tableId,
                                            requestBatches,
                                            requestStartNs,
                                            resp,
                                            err);
                                });
            } catch (Throwable t) {
                completePutIndexRequest(target, tableId, requestBatches, requestStartNs, null, t);
            }
        }

        @Nullable
        private RequestChunk activeChunk(long tableId, RequestChunk chunk) {
            boolean changed = removeInactiveBatches(chunk.batches);
            if (chunk.batches.isEmpty()) {
                return null;
            }
            if (!changed) {
                return chunk;
            }
            SizeAccumulator activeSize =
                    new SizeAccumulator(
                            tableId,
                            chunk.batches.get(0).sourceBucket().getTableId(),
                            requestTimeoutMs);
            for (IndexBatch batch : chunk.batches) {
                activeSize.addBucket(batch);
            }
            return new RequestChunk(chunk.batches, activeSize);
        }

        private boolean removeInactiveBatches(List<IndexBatch> batches) {
            return batches.removeIf(
                    batch -> {
                        if (batch.ownerActive()) {
                            return false;
                        }
                        discardClaimedBatch(batch);
                        return true;
                    });
        }

        private void completePutIndexRequest(
                TargetContext target,
                long tableId,
                List<IndexBatch> chunk,
                long requestStartNs,
                @Nullable PutIndexResponse response,
                @Nullable Throwable error) {
            boolean leadersCurrent = leadersMatch(target.serverId, chunk);
            List<BatchAction> actions = new ArrayList<>();
            lifecycleLock.lock();
            try {
                if (!targetIsCurrentLocked(target) || !leadersCurrent) {
                    invalidateTargetLocked(target);
                    addBatchActionsLocked(actions, chunk, BatchDisposition.REQUEUE);
                } else if (error != null) {
                    invalidateTargetLocked(target);
                    metrics.indexReplicationFailures().inc(chunk.size());
                    for (IndexBatch batch : chunk) {
                        LOG.error(
                                "PutIndex RPC failed for index table {} from {} to {} at source "
                                        + "end offset {}. Retrying with backoff",
                                tableId,
                                batch.sourceBucket(),
                                batch.targetBucket(),
                                batch.sourceEndOffset(),
                                error);
                    }
                    addBatchActionsLocked(actions, chunk, BatchDisposition.REQUEUE);
                } else {
                    addResponseActionsLocked(actions, chunk, response);
                }
            } finally {
                lifecycleLock.unlock();
            }
            metrics.indexReplicationRequestLatencyHistogram()
                    .update((System.nanoTime() - requestStartNs) / 1_000_000L);
            try {
                applyBatchActionsAndNotifyWindows(actions);
            } finally {
                completeAsyncOperation();
            }
        }
    }

    private boolean leadersMatch(int serverId, List<IndexBatch> batches) {
        try {
            for (IndexBatch batch : batches) {
                TableBucket bucket = batch.targetBucket();
                OptionalInt leader =
                        leaderResolver.resolveLeader(bucket.getTableId(), bucket.getBucket());
                if (!leader.isPresent() || leader.getAsInt() != serverId) {
                    return false;
                }
            }
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    private boolean targetIsCurrentLocked(TargetContext target) {
        return lifecyclePhase == LifecyclePhase.OPEN
                && targetsByServer.get(target.serverId) == target;
    }

    private boolean batchesActiveAndClaimed(List<IndexBatch> batches) {
        for (IndexBatch batch : batches) {
            if (!batch.ownerActive() || !sendBuffer.ownsClaim(batch)) {
                return false;
            }
        }
        return true;
    }

    private void invalidateTargetLocked(TargetContext target) {
        targetsByServer.remove(target.serverId, target);
    }

    private void registerAsyncOperationLocked() {
        if (lifecyclePhase != LifecyclePhase.OPEN) {
            throw new IllegalStateException("Cannot register an async operation after close");
        }
        outstandingAsyncOperations++;
    }

    private void completeAsyncOperation() {
        lifecycleLock.lock();
        try {
            if (--outstandingAsyncOperations == 0) {
                lifecycleDrained.signalAll();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private static final class TargetContext {
        private final int serverId;
        private final TabletServerGateway gateway;

        private TargetContext(int serverId, TabletServerGateway gateway) {
            this.serverId = serverId;
            this.gateway = gateway;
        }
    }

    private static final class RequestChunk {
        // Worker-owned until RPC registration; callbacks retain the list unchanged afterward.
        private final List<IndexBatch> batches;
        private final SizeAccumulator size;

        private RequestChunk(List<IndexBatch> batches, SizeAccumulator size) {
            this.batches = batches;
            this.size = size;
        }
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Consolidates whole target batches up to the preferred payload bound and exact hard transport
     * bound. A singleton above the transport bound remains intact and is retried after backoff.
     */
    private List<RequestChunk> splitByRequestBytes(long tableId, List<IndexBatch> batches) {
        List<RequestChunk> chunks = new ArrayList<>();
        List<IndexBatch> current = new ArrayList<>();
        long currentPayloadBytes = 0L;
        long sourceTableId = batches.get(0).sourceBucket().getTableId();
        SizeAccumulator currentSize = new SizeAccumulator(tableId, sourceTableId, requestTimeoutMs);
        for (IndexBatch batch : batches) {
            long batchBytes = batch.encoded().getBytesLength();
            SizeAccumulator candidate = currentSize.copy();
            candidate.addBucket(batch);
            long candidatePayloadBytes = saturatedAdd(currentPayloadBytes, batchBytes);
            boolean exceedsPreferred = candidatePayloadBytes > maxRequestBytes;
            if (!current.isEmpty()
                    && (exceedsPreferred || candidate.exceedsLimit(maxTransportRequestBytes))) {
                chunks.add(new RequestChunk(current, currentSize));
                current = new ArrayList<>();
                currentSize = new SizeAccumulator(tableId, sourceTableId, requestTimeoutMs);
                currentPayloadBytes = 0L;
                currentSize.addBucket(batch);
            } else {
                currentSize = candidate;
            }
            current.add(batch);
            currentPayloadBytes = saturatedAdd(currentPayloadBytes, batchBytes);
        }
        if (!current.isEmpty()) {
            chunks.add(new RequestChunk(current, currentSize));
        }
        return chunks;
    }

    /**
     * Completes a chunk against the per-bucket outcomes carried in the PutIndex response. A bucket
     * is acked only when the response reports no error for it; a bucket that reports an error (or
     * is absent from the response) is re-enqueued for retry. This keeps a failed index mutation
     * from advancing its window's pushed offset, so SYNC visibility never releases a main-table
     * write whose index push has not actually landed.
     *
     * <p>Index replication must resume after operator repair, including authorization or metadata
     * repairs for errors classified as non-retriable by an individual RPC. The task retains source
     * progress and retries with backoff. During sustained failures, its window can yield the staged
     * payload and later rebuild from retained WAL.
     */
    private void addResponseActionsLocked(
            List<BatchAction> actions, List<IndexBatch> batches, PutIndexResponse response) {
        Map<Integer, PbPutIndexRespForBucket> respByBucket = new HashMap<>();
        for (PbPutIndexRespForBucket bucketResp : response.getBucketsRespsList()) {
            respByBucket.put(bucketResp.getBucketId(), bucketResp);
        }
        for (IndexBatch batch : batches) {
            int bucketId = batch.targetBucket().getBucket();
            PbPutIndexRespForBucket bucketResp = respByBucket.get(bucketId);
            boolean failed = bucketResp == null || bucketResp.hasErrorCode();
            if (failed) {
                metrics.indexReplicationFailures().inc();
                if (bucketResp == null) {
                    LOG.error(
                            "PutIndex response missing target {} for source {} at source end "
                                    + "offset {}. Retrying with backoff",
                            batch.targetBucket(),
                            batch.sourceBucket(),
                            batch.sourceEndOffset());
                } else {
                    Errors error = Errors.forCode(bucketResp.getErrorCode());
                    LOG.error(
                            "PutIndex failed from {} to {} at source end offset {} with {} "
                                    + "(code {}), message={}. Retrying with backoff",
                            batch.sourceBucket(),
                            batch.targetBucket(),
                            batch.sourceEndOffset(),
                            error.exceptionName(),
                            bucketResp.getErrorCode(),
                            bucketResp.hasErrorMessage()
                                    ? bucketResp.getErrorMessage()
                                    : error.message());
                }
                addBatchActionLocked(actions, batch, BatchDisposition.REQUEUE);
            } else {
                addBatchActionLocked(actions, batch, BatchDisposition.ACK);
            }
        }
    }

    private void retryOversizedBatch(IndexBatch batch, SizeAccumulator size) {
        RecordTooLargeException failure = size.oversizedFailure(batch, maxTransportRequestBytes);
        if (batch.attempts() == 0) {
            LOG.error(
                    "Cannot send index batch from {} to {}; retrying with backoff until "
                            + "the cluster request limit is adjusted",
                    batch.sourceBucket(),
                    batch.targetBucket(),
                    failure);
        }
        metrics.indexReplicationFailures().inc();
        retryClaimedBatch(batch);
    }

    private void discardClaimedBatch(IndexBatch batch) {
        List<BatchAction> actions = new ArrayList<>(1);
        lifecycleLock.lock();
        try {
            addBatchActionLocked(actions, batch, BatchDisposition.RELEASE);
        } finally {
            lifecycleLock.unlock();
        }
        applyBatchActions(actions);
    }

    private boolean retryClaimedBatch(IndexBatch batch) {
        List<BatchAction> actions = new ArrayList<>();
        lifecycleLock.lock();
        try {
            addBatchActionLocked(actions, batch, BatchDisposition.REQUEUE);
        } finally {
            lifecycleLock.unlock();
        }
        applyBatchActions(actions);
        return !actions.isEmpty()
                && actions.get(0).disposition == BatchDisposition.REQUEUE
                && actions.get(0).transitioned;
    }

    private void addBatchActionsLocked(
            List<BatchAction> actions, List<IndexBatch> batches, BatchDisposition disposition) {
        for (IndexBatch batch : batches) {
            addBatchActionLocked(actions, batch, disposition);
        }
    }

    private void addBatchActionLocked(
            List<BatchAction> actions, IndexBatch batch, BatchDisposition requestedDisposition) {
        if (!sendBuffer.ownsClaim(batch)) {
            return;
        }
        BatchDisposition disposition = requestedDisposition;
        if (lifecyclePhase != LifecyclePhase.OPEN || !batch.ownerActive()) {
            disposition = BatchDisposition.RELEASE;
        }
        actions.add(new BatchAction(batch, disposition));
    }

    private void applyBatchActions(List<BatchAction> actions) {
        for (BatchAction action : actions) {
            if (action.disposition == BatchDisposition.REQUEUE) {
                IndexBatch batch = action.batch;
                action.transitioned = sendBuffer.retryClaim(batch);
                if (action.transitioned) {
                    metrics.indexReplicationRetries().inc();
                }
            } else {
                action.transitioned = sendBuffer.completeClaim(action.batch);
            }
        }
        for (BatchAction action : actions) {
            TableBucket bucket = action.batch.targetBucket();
            if (sendBuffer.hasPending(bucket)) {
                enqueueReadyBucket(bucket);
            }
        }
    }

    private void applyBatchActionsAndNotifyWindows(List<BatchAction> actions) {
        applyBatchActions(actions);
        for (BatchAction action : actions) {
            if (action.disposition != BatchDisposition.ACK || !action.transitioned) {
                continue;
            }
            lifecycleLock.lock();
            try {
                if (lifecyclePhase != LifecyclePhase.OPEN || !action.batch.ownerActive()) {
                    continue;
                }
            } finally {
                lifecycleLock.unlock();
            }
            action.batch.window().onBatchAcked();
        }
    }

    private enum BatchDisposition {
        ACK,
        REQUEUE,
        RELEASE
    }

    private static final class BatchAction {
        private final IndexBatch batch;
        private final BatchDisposition disposition;
        private boolean transitioned;

        private BatchAction(IndexBatch batch, BatchDisposition disposition) {
            this.batch = batch;
            this.disposition = disposition;
        }
    }
}
