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
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutIndexReqForBucket;
import org.apache.fluss.rpc.messages.PbPutIndexRespForBucket;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.ProtoCodecUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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

import static org.apache.fluss.rpc.protocol.MessageCodec.REQUEST_HEADER_LENGTH;
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

    private static final int DEFAULT_ACKS = -1;

    private static final long MAX_CODEC_FRAMED_BYTES = Integer.MAX_VALUE;
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final int PUT_INDEX_BUCKETS_REQ_FIELD_NUMBER = 5;
    private static final int BUCKETS_REQ_TAG_SIZE =
            ProtoCodecUtils.computeVarIntSize(
                    (PUT_INDEX_BUCKETS_REQ_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
                            | ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED);

    private final IndexSendBuffer sendBuffer;
    private final LeaderResolver leaderResolver;
    private final Function<Integer, TabletServerGateway> gatewayFactory;
    private final TabletServerMetricGroup metrics;
    private final SenderWorker[] workers;
    private final AtomicBoolean bucketRescanRequired = new AtomicBoolean();

    /** Base backoff before retrying a failed batch; grows exponentially with attempts. */
    private final long retryBackoffMs;

    /** Upper bound on the exponential retry backoff. */
    private final long retryMaxBackoffMs;

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
    private long nextTargetGeneration;

    /**
     * Creates and starts an index sender.
     *
     * @param sendBuffer shared buffer of index batches awaiting delivery
     * @param leaderResolver resolver for target index-bucket leaders
     * @param gatewayFactory factory for tablet-server gateways
     * @param metrics tablet-server metrics updated by the sender
     * @param numWorkers number of sender workers
     * @param backoffMs idle-worker backoff in milliseconds
     * @param retryBackoffMs initial retry backoff in milliseconds
     * @param retryMaxBackoffMs maximum retry backoff in milliseconds
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
            long retryBackoffMs,
            long retryMaxBackoffMs,
            long maxRequestBytes,
            long maxTransportRequestBytes,
            long requestTimeoutMs) {
        checkArgument(numWorkers > 0, "numWorkers must be positive");
        checkArgument(backoffMs > 0, "backoffMs must be positive");
        checkArgument(retryBackoffMs > 0, "retryBackoffMs must be positive");
        checkArgument(
                retryMaxBackoffMs >= retryBackoffMs,
                "retryMaxBackoffMs must be greater than or equal to retryBackoffMs");
        checkArgument(maxRequestBytes > 0, "maxRequestBytes must be positive");
        checkArgument(maxTransportRequestBytes > 0, "maxTransportRequestBytes must be positive");
        checkArgument(requestTimeoutMs > 0, "requestTimeoutMs must be positive");
        this.sendBuffer = sendBuffer;
        this.leaderResolver = leaderResolver;
        this.gatewayFactory = gatewayFactory;
        this.metrics = metrics;
        this.retryBackoffMs = retryBackoffMs;
        this.retryMaxBackoffMs = retryMaxBackoffMs;
        this.maxRequestBytes = maxRequestBytes;
        this.maxTransportRequestBytes = maxTransportRequestBytes;
        this.requestTimeoutMs = requestTimeoutMs;
        this.workers = new SenderWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            this.workers[i] = new SenderWorker("index-sender-" + i, i, backoffMs);
        }
        sendBuffer.setAppendListener(this::notifyReadyBucket);
        for (TableBucket bucket : sendBuffer.buckets()) {
            enqueueReadyBucket(bucket);
        }
        for (SenderWorker worker : workers) {
            worker.start();
        }
        LOG.info("IndexSender started with {} workers", numWorkers);
    }

    /**
     * Exponential backoff for the given attempt count (1 = first retry), capped at {@link
     * #retryMaxBackoffMs}.
     */
    private long retryDelayMs(int attempts) {
        if (attempts <= 1) {
            return retryBackoffMs;
        }
        long delay = retryBackoffMs;
        for (int i = 1; i < attempts && delay < retryMaxBackoffMs; i++) {
            delay <<= 1;
        }
        return Math.min(delay, retryMaxBackoffMs);
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
            nextTargetGeneration++;
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
        boolean interrupted = false;
        try {
            while (lifecyclePhase != LifecyclePhase.CLOSED) {
                try {
                    lifecycleDrained.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** A single sender worker that owns the buckets hashing to its index. */
    private final class SenderWorker extends ShutdownableThread {

        private final int workerId;
        private final long backoffMs;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private final Deque<TableBucket> readyBuckets = new ArrayDeque<>();
        private final Set<TableBucket> queuedBuckets = new HashSet<>();

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
                        if (!queuedBuckets.add(bucket)) {
                            return;
                        }
                        readyBuckets.addLast(bucket);
                        condition.signalAll();
                    });
        }

        @Nullable
        private TableBucket pollReadyBucket() {
            return inLock(
                    lock,
                    () -> {
                        TableBucket bucket = readyBuckets.pollFirst();
                        if (bucket != null) {
                            queuedBuckets.remove(bucket);
                        }
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
            long now = System.currentTimeMillis();
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
                    IndexBatch batch = sendBuffer.claimFirstReady(bucket, now);
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

            List<IndexBatch> activeClaimed = new ArrayList<>(claimed.size());
            for (IndexBatch batch : claimed) {
                if (!batch.ownerActive()) {
                    discardClaimedBatch(batch);
                    continue;
                }
                RequestSizeAccumulator singleton =
                        newRequestSizeAccumulator(
                                batch.targetBucket().getTableId(),
                                batch.sourceBucket().getTableId(),
                                requestTimeoutMs);
                singleton.addBucket(batch);
                if (isOversized(singleton)) {
                    failOversizedBatch(batch, singleton);
                } else if (batch.ownerActive()) {
                    activeClaimed.add(batch);
                }
            }

            // Group claimed batches by target leader server.
            Map<Integer, List<IndexBatch>> byServer = new HashMap<>();
            for (IndexBatch batch : activeClaimed) {
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
                } else {
                    retryClaimedBatch(batch);
                }
            }

            for (Map.Entry<Integer, List<IndexBatch>> serverEntry : byServer.entrySet()) {
                sendToServer(serverEntry.getKey(), serverEntry.getValue());
            }
            return true;
        }

        private void sendToServer(int serverId, List<IndexBatch> batches) {
            batches = activeBatches(batches);
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
            runAccounting(actions);

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
                    target = new TargetContext(serverId, gateway, ++nextTargetGeneration);
                    targetsByServer.put(serverId, target);
                }
            } finally {
                lifecycleLock.unlock();
            }
            if (failure != null) {
                LOG.warn("Failed to get gateway for server {}, re-enqueuing", serverId, failure);
            }
            runAccounting(actions);
            if (target != null) {
                dispatchIfCurrent(target, batches);
            }
        }

        private void dispatchIfCurrent(TargetContext target, List<IndexBatch> batches) {
            batches = activeBatches(batches);
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
            if (isOversized(requestChunk.size)) {
                for (IndexBatch batch : requestChunk.batches) {
                    failOversizedBatch(batch, requestChunk.size);
                }
                return;
            }
            requestChunk = activeChunk(tableId, requestChunk);
            if (requestChunk == null) {
                return;
            }
            List<IndexBatch> requestBatches = requestChunk.batches;
            PutIndexRequest request = buildRequest(tableId, requestBatches);
            boolean leadersCurrent = leadersMatch(target.serverId, requestBatches);
            List<BatchAction> actions = new ArrayList<>();
            boolean registered = false;
            lifecycleLock.lock();
            try {
                if (!targetIsCurrentLocked(target) || !leadersCurrent) {
                    invalidateTargetLocked(target);
                    addRetryOrReleaseActionsLocked(actions, requestBatches);
                } else if (!batchesActiveAndClaimed(requestBatches)) {
                    addRetryOrReleaseActionsLocked(actions, requestBatches);
                } else {
                    registerAsyncOperationLocked();
                    registered = true;
                }
            } finally {
                lifecycleLock.unlock();
            }
            runAccounting(actions);
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
            List<IndexBatch> active = activeBatches(chunk.batches);
            if (active.isEmpty()) {
                return null;
            }
            if (active.size() == chunk.batches.size()) {
                return chunk;
            }
            RequestSizeAccumulator activeSize =
                    newRequestSizeAccumulator(
                            tableId, active.get(0).sourceBucket().getTableId(), requestTimeoutMs);
            for (IndexBatch batch : active) {
                activeSize.addBucket(batch);
            }
            return new RequestChunk(active, activeSize);
        }

        private List<IndexBatch> activeBatches(List<IndexBatch> batches) {
            List<IndexBatch> active = new ArrayList<>(batches.size());
            for (IndexBatch batch : batches) {
                if (batch.ownerActive()) {
                    active.add(batch);
                } else {
                    discardClaimedBatch(batch);
                }
            }
            return active;
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
                    addRetryOrReleaseActionsLocked(actions, chunk);
                } else if (error != null) {
                    invalidateTargetLocked(target);
                    LOG.warn("PutIndex failed for tableId={}", tableId, error);
                    addRetryOrReleaseActionsLocked(actions, chunk);
                } else {
                    addResponseActionsLocked(actions, chunk, response);
                }
            } finally {
                lifecycleLock.unlock();
            }
            metrics.indexReplicationRequestLatencyHistogram()
                    .update((System.nanoTime() - requestStartNs) / 1_000_000L);
            try {
                runAccountingAndCallbacks(actions);
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
        if (targetsByServer.remove(target.serverId, target)) {
            nextTargetGeneration++;
        }
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
            completeAsyncOperationLocked();
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void completeAsyncOperationLocked() {
        if (--outstandingAsyncOperations == 0) {
            lifecycleDrained.signalAll();
        }
    }

    private static final class TargetContext {
        private final int serverId;
        private final TabletServerGateway gateway;
        /**
         * Sender-local identity; a new value is allocated only after exact-context invalidation.
         */
        private final long generation;

        private TargetContext(int serverId, TabletServerGateway gateway, long generation) {
            this.serverId = serverId;
            this.gateway = gateway;
            this.generation = generation;
        }
    }

    private static final class RequestChunk {
        private final List<IndexBatch> batches;
        private final RequestSizeAccumulator size;

        private RequestChunk(List<IndexBatch> batches, RequestSizeAccumulator size) {
            this.batches = batches;
            this.size = size;
        }
    }

    /** Incremental codec-derived size of one index PutIndex request. */
    static final class RequestSizeAccumulator {
        private final PbPutIndexReqForBucket bucketSizer;
        private long bodyBytes;
        private boolean overflow;
        private boolean codecRepresentable;

        private RequestSizeAccumulator(long tableId, long sourceTableId, long requestTimeoutMs) {
            this.bucketSizer = new PbPutIndexReqForBucket();
            this.bodyBytes =
                    newPutIndexRequest(tableId, sourceTableId, (int) requestTimeoutMs).totalSize();
            this.codecRepresentable = bodyBytes >= 0;
        }

        private RequestSizeAccumulator(RequestSizeAccumulator source) {
            this.bucketSizer = source.bucketSizer;
            this.bodyBytes = source.bodyBytes;
            this.overflow = source.overflow;
            this.codecRepresentable = source.codecRepresentable;
        }

        void addBucket(IndexBatch batch) {
            if (overflow || !codecRepresentable) {
                return;
            }
            try {
                bucketSizer
                        .clear()
                        .setBucketId(batch.targetBucket().getBucket())
                        .setSourceBucketId(batch.sourceBucket().getBucket())
                        .setSourceEndOffset(batch.sourceEndOffset())
                        .setProgressKey(batch.progressKey())
                        .setRecordsBytesView(batch.encoded());
                if (batch.sourceBucket().getPartitionId() != null) {
                    bucketSizer.setSourcePartitionId(batch.sourceBucket().getPartitionId());
                }
                int bucketBodyBytes = bucketSizer.totalSize();
                if (bucketBodyBytes < 0) {
                    codecRepresentable = false;
                    return;
                }
                long bucketBytes =
                        (long) BUCKETS_REQ_TAG_SIZE
                                + ProtoCodecUtils.computeVarIntSize(bucketBodyBytes)
                                + bucketBodyBytes;
                bodyBytes = Math.addExact(bodyBytes, bucketBytes);
                codecRepresentable = bodyBytes <= Integer.MAX_VALUE;
            } catch (ArithmeticException e) {
                overflow = true;
                codecRepresentable = false;
            }
        }

        long framedBytes() {
            if (arithmeticOverflow()) {
                return Long.MAX_VALUE;
            }
            return bodyBytes + REQUEST_HEADER_LENGTH + LENGTH_PREFIX_BYTES;
        }

        boolean arithmeticOverflow() {
            return overflow
                    || bodyBytes > Long.MAX_VALUE - REQUEST_HEADER_LENGTH - LENGTH_PREFIX_BYTES;
        }

        boolean codecRepresentable() {
            return codecRepresentable
                    && !arithmeticOverflow()
                    && framedBytes() <= MAX_CODEC_FRAMED_BYTES;
        }

        private RequestSizeAccumulator copy() {
            return new RequestSizeAccumulator(this);
        }
    }

    private static RequestSizeAccumulator newRequestSizeAccumulator(
            long tableId, long sourceTableId, long requestTimeoutMs) {
        return new RequestSizeAccumulator(tableId, sourceTableId, requestTimeoutMs);
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
     * bound. A singleton above the hard bound remains intact so it can enter terminal failure.
     */
    private List<RequestChunk> splitByRequestBytes(long tableId, List<IndexBatch> batches) {
        List<RequestChunk> chunks = new ArrayList<>();
        List<IndexBatch> current = new ArrayList<>();
        long currentPayloadBytes = 0L;
        long sourceTableId = batches.get(0).sourceBucket().getTableId();
        RequestSizeAccumulator currentSize =
                newRequestSizeAccumulator(tableId, sourceTableId, requestTimeoutMs);
        for (IndexBatch batch : batches) {
            long batchBytes = batch.encoded().getBytesLength();
            RequestSizeAccumulator candidate = currentSize.copy();
            candidate.addBucket(batch);
            long candidatePayloadBytes = saturatedAdd(currentPayloadBytes, batchBytes);
            boolean exceedsPreferred = candidatePayloadBytes > maxRequestBytes;
            if (!current.isEmpty() && (exceedsPreferred || isOversized(candidate))) {
                chunks.add(new RequestChunk(current, currentSize));
                current = new ArrayList<>();
                currentSize = newRequestSizeAccumulator(tableId, sourceTableId, requestTimeoutMs);
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
                if (bucketResp == null) {
                    LOG.warn("PutIndex response missing bucket {}, re-enqueuing", bucketId);
                } else {
                    LOG.warn(
                            "PutIndex failed for bucket {} with error code {}, re-enqueuing",
                            bucketId,
                            bucketResp.getErrorCode());
                }
                addBatchActionLocked(actions, batch, BatchDisposition.REQUEUE);
            } else {
                addBatchActionLocked(actions, batch, BatchDisposition.ACK);
            }
        }
    }

    private boolean isOversized(RequestSizeAccumulator size) {
        return !size.codecRepresentable() || size.framedBytes() > maxTransportRequestBytes;
    }

    private void failOversizedBatch(IndexBatch batch, RequestSizeAccumulator size) {
        RecordTooLargeException failure = oversizedFailure(batch, size);
        IndexReplicationWindow window = batch.window();
        if (!window.tryFail(failure)) {
            return;
        }
        metrics.indexReplicationFailures().inc();
    }

    private RecordTooLargeException oversizedFailure(
            IndexBatch batch, RequestSizeAccumulator size) {
        long tableId = batch.targetBucket().getTableId();
        if (size.arithmeticOverflow()) {
            return new RecordTooLargeException(
                    "Index PutIndex request for table "
                            + tableId
                            + " overflows exact size arithmetic and cannot be represented by "
                            + "the request codec");
        }
        if (!size.codecRepresentable()) {
            return new RecordTooLargeException(
                    "Index PutIndex request for table "
                            + tableId
                            + " is "
                            + size.framedBytes()
                            + " bytes, exceeding codec maximum framed size="
                            + MAX_CODEC_FRAMED_BYTES);
        }
        return new RecordTooLargeException(
                "Index PutIndex request for table "
                        + tableId
                        + " is "
                        + size.framedBytes()
                        + " bytes, exceeding netty.server.max-request-size="
                        + maxTransportRequestBytes);
    }

    private void discardClaimedBatch(IndexBatch batch) {
        List<BatchAction> actions = new ArrayList<>(1);
        lifecycleLock.lock();
        try {
            addBatchActionLocked(actions, batch, BatchDisposition.RELEASE);
        } finally {
            lifecycleLock.unlock();
        }
        runAccounting(actions);
    }

    private void retryClaimedBatch(IndexBatch batch) {
        List<BatchAction> actions = new ArrayList<>();
        lifecycleLock.lock();
        try {
            addRetryOrReleaseActionLocked(actions, batch);
        } finally {
            lifecycleLock.unlock();
        }
        runAccounting(actions);
    }

    private void addRetryOrReleaseActionsLocked(
            List<BatchAction> actions, List<IndexBatch> batches) {
        for (IndexBatch batch : batches) {
            addRetryOrReleaseActionLocked(actions, batch);
        }
    }

    private void addRetryOrReleaseActionLocked(List<BatchAction> actions, IndexBatch batch) {
        addBatchActionLocked(
                actions,
                batch,
                lifecyclePhase == LifecyclePhase.OPEN
                        ? BatchDisposition.REQUEUE
                        : BatchDisposition.RELEASE);
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

    private void runAccounting(List<BatchAction> actions) {
        if (actions.isEmpty()) {
            return;
        }
        for (BatchAction action : actions) {
            if (action.disposition == BatchDisposition.REQUEUE) {
                IndexBatch batch = action.batch;
                long readyAtMs = System.currentTimeMillis() + retryDelayMs(batch.attempts() + 1);
                action.transitioned = sendBuffer.retryClaim(batch, readyAtMs);
                if (action.transitioned) {
                    metrics.indexReplicationRetries().inc();
                }
            } else if (action.disposition == BatchDisposition.ACK) {
                action.transitioned = sendBuffer.acknowledgeClaim(action.batch);
            } else {
                action.transitioned = sendBuffer.discardClaim(action.batch);
            }
        }
        for (BatchAction action : actions) {
            TableBucket bucket = action.batch.targetBucket();
            if (sendBuffer.hasPending(bucket)) {
                enqueueReadyBucket(bucket);
            }
        }
    }

    private void runAccountingAndCallbacks(List<BatchAction> actions) {
        runAccounting(actions);
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

    private PutIndexRequest buildRequest(long tableId, List<IndexBatch> batches) {
        long sourceTableId = batches.get(0).sourceBucket().getTableId();
        PutIndexRequest req = newPutIndexRequest(tableId, sourceTableId, (int) requestTimeoutMs);
        for (IndexBatch batch : batches) {
            BytesView encoded = batch.encoded();
            checkArgument(
                    batch.sourceBucket().getTableId() == sourceTableId,
                    "A PutIndex request cannot mix source tables");
            PbPutIndexReqForBucket pb =
                    req.addBucketsReq().setBucketId(batch.targetBucket().getBucket());
            pb.setSourceBucketId(batch.sourceBucket().getBucket())
                    .setSourceEndOffset(batch.sourceEndOffset())
                    .setProgressKey(batch.progressKey());
            if (batch.sourceBucket().getPartitionId() != null) {
                pb.setSourcePartitionId(batch.sourceBucket().getPartitionId());
            }
            pb.setRecordsBytesView(encoded);
        }
        return req;
    }

    private static PutIndexRequest newPutIndexRequest(
            long tableId, long sourceTableId, int timeoutMs) {
        return new PutIndexRequest()
                .setTableId(tableId)
                .setSourceTableId(sourceTableId)
                .setAcks(DEFAULT_ACKS)
                .setTimeoutMs(timeoutMs);
    }
}
