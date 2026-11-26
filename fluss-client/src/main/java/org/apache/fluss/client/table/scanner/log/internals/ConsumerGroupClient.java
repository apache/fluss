/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log.internals;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.log.LogScannerStatus;
import org.apache.fluss.client.table.scanner.log.OffsetCommitCallback;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeCommitConsumerOffsetsRequest;
import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.ExceptionUtils.stripFutureException;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Discovers and caches the tablet server that owns a consumer group, and commits/fetches consumer
 * offsets to/from it.
 *
 * <p>The scanner and RPC threads synchronize on this client to share the cached owner and its
 * in-flight discovery. RPC and timeout threads publish offset commit results to a concurrent queue.
 * The scanner thread drains the queue and invokes the callbacks.
 */
@Internal
public final class ConsumerGroupClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupClient.class);
    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;

    private final String groupId;
    private final MetadataUpdater metadataUpdater;
    private final LogScannerStatus logScannerStatus;
    private final Clock clock;
    private final long retryBackoffMs;
    private final boolean autoCommitEnabled;
    private final long autoCommitIntervalMs;
    private final long requestTimeoutMs;
    /** The cached owner gateway; null while the owner is unknown. */
    private TabletServerGateway ownerGateway;

    /** The pending owner-discovery request; null while no discovery is in flight. */
    private CompletableFuture<TabletServerGateway> ownerDiscovery;

    private volatile boolean closed;

    /** Submitted commits whose result has yet to be queued. */
    private final AtomicInteger pendingAsyncCommits = new AtomicInteger();

    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits =
            new ConcurrentLinkedQueue<>();

    /** Accessed only by scanner operations (including close), never by RPC callbacks. */
    private PendingOffsetFetchRequest pendingOffsetFetch;

    /** Deadline (in milliseconds) for the next auto-commit; 0 when auto-commit is disabled. */
    private long nextAutoCommitDeadlineMs;

    /** Creates the client for the consumer group configured for this scanner. */
    public ConsumerGroupClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            LogScannerStatus logScannerStatus) {
        this(
                conf,
                metadataUpdater,
                logScannerStatus,
                SystemClock.getInstance(),
                DEFAULT_RETRY_BACKOFF_MS);
    }

    @VisibleForTesting
    ConsumerGroupClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            LogScannerStatus logScannerStatus,
            Clock clock,
            long retryBackoffMs) {
        this.groupId =
                requireNonNull(
                        conf.get(ConfigOptions.CLIENT_CONSUMER_GROUP_ID),
                        "groupId must not be null");
        this.metadataUpdater = requireNonNull(metadataUpdater, "metadataUpdater must not be null");
        this.logScannerStatus =
                requireNonNull(logScannerStatus, "logScannerStatus must not be null");
        this.clock = requireNonNull(clock, "clock must not be null");
        if (retryBackoffMs < 0) {
            throw new IllegalArgumentException("retryBackoffMs must not be negative");
        }
        this.retryBackoffMs = retryBackoffMs;
        this.autoCommitEnabled = conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED);
        this.autoCommitIntervalMs =
                conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL).toMillis();
        if (autoCommitEnabled && autoCommitIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "autoCommitIntervalMs must be positive when auto-commit is enabled");
        }
        this.requestTimeoutMs = conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis();
        if (requestTimeoutMs < 0) {
            throw new IllegalArgumentException("requestTimeoutMs must not be negative");
        }
        this.nextAutoCommitDeadlineMs =
                autoCommitEnabled ? clock.milliseconds() + autoCommitIntervalMs : 0L;
    }

    /** Returns the cached owner gateway or shares the single in-flight discovery request. */
    public CompletableFuture<TabletServerGateway> getOrDiscoverOwner() {
        final CompletableFuture<TabletServerGateway> discovery;

        synchronized (this) {
            checkState(!closed, "Consumer group client is closed.");
            if (ownerGateway != null) {
                return CompletableFuture.completedFuture(ownerGateway);
            }
            if (ownerDiscovery != null && !ownerDiscovery.isCancelled()) {
                return ownerDiscovery;
            }

            discovery = new CompletableFuture<>();
            // Publish the future before initiating discovery, which may complete synchronously.
            ownerDiscovery = discovery;
        }

        try {
            discoverOwner()
                    .thenApply(metadataUpdater::newTabletServerClientForNode)
                    .whenComplete(
                            (gateway, error) -> completeOwnerDiscovery(discovery, gateway, error));
        } catch (Throwable error) {
            completeOwnerDiscovery(discovery, null, error);
        }
        return discovery;
    }

    /** Clears a stale owner so that the next operation performs owner discovery again. */
    public synchronized void markOwnerUnknown() {
        ownerGateway = null;
    }

    private void completeOwnerDiscovery(
            CompletableFuture<TabletServerGateway> discovery,
            TabletServerGateway gateway,
            Throwable error) {
        synchronized (this) {
            if (closed || ownerDiscovery != discovery) {
                // A late response must not restore state after close().
                return;
            }

            if (error == null && !discovery.isCancelled()) {
                ownerGateway = gateway;
            }
            ownerDiscovery = null;
        }

        // Complete the caller-visible future only after the cache and in-flight state are ready
        // for caller callbacks.
        if (error == null) {
            discovery.complete(gateway);
        } else {
            discovery.completeExceptionally(stripCompletionException(error));
        }
    }

    /** Commits a stable offsets snapshot and queues its callback when the RPC finishes. */
    public void commitOffsetsAsync(
            Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback) {
        requireNonNull(callback, "callback must not be null");

        checkState(!closed, "Consumer group client is closed.");
        pendingAsyncCommits.incrementAndGet();
        CompletableFuture<Void> completion = new CompletableFuture<>();
        completion.whenComplete(
                (ignored, error) ->
                        enqueueOffsetCommitCompletion(offsets, callback, asException(error)));
        // TODO: Remove this once #279 adds RPC request timeouts. This only times out the callback;
        // the underlying request can remain in flight.
        FutureUtils.orTimeout(
                completion,
                requestTimeoutMs,
                TimeUnit.MILLISECONDS,
                "Timed out committing consumer offsets for group " + groupId);
        try {
            sendCommit(offsets)
                    .whenComplete(
                            (result, error) -> FutureUtils.doForward(result, error, completion));
        } catch (RuntimeException error) {
            completion.completeExceptionally(error);
        }
    }

    private void enqueueOffsetCommitCompletion(
            Map<TableBucket, OffsetMetadata> offsets,
            OffsetCommitCallback callback,
            @Nullable Exception error) {
        completedOffsetCommits.add(new OffsetCommitCompletion(offsets, callback, error));
        pendingAsyncCommits.decrementAndGet();
        if (closed) {
            // A late response may have arrived after close cleared the queue.
            completedOffsetCommits.clear();
        }
    }

    /** Called by scanner operations to deliver completed callbacks outside the group monitor. */
    public void invokeCompletedOffsetCommitCallbacks() {
        OffsetCommitCompletion completion;
        while (!closed && (completion = completedOffsetCommits.poll()) != null) {
            completion.invoke();
        }
    }

    /** Commits a stable offsets snapshot, retrying retriable failures until the timeout elapses. */
    public boolean commitOffsetsSync(Map<TableBucket, OffsetMetadata> offsets, long timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must not be negative");
        }
        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        return commitOffsetsSyncBeforeDeadline(offsets, deadlineNanos);
    }

    private boolean commitOffsetsSyncBeforeDeadline(
            Map<TableBucket, OffsetMetadata> offsets, long deadlineNanos) {
        if (offsets.isEmpty()) {
            return true;
        }

        do {
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - clock.nanoseconds());
            try {
                sendCommit(offsets).get(Math.max(0L, remainingMs), TimeUnit.MILLISECONDS);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException("Interrupted while committing consumer offsets", e);
            } catch (TimeoutException e) {
                return false;
            } catch (ExecutionException | CancellationException e) {
                retryAfterFailure(
                        e,
                        deadlineNanos,
                        "Failed to commit consumer offsets",
                        "Interrupted while committing consumer offsets");
            }
        } while (clock.nanoseconds() < deadlineNanos);
        return false;
    }

    /** Fetches committed offsets for a stable bucket snapshot, retrying retriable failures. */
    public Map<TableBucket, OffsetMetadata> fetchCommittedOffsets(
            Set<TableBucket> tableBuckets, long timeoutMs) {
        requireNonNull(tableBuckets, "tableBuckets must not be null");
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must not be negative");
        }
        if (tableBuckets.isEmpty()) {
            return Collections.emptyMap();
        }

        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        do {
            PendingOffsetFetchRequest pending = getOrCreateOffsetFetch(tableBuckets);
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - clock.nanoseconds());
            try {
                Map<TableBucket, OffsetMetadata> offsets =
                        pending.response.get(Math.max(0L, remainingMs), TimeUnit.MILLISECONDS);
                if (pendingOffsetFetch == pending) {
                    pendingOffsetFetch = null;
                }
                return offsets;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException(
                        "Interrupted while fetching committed consumer offsets", e);
            } catch (TimeoutException e) {
                return null;
            } catch (ExecutionException | CancellationException e) {
                if (pendingOffsetFetch == pending) {
                    pendingOffsetFetch = null;
                }
                retryAfterFailure(
                        e,
                        deadlineNanos,
                        "Failed to fetch committed consumer offsets",
                        "Interrupted while fetching committed consumer offsets");
            }
        } while (clock.nanoseconds() < deadlineNanos);
        return null;
    }

    private void retryAfterFailure(
            Throwable error,
            long deadlineNanos,
            String failureMessage,
            String interruptionMessage) {
        Throwable cause = stripFutureException(error);
        if (!(cause instanceof RetriableException)) {
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new FlussRuntimeException(failureMessage, cause);
        }
        long backoffMs =
                Math.min(
                        retryBackoffMs,
                        TimeUnit.NANOSECONDS.toMillis(deadlineNanos - clock.nanoseconds()));
        if (backoffMs > 0) {
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException(interruptionMessage, interrupted);
            }
        }
    }

    /**
     * Reads the current offsets and commits them asynchronously only when the interval has elapsed.
     */
    public boolean maybeAutoCommitOffsetsAsync() {
        if (!autoCommitEnabled) {
            return false;
        }
        long nowMs = clock.milliseconds();
        if (nowMs < nextAutoCommitDeadlineMs) {
            return false;
        }
        nextAutoCommitDeadlineMs = nowMs + autoCommitIntervalMs;
        commitOffsetsAsync(
                logScannerStatus.allConsumed(),
                (committed, error) -> {
                    if (error != null) {
                        LOG.warn("Failed to auto-commit offsets for group {}", groupId, error);
                    }
                });
        return true;
    }

    private PendingOffsetFetchRequest getOrCreateOffsetFetch(Set<TableBucket> requestedBuckets) {
        PendingOffsetFetchRequest current = pendingOffsetFetch;
        if (current != null && current.requestedBuckets.equals(requestedBuckets)) {
            return current;
        }

        PendingOffsetFetchRequest created =
                new PendingOffsetFetchRequest(
                        requestedBuckets, sendGetConsumerOffsetsRequest(requestedBuckets));
        if (current != null) {
            current.response.cancel(false);
        }
        pendingOffsetFetch = created;
        return created;
    }

    private CompletableFuture<Map<TableBucket, OffsetMetadata>> sendGetConsumerOffsetsRequest(
            Set<TableBucket> tableBuckets) {
        GetConsumerOffsetsRequest request =
                ClientRpcMessageUtils.makeGetConsumerOffsetsRequest(groupId, tableBuckets);
        return getOrDiscoverOwner()
                .thenCompose(gateway -> gateway.getConsumerOffsets(request))
                .thenApply(ClientRpcMessageUtils::toConsumerOffsets)
                .whenComplete(
                        (ignored, error) -> {
                            if (shouldForgetOwner(stripFutureException(error))) {
                                markOwnerUnknown();
                            }
                        });
    }

    private CompletableFuture<Void> sendCommit(Map<TableBucket, OffsetMetadata> offsets) {
        if (offsets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CommitConsumerOffsetsRequest request = makeCommitConsumerOffsetsRequest(groupId, offsets);
        return getOrDiscoverOwner()
                .thenCompose(gateway -> gateway.commitConsumerOffsets(request))
                .thenApply(ClientRpcMessageUtils::checkCommitConsumerOffsetsResponse)
                .whenComplete(
                        (ignored, error) -> {
                            if (shouldForgetOwner(stripFutureException(error))) {
                                markOwnerUnknown();
                            }
                        });
    }

    private CompletableFuture<ServerNode> discoverOwner() {
        ServerNode anyTabletServer = metadataUpdater.getRandomTabletServer();
        if (anyTabletServer == null) {
            return FutureUtils.completedExceptionally(
                    new ConsumerGroupOwnerNotAvailableException(
                            "No tablet server is currently known to the client for group "
                                    + groupId));
        }

        // Create the gateway directly from the discovered node; looking up its id again could
        // miss the metadata cache if it was refreshed in between.
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(anyTabletServer);

        FindConsumerGroupOwnerRequest request =
                new FindConsumerGroupOwnerRequest().setGroupId(groupId);
        return gateway.findConsumerGroupOwner(request)
                .thenApply(ClientRpcMessageUtils::toConsumerGroupOwner);
    }

    private static boolean shouldForgetOwner(Throwable error) {
        if (error == null) {
            return false;
        }
        Errors errors = Errors.forException(error);
        return errors == Errors.NOT_CONSUMER_GROUP_OWNER
                || errors == Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE
                || errors == Errors.CONSUMER_GROUP_OWNER_LOADING
                || errors == Errors.NETWORK_EXCEPTION;
    }

    private static Exception asException(Throwable error) {
        Throwable cause = stripFutureException(error);
        if (cause == null) {
            return null;
        }
        if (cause instanceof TimeoutException) {
            return new org.apache.fluss.exception.TimeoutException(cause);
        }
        return org.apache.fluss.utils.ExceptionUtils.toException(
                cause instanceof Exception ? cause : new FlussRuntimeException(cause));
    }

    /**
     * Submits an auto-commit if due, waits for outstanding commits, and releases group resources.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(requestTimeoutMs);
        try {
            maybeAutoCommitOffsetsAsync();
            awaitPendingOffsetCommitCallbacks(deadlineNanos);
        } finally {
            releaseGroupResources();
        }
    }

    private void awaitPendingOffsetCommitCallbacks(long deadlineNanos) {
        try {
            // A completed request can still have a callback queued for the closing thread.
            while ((pendingAsyncCommits.get() > 0 || !completedOffsetCommits.isEmpty())
                    && deadlineNanos - clock.nanoseconds() > 0
                    && !Thread.currentThread().isInterrupted()) {
                invokeCompletedOffsetCommitCallbacks();
                if (pendingAsyncCommits.get() > 0) {
                    long remainingNanos = deadlineNanos - clock.nanoseconds();
                    // TODO optimize this to use a single wait with a timeout instead of polling.
                    TimeUnit.NANOSECONDS.sleep(
                            Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(100)));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug(
                    "Interrupted while awaiting offset commit callbacks for group {}", groupId, e);
        }
    }

    private void releaseGroupResources() {
        final CompletableFuture<TabletServerGateway> discovery;
        synchronized (this) {
            closed = true;
            ownerGateway = null;
            discovery = ownerDiscovery;
            ownerDiscovery = null;
        }
        completedOffsetCommits.clear();

        if (discovery != null) {
            // Waiting callers receive a CancellationException. The interrupt flag is false
            // because CompletableFuture cannot interrupt the underlying RPC.
            discovery.cancel(false);
        }
        PendingOffsetFetchRequest pendingFetch = pendingOffsetFetch;
        pendingOffsetFetch = null;
        if (pendingFetch != null) {
            pendingFetch.response.cancel(false);
        }
    }

    private static final class OffsetCommitCompletion {
        private final Map<TableBucket, OffsetMetadata> offsets;
        private final OffsetCommitCallback callback;
        @Nullable private final Exception error;

        private OffsetCommitCompletion(
                Map<TableBucket, OffsetMetadata> offsets,
                OffsetCommitCallback callback,
                @Nullable Exception error) {
            this.offsets = offsets;
            this.callback = callback;
            this.error = error;
        }

        public void invoke() {
            try {
                callback.onComplete(offsets, error);
            } catch (Throwable t) {
                LOG.warn("Offset commit callback threw an uncaught exception", t);
            }
        }
    }

    private static final class PendingOffsetFetchRequest {
        private final Set<TableBucket> requestedBuckets;
        private final CompletableFuture<Map<TableBucket, OffsetMetadata>> response;

        private PendingOffsetFetchRequest(
                Set<TableBucket> requestedBuckets,
                CompletableFuture<Map<TableBucket, OffsetMetadata>> response) {
            this.requestedBuckets = requestedBuckets;
            this.response = response;
        }
    }
}
