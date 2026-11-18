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
import org.apache.fluss.client.table.scanner.log.OffsetCommitCallback;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.ExceptionUtils.stripFutureException;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Discovers and caches the tablet server that owns a consumer group, and commits/fetches consumer
 * offsets to/from it.
 *
 * <p>Discovery is initiated by scanner threads while RPC completion callbacks run on network
 * threads; both coordinate through this object's monitor, mirroring how LogFetcher shares state
 * between the scanner thread and fetch-response callbacks. Owner state is guarded by {@code this},
 * and the monitor is never held across blocking waits. RPC and timeout threads enqueue completed
 * offset-commit callbacks for the scanner thread to invoke without holding that monitor.
 */
@Internal
public final class ConsumerGroupClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupClient.class);
    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;

    private final String groupId;
    private final MetadataUpdater metadataUpdater;
    private final Clock clock;
    private final long retryBackoffMs;
    private final boolean autoCommitEnabled;
    private final long autoCommitIntervalMs;
    private final long asyncCommitTimeoutMs;
    /** The cached owner gateway; null while the owner is unknown. */
    private TabletServerGateway ownerGateway;

    /** The pending owner-discovery request; null while no discovery is in flight. */
    private CompletableFuture<TabletServerGateway> ownerDiscovery;

    /** Whether the manager has been closed; guarded by this object's monitor. */
    private boolean closed;

    /** Commit callbacks not yet queued for delivery; guarded by this object's monitor. */
    private final Set<PendingOffsetCommitCallback> pendingOffsetCommitCallbacks = new HashSet<>();

    /** Completed callbacks awaiting scanner delivery; guarded by this object's monitor. */
    private final Queue<Runnable> completedOffsetCommitCallbacks = new ArrayDeque<>();

    /** Accessed only by scanner operations (including close), never by RPC callbacks. */
    private PendingCommittedOffsetRequest pendingOffsetFetch;
    /** Deadline (in milliseconds) for the next auto-commit; 0 when auto-commit is disabled. */
    private long nextAutoCommitDeadlineMs;

    /**
     * Creates a manager that discovers and caches the owner of one consumer group.
     *
     * @param groupId the consumer group whose owner is discovered
     * @param metadataUpdater the client metadata used to pick a tablet server for discovery and to
     *     open gateways to the discovered owner
     */
    @VisibleForTesting
    ConsumerGroupClient(String groupId, MetadataUpdater metadataUpdater) {
        this(
                groupId,
                metadataUpdater,
                SystemClock.getInstance(),
                DEFAULT_RETRY_BACKOFF_MS,
                false,
                0L,
                ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis());
    }

    public ConsumerGroupClient(
            String groupId,
            MetadataUpdater metadataUpdater,
            boolean autoCommitEnabled,
            long autoCommitIntervalMs,
            long asyncCommitTimeoutMs) {
        this(
                groupId,
                metadataUpdater,
                SystemClock.getInstance(),
                DEFAULT_RETRY_BACKOFF_MS,
                autoCommitEnabled,
                autoCommitIntervalMs,
                asyncCommitTimeoutMs);
    }

    @VisibleForTesting
    ConsumerGroupClient(
            String groupId, MetadataUpdater metadataUpdater, Clock clock, long retryBackoffMs) {
        this(
                groupId,
                metadataUpdater,
                clock,
                retryBackoffMs,
                false,
                0L,
                ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis());
    }

    @VisibleForTesting
    ConsumerGroupClient(
            String groupId,
            MetadataUpdater metadataUpdater,
            Clock clock,
            long retryBackoffMs,
            boolean autoCommitEnabled,
            long autoCommitIntervalMs,
            long asyncCommitTimeoutMs) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.metadataUpdater =
                Objects.requireNonNull(metadataUpdater, "metadataUpdater must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        if (retryBackoffMs < 0) {
            throw new IllegalArgumentException("retryBackoffMs must not be negative");
        }
        this.retryBackoffMs = retryBackoffMs;
        if (autoCommitEnabled && autoCommitIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "autoCommitIntervalMs must be positive when auto-commit is enabled");
        }
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        if (asyncCommitTimeoutMs < 0) {
            throw new IllegalArgumentException("asyncCommitTimeoutMs must not be negative");
        }
        this.asyncCommitTimeoutMs = asyncCommitTimeoutMs;
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
        Objects.requireNonNull(callback, "callback must not be null");

        PendingOffsetCommitCallback pending = new PendingOffsetCommitCallback(offsets, callback);
        CompletableFuture<Void> completion = pending.completion;
        completion.whenComplete(
                (ignored, error) -> enqueueCompletedOffsetCommitCallback(pending, error));
        synchronized (this) {
            checkState(!closed, "Consumer group client is closed.");
            pendingOffsetCommitCallbacks.add(pending);
        }
        // TODO: Remove this once #279 adds RPC request timeouts. This only times out the callback;
        // the underlying request can remain in flight.
        FutureUtils.orTimeout(
                completion,
                asyncCommitTimeoutMs,
                TimeUnit.MILLISECONDS,
                "Timed out committing consumer offsets for group " + groupId);
        try {
            sendCommit(offsets)
                    .whenComplete(
                            (ignored, error) -> {
                                if (error == null) {
                                    completion.complete(null);
                                } else {
                                    completion.completeExceptionally(error);
                                }
                            });
        } catch (RuntimeException error) {
            completion.completeExceptionally(error);
        }
    }

    private void enqueueCompletedOffsetCommitCallback(
            PendingOffsetCommitCallback pending, Throwable error) {
        synchronized (this) {
            if (!pendingOffsetCommitCallbacks.remove(pending)) {
                return;
            }
            completedOffsetCommitCallbacks.add(
                    () -> {
                        try {
                            pending.callback.onComplete(pending.offsets, asException(error));
                        } catch (RuntimeException callbackError) {
                            LOG.warn(
                                    "Consumer offset commit callback failed for group {}",
                                    groupId,
                                    callbackError);
                        }
                    });
        }
    }

    /** Called by scanner operations to deliver completed callbacks outside the group monitor. */
    public void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            Runnable callback;
            synchronized (this) {
                callback = completedOffsetCommitCallbacks.poll();
            }
            if (callback == null) {
                return;
            }
            callback.run();
        }
    }

    /** Commits a stable offsets snapshot, retrying retriable failures until the timeout elapses. */
    public boolean commitOffsetsSync(Map<TableBucket, OffsetMetadata> offsets, long timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must not be negative");
        }
        if (offsets.isEmpty()) {
            return true;
        }

        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
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
        Objects.requireNonNull(tableBuckets, "tableBuckets must not be null");
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must not be negative");
        }
        if (tableBuckets.isEmpty()) {
            return Collections.emptyMap();
        }

        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        do {
            PendingCommittedOffsetRequest pending = getOrCreateOffsetFetch(tableBuckets);
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
    public boolean maybeAutoCommitOffsetsAsync(
            long nowMs, Supplier<Map<TableBucket, OffsetMetadata>> offsets) {
        if (!autoCommitEnabled) {
            return false;
        }
        if (nowMs < nextAutoCommitDeadlineMs) {
            return false;
        }
        nextAutoCommitDeadlineMs = nowMs + autoCommitIntervalMs;
        commitOffsetsAsync(
                offsets.get(),
                (committed, error) -> {
                    if (error != null) {
                        LOG.warn("Failed to auto-commit offsets for group {}", groupId, error);
                    }
                });
        return true;
    }

    /** Performs the final synchronous auto-commit used by scanner shutdown. */
    public boolean maybeAutoCommitOffsetsSync(
            Map<TableBucket, OffsetMetadata> offsets, long timeoutMs) {
        return !autoCommitEnabled || commitOffsetsSync(offsets, timeoutMs);
    }

    private PendingCommittedOffsetRequest getOrCreateOffsetFetch(
            Set<TableBucket> requestedBuckets) {
        PendingCommittedOffsetRequest current = pendingOffsetFetch;
        if (current != null && current.requestedBuckets.equals(requestedBuckets)) {
            return current;
        }

        PendingCommittedOffsetRequest created =
                new PendingCommittedOffsetRequest(
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
        return getOrDiscoverOwner()
                .thenCompose(
                        gateway ->
                                gateway.commitConsumerOffsets(
                                        ClientRpcMessageUtils.makeCommitConsumerOffsetsRequest(
                                                groupId, offsets)))
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

    /** Cancels unfinished commits, queues their callbacks, and forgets the owner. */
    @Override
    public void close() {
        final CompletableFuture<TabletServerGateway> discovery;
        final Set<PendingOffsetCommitCallback> pendingCallbacks;

        synchronized (this) {
            closed = true;
            ownerGateway = null;
            discovery = ownerDiscovery;
            ownerDiscovery = null;
            pendingCallbacks = new HashSet<>(pendingOffsetCommitCallbacks);
            for (PendingOffsetCommitCallback pending : pendingCallbacks) {
                pending.completion.completeExceptionally(
                        new CancellationException(
                                "Consumer group client closed before commit completed"));
                if (pendingOffsetCommitCallbacks.contains(pending)) {
                    // The RPC may have completed the future before its enqueue handler obtained
                    // this monitor. Claim and enqueue the actual outcome before the scanner drains.
                    try {
                        pending.completion.getNow(null);
                        enqueueCompletedOffsetCommitCallback(pending, null);
                    } catch (CompletionException | CancellationException error) {
                        enqueueCompletedOffsetCommitCallback(pending, error);
                    }
                }
            }
        }

        if (discovery != null) {
            // Waiting callers receive a CancellationException. The interrupt flag is false
            // because CompletableFuture cannot interrupt the underlying RPC.
            discovery.cancel(false);
        }
        PendingCommittedOffsetRequest pendingFetch = pendingOffsetFetch;
        pendingOffsetFetch = null;
        if (pendingFetch != null) {
            pendingFetch.response.cancel(false);
        }
    }

    private static final class PendingOffsetCommitCallback {
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private final Map<TableBucket, OffsetMetadata> offsets;
        private final OffsetCommitCallback callback;

        private PendingOffsetCommitCallback(
                Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback) {
            this.offsets = offsets;
            this.callback = callback;
        }
    }

    private static final class PendingCommittedOffsetRequest {
        private final Set<TableBucket> requestedBuckets;
        private final CompletableFuture<Map<TableBucket, OffsetMetadata>> response;

        private PendingCommittedOffsetRequest(
                Set<TableBucket> requestedBuckets,
                CompletableFuture<Map<TableBucket, OffsetMetadata>> response) {
            this.requestedBuckets = requestedBuckets;
            this.response = response;
        }
    }
}
