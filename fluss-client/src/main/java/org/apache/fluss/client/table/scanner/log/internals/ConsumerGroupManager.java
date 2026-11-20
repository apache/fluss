/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.PbServerNode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Discovers and caches the tablet server that owns a consumer group.
 *
 * <p>Discovery is initiated by scanner threads while RPC completion callbacks run on network
 * threads; both coordinate through this object's monitor, mirroring how LogFetcher shares state
 * between the scanner thread and fetch-response callbacks. All mutable state is guarded by {@code
 * this}, and the monitor is never held across blocking waits.
 */
@Internal
public final class ConsumerGroupManager implements AutoCloseable {

    private final String groupId;
    private final MetadataUpdater metadataUpdater;

    /** The cached owner tablet server; null while the owner is unknown. */
    private ServerNode ownerNode;

    /** The pending owner-discovery request; null while no discovery is in flight. */
    private CompletableFuture<ServerNode> ownerDiscovery;

    /** Whether the manager has been closed; guarded by this object's monitor. */
    private boolean closed;

    /**
     * Creates a manager that discovers and caches the owner of one consumer group.
     *
     * @param groupId the consumer group whose owner is discovered
     * @param metadataUpdater the client metadata used to pick a tablet server for discovery and to
     *     open gateways to the discovered owner
     */
    public ConsumerGroupManager(String groupId, MetadataUpdater metadataUpdater) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.metadataUpdater =
                Objects.requireNonNull(metadataUpdater, "metadataUpdater must not be null");
    }

    /** Returns the cached owner or shares the single in-flight owner-discovery request. */
    public CompletableFuture<ServerNode> getOrDiscoverOwner() {
        final CompletableFuture<ServerNode> discovery;

        synchronized (this) {
            checkState(!closed, "Consumer group manager is closed.");
            if (ownerNode != null) {
                return CompletableFuture.completedFuture(ownerNode);
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
                    .whenComplete(
                            (owner, error) -> completeOwnerDiscovery(discovery, owner, error));
        } catch (Throwable error) {
            completeOwnerDiscovery(discovery, null, error);
        }
        return discovery;
    }

    /** Clears a stale owner so that the next operation performs owner discovery again. */
    public synchronized void markOwnerUnknown() {
        ownerNode = null;
    }

    /** Returns a gateway for the cached owner, or null when no usable owner is cached. */
    public synchronized TabletServerGateway cachedOwnerGateway() {
        if (ownerNode == null) {
            return null;
        }
        return metadataUpdater.newTabletServerClientForNode(ownerNode);
    }

    private CompletableFuture<ServerNode> discoverOwner() {
        ServerNode anyTabletServer = metadataUpdater.getRandomTabletServer();
        if (anyTabletServer == null) {
            return failedFuture(
                    new ConsumerGroupOwnerNotAvailableException(
                            "No tablet server is currently known to the client for group "
                                    + groupId));
        }

        TabletServerGateway gateway =
                metadataUpdater.newTabletServerClientForNode(anyTabletServer.id());
        if (gateway == null) {
            return failedFuture(
                    new ConsumerGroupOwnerNotAvailableException(
                            "No reachable tablet server gateway when discovering the owner of "
                                    + "group "
                                    + groupId));
        }

        FindConsumerGroupOwnerRequest request =
                new FindConsumerGroupOwnerRequest().setGroupId(groupId);
        return gateway.findConsumerGroupOwner(request).thenApply(ConsumerGroupManager::decodeOwner);
    }

    private static ServerNode decodeOwner(FindConsumerGroupOwnerResponse response) {
        if (response == null || !response.hasOwner()) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "FindConsumerGroupOwner response does not contain an owner");
        }
        PbServerNode owner = response.getOwner();
        if (!owner.hasHost() || !owner.hasPort()) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "FindConsumerGroupOwner response contains an incomplete owner");
        }
        return new ServerNode(
                owner.getNodeId(), owner.getHost(), owner.getPort(), ServerType.TABLET_SERVER);
    }

    private void completeOwnerDiscovery(
            CompletableFuture<ServerNode> discovery, ServerNode owner, Throwable error) {
        synchronized (this) {
            if (closed || ownerDiscovery != discovery) {
                // A late response must not restore state after close().
                return;
            }

            if (error == null && !discovery.isCancelled()) {
                ownerNode = owner;
            }
            ownerDiscovery = null;
        }

        // Complete the caller-visible future only after the cache and in-flight state are ready
        // for caller callbacks.
        if (error == null) {
            discovery.complete(owner);
        } else {
            discovery.completeExceptionally(stripCompletionException(error));
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

    /** Forgets the cached owner and cancels the in-flight discovery, if any. */
    @Override
    public void close() {
        final CompletableFuture<ServerNode> discovery;

        synchronized (this) {
            closed = true;
            ownerNode = null;
            discovery = ownerDiscovery;
            ownerDiscovery = null;
        }

        if (discovery != null) {
            // Waiting callers receive a CancellationException. The interrupt flag is false
            // because CompletableFuture cannot interrupt the underlying RPC.
            discovery.cancel(false);
        }
    }
}
