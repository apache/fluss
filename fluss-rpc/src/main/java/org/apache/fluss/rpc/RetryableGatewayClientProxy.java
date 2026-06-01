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

package org.apache.fluss.rpc;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A proxy that wraps an existing {@link RpcGateway} proxy and adds automatic retry with metadata
 * refresh on retriable (network) errors.
 *
 * <p>This is designed to solve the stale metadata problem where cached server addresses become
 * invalid (e.g., during rolling upgrades in Kubernetes). When an RPC call fails with a {@link
 * RetriableException}, this proxy triggers a metadata refresh callback and retries the request with
 * potentially updated server addresses.
 *
 * <p>The retry flow for a cluster with stale tablet servers:
 *
 * <ol>
 *   <li>RPC fails with {@link RetriableException} (e.g., connection refused to stale IP)
 *   <li>Metadata refresh is triggered, which loops until a live server is reached or the cluster is
 *       re-initialized from bootstrap servers
 *   <li>The RPC is retried once with the refreshed server addresses
 * </ol>
 *
 * <p>A single retry is sufficient because {@code metadataRefreshAction} is expected to fully
 * recover the cluster node list on its own; the retry only validates the refreshed addresses and
 * absorbs transient errors right after a server/leader switch.
 *
 * <p>Concurrent retriers share a single in-flight refresh: when many RPCs fail at once (e.g.,
 * during a rolling upgrade), they all piggyback on the first refresh future instead of each
 * scheduling its own redundant refresh. This avoids piling up N refresh tasks behind the metadata
 * updater's lock and keeps the data plane's table/partition refreshes from queueing behind admin
 * retries.
 */
@Internal
public class RetryableGatewayClientProxy implements InvocationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryableGatewayClientProxy.class);

    private final Object delegate;
    private final Runnable metadataRefreshAction;
    private final Executor refreshExecutor;

    /**
     * Holds the currently in-flight metadata refresh, if any. Concurrent retriers piggyback on this
     * future to coalesce duplicate refreshes; once the future completes the reference is cleared so
     * subsequent failures trigger a fresh refresh.
     */
    private final AtomicReference<CompletableFuture<Void>> inFlightRefresh =
            new AtomicReference<>();

    RetryableGatewayClientProxy(
            Object delegate, Runnable metadataRefreshAction, Executor refreshExecutor) {
        this.delegate = delegate;
        this.metadataRefreshAction = metadataRefreshAction;
        this.refreshExecutor = refreshExecutor;
    }

    /**
     * Creates a retryable proxy wrapping an existing gateway proxy. On {@link RetriableException},
     * the proxy will invoke {@code metadataRefreshAction} and retry the failed RPC call once.
     *
     * @param delegate the underlying gateway proxy to wrap
     * @param metadataRefreshAction callback to refresh metadata (e.g., update cluster info)
     * @param refreshExecutor executor on which {@code metadataRefreshAction} is run; must NOT be a
     *     Netty event loop and ideally should be a dedicated, single-thread executor (the in-flight
     *     refresh is already coalesced to at most one concurrent task)
     * @param gatewayClass the gateway interface class
     * @param <T> the gateway type
     * @return a retryable gateway proxy
     */
    public static <T extends RpcGateway> T createRetryableGatewayProxy(
            T delegate,
            Runnable metadataRefreshAction,
            Executor refreshExecutor,
            Class<T> gatewayClass) {
        ClassLoader classLoader = gatewayClass.getClassLoader();

        @SuppressWarnings("unchecked")
        T proxy =
                (T)
                        Proxy.newProxyInstance(
                                classLoader,
                                new Class<?>[] {gatewayClass},
                                new RetryableGatewayClientProxy(
                                        delegate, metadataRefreshAction, refreshExecutor));
        return proxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return invokeWithRetry(method, args, true);
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> invokeWithRetry(Method method, Object[] args, boolean retry) {
        CompletableFuture<T> future;
        try {
            future = (CompletableFuture<T>) method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e.getCause());
            return failed;
        } catch (Exception e) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        future.whenComplete(
                (result, throwable) -> {
                    if (throwable == null) {
                        resultFuture.complete(result);
                        return;
                    }
                    Throwable cause = ExceptionUtils.stripCompletionException(throwable);
                    if (!(cause instanceof RetriableException) || !retry) {
                        resultFuture.completeExceptionally(cause);
                        return;
                    }
                    LOG.warn(
                            "RPC call {} failed with retriable error, "
                                    + "refreshing metadata and retrying once.",
                            method.getName(),
                            cause);
                    // Coalesce concurrent refreshes so N parallel failing calls trigger only one
                    // metadata refresh (and one round of MetadataUpdater lock contention).
                    coalescedRefresh()
                            .thenCompose(
                                    ignored ->
                                            RetryableGatewayClientProxy.this.<T>invokeWithRetry(
                                                    method, args, false))
                            .whenComplete(
                                    (retryResult, retryError) -> {
                                        if (retryError != null) {
                                            resultFuture.completeExceptionally(
                                                    ExceptionUtils.stripCompletionException(
                                                            retryError));
                                        } else {
                                            resultFuture.complete(retryResult);
                                        }
                                    });
                });
        return resultFuture;
    }

    /**
     * Returns a future that completes when a metadata refresh has finished. Concurrent callers that
     * arrive while a refresh is in flight all receive the same future and therefore wait on a
     * single shared refresh, instead of each running their own.
     */
    private CompletableFuture<Void> coalescedRefresh() {
        while (true) {
            CompletableFuture<Void> existing = inFlightRefresh.get();
            if (existing != null && !existing.isDone()) {
                return existing;
            }
            CompletableFuture<Void> mine = new CompletableFuture<>();
            if (inFlightRefresh.compareAndSet(existing, mine)) {
                // Run the metadata refresh on the dedicated executor: the failed future is
                // typically completed on a Netty EventLoop (see ServerConnection#close on a
                // connection reset), and refreshClusterUntilAvailable can take the
                // MetadataUpdater lock, issue further RPCs, and back off with sleeps in the
                // bootstrap path -- all of which would freeze every connection sharing that
                // EventLoop if run inline. ForkJoinPool.commonPool() is also unsuitable because
                // commonPool workers are sized for non-blocking CPU work and may even fall back
                // to the caller thread on small containers.
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                metadataRefreshAction.run();
                            } catch (Exception e) {
                                LOG.warn("Failed to refresh metadata during retry", e);
                            } finally {
                                // Complete first so piggybackers proceed, then clear the slot so
                                // future failures start a fresh refresh round.
                                mine.complete(null);
                                inFlightRefresh.compareAndSet(mine, null);
                            }
                        },
                        refreshExecutor);
                return mine;
            }
        }
    }
}
