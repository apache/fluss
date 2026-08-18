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
import java.util.function.Predicate;

/**
 * A proxy that wraps an existing {@link RpcGateway} proxy and adds automatic metadata refresh on
 * errors, plus an optional single retry of a subset of them.
 *
 * <p>This is designed to solve the stale metadata problem where cached server addresses become
 * invalid (e.g., during rolling upgrades in Kubernetes) or a cached coordinator leader becomes a
 * standby after failover. When an RPC fails with an error accepted by {@code refreshPredicate},
 * this proxy triggers a metadata refresh callback; when the error is also accepted by {@code
 * retryPredicate}, it retries the request once with the potentially updated server addresses.
 *
 * <p>Separating the two predicates lets a write gateway refresh metadata on any recoverable error
 * (so the connection is repointed and a later manual retry can succeed) while only auto-retrying
 * failures that are provably safe to replay -- e.g. {@code NotCoordinatorLeaderException}, which
 * the server raises before executing a non-idempotent mutation. Read-only gateways use {@link
 * RetriableException} for both.
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
    private final Predicate<Throwable> refreshPredicate;
    private final Predicate<Throwable> retryPredicate;

    /**
     * Holds the currently in-flight metadata refresh, if any. Concurrent retriers piggyback on this
     * future to coalesce duplicate refreshes; once the future completes the reference is cleared so
     * subsequent failures trigger a fresh refresh.
     */
    private final AtomicReference<CompletableFuture<Void>> inFlightRefresh =
            new AtomicReference<>();

    RetryableGatewayClientProxy(
            Object delegate,
            Runnable metadataRefreshAction,
            Executor refreshExecutor,
            Predicate<Throwable> refreshPredicate,
            Predicate<Throwable> retryPredicate) {
        this.delegate = delegate;
        this.metadataRefreshAction = metadataRefreshAction;
        this.refreshExecutor = refreshExecutor;
        this.refreshPredicate = refreshPredicate;
        this.retryPredicate = retryPredicate;
    }

    /**
     * Creates a retryable proxy wrapping an existing gateway proxy. On {@link RetriableException},
     * the proxy will invoke {@code metadataRefreshAction} and retry the failed RPC call once. This
     * is suitable for read-only gateways where retrying any network error is safe.
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
        Predicate<Throwable> retriable = cause -> cause instanceof RetriableException;
        return createRetryableGatewayProxy(
                delegate,
                metadataRefreshAction,
                refreshExecutor,
                retriable,
                retriable,
                gatewayClass);
    }

    /**
     * Creates a retryable proxy wrapping an existing gateway proxy. On a failure, the proxy invokes
     * {@code metadataRefreshAction} when {@code refreshPredicate} accepts the cause, and then
     * retries the RPC once only when {@code retryPredicate} also accepts it.
     *
     * <p>This lets a write gateway refresh metadata on any recoverable error (repointing the stale
     * coordinator connection so a later manual retry can succeed) while auto-retrying only failures
     * that are safe to replay, e.g. {@code NotCoordinatorLeaderException}, which the server raises
     * before executing the mutation. Errors such as {@code NetworkException} may already have
     * executed on the server, so they refresh metadata but are not auto-retried.
     *
     * @param delegate the underlying gateway proxy to wrap
     * @param metadataRefreshAction callback to refresh metadata (e.g., update cluster info)
     * @param refreshExecutor executor on which {@code metadataRefreshAction} is run; must NOT be a
     *     Netty event loop and ideally should be a dedicated, single-thread executor (the in-flight
     *     refresh is already coalesced to at most one concurrent task)
     * @param refreshPredicate decides whether a failure cause should trigger a metadata refresh
     * @param retryPredicate decides whether a failure cause should additionally be retried once
     *     (should be a subset of {@code refreshPredicate})
     * @param gatewayClass the gateway interface class
     * @param <T> the gateway type
     * @return a retryable gateway proxy
     */
    public static <T extends RpcGateway> T createRetryableGatewayProxy(
            T delegate,
            Runnable metadataRefreshAction,
            Executor refreshExecutor,
            Predicate<Throwable> refreshPredicate,
            Predicate<Throwable> retryPredicate,
            Class<T> gatewayClass) {
        ClassLoader classLoader = gatewayClass.getClassLoader();

        @SuppressWarnings("unchecked")
        T proxy =
                (T)
                        Proxy.newProxyInstance(
                                classLoader,
                                new Class<?>[] {gatewayClass},
                                new RetryableGatewayClientProxy(
                                        delegate,
                                        metadataRefreshAction,
                                        refreshExecutor,
                                        refreshPredicate,
                                        retryPredicate));
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
                    // Only the initial attempt may refresh/retry; a failed retry gives up.
                    if (!retry) {
                        resultFuture.completeExceptionally(cause);
                        return;
                    }
                    boolean shouldRetry = retryPredicate.test(cause);
                    boolean shouldRefresh = shouldRetry || refreshPredicate.test(cause);
                    if (!shouldRefresh) {
                        resultFuture.completeExceptionally(cause);
                        return;
                    }
                    LOG.warn(
                            "RPC call {} failed, refreshing metadata{}.",
                            method.getName(),
                            shouldRetry ? " and retrying once" : " (not retrying)",
                            cause);
                    // Coalesce concurrent refreshes so N parallel failing calls trigger only one
                    // metadata refresh (and one round of MetadataUpdater lock contention).
                    coalescedRefresh()
                            .thenCompose(
                                    ignored -> {
                                        if (shouldRetry) {
                                            return RetryableGatewayClientProxy.this
                                                    .<T>invokeWithRetry(method, args, false);
                                        }
                                        // Metadata was refreshed but the failure is not safe to
                                        // auto-retry; surface the original error for a manual
                                        // retry.
                                        CompletableFuture<T> notRetried = new CompletableFuture<>();
                                        notRetried.completeExceptionally(cause);
                                        return notRetried;
                                    })
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
