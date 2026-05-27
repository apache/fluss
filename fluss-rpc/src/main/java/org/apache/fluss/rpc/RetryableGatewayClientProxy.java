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

/**
 * A proxy that wraps an existing {@link RpcGateway} proxy and adds automatic retry with metadata
 * refresh on retriable (network) errors.
 *
 * <p>This is designed to solve the stale metadata problem where cached server addresses become
 * invalid (e.g., during rolling upgrades in Kubernetes). When an RPC call fails with a {@link
 * RetriableException}, this proxy triggers a metadata refresh callback and retries the request with
 * potentially updated server addresses.
 *
 * <p>The retry flow for a cluster with N stale tablet servers:
 *
 * <ol>
 *   <li>RPC fails with {@link RetriableException} (e.g., connection refused to stale IP)
 *   <li>Metadata refresh is triggered, which marks the failed server as unavailable
 *   <li>After N failed refreshes, all servers are marked unavailable, triggering re-initialization
 *       from bootstrap servers
 *   <li>The next retry succeeds with the refreshed server addresses
 * </ol>
 */
@Internal
public class RetryableGatewayClientProxy implements InvocationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryableGatewayClientProxy.class);

    private final Object delegate;
    private final Runnable metadataRefreshAction;
    private final int maxRetries;

    RetryableGatewayClientProxy(Object delegate, Runnable metadataRefreshAction, int maxRetries) {
        this.delegate = delegate;
        this.metadataRefreshAction = metadataRefreshAction;
        this.maxRetries = maxRetries;
    }

    /**
     * Creates a retryable proxy wrapping an existing gateway proxy. On {@link RetriableException},
     * the proxy will invoke {@code metadataRefreshAction} and retry the failed RPC call.
     *
     * @param delegate the underlying gateway proxy to wrap
     * @param metadataRefreshAction callback to refresh metadata (e.g., update cluster info)
     * @param maxRetries maximum number of retries before propagating the error
     * @param gatewayClass the gateway interface class
     * @param <T> the gateway type
     * @return a retryable gateway proxy
     */
    public static <T extends RpcGateway> T createRetryableGatewayProxy(
            T delegate, Runnable metadataRefreshAction, int maxRetries, Class<T> gatewayClass) {
        ClassLoader classLoader = gatewayClass.getClassLoader();

        @SuppressWarnings("unchecked")
        T proxy =
                (T)
                        Proxy.newProxyInstance(
                                classLoader,
                                new Class<?>[] {gatewayClass},
                                new RetryableGatewayClientProxy(
                                        delegate, metadataRefreshAction, maxRetries));
        return proxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return invokeWithRetry(method, args, 0);
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> invokeWithRetry(Method method, Object[] args, int attempt) {
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
                    if (!(cause instanceof RetriableException) || attempt >= maxRetries) {
                        resultFuture.completeExceptionally(cause);
                        return;
                    }
                    LOG.warn(
                            "RPC call {} failed with retriable error (attempt {}/{}), "
                                    + "refreshing metadata and retrying.",
                            method.getName(),
                            attempt + 1,
                            maxRetries,
                            cause);
                    // Run metadata refresh and retry on a separate thread to avoid
                    // blocking Netty IO threads that may complete the failed future.
                    CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            metadataRefreshAction.run();
                                        } catch (Exception e) {
                                            LOG.warn("Failed to refresh metadata during retry", e);
                                        }
                                    })
                            .thenCompose(
                                    ignored ->
                                            RetryableGatewayClientProxy.this.<T>invokeWithRetry(
                                                    method, args, attempt + 1))
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
}
