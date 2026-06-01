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

import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.AuthenticateRequest;
import org.apache.fluss.rpc.messages.AuthenticateResponse;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RetryableGatewayClientProxy}. */
class RetryableGatewayClientProxyTest {

    private static final Executor REFRESH_EXECUTOR = ForkJoinPool.commonPool();

    @Test
    void testSuccessfulCallWithoutRetry() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        RpcGateway delegate = createGateway(callCount, 0);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate,
                        refreshCount::incrementAndGet,
                        REFRESH_EXECUTOR,
                        RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThat(result.get()).isNotNull();
        assertThat(callCount.get()).isEqualTo(1);
        assertThat(refreshCount.get()).isEqualTo(0);
    }

    @Test
    void testRetryOnNetworkExceptionThenSuccess() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        // Fail with NetworkException for the first call, then succeed on the single retry
        RpcGateway delegate = createGateway(callCount, 1);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate,
                        refreshCount::incrementAndGet,
                        REFRESH_EXECUTOR,
                        RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThat(result.get()).isNotNull();
        // Initial call + 1 retry = 2 total calls
        assertThat(callCount.get()).isEqualTo(2);
        // Metadata refresh should be called once before the retry
        assertThat(refreshCount.get()).isEqualTo(1);
    }

    @Test
    void testExhaustsRetriesAndPropagatesError() {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        // Always fail with NetworkException (more failures than the single retry can recover)
        RpcGateway delegate = createGateway(callCount, Integer.MAX_VALUE);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate,
                        refreshCount::incrementAndGet,
                        REFRESH_EXECUTOR,
                        RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .rootCause()
                .isInstanceOf(NetworkException.class)
                .hasMessageContaining("Simulated network error");
        // Initial call + 1 retry = 2 total calls
        assertThat(callCount.get()).isEqualTo(2);
        // Metadata refresh should be called once before the single retry
        assertThat(refreshCount.get()).isEqualTo(1);
    }

    @Test
    void testNonRetriableExceptionNotRetried() {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        // Fail with a non-retriable exception
        RpcGateway delegate =
                new TestRpcGateway() {
                    @Override
                    public CompletableFuture<ApiVersionsResponse> apiVersions(
                            ApiVersionsRequest request) {
                        callCount.incrementAndGet();
                        CompletableFuture<ApiVersionsResponse> future = new CompletableFuture<>();
                        future.completeExceptionally(
                                new TableNotExistException("table does not exist"));
                        return future;
                    }
                };

        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate,
                        refreshCount::incrementAndGet,
                        REFRESH_EXECUTOR,
                        RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .rootCause()
                .isInstanceOf(TableNotExistException.class)
                .hasMessageContaining("table does not exist");
        // Should only be called once - no retries for non-retriable exceptions
        assertThat(callCount.get()).isEqualTo(1);
        assertThat(refreshCount.get()).isEqualTo(0);
    }

    @Test
    void testMetadataRefreshFailureDoesNotPreventRetry() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        // Fail first call, succeed second call
        RpcGateway delegate = createGateway(callCount, 1);

        // Metadata refresh throws exception
        Runnable failingRefresh =
                () -> {
                    refreshCount.incrementAndGet();
                    throw new RuntimeException("Simulated refresh failure");
                };

        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate, failingRefresh, REFRESH_EXECUTOR, RpcGateway.class);

        // Should still succeed because the retry goes through even if refresh fails
        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThat(result.get()).isNotNull();
        assertThat(callCount.get()).isEqualTo(2);
        assertThat(refreshCount.get()).isEqualTo(1);
    }

    /**
     * Verifies that when many failing RPCs run concurrently, only a single metadata refresh is
     * issued: all retriers piggyback on the same in-flight refresh future. This prevents the
     * pile-up of N redundant refresh tasks behind the metadata updater's lock.
     */
    @Test
    void testConcurrentFailingCallsShareSingleRefresh() throws Exception {
        int n = 8;
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);
        CountDownLatch refreshGate = new CountDownLatch(1);

        // First n calls fail, subsequent calls succeed (so each retrier's retry will succeed).
        RpcGateway delegate = createGateway(callCount, n);

        // Block the refresh until the test releases it, so all retriers have a chance to arrive
        // and either start or piggyback on the in-flight refresh.
        Runnable blockingRefresh =
                () -> {
                    refreshCount.incrementAndGet();
                    try {
                        refreshGate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate, blockingRefresh, REFRESH_EXECUTOR, RpcGateway.class);

        // Fire n calls; each fails on its initial invocation and queues for a refresh.
        List<CompletableFuture<ApiVersionsResponse>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(proxy.apiVersions(new ApiVersionsRequest()));
        }

        // Wait until the (single) refresh task has actually started.
        long deadline = System.currentTimeMillis() + 5000;
        while (refreshCount.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(refreshCount.get()).as("exactly one refresh should have started").isEqualTo(1);

        // Release the refresh; all retriers should now proceed with their single retry.
        refreshGate.countDown();
        for (CompletableFuture<ApiVersionsResponse> f : futures) {
            assertThat(f.get(5, TimeUnit.SECONDS)).isNotNull();
        }

        // Initial n failing calls + n successful retries = 2n total invocations.
        assertThat(callCount.get()).isEqualTo(2 * n);
        // The critical assertion: n concurrent failures triggered only ONE refresh.
        assertThat(refreshCount.get())
                .as("concurrent retriers must share a single refresh")
                .isEqualTo(1);
    }

    /**
     * Creates a test gateway that fails with {@link NetworkException} for the first {@code
     * failCount} invocations, then returns a successful response.
     */
    private static RpcGateway createGateway(AtomicInteger callCount, int failCount) {
        return new TestRpcGateway() {
            @Override
            public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
                int count = callCount.incrementAndGet();
                CompletableFuture<ApiVersionsResponse> future = new CompletableFuture<>();
                if (count <= failCount) {
                    future.completeExceptionally(
                            new NetworkException("Simulated network error on call " + count));
                } else {
                    future.complete(new ApiVersionsResponse());
                }
                return future;
            }
        };
    }

    /** Base test implementation of {@link RpcGateway} that throws on unimplemented methods. */
    private abstract static class TestRpcGateway implements RpcGateway {

        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<AuthenticateResponse> authenticate(AuthenticateRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}
