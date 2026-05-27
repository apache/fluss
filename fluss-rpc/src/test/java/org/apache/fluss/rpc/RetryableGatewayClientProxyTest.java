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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RetryableGatewayClientProxy}. */
class RetryableGatewayClientProxyTest {

    @Test
    void testSuccessfulCallWithoutRetry() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        RpcGateway delegate = createGateway(callCount, 0);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate, refreshCount::incrementAndGet, 3, RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThat(result.get()).isNotNull();
        assertThat(callCount.get()).isEqualTo(1);
        assertThat(refreshCount.get()).isEqualTo(0);
    }

    @Test
    void testRetryOnNetworkExceptionThenSuccess() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        // Fail with NetworkException for the first 2 calls, then succeed
        RpcGateway delegate = createGateway(callCount, 2);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate, refreshCount::incrementAndGet, 3, RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThat(result.get()).isNotNull();
        // Initial call + 2 retries = 3 total calls
        assertThat(callCount.get()).isEqualTo(3);
        // Metadata refresh should be called before each retry
        assertThat(refreshCount.get()).isEqualTo(2);
    }

    @Test
    void testExhaustsRetriesAndPropagatesError() {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        // Always fail with NetworkException (more failures than max retries)
        RpcGateway delegate = createGateway(callCount, Integer.MAX_VALUE);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate, refreshCount::incrementAndGet, 3, RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .rootCause()
                .isInstanceOf(NetworkException.class)
                .hasMessageContaining("Simulated network error");
        // Initial call + 3 retries = 4 total calls
        assertThat(callCount.get()).isEqualTo(4);
        // Metadata refresh should be called for each retry attempt
        assertThat(refreshCount.get()).isEqualTo(3);
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
                        delegate, refreshCount::incrementAndGet, 3, RpcGateway.class);

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
                        delegate, failingRefresh, 3, RpcGateway.class);

        // Should still succeed because the retry goes through even if refresh fails
        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThat(result.get()).isNotNull();
        assertThat(callCount.get()).isEqualTo(2);
        assertThat(refreshCount.get()).isEqualTo(1);
    }

    @Test
    void testRetryWithMaxRetriesZeroDoesNotRetry() {
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger refreshCount = new AtomicInteger(0);

        RpcGateway delegate = createGateway(callCount, Integer.MAX_VALUE);
        RpcGateway proxy =
                RetryableGatewayClientProxy.createRetryableGatewayProxy(
                        delegate, refreshCount::incrementAndGet, 0, RpcGateway.class);

        CompletableFuture<ApiVersionsResponse> result = proxy.apiVersions(new ApiVersionsRequest());
        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .rootCause()
                .isInstanceOf(NetworkException.class);
        // Only the initial call, no retries
        assertThat(callCount.get()).isEqualTo(1);
        assertThat(refreshCount.get()).isEqualTo(0);
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
