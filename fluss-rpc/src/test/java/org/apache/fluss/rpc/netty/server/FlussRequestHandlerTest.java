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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussRequestHandler}. */
class FlussRequestHandlerTest {

    @Test
    void testCompletesActionsAfterSuccessfulInvocation() {
        CompletableFuture<ApiVersionsResponse> serviceResponse =
                CompletableFuture.completedFuture(new ApiVersionsResponse());
        TestingActionGatewayService service = new TestingActionGatewayService(serviceResponse);
        FlussRequest request = createApiVersionsRequest();

        new FlussRequestHandler(service).processRequest(request);

        assertThat(service.completedActions()).isOne();
        assertThat(request.getResponseFuture()).isCompletedWithValue(serviceResponse.join());
    }

    @Test
    void testCompletesActionsAfterSynchronousInvocationFailure() {
        IllegalStateException expected = new IllegalStateException("expected test failure");
        TestingActionGatewayService service = new TestingActionGatewayService(expected);
        FlussRequest request = createApiVersionsRequest();

        new FlussRequestHandler(service).processRequest(request);

        assertThat(service.completedActions()).isOne();
        assertThatThrownBy(request.getResponseFuture()::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(expected);
    }

    @Test
    void testCompletesActionsBeforeAsynchronousResponseFinishes() {
        CompletableFuture<ApiVersionsResponse> serviceResponse = new CompletableFuture<>();
        TestingActionGatewayService service = new TestingActionGatewayService(serviceResponse);
        FlussRequest request = createApiVersionsRequest();

        new FlussRequestHandler(service).processRequest(request);

        assertThat(service.completedActions()).isOne();
        assertThat(request.getResponseFuture()).isNotDone();

        ApiVersionsResponse response = new ApiVersionsResponse();
        serviceResponse.complete(response);
        assertThat(request.getResponseFuture()).isCompletedWithValue(response);
        assertThat(service.completedActions()).isOne();
    }

    private static FlussRequest createApiVersionsRequest() {
        return new FlussRequest(
                ApiKeys.API_VERSIONS.id,
                ApiKeys.API_VERSIONS.highestSupportedVersion,
                1,
                ApiManager.forApiKey(ApiKeys.API_VERSIONS.id),
                new ApiVersionsRequest(),
                Unpooled.EMPTY_BUFFER,
                "FLUSS",
                false,
                FlussPrincipal.ANONYMOUS,
                InetAddress.getLoopbackAddress(),
                new CompletableFuture<ApiMessage>());
    }

    private static final class TestingActionGatewayService extends TestingGatewayService {
        private final CompletableFuture<ApiVersionsResponse> response;
        private final RuntimeException synchronousFailure;
        private final AtomicInteger completedActions = new AtomicInteger();

        private TestingActionGatewayService(CompletableFuture<ApiVersionsResponse> response) {
            this.response = response;
            this.synchronousFailure = null;
        }

        private TestingActionGatewayService(RuntimeException synchronousFailure) {
            this.response = null;
            this.synchronousFailure = synchronousFailure;
        }

        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            if (synchronousFailure != null) {
                throw synchronousFailure;
            }
            return response;
        }

        @Override
        public void tryCompleteActions() {
            completedActions.incrementAndGet();
        }

        private int completedActions() {
            return completedActions.get();
        }
    }
}
