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

package org.apache.fluss.kafka.dispatcher;

import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests request routing and failure handling without a concrete API implementation. */
class KafkaRequestDispatcherTest {

    @Test
    void testUnregisteredApiReturnsUnsupportedVersion() {
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.freeze();

        AbstractResponse response =
                new KafkaRequestDispatcher(registry, new KafkaErrorMapper())
                        .dispatch(request((short) 0))
                        .join();

        assertThat(response.errorCounts()).containsEntry(Errors.UNSUPPORTED_VERSION, 1);
    }

    @Test
    void testUnsupportedVersionDoesNotInvokeHandler() {
        KafkaRequestDispatcher dispatcher =
                dispatcher(
                        (context, request) -> {
                            throw new AssertionError(
                                    "Unsupported versions must not be dispatched.");
                        });

        AbstractResponse response = dispatcher.dispatch(request((short) 1)).join();

        assertThat(response.errorCounts()).containsEntry(Errors.UNSUPPORTED_VERSION, 1);
    }

    @Test
    void testDispatchWaitsForHandlerAndPreservesContext() {
        CompletableFuture<AbstractResponse> handlerResult = new CompletableFuture<>();
        AtomicReference<KafkaRequestContext> receivedContext = new AtomicReference<>();
        KafkaRequestDispatcher dispatcher =
                dispatcher(
                        (context, request) -> {
                            receivedContext.set(context);
                            return handlerResult;
                        });
        KafkaRequest request = request((short) 0);

        CompletableFuture<AbstractResponse> result = dispatcher.dispatch(request);

        assertThat(result).isNotDone();
        assertThat(receivedContext.get().clientId()).isEqualTo("client");
        assertThat(receivedContext.get().correlationId()).isEqualTo(42);
        assertThat(receivedContext.get().listenerName()).isEqualTo("KAFKA");
        assertThat(receivedContext.get().apiKey()).isEqualTo(ApiKeys.API_VERSIONS);
        assertThat(receivedContext.get().apiVersion()).isZero();
        AbstractResponse response = new ApiVersionsResponse(new ApiVersionsResponseData());
        handlerResult.complete(response);
        assertThat(result.join()).isSameAs(response);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSynchronousAndAsynchronousFailuresBecomeErrorResponses(boolean synchronous) {
        InvalidRequestException failure = new InvalidRequestException("invalid request");
        KafkaRequestDispatcher dispatcher =
                dispatcher(
                        (context, request) -> {
                            if (synchronous) {
                                throw failure;
                            }
                            CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
                            result.completeExceptionally(new CompletionException(failure));
                            return result;
                        });

        AbstractResponse response = dispatcher.dispatch(request((short) 0)).join();

        assertThat(response.errorCounts()).containsEntry(Errors.INVALID_REQUEST, 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNullFutureAndNullResponseBecomeErrorResponses(boolean nullFuture) {
        KafkaRequestDispatcher dispatcher =
                dispatcher(
                        (context, request) ->
                                nullFuture ? null : CompletableFuture.completedFuture(null));

        AbstractResponse response = dispatcher.dispatch(request((short) 0)).join();

        assertThat(response.errorCounts()).containsEntry(Errors.UNKNOWN_SERVER_ERROR, 1);
    }

    private static KafkaRequestDispatcher dispatcher(
            BiFunction<KafkaRequestContext, ApiVersionsRequest, CompletableFuture<AbstractResponse>>
                    action) {
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.register(
                new KafkaApiHandler<ApiVersionsRequest>() {
                    @Override
                    public KafkaApiSpec apiSpec() {
                        return new KafkaApiSpec(ApiKeys.API_VERSIONS, (short) 0, (short) 0, true);
                    }

                    @Override
                    public CompletableFuture<? extends AbstractResponse> handle(
                            KafkaRequestContext context, ApiVersionsRequest request) {
                        return action.apply(context, request);
                    }
                });
        registry.freeze();
        return new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    private static KafkaRequest request(short version) {
        KafkaRequest request = mock(KafkaRequest.class);
        when(request.apiKey()).thenReturn(ApiKeys.API_VERSIONS);
        when(request.apiVersion()).thenReturn(version);
        when(request.request()).thenReturn(new ApiVersionsRequest.Builder().build(version));
        when(request.header())
                .thenReturn(new RequestHeader(ApiKeys.API_VERSIONS, version, "client", 42));
        when(request.listenerName()).thenReturn("KAFKA");
        when(request.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        return request;
    }
}
