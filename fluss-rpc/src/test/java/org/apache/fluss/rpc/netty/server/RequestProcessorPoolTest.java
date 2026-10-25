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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests ownership and shutdown of handlers shared by request processors. */
class RequestProcessorPoolTest {

    @Test
    void testSharedHandlerClosesOnceAfterAllProcessorsStop() throws Exception {
        TestingRequestHandler handler = new TestingRequestHandler();
        RequestProcessorPool pool = pool(handler);
        TestingRequest first = new TestingRequest();
        TestingRequest second = new TestingRequest();
        try {
            pool.start();
            pool.getRequestChannels()[0].putRequest(first);
            pool.getRequestChannels()[1].putRequest(second);
            first.started.get(10, TimeUnit.SECONDS);
            second.started.get(10, TimeUnit.SECONDS);

            CompletableFuture<Void> closed = pool.closeAsync();
            CompletableFuture<Void> closedAgain = pool.closeAsync();
            assertThat(closed).isNotDone();
            assertThat(closedAgain).isNotDone();
            assertThat(handler.closeCalls).hasValue(0);

            first.release.complete(null);
            first.bufferReleased.get(10, TimeUnit.SECONDS);
            assertThat(closed).isNotDone();
            assertThat(handler.closeCalls).hasValue(0);

            second.release.complete(null);
            CompletableFuture.allOf(closed, closedAgain).get(10, TimeUnit.SECONDS);
            assertThat(first.bufferReleased).isCompleted();
            assertThat(second.bufferReleased).isCompleted();
            pool.closeAsync().get(10, TimeUnit.SECONDS);
            assertThat(handler.closeCalls).hasValue(1);
        } finally {
            first.release.complete(null);
            second.release.complete(null);
            pool.closeAsync().handle((ignored, failure) -> null).get(10, TimeUnit.SECONDS);
        }
    }

    private static RequestProcessorPool pool(TestingRequestHandler handler) {
        MetricGroup metrics = NOPMetricsGroup.newInstance();
        return new RequestProcessorPool(
                2,
                10,
                new TestingGatewayService(),
                Collections.singletonList(testingProtocol(handler)),
                RequestsMetrics.createCoordinatorServerRequestMetrics(metrics));
    }

    private static NetworkProtocolPlugin testingProtocol(RequestHandler<?> handler) {
        return new NetworkProtocolPlugin() {
            @Override
            public String name() {
                return handler.requestType().name();
            }

            @Override
            public void setup(Configuration conf) {}

            @Override
            public List<String> listenerNames() {
                return Collections.emptyList();
            }

            @Override
            public ChannelHandler createChannelHandler(
                    RequestChannel[] channels, String listenerName) {
                return null;
            }

            @Override
            public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
                return handler;
            }
        };
    }

    private static class TestingRequestHandler implements RequestHandler<TestingRequest> {
        private final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public RequestType requestType() {
            return RequestType.FLUSS;
        }

        @Override
        public void processRequest(TestingRequest request) {
            request.started.complete(null);
            try {
                request.release.get(10, TimeUnit.SECONDS);
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }

    private static class TestingRequest implements RpcRequest {
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CompletableFuture<Void> bufferReleased = new CompletableFuture<>();

        @Override
        public RequestType getRequestType() {
            return RequestType.FLUSS;
        }

        @Override
        public void releaseBuffer() {
            bufferReleased.complete(null);
        }
    }
}
