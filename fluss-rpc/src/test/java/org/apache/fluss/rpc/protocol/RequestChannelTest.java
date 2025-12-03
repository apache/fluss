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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.netty.server.FlussRequest;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RpcRequest;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.EmptyByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelConfig;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelId;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelMetadata;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelProgressivePromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoop;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.MessageSizeEstimator;
import org.apache.fluss.shaded.netty4.io.netty.channel.RecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.WriteBufferWaterMark;
import org.apache.fluss.shaded.netty4.io.netty.util.Attribute;
import org.apache.fluss.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.SingleThreadEventExecutor;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link RequestChannel}. */
public class RequestChannelTest {

    @Test
    void testRequestsFIFO() throws Exception {
        RequestChannel channel = new RequestChannel(100);

        // 1. Same request type, Use FIFO.
        List<RpcRequest> rpcRequests = new ArrayList<>();
        // push rpc requests
        for (int i = 0; i < 100; i++) {
            RpcRequest rpcRequest =
                    new FlussRequest(
                            ApiKeys.GET_TABLE_INFO.id,
                            (short) 0,
                            i,
                            null,
                            new GetTableInfoRequest(),
                            new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                            "FLUSS",
                            true,
                            null,
                            null,
                            new CompletableFuture<>());
            channel.putRequest(rpcRequest);
            rpcRequests.add(rpcRequest);
        }
        // pop rpc requests
        for (int i = 0; i < 100; i++) {
            RpcRequest gotRequest = channel.pollRequest(100);
            assertThat(gotRequest).isEqualTo(rpcRequests.get(i));
        }

        // 2. Different request type, Use FIFO.
        RpcRequest rpcRequest1 =
                new FlussRequest(
                        ApiKeys.GET_TABLE_INFO.id,
                        (short) 0,
                        3,
                        null,
                        new GetTableInfoRequest(),
                        new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                        "FLUSS",
                        true,
                        null,
                        null,
                        new CompletableFuture<>());
        RpcRequest rpcRequest2 =
                new FlussRequest(
                        ApiKeys.FETCH_LOG.id,
                        (short) 0,
                        100,
                        null,
                        new FetchLogRequest().setMaxBytes(100).setFollowerServerId(2),
                        new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                        "FLUSS",
                        true,
                        null,
                        null,
                        new CompletableFuture<>());
        channel.putRequest(rpcRequest1);
        channel.putRequest(rpcRequest2);
        RpcRequest rpcRequest = channel.pollRequest(100);
        assertThat(rpcRequest).isEqualTo(rpcRequest1);
        rpcRequest = channel.pollRequest(100);
        assertThat(rpcRequest).isEqualTo(rpcRequest2);
    }

    /**
     * Test that backpressure is activated when queue size exceeds threshold and deactivated when
     * queue size drops below resume threshold. This verifies the complete backpressure lifecycle.
     */
    @Test
    void testBackpressureActivationAndDeactivation() throws Exception {
        int backpressureThreshold = 100;
        int resumeThreshold = 50; // 50% of backpressureThreshold
        RequestChannel channel = new RequestChannel(backpressureThreshold);

        // Create a test channel that can track autoRead state changes
        TestChannel testChannel = new TestChannel();
        channel.registerChannel(testChannel);

        // Initially, channel should have autoRead enabled
        assertThat(testChannel.isAutoRead()).isTrue();

        // Step 1: Fill queue to trigger backpressure
        // Add requests until we exceed the backpressure threshold
        for (int i = 0; i < backpressureThreshold; i++) {
            channel.putRequest(createTestRequest(i));
        }

        // Wait for backpressure to be applied (channel operations are async via eventLoop)
        // The backpressure should be triggered when we add the threshold-th request
        testChannel.waitForAutoReadChange(false, 2, TimeUnit.SECONDS);

        // Verify backpressure is active: autoRead should be false
        assertThat(testChannel.isAutoRead()).isFalse();
        assertThat(channel.requestsCount()).isGreaterThanOrEqualTo(backpressureThreshold);

        // Step 2: Consume requests until queue size drops below resume threshold
        // We need to consume enough requests to go from >=100 to <=50
        // So we need to consume at least 51 requests (100 - 50 + 1 = 51)
        int requestsToConsume = backpressureThreshold - resumeThreshold + 1;
        for (int i = 0; i < requestsToConsume; i++) {
            RpcRequest request = channel.pollRequest(10);
            assertThat(request).isNotNull();
        }

        // Wait for backpressure to be released
        testChannel.waitForAutoReadChange(true, 2, TimeUnit.SECONDS);

        // Verify backpressure is released: autoRead should be true again
        assertThat(testChannel.isAutoRead()).isTrue();
        assertThat(channel.requestsCount()).isLessThanOrEqualTo(resumeThreshold);

        // Clean up
        channel.unregisterChannel(testChannel);
    }

    /** Helper method to create a test RpcRequest with a unique identifier. */
    private RpcRequest createTestRequest(int id) {
        return new FlussRequest(
                ApiKeys.GET_TABLE_INFO.id,
                (short) 0,
                id,
                null,
                new GetTableInfoRequest(),
                new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                "FLUSS",
                true,
                null,
                null,
                new CompletableFuture<>());
    }

    /**
     * A test Channel implementation that tracks autoRead state changes for backpressure testing.
     */
    private static class TestChannel implements Channel {
        private final AtomicBoolean autoRead = new AtomicBoolean(true);
        private final TestChannelConfig config = new TestChannelConfig(this);
        private final TestEventLoop eventLoop = new TestEventLoop();
        private final ChannelId channelId = new TestChannelId();
        private final SocketAddress remoteAddress = new InetSocketAddress("localhost", 8080);

        @Override
        public ChannelConfig config() {
            return config;
        }

        @Override
        public EventLoop eventLoop() {
            return eventLoop;
        }

        @Override
        public ChannelId id() {
            return channelId;
        }

        @Override
        public SocketAddress remoteAddress() {
            return remoteAddress;
        }

        @Override
        public boolean isActive() {
            return true;
        }

        boolean isAutoRead() {
            return autoRead.get();
        }

        void waitForAutoReadChange(boolean expectedValue, long timeout, TimeUnit unit)
                throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (autoRead.get() != expectedValue && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(autoRead.get())
                    .as("AutoRead should be " + expectedValue + " within timeout")
                    .isEqualTo(expectedValue);
        }

        // Minimal implementation of other required methods
        @Override
        public ChannelPipeline pipeline() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBufAllocator alloc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Channel read() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Channel flush() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture write(Object msg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelPromise newPromise() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelPromise voidPromise() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWritable() {
            return true;
        }

        @Override
        public long bytesBeforeUnwritable() {
            return Long.MAX_VALUE;
        }

        @Override
        public long bytesBeforeWritable() {
            return 0;
        }

        @Override
        public Unsafe unsafe() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture closeFuture() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public boolean isRegistered() {
            return true;
        }

        @Override
        public SocketAddress localAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture connect(
                SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture disconnect() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture deregister() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Channel parent() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelMetadata metadata() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return false;
        }

        @Override
        public int compareTo(Channel o) {
            return id().asLongText().compareTo(o.id().asLongText());
        }

        /** Test ChannelConfig that tracks autoRead state. */
        private static class TestChannelConfig implements ChannelConfig {
            private final TestChannel channel;

            TestChannelConfig(TestChannel channel) {
                this.channel = channel;
            }

            @Override
            public boolean isAutoRead() {
                return channel.autoRead.get();
            }

            @Override
            public ChannelConfig setAutoRead(boolean autoRead) {
                channel.autoRead.set(autoRead);
                return this;
            }

            @Override
            public boolean isAutoClose() {
                return false;
            }

            @Override
            public ChannelConfig setAutoClose(boolean autoClose) {
                return this;
            }

            // Minimal implementation of other required methods
            @Override
            public <T> T getOption(ChannelOption<T> option) {
                return null;
            }

            @Override
            public <T> boolean setOption(ChannelOption<T> option, T value) {
                return false;
            }

            @Override
            public Map<ChannelOption<?>, Object> getOptions() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean setOptions(Map<ChannelOption<?>, ?> options) {
                return false;
            }

            @Override
            public int getConnectTimeoutMillis() {
                return 0;
            }

            @Override
            public ChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
                return this;
            }

            @Override
            public int getMaxMessagesPerRead() {
                return 1;
            }

            @Override
            public ChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
                return this;
            }

            @Override
            public int getWriteSpinCount() {
                return 16;
            }

            @Override
            public ChannelConfig setWriteSpinCount(int writeSpinCount) {
                return this;
            }

            @Override
            public ByteBufAllocator getAllocator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelConfig setAllocator(ByteBufAllocator allocator) {
                return this;
            }

            @Override
            public RecvByteBufAllocator getRecvByteBufAllocator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
                return this;
            }

            @Override
            public int getWriteBufferHighWaterMark() {
                return 0;
            }

            @Override
            public ChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
                return this;
            }

            @Override
            public int getWriteBufferLowWaterMark() {
                return 0;
            }

            @Override
            public ChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
                return this;
            }

            @Override
            public WriteBufferWaterMark getWriteBufferWaterMark() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelConfig setWriteBufferWaterMark(
                    WriteBufferWaterMark writeBufferWaterMark) {
                return this;
            }

            @Override
            public MessageSizeEstimator getMessageSizeEstimator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
                return this;
            }
        }

        /** Test EventLoop that executes tasks immediately in the current thread for testing. */
        private static class TestEventLoop extends SingleThreadEventExecutor implements EventLoop {
            TestEventLoop() {
                super(null, new DefaultThreadFactory("test"), false);
            }

            @Override
            protected void run() {
                // No-op, tasks are executed synchronously
            }

            @Override
            public void execute(Runnable task) {
                // Execute immediately in current thread for testing
                task.run();
            }

            @Override
            public EventLoopGroup parent() {
                throw new UnsupportedOperationException();
            }

            @Override
            public EventLoop next() {
                return this;
            }

            @Override
            public ChannelFuture register(Channel channel) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelFuture register(Channel channel, ChannelPromise promise) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelPromise register(ChannelPromise promise) {
                throw new UnsupportedOperationException();
            }
        }

        /** Simple ChannelId implementation for testing. */
        private static class TestChannelId implements ChannelId {
            @Override
            public String asShortText() {
                return "test-channel";
            }

            @Override
            public String asLongText() {
                return "test-channel-long";
            }

            @Override
            public int compareTo(ChannelId o) {
                return asLongText().compareTo(o.asLongText());
            }
        }
    }
}
