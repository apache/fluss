/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.rpc.netty.server.RequestChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import com.alibaba.fluss.utils.MathUtils;

import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

/**
 * A decoder that decodes the incoming ByteBuf into Kafka requests and sends them to the
 * corresponding RequestChannel.
 */
public class KafkaCommandDecoder extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommandDecoder.class);

    private final RequestChannel[] requestChannels;
    private final int numChannels;

    // Need to use a Queue to store the inflight responses, because Kafka clients require the
    // responses to be sent in order.
    // See: org.apache.kafka.clients.InFlightRequests#completeNext
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses =
            new ConcurrentLinkedDeque<>();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);
    protected volatile ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;

    public KafkaCommandDecoder(RequestChannel[] requestChannels) {
        super(false);
        this.requestChannels = requestChannels;
        this.numChannels = requestChannels.length;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        boolean needRelease = false;
        try {
            KafkaRequest request = parseRequest(ctx, future, buffer);
            inflightResponses.addLast(request);
            future.whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());
            int channelIndex =
                    MathUtils.murmurHash(ctx.channel().id().asLongText().hashCode()) % numChannels;
            requestChannels[channelIndex].putRequest(request);

            if (!isActive.get()) {
                LOG.warn("Received a request on an inactive channel: {}", remoteAddress);
                request.fail(new LeaderNotAvailableException("Channel is inactive"));
                needRelease = true;
            }
        } catch (Throwable t) {
            needRelease = true;
            LOG.error("Error handling request", t);
            future.completeExceptionally(t);
        } finally {
            if (needRelease) {
                ReferenceCountUtil.release(buffer);
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        isActive.set(true);
        LOG.info("New connection from {}", ctx.channel().remoteAddress());
        // TODO Channel metrics
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOG.info("Connection closed from {}", ctx.channel().remoteAddress());
        // TODO Channel metrics
    }

    private void sendResponse(ChannelHandlerContext ctx) {
        KafkaRequest request;
        while ((request = inflightResponses.peekFirst()) != null) {
            CompletableFuture<AbstractResponse> f = request.future();
            ApiKeys apiKey = request.apiKey();
            boolean isDone = f.isDone();
            boolean cancelled = request.cancelled();

            if (apiKey.equals(PRODUCE)) {
                ProduceRequest produceRequest = request.request();
                if (produceRequest.acks() == 0 && isDone) {
                    // if acks=0, we don't need to wait for the response to be sent
                    inflightResponses.pollFirst();
                    request.releaseBuffer();
                    continue;
                }
            }

            if (!isDone) {
                break;
            }

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseBuffer();
                continue;
            }

            inflightResponses.pollFirst();
            if (isActive.get()) {
                ByteBuf buffer = request.responseBuffer();
                ctx.writeAndFlush(buffer);
            } else {
                request.releaseBuffer();
            }
        }
    }

    protected void close() {
        isActive.set(false);
        ctx.close();
        LOG.warn(
                "Close channel {} with {} pending requests.",
                remoteAddress,
                inflightResponses.size());
        for (KafkaRequest request : inflightResponses) {
            request.cancel();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    private static KafkaRequest parseRequest(
            ChannelHandlerContext ctx, CompletableFuture<AbstractResponse> future, ByteBuf buffer) {
        ByteBuffer nioBuffer = buffer.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaRequest(
                    API_VERSIONS, header.apiVersion(), header, request, buffer, ctx, future);
        }
        RequestAndSize request =
                AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(
                header.apiKey(), header.apiVersion(), header, request.request, buffer, ctx, future);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS
                && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
