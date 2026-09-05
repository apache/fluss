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

package org.apache.fluss.kafka;

import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.MathUtils;

import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
    private final String listenerName;
    private final KafkaSaslConnection saslConnection;

    // Need to use a Queue to store the inflight responses, because Kafka clients require the
    // responses to be sent in order.
    // See: org.apache.kafka.clients.InFlightRequests#completeNext
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses =
            new ConcurrentLinkedDeque<>();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);
    protected volatile ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;

    /** Creates a decoder for a PLAINTEXT Kafka connection. */
    public KafkaCommandDecoder(RequestChannel[] requestChannels, String listenerName) {
        this(requestChannels, listenerName, null);
    }

    /** Creates a decoder that requires SASL when an authenticator supplier is provided. */
    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier) {
        super(false);
        this.requestChannels = requestChannels;
        this.numChannels = requestChannels.length;
        this.listenerName = listenerName;
        this.saslConnection =
                authenticatorSupplier == null
                        ? KafkaSaslConnection.plaintext()
                        : KafkaSaslConnection.sasl(authenticatorSupplier);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        try {
            ByteBuffer nioBuffer = buffer.nioBuffer();
            RequestHeader header = RequestHeader.parse(nioBuffer);
            if (!saslConnection.isRequestAllowed(header.apiKey())) {
                LOG.warn(
                        "Rejecting Kafka API {} before authentication completes on listener {}",
                        header.apiKey(),
                        listenerName);
                close();
                return;
            }
            KafkaRequest request =
                    parseRequest(
                            ctx, future, buffer, listenerName, saslConnection, header, nioBuffer);
            inflightResponses.addLast(request);
            future.whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());
            int channelIndex =
                    MathUtils.murmurHash(ctx.channel().id().asLongText().hashCode()) % numChannels;
            // The worker and the ordered-response queue own independent references. This lets a
            // disconnect release response-side ownership without invalidating a Produce request
            // that is still waiting in the shared RequestChannel.
            request.retainBufferForProcessor();
            requestChannels[channelIndex].putRequest(request);

            if (!isActive.get()) {
                LOG.warn("Received a request on an inactive channel: {}", remoteAddress);
                request.fail(new LeaderNotAvailableException("Channel is inactive"));
            }
        } catch (Throwable t) {
            LOG.error("Error handling request", t);
            close();
        } finally {
            // KafkaRequest retains the buffer because Kafka record sets can reference its memory
            // asynchronously. Release the decoder's ownership on every path; the request releases
            // its retained reference after response handling or cancellation.
            ReferenceCountUtil.release(buffer);
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
        LOG.info("Connection closed from {}", ctx.channel().remoteAddress());
        deactivate();
        super.channelInactive(ctx);
        // TODO Channel metrics
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
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

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseBuffer();
                continue;
            }

            if (!isDone) {
                break;
            }

            inflightResponses.pollFirst();
            if (isActive.get()) {
                ByteBuf buffer = request.responseBuffer();
                ChannelFuture responseFuture = ctx.writeAndFlush(buffer);
                if (request.shouldCloseConnectionAfterResponse()) {
                    isActive.set(false);
                    saslConnection.close();
                    responseFuture.addListener(
                            ignored -> {
                                releasePendingRequests();
                                ctx.close();
                            });
                    break;
                }
            } else {
                request.releaseBuffer();
            }
        }
    }

    protected void close() {
        deactivate();
        if (ctx != null) {
            ctx.close();
        }
        LOG.warn(
                "Close channel {} with {} pending requests.",
                remoteAddress,
                inflightResponses.size());
    }

    private void deactivate() {
        isActive.set(false);
        saslConnection.close();
        releasePendingRequests();
    }

    private void releasePendingRequests() {
        KafkaRequest request;
        while ((request = inflightResponses.pollFirst()) != null) {
            request.cancel();
            request.releaseBuffer();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    private static KafkaRequest parseRequest(
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future,
            ByteBuf buffer,
            String listenerName,
            KafkaSaslConnection saslConnection,
            RequestHeader header,
            ByteBuffer nioBuffer) {
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest(
                            new ApiVersionsRequestData(),
                            API_VERSIONS.oldestVersion(),
                            header.apiVersion());
            return new KafkaRequest(
                    API_VERSIONS,
                    header.apiVersion(),
                    header,
                    request,
                    listenerName,
                    saslConnection,
                    buffer,
                    ctx,
                    future);
        }
        RequestAndSize request =
                AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(
                header.apiKey(),
                header.apiVersion(),
                header,
                request.request,
                listenerName,
                saslConnection,
                buffer,
                ctx,
                future);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS
                && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
