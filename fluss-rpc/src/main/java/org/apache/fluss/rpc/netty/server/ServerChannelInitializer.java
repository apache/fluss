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

import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.rpc.netty.NettyChannelInitializer;
import org.apache.fluss.rpc.netty.ssl.SslContextFactory;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.function.Supplier;

import static org.apache.fluss.rpc.protocol.MessageCodec.encodeServerFailure;

/**
 * A specialized {@link ChannelInitializer} for initializing {@link SocketChannel} instances that
 * will be used by the server to handle the init request for the client.
 */
final class ServerChannelInitializer extends NettyChannelInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(ServerChannelInitializer.class);

    private final RequestChannel[] requestChannels;
    private final ApiManager apiManager;
    private final String endpointListenerName;
    private final boolean isInternal;
    private final RequestsMetrics requestsMetrics;
    private final int maxRequestSize;
    private final Supplier<ServerAuthenticator> authenticatorSupplier;
    /** The TLS context of this listener, or {@code null} when the listener is plaintext. */
    @Nullable private final SslContext sslContext;

    private final boolean requireClientAuth;

    public ServerChannelInitializer(
            RequestChannel[] requestChannels,
            ApiManager apiManager,
            String endpointListenerName,
            boolean isInternal,
            RequestsMetrics requestsMetrics,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            Supplier<ServerAuthenticator> authenticatorSupplier,
            @Nullable SslContext sslContext,
            boolean requireClientAuth) {
        super(maxIdleTimeSeconds);
        this.requestChannels = requestChannels;
        this.apiManager = apiManager;
        this.endpointListenerName = endpointListenerName;
        this.isInternal = isInternal;
        this.requestsMetrics = requestsMetrics;
        this.maxRequestSize = maxRequestSize;
        this.authenticatorSupplier = authenticatorSupplier;
        this.sslContext = sslContext;
        this.requireClientAuth = requireClientAuth;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        super.initChannel(ch);
        if (sslContext != null) {
            // The handlers are added in pipeline order. The record check comes before the
            // SslHandler, so that it reads the raw bytes and its reply is not encrypted.
            ch.pipeline().addFirst("tls-record-check", new TlsRecordCheck());
            // Before every other handler, so framing and authentication run inside the encrypted
            // channel.
            ch.pipeline()
                    .addAfter(
                            "tls-record-check",
                            "ssl",
                            SslContextFactory.createServerSslHandler(
                                    sslContext, ch.alloc(), requireClientAuth));
            // Writes travel from the end of the pipeline to the start, so this handler sees each
            // write before SslHandler does.
            ch.pipeline().addAfter("ssl", "fileRegionToByteBuf", new TlsFileRegionHandler());
        }
        // initialBytesToStrip=0 to include the frame size field after decoding
        addFrameDecoder(ch, maxRequestSize, 0, false);
        addIdleStateHandler(ch);
        ServerAuthenticator serverAuthenticator = authenticatorSupplier.get();
        LOG.debug(
                "initial a channel for listener {} with protocol {}",
                endpointListenerName,
                serverAuthenticator.protocol());
        // TODO: we can introduce a smarter and dynamic strategy to distribute requests to channels
        ch.pipeline()
                .addLast(
                        "handler",
                        new NettyServerHandler(
                                requestChannels[
                                        MathUtils.murmurHash(ch.id().hashCode())
                                                % requestChannels.length],
                                apiManager,
                                endpointListenerName,
                                isInternal,
                                requestsMetrics,
                                serverAuthenticator));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Unexpected exception caught in server channel initializer.", cause);
        super.exceptionCaught(ctx, cause);
    }

    /**
     * Rejects a plaintext client on a TLS listener at its first byte.
     *
     * <p>Without this handler the client waits for the handshake timeout of the server (10 seconds
     * by default) and then fails with a retriable disconnect.
     *
     * <p>A TLS client starts with a handshake record, whose first byte is 22. A Fluss request
     * starts with its frame length, whose first byte is smaller for any frame below 352 MiB. On
     * another first byte this handler answers with a plaintext failure frame, which the client
     * reports as a non-retriable {@link AuthenticationException}, and closes the connection. It
     * removes itself after the first byte of a TLS client.
     */
    private final class TlsRecordCheck extends ChannelInboundHandlerAdapter {

        private static final int TLS_HANDSHAKE_CONTENT_TYPE = 22;

        private boolean rejected;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg;
            if (rejected) {
                buf.release();
                return;
            }
            if (!buf.isReadable()) {
                ctx.fireChannelRead(msg);
                return;
            }
            if (buf.getUnsignedByte(buf.readerIndex()) == TLS_HANDSHAKE_CONTENT_TYPE) {
                ctx.pipeline().remove(this);
                ctx.fireChannelRead(msg);
                return;
            }
            rejected = true;
            buf.release();
            ApiError error =
                    ApiError.fromThrowable(
                            new AuthenticationException(
                                    "Listener '"
                                            + endpointListenerName
                                            + "' requires TLS, but the client sent plaintext. "
                                            + "Set 'client.security.ssl.enabled' to true on "
                                            + "the client."));
            ctx.writeAndFlush(encodeServerFailure(ctx.alloc(), error))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }
}
