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

package com.alibaba.fluss.rpc.netty;

import com.alibaba.fluss.shaded.netty4.io.netty.channel.Channel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.epoll.Epoll;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.epoll.EpollServerSocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.ServerSocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.unix.Errors;
import com.alibaba.fluss.shaded.netty4.io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.CompletableFuture;

/** Utils of netty. */
public class NettyUtils {

    /** @return an EventLoopGroup suitable for the current platform */
    public static EventLoopGroup newEventLoopGroup(int nThreads, String threadNamePrefix) {
        if (Epoll.isAvailable()) {
            // Regular Epoll based event loop
            String threadName = threadNamePrefix + "(EPOLL)";
            return new EpollEventLoopGroup(nThreads, new DefaultThreadFactory(threadName, true));
        } else {
            // Fallback to NIO
            String threadName = threadNamePrefix + "(NIO)";
            return new NioEventLoopGroup(nThreads, new DefaultThreadFactory(threadName, true));
        }
    }

    /** Return a SocketChannel class suitable for the given EventLoopGroup implementation. */
    public static Class<? extends SocketChannel> getClientSocketChannelClass(
            EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollSocketChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }

    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass(
            EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    public static CompletableFuture<Void> shutdownGroup(EventLoopGroup group) {
        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        if (group != null) {
            group.shutdownGracefully()
                    .addListener(
                            finished -> {
                                if (finished.isSuccess()) {
                                    shutdownFuture.complete(null);
                                } else {
                                    shutdownFuture.completeExceptionally(finished.cause());
                                }
                            });
        } else {
            shutdownFuture.complete(null);
        }
        return shutdownFuture;
    }

    public static CompletableFuture<Void> shutdownChannel(Channel channel) {
        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        if (channel != null) {
            channel.close()
                    .addListener(
                            finished -> {
                                if (finished.isSuccess()) {
                                    shutdownFuture.complete(null);
                                } else {
                                    shutdownFuture.completeExceptionally(finished.cause());
                                }
                            });
        } else {
            shutdownFuture.complete(null);
        }
        return shutdownFuture;
    }

    /**
     * check whether the provided {@link Throwable} represents a bind failure.
     *
     * @param t The {@link Throwable} object to be checked for bind failure.
     * @return {@code true} if the provided {@link Throwable} represents a bind failure, {@code
     *     false} otherwise.
     */
    public static boolean isBindFailure(Throwable t) {
        return t instanceof java.net.BindException
                || (t instanceof Errors.NativeIoException
                        && t.getMessage() != null
                        && t.getMessage().matches("^bind\\(.*\\) failed:.*"))
                || (t.getCause() != null && isBindFailure(t.getCause()));
    }
}
