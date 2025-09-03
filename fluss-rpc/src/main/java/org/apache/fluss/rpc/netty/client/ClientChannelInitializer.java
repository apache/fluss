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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.rpc.netty.NettyChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;

/**
 * A specialized {@link ChannelInitializer} for initializing {@link SocketChannel} instances that
 * will be used by the client to handle the init request for the server.
 */
final class ClientChannelInitializer extends NettyChannelInitializer {

    public ClientChannelInitializer(long maxIdleTimeSeconds) {
        super(maxIdleTimeSeconds);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        // NettyClientHandler will be added dynamically when connection is built
        super.initChannel(ch);
        addFrameDecoder(ch, Integer.MAX_VALUE, 0);
        addIdleStateHandler(ch);
    }
}
