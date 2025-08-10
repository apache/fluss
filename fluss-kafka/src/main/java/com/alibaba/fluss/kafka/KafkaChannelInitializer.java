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

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.rpc.netty.NettyChannelInitializer;
import com.alibaba.fluss.rpc.netty.server.RequestChannel;

import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;

/**
 * A {@link ChannelInitializer} for initializing {@link SocketChannel} instances that will be used
 * by the server to handle the Kafka requests for the client.
 */
public class KafkaChannelInitializer extends NettyChannelInitializer {
    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    private final RequestChannel[] requestChannels;
    private final LengthFieldPrepender prepender = new LengthFieldPrepender(4);

    public KafkaChannelInitializer(RequestChannel[] requestChannels, long maxIdleTimeSeconds) {
        super(maxIdleTimeSeconds);
        this.requestChannels = requestChannels;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        super.initChannel(ch);
        addIdleStateHandler(ch);
        ch.pipeline().addLast(prepender);
        addFrameDecoder(ch, MAX_FRAME_LENGTH, 4);
        ch.pipeline().addLast("flowController", new FlowControlHandler());
        ch.pipeline().addLast(new KafkaCommandDecoder(requestChannels));
    }
}
