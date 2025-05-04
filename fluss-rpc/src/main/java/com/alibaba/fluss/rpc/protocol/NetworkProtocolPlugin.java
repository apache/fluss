/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.rpc.protocol;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.plugin.Plugin;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.netty.server.RequestChannel;
import com.alibaba.fluss.rpc.netty.server.RequestHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.List;

/** A network protocol plugin that provides the server side implementation of a network protocol. */
public interface NetworkProtocolPlugin extends Plugin {

    String FLUSS_PROTOCOL_NAME = RequestType.FLUSS.name();
    String KAFKA_PROTOCOL_NAME = RequestType.KAFKA.name();

    /** Returns the name of the protocol. */
    String name();

    /** Setup network protocol plugin with the given {@link Configuration}. */
    void setup(Configuration conf);

    /** Returns the names of the listeners that the protocol binds to. */
    List<String> listenerNames();

    /**
     * Creates a Netty {@link ChannelHandler} for handling server side I/O events and operations of
     * the network protocol.
     */
    ChannelHandler createChannelHandler(RequestChannel[] requestChannels, String listenerName);

    /**
     * Creates a {@link RequestHandler} for processing the incoming requests of the network
     * protocol.
     */
    RequestHandler<?> createRequestHandler(RpcGatewayService service);
}
