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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.NetworkProtocolPlugin;
import com.alibaba.fluss.security.auth.AuthenticationFactory;
import com.alibaba.fluss.security.auth.PlainTextAuthenticationPlugin;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.List;
import java.util.Optional;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin {
    private final ApiManager apiManager;
    private final List<String> listeners;
    private final RequestsMetrics requestsMetrics;
    private Configuration conf;

    public FlussProtocolPlugin(
            ServerType serverType, List<String> listeners, RequestsMetrics requestsMetrics) {
        this.apiManager = new ApiManager(serverType);
        this.listeners = listeners;
        this.requestsMetrics = requestsMetrics;
    }

    @Override
    public String name() {
        return FLUSS_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public List<String> listenerNames() {
        return listeners;
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        return new ServerChannelInitializer(
                requestChannels,
                apiManager,
                listenerName,
                listenerName.equals(conf.get(ConfigOptions.INTERNAL_LISTENER_NAME)),
                requestsMetrics,
                conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                Optional.ofNullable(
                                AuthenticationFactory.loadServerAuthenticatorSuppliers(conf)
                                        .get(listenerName))
                        .orElse(PlainTextAuthenticationPlugin.PlainTextServerAuthenticator::new));
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        return new FlussRequestHandler(service);
    }

    @VisibleForTesting
    ApiManager getApiManager() {
        return apiManager;
    }
}
