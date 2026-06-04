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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.PlainTextAuthenticationPlugin;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {
    private final ApiManager apiManager;
    private final List<String> listeners;
    private final RequestsMetrics requestsMetrics;
    private volatile Configuration conf;

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
        this.conf = enrichWithJaasConfig(conf);
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
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                Optional.ofNullable(
                                AuthenticationFactory.loadServerAuthenticatorSuppliers(
                                                () -> this.conf)
                                        .get(listenerName))
                        .orElse(PlainTextAuthenticationPlugin.PlainTextServerAuthenticator::new));
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        return new FlussRequestHandler(service);
    }

    // --- ServerReconfigurable ---

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        List<String> users = newConfig.get(ConfigOptions.SERVER_SASL_USERS);
        if (users == null) {
            return;
        }
        Set<String> uniqueUsernames = new HashSet<>();
        for (int i = 0; i < users.size(); i++) {
            String entry = users.get(i).trim();
            int colonIdx = entry.indexOf(':');
            if (colonIdx <= 0 || colonIdx == entry.length() - 1) {
                throw new ConfigException(
                        String.format(
                                "security.sasl.users[%d] must be in 'username:password' format, but got '%s'.",
                                i, entry));
            }
            String username = entry.substring(0, colonIdx);
            if (!uniqueUsernames.add(username)) {
                throw new ConfigException(
                        "security.sasl.users must not contain duplicate usernames: '"
                                + username
                                + "'.");
            }
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        this.conf = enrichWithJaasConfig(newConfig);
    }

    /**
     * Enriches the given configuration with a generated JAAS config string derived from the
     * security.sasl.users list (format: 'username:password'). If the list is not present, the
     * original configuration is returned unchanged.
     */
    private static Configuration enrichWithJaasConfig(Configuration config) {
        List<String> users = config.get(ConfigOptions.SERVER_SASL_USERS);
        if (users == null) {
            return config;
        }
        StringBuilder sb =
                new StringBuilder(
                        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required");
        for (String entry : users) {
            int colonIdx = entry.indexOf(':');
            checkArgument(colonIdx > 0, "Invalid user entry format: '%s'", entry);
            String username = entry.substring(0, colonIdx);
            String password = entry.substring(colonIdx + 1);
            sb.append(String.format(" user_%s=\"%s\"", username, password));
        }
        sb.append(";");
        Configuration enriched = new Configuration(config);
        enriched.setString("security.sasl.plain.jaas.config", sb.toString());
        return enriched;
    }

    @VisibleForTesting
    ApiManager getApiManager() {
        return apiManager;
    }

    @VisibleForTesting
    @Nullable
    Configuration getConf() {
        return conf;
    }
}
