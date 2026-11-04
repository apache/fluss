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

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.netty.ssl.SslConfig;
import org.apache.fluss.rpc.netty.ssl.SslContextFactory;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.PlainTextAuthenticationPlugin;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServerConfigManager;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private final ApiManager apiManager;
    private final List<String> listeners;
    private final RequestsMetrics requestsMetrics;
    private Configuration conf;
    private PlainSaslServerConfigManager plainSaslServerConfigManager;
    @Nullable private SslConfig sslConfig;
    /** Shared by every TLS listener; {@code null} when no listener enables TLS. */
    @Nullable private SslContext sslContext;

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
        this.plainSaslServerConfigManager = new PlainSaslServerConfigManager(conf);
        this.conf = plainSaslServerConfigManager.getConfiguration();
        // Built once here so that invalid TLS settings fail the server at startup rather than
        // every accepted connection later.
        this.sslConfig = SslConfig.fromServerConfig(conf).orElse(null);
        if (sslConfig != null) {
            checkTlsListenersExist(sslConfig.enabledListeners());
            checkInternalListenerIsPlaintext(sslConfig.enabledListeners());
            this.sslContext = SslContextFactory.createServerSslContext(sslConfig);
        }
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
                                AuthenticationFactory.loadServerAuthenticatorSuppliers(conf)
                                        .get(listenerName))
                        .orElse(PlainTextAuthenticationPlugin.PlainTextServerAuthenticator::new),
                isTlsListener(listenerName) ? sslContext : null,
                sslConfig != null && sslConfig.requiresClientAuth(listenerName));
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        return new FlussRequestHandler(service);
    }

    private boolean isTlsListener(String listenerName) {
        return sslConfig != null && sslConfig.enabledListeners().contains(listenerName);
    }

    /**
     * A misspelled name in {@code security.ssl.enabled.listeners} would otherwise leave the
     * intended listener accepting plaintext without any notice.
     */
    private void checkTlsListenersExist(List<String> tlsListeners) {
        List<String> unknown = new ArrayList<>(tlsListeners);
        unknown.removeAll(listeners);
        if (!unknown.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Listener(s) %s in '%s' are not Fluss protocol listeners %s. TLS is supported "
                            + "only on Fluss protocol listeners; check the name for a typo, or "
                            + "remove listeners served by another protocol (e.g. Kafka).",
                    unknown, ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key(), listeners);
        }
    }

    /**
     * Servers connect to each other over the internal listener without TLS, so TLS there would fail
     * every request between servers.
     *
     * <p>TODO: remove once the client side supports TLS (#3797).
     */
    private void checkInternalListenerIsPlaintext(List<String> tlsListeners) {
        String internalListener = conf.get(ConfigOptions.INTERNAL_LISTENER_NAME);
        if (tlsListeners.contains(internalListener)) {
            throw new IllegalConfigurationException(
                    "TLS is not supported yet on the internal listener '%s' (see '%s'): servers connect to each other without TLS. Remove it from '%s'.",
                    internalListener,
                    ConfigOptions.INTERNAL_LISTENER_NAME.key(),
                    ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key());
        }
    }

    // --- ServerReconfigurable ---

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        plainSaslServerConfigManager.validate(newConfig);
    }

    @Override
    public void validate(Configuration newConfig, @Nullable FlussPrincipal requester)
            throws ConfigException {
        plainSaslServerConfigManager.validate(newConfig, requester);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        plainSaslServerConfigManager.reconfigure(newConfig);
    }
}
