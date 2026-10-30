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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServerConfigManager;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.tablet.TabletService;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import org.apache.kafka.common.errors.InvalidTopicException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private static final String SASL_AUTH_PROTOCOL = "sasl";
    private static final String PLAINTEXT_AUTH_PROTOCOL = "plaintext";

    private Configuration conf;
    private KafkaTopicMapper topicMapper;
    private PlainSaslServerConfigManager plainSaslServerConfigManager;
    private Map<String, Supplier<ServerAuthenticator>> authenticatorSuppliers =
            Collections.emptyMap();
    private Set<String> saslListenerNames = Collections.emptySet();
    private final Map<String, KafkaRequestMetrics> requestMetricsByListener = new HashMap<>();

    @Override
    public String name() {
        return KAFKA_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        validateKafkaAuthenticationConfiguration(conf);
        this.saslListenerNames = saslListenerNames(conf);
        this.plainSaslServerConfigManager = new PlainSaslServerConfigManager(conf);
        this.conf = plainSaslServerConfigManager.getConfiguration();
        this.topicMapper = new KafkaTopicMapper(this.conf);
        this.authenticatorSuppliers =
                AuthenticationFactory.loadServerAuthenticatorSuppliers(this.conf);
    }

    @Override
    public List<String> listenerNames() {
        return conf.get(ConfigOptions.KAFKA_LISTENER_NAMES);
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        return createChannelHandler(requestChannels, listenerName, null);
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels,
            String listenerName,
            @Nullable MetricGroup serverMetricGroup) {
        Supplier<ServerAuthenticator> authenticatorSupplier = null;
        if (saslListenerNames.contains(listenerName)) {
            authenticatorSupplier =
                    checkNotNull(
                            authenticatorSuppliers.get(listenerName),
                            "No SASL server authenticator is configured for Kafka listener %s.",
                            listenerName);
        }
        return new KafkaChannelInitializer(
                requestChannels,
                listenerName,
                conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                authenticatorSupplier,
                serverMetricGroup == null
                        ? null
                        : requestMetricsByListener.computeIfAbsent(
                                listenerName,
                                name ->
                                        new KafkaRequestMetrics(
                                                serverMetricGroup
                                                        .addGroup("kafka")
                                                        .addGroup("listener", name),
                                                topic ->
                                                        tableRequestMetricGroup(
                                                                serverMetricGroup, name, topic))));
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        checkArgument(
                service instanceof TabletService,
                "Kafka protocol endpoints only supports TabletServer, but the service is %s",
                service.getClass().getSimpleName());
        return new KafkaRequestHandler(new KafkaTopicMapper(conf), (TabletService) service);
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        validateKafkaAuthenticationConfiguration(newConfig);
        plainSaslServerConfigManager.validate(newConfig);
    }

    @Override
    public void validate(Configuration newConfig, @Nullable FlussPrincipal requester)
            throws ConfigException {
        validateKafkaAuthenticationConfiguration(newConfig);
        plainSaslServerConfigManager.validate(newConfig, requester);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        plainSaslServerConfigManager.reconfigure(newConfig);
    }

    @Nullable
    private MetricGroup tableRequestMetricGroup(
            MetricGroup serverMetricGroup, String listenerName, String topicName) {
        if (!(serverMetricGroup instanceof TabletServerMetricGroup)) {
            return null;
        }
        final TableMetricGroup tableGroup;
        try {
            tableGroup =
                    ((TabletServerMetricGroup) serverMetricGroup)
                            .getTableMetricGroup(topicMapper.toTablePath(topicName));
        } catch (InvalidTopicException e) {
            return null;
        }
        if (tableGroup == null || tableGroup.isClosed()) {
            return null;
        }
        return tableGroup
                .addGroup("kafka")
                .addGroup("listener", listenerName)
                .addGroup("request", "PRODUCE");
    }

    private static void validateKafkaAuthenticationConfiguration(Configuration configuration) {
        Map<String, String> protocolMap =
                configuration.get(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        List<String> kafkaListeners = configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES);
        boolean saslEnabled = false;
        for (String listenerName : kafkaListeners) {
            String protocol = protocolMap.get(listenerName);
            if (protocol == null) {
                continue;
            }
            if (PLAINTEXT_AUTH_PROTOCOL.equalsIgnoreCase(protocol)) {
                continue;
            }
            if (!SASL_AUTH_PROTOCOL.equalsIgnoreCase(protocol)) {
                throw new ConfigException(
                        String.format(
                                "Kafka listener '%s' supports only PLAINTEXT or SASL authentication, but '%s' is configured.",
                                listenerName, protocol));
            }
            saslEnabled = true;
        }
        if (!saslEnabled) {
            return;
        }

        List<String> mechanisms =
                configuration.get(ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG);
        if (mechanisms == null
                || !mechanisms.stream()
                        .anyMatch(mechanism -> "PLAIN".equalsIgnoreCase(mechanism))) {
            throw new ConfigException(
                    "Kafka SASL listeners require PLAIN in security.sasl.enabled.mechanisms.");
        }
    }

    private static Set<String> saslListenerNames(Configuration configuration) {
        Map<String, String> protocolMap =
                configuration.get(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        Set<String> listenerNames = new HashSet<>();
        for (String listenerName : configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES)) {
            if (SASL_AUTH_PROTOCOL.equalsIgnoreCase(protocolMap.get(listenerName))) {
                listenerNames.add(listenerName);
            }
        }
        return Collections.unmodifiableSet(listenerNames);
    }
}
