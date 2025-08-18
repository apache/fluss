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

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.utils.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;

/**
 * Endpoint is what fluss server is listened for. It includes host, port and listener name. Listener
 * name is used for routing, all the fluss servers can have the same listener names to listen. For
 * example, coordinator server and tablet server can use internal listener to communicate with each
 * other. And If a client connect to a server with a host and port, it can only see the other
 * server's same listener.
 */
@Internal
public class Endpoint {
    private static final Pattern ENDPOINT_PARSE_EXP =
            Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");

    private final String host;
    private final int port;
    private final String listenerName;

    public Endpoint(String host, int port, String listenerName) {
        this.host = host;
        this.port = port;
        this.listenerName = listenerName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getListenerName() {
        return listenerName;
    }

    public static List<Endpoint> loadBindEndpoints(Configuration conf, ServerType serverType) {
        if (conf.getOptional(ConfigOptions.BIND_LISTENERS).isPresent()
                && getHost(conf, serverType).isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Config options are incompatible. Only one of '%s' and '%s' must be set.",
                            ConfigOptions.BIND_LISTENERS.key(),
                            serverType == ServerType.COORDINATOR
                                    ? ConfigOptions.COORDINATOR_HOST.key()
                                    : ConfigOptions.TABLET_SERVER_HOST.key()));
        }

        if (conf.getOptional(ConfigOptions.BIND_LISTENERS).isPresent()) {
            String listeners = conf.getString(ConfigOptions.BIND_LISTENERS);
            List<Endpoint> endpoints = fromListenersString(listeners, true);
            String internalListenerName = conf.get(ConfigOptions.INTERNAL_LISTENER_NAME);
            if (endpoints.stream()
                    .noneMatch(
                            endpoint -> endpoint.getListenerName().equals(internalListenerName))) {
                throw new IllegalArgumentException(
                        String.format(
                                "Internal listener name %s is not included in listeners %s",
                                internalListenerName, listeners));
            }
            return endpoints;
        }

        if (conf.getOptional(ConfigOptions.INTERNAL_LISTENER_NAME).isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Config '%s' cannot be set without '%s'",
                            ConfigOptions.INTERNAL_LISTENER_NAME.key(),
                            ConfigOptions.BIND_LISTENERS.key()));
        }

        Optional<String> maybeHost = getHost(conf, serverType);

        // backward compatibility
        if (maybeHost.isPresent()) {
            String port = getPort(conf, serverType);
            return Collections.singletonList(
                    new Endpoint(maybeHost.get(), Integer.parseInt(port), DEFAULT_LISTENER_NAME));
        }

        throw new IllegalArgumentException(
                String.format("The '%s' is not configured.", ConfigOptions.BIND_LISTENERS.key()));
    }

    public static List<Endpoint> loadAdvertisedEndpoints(
            List<Endpoint> bindEndpoints, Configuration conf) {
        List<Endpoint> advertisedEndpoints =
                fromListenersString(conf.getString(ConfigOptions.ADVERTISED_LISTENERS), false);
        Set<String> bindListenerNames =
                bindEndpoints.stream().map(Endpoint::getListenerName).collect(Collectors.toSet());
        Map<String, Endpoint> advertisedEndpointMap = new HashMap<>();
        for (Endpoint advertisedEndpoint : advertisedEndpoints) {
            if (bindListenerNames.contains(advertisedEndpoint.listenerName)) {
                advertisedEndpointMap.put(advertisedEndpoint.listenerName, advertisedEndpoint);
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "advertised listener name %s is not included in listeners %s",
                                advertisedEndpoint.listenerName,
                                Endpoint.toListenersString(bindEndpoints)));
            }
        }

        return bindEndpoints.stream()
                .map(
                        endpoint ->
                                advertisedEndpointMap.getOrDefault(
                                        endpoint.getListenerName(), endpoint))
                .collect(Collectors.toList());
    }

    public static List<Endpoint> fromListenersString(String listeners) {
        return fromListenersString(listeners, true);
    }

    private static List<Endpoint> fromListenersString(
            String listeners, boolean requireDistinctPorts) {
        if (StringUtils.isNullOrWhitespaceOnly(listeners)) {
            return Collections.emptyList();
        }

        List<Endpoint> endpoints =
                Arrays.stream(listeners.split(","))
                        .map(Endpoint::fromListenerString)
                        .collect(Collectors.toList());

        Set<String> distinctListenerNames =
                endpoints.stream().map(Endpoint::getListenerName).collect(Collectors.toSet());
        if (endpoints.size() != distinctListenerNames.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Each listener must have a different name, listeners: %s", listeners));
        }

        if (requireDistinctPorts) {
            List<Integer> portsExcludingZero =
                    endpoints.stream()
                            .map(Endpoint::getPort)
                            .filter(port -> port != 0)
                            .collect(Collectors.toList());
            Set<Integer> distinctPorts = new HashSet<>(portsExcludingZero);
            if (portsExcludingZero.size() != distinctPorts.size()) {
                throw new IllegalArgumentException(
                        String.format(
                                "There is more than one endpoint with the same port, listeners: %s",
                                listeners));
            }
        }

        return endpoints;
    }

    private static Optional<String> getHost(Configuration conf, ServerType serverType) {
        return serverType == ServerType.COORDINATOR
                ? conf.getOptional(ConfigOptions.COORDINATOR_HOST)
                : conf.getOptional(ConfigOptions.TABLET_SERVER_HOST);
    }

    private static String getPort(Configuration conf, ServerType serverType) {
        return serverType == ServerType.COORDINATOR
                ? conf.get(ConfigOptions.COORDINATOR_PORT)
                : conf.get(ConfigOptions.TABLET_SERVER_PORT);
    }

    /**
     * Create Endpoint object from {@code listenerString}.
     *
     * @param listenerString the format is listener_name://host:port or listener_name://[ipv6
     *     host]:port for example: INTERNAL://my_host:9092, CLIENT://my_host:9093 or
     *     REPLICATION://[::1]:9094
     */
    private static Endpoint fromListenerString(String listenerString) {
        Matcher matcher = ENDPOINT_PARSE_EXP.matcher(listenerString.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid endpoint format: " + listenerString);
        }

        return new Endpoint(matcher.group(2), Integer.parseInt(matcher.group(3)), matcher.group(1));
    }

    public static String toListenersString(List<Endpoint> endpoints) {
        return endpoints.stream().map(Endpoint::listenerString).collect(Collectors.joining(","));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Endpoint endpoint = (Endpoint) o;
        return port == endpoint.port
                && Objects.equals(host, endpoint.host)
                && Objects.equals(listenerName, endpoint.listenerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, listenerName);
    }

    public String listenerString() {
        return listenerName + "://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return listenerString();
    }
}
