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

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.utils.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Endpoint is what fluss server is listened for. It includes host, port and listener name. Listener
 * name is used for routing, all fluss server can have same listener names to listen . For example,
 * coordinator server and tablet sever can use internal lister to communicate with each other. And
 * If a client connect to a server with a host and port, it can only see the other server's same
 * listener.
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

    public static List<Endpoint> parseEndpoints(String listeners) {
        if (StringUtils.isNullOrWhitespaceOnly(listeners)) {
            return Collections.emptyList();
        }
        return Arrays.stream(listeners.split(","))
                .map(Endpoint::fromString)
                .collect(Collectors.toList());
    }

    /**
     * Create EndPoint object from `endpointString`.
     *
     * @param listener the format is listener_name://host:port or listener_name://[ipv6 host]:port
     *     for example: INTERNAL://my_host:9092, CLIENT://my_host:9093 or REPLICATION://[::1]:9094
     */
    private static Endpoint fromString(String listener) {
        Matcher matcher = ENDPOINT_PARSE_EXP.matcher(listener.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid endpoint format: " + listener);
        }

        return new Endpoint(matcher.group(2), Integer.parseInt(matcher.group(3)), matcher.group(1));
    }

    public static String toListenerString(List<Endpoint> endpoints) {
        return endpoints.stream().map(Endpoint::toString).collect(Collectors.joining(","));
    }

    public static List<Endpoint> getRegisteredEndpoint(
            List<Endpoint> bindEndpoints, List<Endpoint> advisedEndpoints) {
        Map<String, Endpoint> advisedEndpointMap =
                advisedEndpoints.stream()
                        .collect(Collectors.toMap(Endpoint::getListenerName, endpoint -> endpoint));
        return bindEndpoints.stream()
                .map(
                        endpoint ->
                                advisedEndpointMap.getOrDefault(
                                        endpoint.getListenerName(), endpoint))
                .collect(Collectors.toList());
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

    @Override
    public String toString() {
        return listenerName + "://" + host + ":" + port;
    }
}
