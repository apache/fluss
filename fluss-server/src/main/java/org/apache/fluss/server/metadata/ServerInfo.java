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

package org.apache.fluss.server.metadata;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.server.exception.EndpointNotAvailableException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ServerInfo is used to save the endpoint metadata in controller and synchronize for each server.
 */
public class ServerInfo {
    private final Integer id;

    /**
     * the rack of the tabletServer, which is used to in rack aware bucket assignment for fault
     * tolerance. Examples: "rack1", "cn-hangzhou-server10"
     */
    private @Nullable final String rack;

    private final Map<String, Endpoint> endpointMap;
    private final ServerType serverType;

    public ServerInfo(
            Integer id, @Nullable String rack, List<Endpoint> endpoints, ServerType serverType) {
        this.id = id;
        this.rack = rack;
        this.endpointMap =
                endpoints.stream()
                        .collect(Collectors.toMap(Endpoint::getListenerName, endpoint -> endpoint));
        this.serverType = serverType;
    }

    public Integer id() {
        return id;
    }

    public @Nullable String rack() {
        return rack;
    }

    @Nullable
    public Endpoint endpoint(String listenerName) {
        return endpointMap.get(listenerName);
    }

    public Endpoint endpointOrThrow(String listenerName) {
        Endpoint endpoint = endpoint(listenerName);
        if (endpoint == null) {
            throw new EndpointNotAvailableException(
                    String.format(
                            "Endpoint with listener name: %s not found for %s %d",
                            listenerName, serverType.name().toLowerCase(), id));
        }
        return endpoint;
    }

    @Nullable
    public ServerNode node(String listenerName) {
        Endpoint endpoint = endpoint(listenerName);
        if (endpoint == null) {
            return null;
        }
        return new ServerNode(id, endpoint.getHost(), endpoint.getPort(), serverType, rack);
    }

    public ServerNode nodeOrThrow(String listenerName) {
        Endpoint endpoint = endpointOrThrow(listenerName);
        return new ServerNode(id, endpoint.getHost(), endpoint.getPort(), serverType, rack);
    }

    public List<Endpoint> endpoints() {
        return new ArrayList<>(endpointMap.values());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerInfo that = (ServerInfo) o;
        return Objects.equals(id, that.id)
                && Objects.equals(rack, that.rack)
                && Objects.equals(endpointMap, that.endpointMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rack, endpointMap);
    }

    @Override
    public String toString() {
        return "ServerInfo{"
                + "id="
                + id
                + ", rack="
                + rack
                + ", endpoints="
                + endpointMap.values()
                + ", type="
                + serverType
                + '}';
    }
}
