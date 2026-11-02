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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.server.metadata.TabletServerResource;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The register information of tablet server stored in {@link ZkData.ServerIdZNode}.
 *
 * @see TabletServerRegistrationJsonSerde for json serialization and deserialization.
 */
public class TabletServerRegistration {
    private final @Nullable String rack;
    private final List<Endpoint> endpoints;
    private final long registerTimestamp;
    private final TabletServerResource resource;
    private final int serverProtocolVersion;

    /**
     * Creates a tablet-server registration without resource information, advertising the legacy
     * protocol version {@code 0}.
     */
    public TabletServerRegistration(
            @Nullable String rack, List<Endpoint> endpoints, long registerTimestamp) {
        this(rack, endpoints, registerTimestamp, TabletServerResource.unknown(), 0);
    }

    /**
     * Creates a tablet-server registration with resource information, advertising the legacy
     * protocol version {@code 0}.
     */
    public TabletServerRegistration(
            @Nullable String rack,
            List<Endpoint> endpoints,
            long registerTimestamp,
            TabletServerResource resource) {
        this(rack, endpoints, registerTimestamp, resource, 0);
    }

    /**
     * Creates a tablet-server registration with resource information, advertising the given server
     * protocol version.
     */
    public TabletServerRegistration(
            @Nullable String rack,
            List<Endpoint> endpoints,
            long registerTimestamp,
            TabletServerResource resource,
            int serverProtocolVersion) {
        this.rack = rack;
        this.endpoints = endpoints;
        this.registerTimestamp = registerTimestamp;
        this.resource = resource;
        this.serverProtocolVersion = serverProtocolVersion;
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public long getRegisterTimestamp() {
        return registerTimestamp;
    }

    public @Nullable String getRack() {
        return rack;
    }

    public TabletServerResource getResource() {
        return resource;
    }

    /** Returns the protocol version advertised by this tablet server. */
    public int getServerProtocolVersion() {
        return serverProtocolVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TabletServerRegistration that = (TabletServerRegistration) o;
        return registerTimestamp == that.registerTimestamp
                && serverProtocolVersion == that.serverProtocolVersion
                && Objects.equals(endpoints, that.endpoints)
                && Objects.equals(rack, that.rack)
                && Objects.equals(resource, that.resource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoints, registerTimestamp, rack, resource, serverProtocolVersion);
    }

    @Override
    public String toString() {
        return "TabletServerRegistration{"
                + "endpoints="
                + endpoints
                + ", registerTimestamp="
                + registerTimestamp
                + ", rack='"
                + rack
                + "', resource="
                + resource
                + ", serverProtocolVersion="
                + serverProtocolVersion
                + '}';
    }
}
