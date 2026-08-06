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
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * The register information of tablet server stored in {@link ZkData.ServerIdZNode}.
 *
 * @see TabletServerRegistrationJsonSerde for json serialization and deserialization.
 */
public class TabletServerRegistration {
    public static final String REMOTE_MANIFEST_VERSION_DISPATCH_CAPABILITY =
            "remote-manifest-version-dispatch";

    private final @Nullable String rack;
    private final List<Endpoint> endpoints;
    private final long registerTimestamp;
    private final TabletServerResource resource;
    private final Set<String> capabilities;

    public TabletServerRegistration(
            @Nullable String rack, List<Endpoint> endpoints, long registerTimestamp) {
        this(rack, endpoints, registerTimestamp, TabletServerResource.unknown(), emptySet());
    }

    public TabletServerRegistration(
            @Nullable String rack,
            List<Endpoint> endpoints,
            long registerTimestamp,
            TabletServerResource resource) {
        this(rack, endpoints, registerTimestamp, resource, emptySet());
    }

    public TabletServerRegistration(
            @Nullable String rack,
            List<Endpoint> endpoints,
            long registerTimestamp,
            TabletServerResource resource,
            Set<String> capabilities) {
        this.rack = rack;
        this.endpoints = endpoints;
        this.registerTimestamp = registerTimestamp;
        this.resource = resource;
        this.capabilities = unmodifiableSet(new TreeSet<>(capabilities));
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

    public Set<String> getCapabilities() {
        return capabilities;
    }

    public boolean supportsCapability(String capability) {
        return capabilities.contains(capability);
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
                && Objects.equals(endpoints, that.endpoints)
                && Objects.equals(rack, that.rack)
                && Objects.equals(resource, that.resource)
                && Objects.equals(capabilities, that.capabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoints, registerTimestamp, rack, resource, capabilities);
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
                + ", capabilities="
                + capabilities
                + '}';
    }
}
