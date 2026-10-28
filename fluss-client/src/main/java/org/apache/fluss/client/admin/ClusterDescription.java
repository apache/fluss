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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.cluster.ServerNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Cluster information returned by {@link Admin#describeCluster()}.
 *
 * <p>Both tablet server maps are keyed by server ID. The node map contains only live servers,
 * including their addresses, racks and tags. The load map also contains offline servers that still
 * have assigned replicas. A missing load entry means statistics are unavailable, never that the
 * server has zero replicas or is safe to remove.
 *
 * @since 1.1
 */
@PublicEvolving
public final class ClusterDescription {

    @Nullable private final ServerNode coordinatorServer;
    private final Map<Integer, ServerNode> tabletServers;
    private final Map<Integer, TabletServerDescription> tabletServerLoads;

    /** Creates a cluster description with defensive copies of the server maps. */
    public ClusterDescription(
            @Nullable ServerNode coordinatorServer,
            Map<Integer, ServerNode> tabletServers,
            Map<Integer, TabletServerDescription> tabletServerLoads) {
        this.coordinatorServer = coordinatorServer;
        this.tabletServers = Collections.unmodifiableMap(new TreeMap<>(tabletServers));
        this.tabletServerLoads = Collections.unmodifiableMap(new TreeMap<>(tabletServerLoads));
    }

    /** Returns the coordinator, or an empty optional if it was not reported. */
    public Optional<ServerNode> getCoordinatorServer() {
        return Optional.ofNullable(coordinatorServer);
    }

    /** Returns the live tablet servers by server ID, in an immutable map ordered by ID. */
    public Map<Integer, ServerNode> getTabletServers() {
        return tabletServers;
    }

    /**
     * Returns the reported replica loads by server ID, in an immutable map ordered by ID.
     *
     * <p>Live servers with no replicas have an explicit zero load. Offline servers with assigned
     * replicas are also included. Missing entries indicate unavailable statistics; in particular, a
     * server that does not report replica loads may return an empty map.
     */
    public Map<Integer, TabletServerDescription> getTabletServerLoads() {
        return tabletServerLoads;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusterDescription)) {
            return false;
        }
        ClusterDescription that = (ClusterDescription) o;
        return Objects.equals(coordinatorServer, that.coordinatorServer)
                && tabletServers.equals(that.tabletServers)
                // ServerNode equality describes node identity and does not compare server tags.
                && tabletServers.entrySet().stream()
                        .allMatch(
                                entry ->
                                        entry.getValue().serverTag()
                                                == that.tabletServers
                                                        .get(entry.getKey())
                                                        .serverTag())
                && tabletServerLoads.equals(that.tabletServerLoads);
    }

    @Override
    public int hashCode() {
        int tagsHash = 1;
        for (ServerNode node : tabletServers.values()) {
            tagsHash = 31 * tagsHash + Objects.hashCode(node.serverTag());
        }
        return Objects.hash(coordinatorServer, tabletServers, tabletServerLoads, tagsHash);
    }

    @Override
    public String toString() {
        return "ClusterDescription{"
                + "coordinatorServer="
                + coordinatorServer
                + ", tabletServers="
                + tabletServers
                + ", tabletServerLoads="
                + tabletServerLoads
                + '}';
    }
}
