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

package org.apache.fluss.server.coordinator.rebalance.model;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A class that holds the information of the rack, including its liveness tabletServers and
 * replicas. A rack object is created as part of a cluster structure.
 */
public class RackModel {
    public static final String DEFAULT_RACK = "default_rack";

    private final String rack;
    private final Map<Integer, ServerModel> servers;

    public RackModel(String rack) {
        this.rack = rack;
        this.servers = new HashMap<>();
    }

    @Nullable
    ReplicaModel removeReplica(int serverId, TableBucket tableBucket) {
        ServerModel server = servers.get(serverId);
        if (server != null) {
            return server.removeReplica(tableBucket);
        }

        return null;
    }

    void addReplica(ReplicaModel replica) {
        replica.server().putReplica(replica.tableBucket(), replica);
    }

    public String rack() {
        return rack;
    }

    @Nullable
    ServerModel server(int serverId) {
        return servers.get(serverId);
    }

    public void addServer(ServerModel server) {
        servers.put(server.id(), server);
    }

    public Set<Long> tables() {
        Set<Long> tables = new HashSet<>();

        for (ServerModel server : servers.values()) {
            tables.addAll(server.tables());
        }
        return tables;
    }

    @Override
    public String toString() {
        return String.format("RackModel[rack=%s,servers=%s]", rack, servers.size());
    }
}
