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

package org.apache.fluss.server.metrics;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import java.util.Map;

/** Metric group containing node metrics shared by coordinator and tablet servers. */
final class ServerNodeMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "server";

    private final String clusterId;
    private final String hostname;
    private final String serverId;
    private final ServerType serverType;

    ServerNodeMetricGroup(
            MetricRegistry registry,
            String clusterId,
            String hostname,
            String serverId,
            ServerType serverType) {
        super(registry, new String[] {clusterId, hostname, NAME}, null);
        this.clusterId = clusterId;
        this.hostname = hostname;
        this.serverId = serverId;
        this.serverType = serverType;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("cluster_id", clusterId);
        variables.put("host", hostname);
        variables.put("server_id", String.valueOf(serverId));
        variables.put("server_type", serverType.name().toLowerCase());
    }
}
