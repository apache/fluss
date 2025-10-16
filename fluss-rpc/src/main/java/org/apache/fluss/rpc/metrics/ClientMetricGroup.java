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

package org.apache.fluss.rpc.metrics;

import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import javax.annotation.Nullable;

import java.util.Map;

/** The metric group for clients. */
public class ClientMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "client";

    private final String clientId;
    private final @Nullable String clusterId;

    public ClientMetricGroup(MetricRegistry registry, String clientId) {
        this(registry, null, clientId);
    }

    public ClientMetricGroup(MetricRegistry registry, @Nullable String clusterId, String clientId) {
        super(registry, new String[] {NAME}, null);
        this.clientId = clientId;
        this.clusterId = clusterId;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("client_id", clientId);
        if (clusterId != null) {
            variables.put("cluster_id", clusterId);
        } else {
            variables.put("cluster_id", "");
        }
    }

    public MetricRegistry getMetricRegistry() {
        return registry;
    }

    public ConnectionMetricGroup createConnectionMetricGroup(String serverId) {
        return new ConnectionMetricGroup(registry, serverId, this);
    }
}
