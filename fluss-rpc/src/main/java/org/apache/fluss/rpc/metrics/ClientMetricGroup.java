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
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;

/** The metric group for clients. */
public class ClientMetricGroup extends AbstractMetricGroup {
    private final Set<ConnectionMetricGroup> connectionMetricGroups;

    private static final String NAME = "client";

    private final String clientId;

    public ClientMetricGroup(MetricRegistry registry, String clientId) {
        super(registry, new String[] {NAME}, null);
        this.clientId = clientId;
        this.connectionMetricGroups = new HashSet<>();
        this.gauge(
                MetricNames.CLIENT_REQUESTS_RATE_AVG,
                () -> getMetricsAvg(ConnectionMetricGroup.Metrics::requestRate));
        this.gauge(
                MetricNames.CLIENT_REQUESTS_RATE_TOTAL,
                () -> getMetricsSum(ConnectionMetricGroup.Metrics::requestRate));
        this.gauge(
                MetricNames.CLIENT_RESPONSES_RATE_AVG,
                () -> getMetricsAvg(ConnectionMetricGroup.Metrics::responseRate));
        this.gauge(
                MetricNames.CLIENT_RESPONSES_RATE_TOTAL,
                () -> getMetricsSum(ConnectionMetricGroup.Metrics::responseRate));
        this.gauge(
                MetricNames.CLIENT_BYTES_IN_RATE_AVG,
                () -> getMetricsAvg(ConnectionMetricGroup.Metrics::byteInRate));
        this.gauge(
                MetricNames.CLIENT_BYTES_IN_RATE_TOTAL,
                () -> getMetricsSum(ConnectionMetricGroup.Metrics::byteInRate));
        this.gauge(
                MetricNames.CLIENT_BYTES_OUT_RATE_AVG,
                () -> getMetricsAvg(ConnectionMetricGroup.Metrics::byteOutRate));
        this.gauge(
                MetricNames.CLIENT_BYTES_OUT_RATE_TOTAL,
                () -> getMetricsSum(ConnectionMetricGroup.Metrics::byteOutRate));
        this.gauge(
                MetricNames.CLIENT_REQUEST_LATENCY_MS_AVG,
                () -> getMetricsAvg(ConnectionMetricGroup.Metrics::requestLatencyMs));
        this.gauge(
                MetricNames.CLIENT_REQUEST_LATENCY_MS_MAX,
                () -> getMetricsMax(ConnectionMetricGroup.Metrics::requestLatencyMs));
        this.gauge(
                MetricNames.CLIENT_REQUESTS_IN_FLIGHT_TOTAL,
                () -> getMetricsSum(ConnectionMetricGroup.Metrics::requestsInFlight));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("client_id", clientId);
    }

    public MetricRegistry getMetricRegistry() {
        return registry;
    }

    public ConnectionMetricGroup createConnectionMetricGroup(String serverId) {
        // Only expose aggregate metrics to reduce the reporter pressure.
        ConnectionMetricGroup connectionMetricGroup =
                new ConnectionMetricGroup(NOPMetricRegistry.INSTANCE, serverId, this);
        connectionMetricGroups.add(connectionMetricGroup);
        return connectionMetricGroup;
    }

    public void removeConnectionMetricGroup(ConnectionMetricGroup connectionMetricGroup) {
        connectionMetricGroups.remove(connectionMetricGroup);
    }

    private double getMetricsAvg(ToLongFunction<ConnectionMetricGroup.Metrics> metricGetter) {
        return connectionMetricGroups.stream()
                .flatMap(
                        connectionMetricGroup ->
                                connectionMetricGroup.metricsByRequestName.values().stream())
                .mapToLong(metricGetter)
                .average()
                .orElse(0);
    }

    private long getMetricsSum(ToLongFunction<ConnectionMetricGroup.Metrics> metricGetter) {
        return connectionMetricGroups.stream()
                .flatMap(
                        connectionMetricGroup ->
                                connectionMetricGroup.metricsByRequestName.values().stream())
                .mapToLong(metricGetter)
                .sum();
    }

    private long getMetricsMax(ToLongFunction<ConnectionMetricGroup.Metrics> metricGetter) {
        return connectionMetricGroups.stream()
                .flatMap(
                        connectionMetricGroup ->
                                connectionMetricGroup.metricsByRequestName.values().stream())
                .mapToLong(metricGetter)
                .max()
                .orElse(0);
    }
}
