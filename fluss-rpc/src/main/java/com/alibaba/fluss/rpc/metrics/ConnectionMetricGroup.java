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

package com.alibaba.fluss.rpc.metrics;

import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.ThreadSafeSimpleCounter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.utils.MapUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** Metrics for ServerConnection with {@link ClientMetricGroup} as parent group. */
public class ConnectionMetricGroup extends AbstractMetricGroup {
    private static final List<ApiKeys> REPORT_API_KEYS =
            Arrays.asList(ApiKeys.PRODUCE_LOG, ApiKeys.FETCH_LOG, ApiKeys.PUT_KV, ApiKeys.LOOKUP);

    private final String serverId;

    /** Metrics for different request/response metrics with specify {@link ApiKeys}. */
    private final Map<String, Metrics> metricsByRequestName = MapUtils.newConcurrentHashMap();

    public ConnectionMetricGroup(
            MetricRegistry registry, String serverId, ClientMetricGroup parent) {
        super(registry, makeScope(parent, serverId), parent);
        this.serverId = serverId;
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("server_id", serverId);
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "";
    }

    @Override
    protected String createLogicalScope(CharacterFilter filter, char delimiter) {
        // ignore this metric group name in logical scope
        return parent.getLogicalScope(filter, delimiter);
    }

    // ------------------------------------------------------------------------
    //  request metrics
    // ------------------------------------------------------------------------

    public void updateMetricsBeforeSendRequest(ApiKeys apikey, int outBytes) {
        Metrics metrics = getOrCreateRequestMetrics(apikey);
        if (metrics != null) {
            metrics.requests.inc();
            metrics.outGoingBytes.inc(outBytes);
            metrics.requestsInFlight.getAndIncrement();
        }
    }

    public void updateMetricsAfterGetResponse(ApiKeys apikey, long requestStartTime, int inBytes) {
        Metrics metrics = getOrCreateRequestMetrics(apikey);
        if (metrics != null) {
            metrics.responses.inc();
            metrics.inComingBytes.inc(inBytes);
            metrics.requestsInFlight.getAndDecrement();
            metrics.requestLatencyMs = System.currentTimeMillis() - requestStartTime;
        }
    }

    @Nullable
    private Metrics getOrCreateRequestMetrics(ApiKeys apikey) {
        if (!REPORT_API_KEYS.contains(apikey)) {
            return null;
        }

        return metricsByRequestName.computeIfAbsent(
                apikey.name(), keyName -> new Metrics(this.addGroup("request", keyName)));
    }

    private static final class Metrics {
        final Counter requests;
        final Counter responses;
        final Counter inComingBytes;
        final Counter outGoingBytes;

        volatile long requestLatencyMs;
        final AtomicInteger requestsInFlight;

        private Metrics(MetricGroup metricGroup) {
            requests = new ThreadSafeSimpleCounter();
            metricGroup.meter(MetricNames.CLIENT_REQUESTS_RATE, new MeterView(requests));
            responses = new ThreadSafeSimpleCounter();
            metricGroup.meter(MetricNames.CLIENT_RESPONSES_RATE, new MeterView(responses));
            inComingBytes = new ThreadSafeSimpleCounter();
            metricGroup.meter(MetricNames.CLIENT_BYTES_IN_RATE, new MeterView(inComingBytes));
            outGoingBytes = new ThreadSafeSimpleCounter();
            metricGroup.meter(MetricNames.CLIENT_BYTES_OUT_RATE, new MeterView(outGoingBytes));
            metricGroup.gauge(MetricNames.CLIENT_REQUEST_LATENCY_MS, () -> requestLatencyMs);
            requestsInFlight = new AtomicInteger(0);
            metricGroup.gauge(MetricNames.CLIENT_REQUESTS_IN_FLIGHT, requestsInFlight::get);
        }
    }
}
