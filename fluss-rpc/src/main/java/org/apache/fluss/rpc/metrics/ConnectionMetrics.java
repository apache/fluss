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

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Metrics for ServerConnection with {@link ClientMetricGroup} as parent group. */
public class ConnectionMetrics {
    private static final List<ApiKeys> REPORT_API_KEYS =
            Arrays.asList(ApiKeys.PRODUCE_LOG, ApiKeys.FETCH_LOG, ApiKeys.PUT_KV, ApiKeys.LOOKUP);

    private final String serverId;
    private final ClientMetricGroup clientMetricGroup;

    /** Metrics for different request/response metrics with specify {@link ApiKeys}. */
    final Map<String, Metrics> metricsByRequestName = MapUtils.newConcurrentHashMap();

    public ConnectionMetrics(String serverId, ClientMetricGroup clientMetricGroup) {
        this.serverId = serverId;
        this.clientMetricGroup = clientMetricGroup;
    }

    public void close() {
        clientMetricGroup.removeConnectionMetricGroup(serverId, this);
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
    Metrics getOrCreateRequestMetrics(ApiKeys apikey) {
        if (!REPORT_API_KEYS.contains(apikey)) {
            return null;
        }

        return metricsByRequestName.computeIfAbsent(apikey.name(), keyName -> new Metrics());
    }

    static final class Metrics {
        final Counter requests;
        final Counter responses;
        final Counter inComingBytes;
        final Counter outGoingBytes;

        volatile long requestLatencyMs;
        final AtomicInteger requestsInFlight;

        private Metrics() {
            requests = new ThreadSafeSimpleCounter();
            responses = new ThreadSafeSimpleCounter();
            inComingBytes = new ThreadSafeSimpleCounter();
            outGoingBytes = new ThreadSafeSimpleCounter();
            requestsInFlight = new AtomicInteger(0);
        }

        long requestRate() {
            return requests.getCount();
        }

        long responseRate() {
            return responses.getCount();
        }

        long byteInRate() {
            return inComingBytes.getCount();
        }

        long byteOutRate() {
            return outGoingBytes.getCount();
        }

        long requestLatencyMs() {
            return requestLatencyMs;
        }

        long requestsInFlight() {
            return requestsInFlight.get();
        }
    }
}
