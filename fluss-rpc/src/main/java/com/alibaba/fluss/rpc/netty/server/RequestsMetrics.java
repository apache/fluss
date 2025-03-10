/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.DescriptiveStatisticsHistogram;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.ThreadSafeSimpleCounter;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.rpc.protocol.ApiKeys;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A class wrapping the metrics registered for different request types. It's mainly used to simplify
 * the metric update for a specified request type.
 */
public class RequestsMetrics {

    // a map from request name to the metrics registered for the request name
    private final Map<String, Metrics> metricsByRequest = new HashMap<>();

    // the request metric group
    private final MetricGroup requestMetricGroup;

    private RequestsMetrics(MetricGroup serverMetricsGroup, Collection<ApiKeys> apiKeys) {
        for (ApiKeys apiKey : apiKeys) {
            // we create a metrics group for each type of request, with the request type
            // as variable
            if (apiKey == ApiKeys.FETCH_LOG) {
                // if it's fetch, we need two metrics group, one for client, one for follower
                addMetrics(serverMetricsGroup, toRequestName(apiKey, true));
                addMetrics(serverMetricsGroup, toRequestName(apiKey, false));
            } else {
                addMetrics(serverMetricsGroup, toRequestName(apiKey, false));
            }
        }
        this.requestMetricGroup = serverMetricsGroup.addGroup("request");
    }

    public static RequestsMetrics createTabletServerRequestMetrics(MetricGroup serverMetricsGroup) {
        List<ApiKeys> apiKeys =
                Arrays.asList(
                        ApiKeys.PRODUCE_LOG,
                        ApiKeys.PUT_KV,
                        ApiKeys.LOOKUP,
                        ApiKeys.FETCH_LOG,
                        ApiKeys.PREFIX_LOOKUP);
        return new RequestsMetrics(serverMetricsGroup, apiKeys);
    }

    public static RequestsMetrics createCoordinatorServerRequestMetrics(
            MetricGroup serverMetricsGroup) {
        // now, no need to register any metrics of request for coordinator server
        return new RequestsMetrics(serverMetricsGroup, Collections.emptySet());
    }

    /** Create a gauge metric in the request metric group. */
    <T, G extends Gauge<T>> void gauge(String name, G gauge) {
        requestMetricGroup.gauge(name, gauge);
    }

    /** Add a metric group for given request name. */
    private void addMetrics(MetricGroup parentMetricGroup, String requestName) {
        metricsByRequest.put(
                requestName, new Metrics(parentMetricGroup.addGroup("request", requestName)));
    }

    private static String toRequestName(ApiKeys apiKeys, boolean isFromFollower) {
        switch (apiKeys) {
            case PRODUCE_LOG:
                return "produceLog";
            case PUT_KV:
                return "putKv";
            case LOOKUP:
                return "lookup";
            case PREFIX_LOOKUP:
                return "prefixLookup";
            case FETCH_LOG:
                return isFromFollower ? "fetchLogFollower" : "fetchLogClient";
            default:
                return "unknown";
        }
    }

    public Optional<Metrics> getMetrics(short apiKey, boolean isFromFollower) {
        String requestName = toRequestName(ApiKeys.forId(apiKey), isFromFollower);
        return Optional.ofNullable(metricsByRequest.get(requestName));
    }

    /** A class wrapping all registered metrics for a given request type. */
    public static final class Metrics {
        private static final int WINDOW_SIZE = 1024;
        private final Counter requestsCount;
        private final Counter errorsCount;

        private final Histogram requestBytes;

        private final Histogram requestQueueTimeMs;
        private final Histogram requestProcessTimeMs;
        private final Histogram responseSendTimeMs;
        private final Histogram totalTimeMs;

        private Metrics(MetricGroup metricGroup) {
            requestsCount = new ThreadSafeSimpleCounter();
            metricGroup.meter(MetricNames.REQUESTS_RATE, new MeterView(requestsCount));
            errorsCount = new ThreadSafeSimpleCounter();
            metricGroup.meter(MetricNames.ERRORS_RATE, new MeterView(errorsCount));

            requestBytes =
                    metricGroup.histogram(
                            MetricNames.REQUEST_BYTES,
                            new DescriptiveStatisticsHistogram(WINDOW_SIZE));
            requestQueueTimeMs =
                    metricGroup.histogram(
                            MetricNames.REQUEST_QUEUE_TIME_MS,
                            new DescriptiveStatisticsHistogram(WINDOW_SIZE));
            requestProcessTimeMs =
                    metricGroup.histogram(
                            MetricNames.REQUEST_PROCESS_TIME_MS,
                            new DescriptiveStatisticsHistogram(WINDOW_SIZE));
            responseSendTimeMs =
                    metricGroup.histogram(
                            MetricNames.RESPONSE_SEND_TIME_MS,
                            new DescriptiveStatisticsHistogram(WINDOW_SIZE));
            totalTimeMs =
                    metricGroup.histogram(
                            MetricNames.REQUEST_TOTAL_TIME_MS,
                            new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        }

        public Counter getRequestsCount() {
            return requestsCount;
        }

        public Counter getErrorsCount() {
            return errorsCount;
        }

        public Histogram getRequestBytes() {
            return requestBytes;
        }

        public Histogram getRequestQueueTimeMs() {
            return requestQueueTimeMs;
        }

        public Histogram getRequestProcessTimeMs() {
            return requestProcessTimeMs;
        }

        public Histogram getResponseSendTimeMs() {
            return responseSendTimeMs;
        }

        public Histogram getTotalTimeMs() {
            return totalTimeMs;
        }
    }
}
