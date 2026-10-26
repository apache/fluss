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

package org.apache.fluss.kafka;

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Request metrics shared by all Kafka connections on a listener. */
final class KafkaRequestMetrics {

    static final String FAILED_REQUESTS_PER_SECOND = "failedRequestsPerSecond";
    static final String RESPONSE_QUEUE_TIME_MS = "responseQueueTimeMs";
    static final String IN_FLIGHT_REQUESTS = "inFlightRequests";

    private final MetricGroup metricGroup;
    private final Clock clock;
    private final Map<ApiKeys, Metrics> metricsByApi = new ConcurrentHashMap<>();
    private final Function<String, MetricGroup> tableMetricGroupLookup;
    // Table groups own metric registration and close when their last local bucket is removed.
    // Weak keys let the cache release those groups without retaining deleted tables.
    private final Map<MetricGroup, Metrics> metricsByTableGroup =
            Collections.synchronizedMap(new WeakHashMap<>());

    KafkaRequestMetrics(MetricGroup metricGroup) {
        this(metricGroup, SystemClock.getInstance());
    }

    KafkaRequestMetrics(MetricGroup metricGroup, Clock clock) {
        this(metricGroup, topic -> null, clock);
    }

    KafkaRequestMetrics(
            MetricGroup metricGroup, Function<String, MetricGroup> tableMetricGroupLookup) {
        this(metricGroup, tableMetricGroupLookup, SystemClock.getInstance());
    }

    KafkaRequestMetrics(
            MetricGroup metricGroup,
            Function<String, MetricGroup> tableMetricGroupLookup,
            Clock clock) {
        this.metricGroup = metricGroup;
        this.tableMetricGroupLookup = tableMetricGroupLookup;
        this.clock = clock;
    }

    long nanoTime() {
        return clock.nanoseconds();
    }

    RequestMetrics startRequest(AbstractRequest request, long receivedTimeNs) {
        Map<String, Metrics> tableMetrics = Collections.emptyMap();
        if (request instanceof ProduceRequest) {
            tableMetrics = new HashMap<>();
            for (TopicProduceData topic : ((ProduceRequest) request).data().topicData()) {
                MetricGroup tableGroup = tableMetricGroupLookup.apply(topic.name());
                if (tableGroup != null) {
                    tableMetrics.put(
                            topic.name(),
                            metricsByTableGroup.computeIfAbsent(tableGroup, Metrics::new));
                }
            }
        }
        return startRequest(request.apiKey(), receivedTimeNs, tableMetrics);
    }

    RequestMetrics startRequest(ApiKeys apiKey, long receivedTimeNs) {
        return startRequest(apiKey, receivedTimeNs, Collections.emptyMap());
    }

    private RequestMetrics startRequest(
            ApiKeys apiKey, long receivedTimeNs, Map<String, Metrics> tableMetrics) {
        Metrics metrics =
                metricsByApi.computeIfAbsent(
                        apiKey, key -> new Metrics(metricGroup.addGroup("request", key.name())));
        return new RequestMetrics(metrics, tableMetrics, clock, receivedTimeNs);
    }

    private static final class Metrics {
        private static final int WINDOW_SIZE = 1024;

        private final Counter requests = new ThreadSafeSimpleCounter();
        private final Counter failures = new ThreadSafeSimpleCounter();
        private final Counter inFlight = new ThreadSafeSimpleCounter();
        private final Histogram queueTime;
        private final Histogram processTime;
        private final Histogram responseQueueTime;
        private final Histogram totalTime;

        private Metrics(MetricGroup group) {
            group.meter(MetricNames.REQUESTS_RATE, new MeterView(requests));
            group.meter(FAILED_REQUESTS_PER_SECOND, new MeterView(failures));
            group.gauge(IN_FLIGHT_REQUESTS, inFlight::getCount);
            queueTime = histogram(group, MetricNames.REQUEST_QUEUE_TIME_MS);
            processTime = histogram(group, MetricNames.REQUEST_PROCESS_TIME_MS);
            responseQueueTime = histogram(group, RESPONSE_QUEUE_TIME_MS);
            totalTime = histogram(group, MetricNames.REQUEST_TOTAL_TIME_MS);
        }

        private static Histogram histogram(MetricGroup group, String name) {
            return group.histogram(name, new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        }
    }

    /** Coordinates worker completion, ordered response sending, and connection cancellation. */
    static final class RequestMetrics {
        private final Metrics metrics;
        private final Map<String, Metrics> tableMetrics;
        private final Set<Metrics> allMetrics;
        private final Set<Metrics> failedMetrics = new HashSet<>();
        private final Clock clock;
        private final long receivedTimeNs;
        private final long enqueuedTimeNs;
        private long processingStartNs;
        private long processingEndNs;
        private boolean processingStarted;
        private boolean processingCompleted;
        private boolean responseDequeued;
        private boolean finished;

        private RequestMetrics(
                Metrics metrics,
                Map<String, Metrics> tableMetrics,
                Clock clock,
                long receivedTimeNs) {
            this.metrics = metrics;
            this.tableMetrics = tableMetrics;
            this.allMetrics = new LinkedHashSet<>(tableMetrics.values());
            this.allMetrics.add(metrics);
            this.clock = clock;
            this.receivedTimeNs = receivedTimeNs;
            this.enqueuedTimeNs = clock.nanoseconds();
            for (Metrics target : allMetrics) {
                target.requests.inc();
                target.inFlight.inc();
            }
        }

        synchronized void processingStarted() {
            if (finished || processingStarted) {
                return;
            }
            processingStarted = true;
            processingStartNs = clock.nanoseconds();
            for (Metrics target : allMetrics) {
                update(target.queueTime, processingStartNs - enqueuedTimeNs);
            }
        }

        synchronized void processingCompleted(
                @Nullable AbstractResponse response, boolean noResponse) {
            if (finished || processingCompleted) {
                return;
            }
            processingCompleted = true;
            processingEndNs = clock.nanoseconds();
            if (processingStarted) {
                for (Metrics target : allMetrics) {
                    update(target.processTime, processingEndNs - processingStartNs);
                }
            }
            if (response == null) {
                markAllFailed();
            } else {
                for (Map.Entry<Errors, Integer> error : response.errorCounts().entrySet()) {
                    if (error.getKey() != Errors.NONE && error.getValue() > 0) {
                        markFailed(metrics);
                        break;
                    }
                }
                if (response instanceof ProduceResponse) {
                    markFailedTables((ProduceResponse) response);
                }
            }
            if (noResponse) {
                finish(false);
            }
        }

        synchronized void responseDequeued() {
            if (finished || !processingCompleted || responseDequeued) {
                return;
            }
            responseDequeued = true;
            long durationNs = clock.nanoseconds() - processingEndNs;
            for (Metrics target : allMetrics) {
                update(target.responseQueueTime, durationNs);
            }
        }

        synchronized void finish(boolean failure) {
            if (finished) {
                return;
            }
            finished = true;
            if (failure) {
                markAllFailed();
            }
            long durationNs = clock.nanoseconds() - receivedTimeNs;
            for (Metrics target : allMetrics) {
                target.inFlight.dec();
                update(target.totalTime, durationNs);
            }
        }

        synchronized void responseFailed() {
            if (!finished) {
                markAllFailed();
            }
        }

        private void markFailedTables(ProduceResponse response) {
            for (TopicProduceResponse topic : response.data().responses()) {
                Metrics target = tableMetrics.get(topic.name());
                if (target == null) {
                    continue;
                }
                for (PartitionProduceResponse partition : topic.partitionResponses()) {
                    if (partition.errorCode() != Errors.NONE.code()) {
                        markFailed(target);
                        break;
                    }
                }
            }
        }

        private void markAllFailed() {
            for (Metrics target : allMetrics) {
                markFailed(target);
            }
        }

        private void markFailed(Metrics target) {
            if (failedMetrics.add(target)) {
                target.failures.inc();
            }
        }

        private static void update(Histogram histogram, long durationNs) {
            histogram.update(TimeUnit.NANOSECONDS.toMillis(durationNs));
        }
    }
}
