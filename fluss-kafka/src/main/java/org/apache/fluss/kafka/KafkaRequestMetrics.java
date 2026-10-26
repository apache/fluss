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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Request metrics shared by all Kafka connections on a listener. */
final class KafkaRequestMetrics {

    static final String FAILED_REQUESTS_PER_SECOND = "failedRequestsPerSecond";
    static final String RESPONSE_QUEUE_TIME_MS = "responseQueueTimeMs";
    static final String IN_FLIGHT_REQUESTS = "inFlightRequests";

    private final MetricGroup metricGroup;
    private final Clock clock;
    private final Function<String, MetricGroup> tableMetricGroupLookup;
    // Table groups own registration; weak keys avoid retaining removed tables.
    private final Map<MetricGroup, Metrics> metricsByGroup =
            Collections.synchronizedMap(new WeakHashMap<>());

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
                            topic.name(), metricsByGroup.computeIfAbsent(tableGroup, Metrics::new));
                }
            }
        }
        Metrics apiMetrics =
                metricsByGroup.computeIfAbsent(
                        metricGroup.addGroup("request", request.apiKey().name()), Metrics::new);
        return new RequestMetrics(apiMetrics, tableMetrics, clock, receivedTimeNs);
    }

    private enum Stage {
        QUEUED,
        PROCESSING,
        COMPLETED,
        SENDING,
        FINISHED
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
        private final Metrics apiMetrics;
        private final Map<String, Metrics> tableMetrics;
        private final Set<Metrics> allMetrics;
        private final Set<Metrics> failedMetrics = new HashSet<>();
        private final Clock clock;
        private final long receivedTimeNs;
        private long stageStartNs;
        private Stage stage = Stage.QUEUED;

        private RequestMetrics(
                Metrics apiMetrics,
                Map<String, Metrics> tableMetrics,
                Clock clock,
                long receivedTimeNs) {
            this.apiMetrics = apiMetrics;
            this.tableMetrics = tableMetrics;
            this.allMetrics = new LinkedHashSet<>(tableMetrics.values());
            this.allMetrics.add(apiMetrics);
            this.clock = clock;
            this.receivedTimeNs = receivedTimeNs;
            this.stageStartNs = clock.nanoseconds();
            for (Metrics target : allMetrics) {
                target.requests.inc();
                target.inFlight.inc();
            }
        }

        synchronized void processingStarted() {
            if (stage != Stage.QUEUED) {
                return;
            }
            long now = clock.nanoseconds();
            for (Metrics target : allMetrics) {
                update(target.queueTime, now - stageStartNs);
            }
            stageStartNs = now;
            stage = Stage.PROCESSING;
        }

        synchronized void processingCompleted(
                @Nullable AbstractResponse response, boolean noResponse) {
            if (stage != Stage.QUEUED && stage != Stage.PROCESSING) {
                return;
            }
            long now = clock.nanoseconds();
            if (stage == Stage.PROCESSING) {
                for (Metrics target : allMetrics) {
                    update(target.processTime, now - stageStartNs);
                }
            }
            stageStartNs = now;
            stage = Stage.COMPLETED;
            if (response == null) {
                markFailed();
            } else {
                for (Map.Entry<Errors, Integer> error : response.errorCounts().entrySet()) {
                    if (error.getKey() != Errors.NONE && error.getValue() > 0) {
                        markFailed(apiMetrics);
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
            if (stage != Stage.COMPLETED) {
                return;
            }
            long durationNs = clock.nanoseconds() - stageStartNs;
            for (Metrics target : allMetrics) {
                update(target.responseQueueTime, durationNs);
            }
            stage = Stage.SENDING;
        }

        synchronized void finish(boolean failure) {
            if (stage == Stage.FINISHED) {
                return;
            }
            if (failure) {
                markFailed();
            }
            stage = Stage.FINISHED;
            long durationNs = clock.nanoseconds() - receivedTimeNs;
            for (Metrics target : allMetrics) {
                target.inFlight.dec();
                update(target.totalTime, durationNs);
            }
        }

        synchronized void markFailed() {
            if (stage != Stage.FINISHED) {
                for (Metrics target : allMetrics) {
                    markFailed(target);
                }
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
