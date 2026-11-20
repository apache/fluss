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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.CoordinatorAddress;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorEventManager}. */
class CoordinatorEventManagerTest {

    /**
     * Verifies that coordinator metrics are updated immediately after startup, even when no events
     * arrive. This tests the fix for the issue where metrics like
     * fluss_coordinator_activeTabletServerCount remained 0 after coordinator restart if no client
     * requests came in.
     *
     * <p>The fix has two parts:
     *
     * <ul>
     *   <li>lastMetricsUpdateTime initialized to 0 (triggers immediate first update)
     *   <li>queue.poll(timeout) instead of queue.take() (thread wakes up periodically)
     * </ul>
     */
    @Test
    void testMetricsUpdatedImmediatelyOnStartup() {
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        context.addLiveCoordinator(
                new CoordinatorAddress("coordinator-0", Collections.emptyList()));
        context.addLiveTabletServer(
                new ServerInfo(
                        0,
                        "rack0",
                        Endpoint.fromListenersString("CLIENT://host0:1000"),
                        ServerType.TABLET_SERVER));
        context.addLiveTabletServer(
                new ServerInfo(
                        1,
                        "rack1",
                        Endpoint.fromListenersString("CLIENT://host1:1000"),
                        ServerType.TABLET_SERVER));

        AtomicInteger metricsUpdateCount = new AtomicInteger(0);
        EventProcessor testProcessor =
                event -> {
                    if (event instanceof AccessContextEvent) {
                        processAccessContext((AccessContextEvent<?>) event, context);
                        metricsUpdateCount.incrementAndGet();
                    }
                };

        TestingMetricRegistry registry = new TestingMetricRegistry();
        CoordinatorMetricGroup metricGroup =
                new CoordinatorMetricGroup(registry, "cluster1", "host", "0");

        CoordinatorEventManager manager = new CoordinatorEventManager(testProcessor, metricGroup);
        manager.start();

        try {
            // The first doWork() should trigger metrics update immediately
            // (lastMetricsUpdateTime=0) without waiting for any events.
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(metricsUpdateCount.get()).isGreaterThan(0));

            Gauge<?> activeTabletServerCount =
                    (Gauge<?>) registry.metrics.get(MetricNames.ACTIVE_TABLET_SERVER_COUNT);
            // The gauge reads the volatile tabletServerCount field which is updated
            // after the AccessContextEvent future completes. Use retry to handle
            // the brief window between metricsUpdateCount increment (inside the
            // event processor) and the volatile field assignment (after
            // getResultFuture().get() in updateMetricsViaAccessContext).
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(activeTabletServerCount.getValue()).isEqualTo(2));
        } finally {
            manager.close();
            metricGroup.close();
        }
    }

    @Test
    void testMetricsUpdatedAfterLeadershipChanges() throws Exception {
        TestingMetricRegistry registry = new TestingMetricRegistry();
        CoordinatorMetricGroup metricGroup =
                new CoordinatorMetricGroup(registry, "cluster1", "host", "0");
        Map<String, Metric> previousMetrics = new HashMap<>();

        try {
            for (int term = 0; term < 3; term++) {
                int tabletServerCount = 9 + term * 3;
                CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
                for (int id = 0; id < tabletServerCount; id++) {
                    context.addLiveTabletServer(
                            new ServerInfo(
                                    id,
                                    "rack0",
                                    Endpoint.fromListenersString("CLIENT://host" + id + ":1000"),
                                    ServerType.TABLET_SERVER));
                }
                CoordinatorEventManager manager =
                        new CoordinatorEventManager(
                                event ->
                                        processAccessContext(
                                                (AccessContextEvent<?>) event, context),
                                metricGroup);
                manager.start();
                try {
                    registry.metrics.forEach(
                            (name, metric) -> {
                                if (metric instanceof Gauge
                                        && !name.equals(MetricNames.ACTIVE_COORDINATOR_COUNT)) {
                                    assertThat(metric).isNotSameAs(previousMetrics.get(name));
                                }
                            });
                    Gauge<?> activeTabletServerCount =
                            (Gauge<?>) registry.metrics.get(MetricNames.ACTIVE_TABLET_SERVER_COUNT);
                    retry(
                            Duration.ofSeconds(10),
                            () ->
                                    assertThat(activeTabletServerCount.getValue())
                                            .isEqualTo(tabletServerCount));
                    Histogram eventQueueTime =
                            (Histogram) registry.metrics.get(MetricNames.EVENT_QUEUE_TIME_MS);
                    assertThat(eventQueueTime)
                            .isNotSameAs(previousMetrics.get(MetricNames.EVENT_QUEUE_TIME_MS));
                    assertThat(eventQueueTime.getCount()).isZero();
                    AccessContextEvent<Void> event = new AccessContextEvent<>(ignored -> null);
                    manager.put(event);
                    event.getResultFuture().get(10, TimeUnit.SECONDS);
                    assertThat(eventQueueTime.getCount()).isEqualTo(1);
                    previousMetrics.clear();
                    previousMetrics.putAll(registry.metrics);
                } finally {
                    manager.close();
                }
                assertThat(registry.metrics).isEmpty();
                assertThat(registry.groups).isEmpty();
            }
        } finally {
            metricGroup.close();
        }
    }

    @Test
    void testCloseUnregistersMetrics() {
        TestingMetricRegistry registry = new TestingMetricRegistry();
        CoordinatorMetricGroup metricGroup =
                new CoordinatorMetricGroup(registry, "cluster1", "host", "0");
        Gauge<Integer> serverMetric = metricGroup.gauge("serverMetric", () -> 42);
        CoordinatorEventManager manager = new CoordinatorEventManager(event -> {}, metricGroup);

        try {
            assertThat(registry.metrics)
                    .containsOnlyKeys(
                            "serverMetric",
                            MetricNames.EVENT_QUEUE_TIME_MS,
                            MetricNames.ACTIVE_COORDINATOR_COUNT,
                            MetricNames.ALIVE_COORDINATOR_COUNT,
                            MetricNames.ACTIVE_TABLET_SERVER_COUNT,
                            MetricNames.OFFLINE_BUCKET_COUNT,
                            MetricNames.BUCKET_COUNT,
                            MetricNames.TABLE_COUNT,
                            MetricNames.LAKE_TABLE_COUNT,
                            MetricNames.PARTITION_COUNT,
                            MetricNames.REPLICAS_TO_DELETE_COUNT,
                            MetricNames.PENDING_LEADER_ACTIVATION_COUNT);
            registry.groups
                    .values()
                    .forEach(
                            group -> {
                                assertThat(group.getScopeComponents())
                                        .containsExactly(metricGroup.getScopeComponents());
                                assertThat(group.getAllVariables())
                                        .isEqualTo(metricGroup.getAllVariables());
                            });
            manager.close();
            manager.close();
            assertThat(registry.metrics).containsOnlyKeys("serverMetric");
            assertThat(registry.metrics.get("serverMetric")).isSameAs(serverMetric);
            assertThat(metricGroup.isClosed()).isFalse();
        } finally {
            manager.close();
            metricGroup.close();
        }
    }

    private static <T> void processAccessContext(
            AccessContextEvent<T> event, CoordinatorContext context) {
        try {
            T result = event.getAccessFunction().apply(context);
            event.getResultFuture().complete(result);
        } catch (Throwable t) {
            event.getResultFuture().completeExceptionally(t);
        }
    }

    private static class TestingMetricRegistry extends NOPMetricRegistry {
        private final Map<String, Metric> metrics = new ConcurrentHashMap<>();
        private final Map<String, AbstractMetricGroup> groups = new ConcurrentHashMap<>();

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            if ("coordinator".equals(group.getLogicalScope(input -> input, '.'))) {
                metrics.put(metricName, metric);
                groups.put(metricName, group);
            }
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
            if ("coordinator".equals(group.getLogicalScope(input -> input, '.'))) {
                metrics.remove(metricName, metric);
                groups.remove(metricName, group);
            }
        }
    }
}
