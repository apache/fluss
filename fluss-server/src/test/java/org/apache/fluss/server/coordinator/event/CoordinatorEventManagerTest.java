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
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.ZkEpoch;

import org.junit.jupiter.api.Test;

import java.time.Duration;
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
        context.addLiveCoordinator("coordinator-0");
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

        CoordinatorEventManager manager =
                new CoordinatorEventManager(testProcessor, TestingMetricGroups.COORDINATOR_METRICS);
        manager.start();

        try {
            // The first doWork() should trigger metrics update immediately
            // (lastMetricsUpdateTime=0) without waiting for any events.
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(metricsUpdateCount.get()).isGreaterThan(0));

            Gauge activeTabletServerCount =
                    (Gauge)
                            TestingMetricGroups.COORDINATOR_METRICS
                                    .getMetrics()
                                    .get(MetricNames.ACTIVE_TABLET_SERVER_COUNT);
            assertThat(activeTabletServerCount.getValue()).isEqualTo(2);
        } finally {
            manager.close();
            TestingMetricGroups.COORDINATOR_METRICS.close();
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
}
