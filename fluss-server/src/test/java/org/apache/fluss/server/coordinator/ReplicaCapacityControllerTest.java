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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.InsufficientKvLeaderReplicaCapacityException;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ReplicaCapacityController}. */
class ReplicaCapacityControllerTest {

    @Test
    void testCapacityUsesAverageMemoryForUnknownTabletServers() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(
                        Arrays.asList(
                                tabletServer(0, 100L),
                                tabletServer(1, 100L),
                                unknownMemoryTabletServer(2),
                                tabletServer(3, 10000L))),
                Collections.singletonMap(3, ServerTag.PERMANENT_OFFLINE));

        ReplicaCapacityController controller =
                new ReplicaCapacityController(configWithMemoryReserved(10), metadataCache);

        assertThat(controller.getKvLeaderReplicaCapacity()).isEqualTo(30);
    }

    @Test
    void testRegistersKvLeaderReplicaMetrics() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(Arrays.asList(tabletServer(0, 100L), tabletServer(1, 100L))),
                Collections.emptyMap());
        CoordinatorMetricGroup metricGroup =
                new CoordinatorMetricGroup(NOPMetricRegistry.INSTANCE, "cluster", "localhost", "0");

        try {
            ReplicaCapacityController controller =
                    new ReplicaCapacityController(
                            configWithMemoryReserved(10), metadataCache, metricGroup);
            controller.updateObservedKvLeaderReplicaCount(3);

            Gauge<?> countGauge =
                    (Gauge<?>) metricGroup.getMetrics().get(MetricNames.KV_LEADER_REPLICA_COUNT);
            Gauge<?> capacityGauge =
                    (Gauge<?>) metricGroup.getMetrics().get(MetricNames.KV_LEADER_REPLICA_CAPACITY);
            assertThat(countGauge.getValue()).isEqualTo(3L);
            assertThat(capacityGauge.getValue()).isEqualTo(20L);
        } finally {
            metricGroup.close();
        }
    }

    @Test
    void testCapacityDisabledIfKnownMemoryTabletServersAreNotMajority() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(
                        Arrays.asList(
                                tabletServer(0, 100L),
                                unknownMemoryTabletServer(1),
                                unknownMemoryTabletServer(2))),
                Collections.emptyMap());

        ReplicaCapacityController controller =
                new ReplicaCapacityController(configWithMemoryReserved(10), metadataCache);

        assertThat(controller.getKvLeaderReplicaCapacity())
                .isEqualTo(ReplicaCapacityController.CAPACITY_LIMIT_DISABLED);
    }

    @Test
    void testCapacityControlDisabledByDefault() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(Arrays.asList(tabletServer(0, 100L), tabletServer(1, 100L))),
                Collections.emptyMap());

        ReplicaCapacityController controller =
                new ReplicaCapacityController(new Configuration(), metadataCache);

        assertThat(controller.getKvLeaderReplicaMemoryReservedBytes()).isZero();
        assertThat(controller.getKvLeaderReplicaCapacity())
                .isEqualTo(ReplicaCapacityController.CAPACITY_LIMIT_DISABLED);
        controller.checkCanCreateKvLeaderReplicas(201);
        assertThat(controller.getKvLeaderReplicaCount()).isZero();
    }

    @Test
    void testAdmissionCheckUsesObservedCountWithoutReservation() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(
                        Arrays.asList(
                                tabletServer(0, 100L),
                                tabletServer(1, 100L),
                                unknownMemoryTabletServer(2))),
                Collections.emptyMap());

        ReplicaCapacityController controller =
                new ReplicaCapacityController(configWithMemoryReserved(10), metadataCache);
        controller.updateObservedKvLeaderReplicaCount(20);
        controller.checkCanCreateKvLeaderReplicas(10);

        assertThat(controller.getKvLeaderReplicaCount()).isEqualTo(20);
        assertThatThrownBy(() -> controller.checkCanCreateKvLeaderReplicas(11))
                .isInstanceOf(InsufficientKvLeaderReplicaCapacityException.class)
                .hasMessageContaining("observedKvLeaderReplicaCount=20")
                .hasMessageContaining("requestedKvLeaderReplicaCount=11")
                .hasMessageContaining("kvLeaderReplicaCapacity=30");
        assertThat(controller.getKvLeaderReplicaCount()).isEqualTo(20);
    }

    @Test
    void testAdmissionUsesLatestMemoryReservedConfig() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(Arrays.asList(tabletServer(0, 100L), tabletServer(1, 100L))),
                Collections.emptyMap());
        ReplicaCapacityController controller =
                new ReplicaCapacityController(configWithMemoryReserved(10), metadataCache);
        controller.updateObservedKvLeaderReplicaCount(15);

        controller.checkCanCreateKvLeaderReplicas(5);
        controller.reconfigure(configWithMemoryReserved(20));

        assertThatThrownBy(() -> controller.checkCanCreateKvLeaderReplicas(1))
                .isInstanceOf(InsufficientKvLeaderReplicaCapacityException.class)
                .hasMessageContaining("kvLeaderReplicaCapacity=10");
    }

    @Test
    void testReconfigureMemoryReserved() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(Arrays.asList(tabletServer(0, 100L), tabletServer(1, 100L))),
                Collections.emptyMap());
        ReplicaCapacityController controller =
                new ReplicaCapacityController(configWithMemoryReserved(10), metadataCache);

        controller.reconfigure(configWithMemoryReserved(20));

        assertThat(controller.getKvLeaderReplicaMemoryReservedBytes()).isEqualTo(20);
        assertThat(controller.getKvLeaderReplicaCapacity()).isEqualTo(10);

        Configuration disabledConfig = configWithMemoryReserved(0);
        controller.validate(disabledConfig);
        controller.reconfigure(disabledConfig);
        assertThat(controller.getKvLeaderReplicaMemoryReservedBytes()).isZero();
        assertThat(controller.getKvLeaderReplicaCapacity())
                .isEqualTo(ReplicaCapacityController.CAPACITY_LIMIT_DISABLED);

        controller.reconfigure(configWithMemoryReserved(10));
        assertThat(controller.getKvLeaderReplicaCapacity()).isEqualTo(20);
    }

    private static Configuration configWithMemoryReserved(long bytes) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED, new MemorySize(bytes));
        return conf;
    }

    private static ServerInfo tabletServer(int serverId, long memoryBytes) {
        return new ServerInfo(
                serverId,
                null,
                Endpoint.fromListenersString("INTERNAL://localhost:" + (10000 + serverId)),
                ServerType.TABLET_SERVER,
                new TabletServerResource(null, memoryBytes));
    }

    private static ServerInfo unknownMemoryTabletServer(int serverId) {
        return new ServerInfo(
                serverId,
                null,
                Endpoint.fromListenersString("INTERNAL://localhost:" + (10000 + serverId)),
                ServerType.TABLET_SERVER,
                TabletServerResource.unknown());
    }
}
