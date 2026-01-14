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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.AutoPartitionManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.LakeTableTieringManager;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.concurrent.Executors;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RebalanceManager}. */
public class RebalanceManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;

    private CoordinatorMetadataCache serverMetadataCache;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private AutoPartitionManager autoPartitionManager;
    private LakeTableTieringManager lakeTableTieringManager;
    private RebalanceManager rebalanceManager;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() {
        serverMetadataCache = new CoordinatorMetadataCache();
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        autoPartitionManager =
                new AutoPartitionManager(serverMetadataCache, metadataManager, new Configuration());
        lakeTableTieringManager = new LakeTableTieringManager();
        CoordinatorEventProcessor eventProcessor = buildCoordinatorEventProcessor();
        rebalanceManager = new RebalanceManager(eventProcessor, zookeeperClient);
        rebalanceManager.startup();
    }

    @AfterEach
    void afterEach() throws Exception {
        zookeeperClient.deleteRebalanceTask();
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
    }

    @Test
    void testRebalanceWithoutTask() throws Exception {
        assertThat(rebalanceManager.getRebalanceId()).isNull();
        assertThat(rebalanceManager.getRebalanceStatus()).isNull();

        String rebalanceId = "test-rebalance-id";
        RebalanceTask rebalanceTask = new RebalanceTask(rebalanceId, NOT_STARTED, new HashMap<>());
        zookeeperClient.registerRebalanceTask(rebalanceTask);
        assertThat(zookeeperClient.getRebalanceTask()).hasValue(rebalanceTask);

        // register a rebalance task with empty plan.
        rebalanceManager.registerRebalance(rebalanceId, new HashMap<>(), NOT_STARTED);

        assertThat(rebalanceManager.getRebalanceId()).isEqualTo(rebalanceId);
        RebalanceStatus status = rebalanceManager.getRebalanceStatus();
        assertThat(status).isNotNull();
        assertThat(status).isEqualTo(COMPLETED);
        assertThat(zookeeperClient.getRebalanceTask())
                .hasValue(new RebalanceTask(rebalanceId, COMPLETED, new HashMap<>()));
    }

    private CoordinatorEventProcessor buildCoordinatorEventProcessor() {
        return new CoordinatorEventProcessor(
                zookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(),
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                new Configuration(),
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")),
                metadataManager);
    }
}
