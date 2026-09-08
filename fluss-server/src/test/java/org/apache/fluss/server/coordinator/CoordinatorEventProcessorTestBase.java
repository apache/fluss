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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdsZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Executors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;

/**
 * The shared lifecycle harness for {@link CoordinatorEventProcessor} unit tests: a ZooKeeper test
 * cluster, a coordinator event processor rebuilt per test, and the cleanup between tests.
 *
 * <p>Extracted from {@code CoordinatorEventProcessorTest} to deduplicate the harness and keep the
 * test class within the checkstyle file-length limit.
 */
class CoordinatorEventProcessorTestBase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;
    protected static MetadataManager metadataManager;
    protected static ZkEpoch zkEpoch;

    protected CoordinatorEventProcessor eventProcessor;
    protected final String defaultDatabase = "db";
    protected TestCoordinatorChannelManager testCoordinatorChannelManager;
    protected AutoPartitionManager autoPartitionManager;
    protected LakeTableTieringManager lakeTableTieringManager;
    protected CompletedSnapshotStoreManager completedSnapshotStoreManager;
    protected CoordinatorMetadataCache serverMetadataCache;
    protected ReplicaCapacityController replicaCapacityController;
    protected KvSnapshotLeaseManager kvSnapshotLeaseManager;
    protected Scheduler scheduler;
    protected String remoteDataDir;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        // register coordinator server
        zookeeperClient.registerCoordinatorLeader(
                new CoordinatorAddress(
                        "2", Endpoint.fromListenersString("CLIENT://localhost:10012")));

        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("2");
        // register 3 tablet servers
        for (int i = 0; i < 3; i++) {
            zookeeperClient.registerTabletServer(
                    i,
                    new TabletServerRegistration(
                            "rack" + i,
                            Collections.singletonList(
                                    new Endpoint("host" + i, 1000, DEFAULT_LISTENER_NAME)),
                            System.currentTimeMillis()));
        }
    }

    @BeforeEach
    void beforeEach() {
        serverMetadataCache = new CoordinatorMetadataCache();
        // set a test channel manager for the context
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        lakeTableTieringManager =
                new LakeTableTieringManager(TestingMetricGroups.LAKE_TIERING_METRICS);
        remoteDataDir = zookeeperClient.getDefaultRemoteDataDir();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        replicaCapacityController = new ReplicaCapacityController(conf, serverMetadataCache);
        autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        metadataManager,
                        new RemoteDirDynamicLoader(conf),
                        conf,
                        replicaCapacityController);
        kvSnapshotLeaseManager =
                new KvSnapshotLeaseManager(
                        Duration.ofMinutes(10).toMillis(),
                        zookeeperClient,
                        remoteDataDir,
                        SystemClock.getInstance(),
                        TestingMetricGroups.COORDINATOR_METRICS);
        kvSnapshotLeaseManager.start();

        scheduler = new FlussScheduler(1);
        scheduler.startup();

        eventProcessor = buildCoordinatorEventProcessor();
        eventProcessor.startup();
        metadataManager.createDatabase(
                defaultDatabase, DatabaseDescriptor.builder().build(), false);
        completedSnapshotStoreManager = eventProcessor.completedSnapshotStoreManager();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (eventProcessor != null) {
            eventProcessor.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        metadataManager.dropDatabase(defaultDatabase, false, true);
        // clear the assignment info for all tables;
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(TableIdsZNode.path());
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupPath(PartitionIdsZNode.path());
    }

    protected CoordinatorEventProcessor buildCoordinatorEventProcessor() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        conf.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_RETRY_DELAY, Duration.ofDays(1));
        return new CoordinatorEventProcessor(
                zookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(zkEpoch),
                replicaCapacityController,
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                conf,
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")),
                metadataManager,
                kvSnapshotLeaseManager,
                scheduler,
                SystemClock.getInstance());
    }
}
