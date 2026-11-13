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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.server.coordinator.TestingControlledNotifyGateway.PendingNotify;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.entity.AdjustIsrResultForBucket;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdsZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrResponseData;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

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
        return buildCoordinatorEventProcessor(SystemClock.getInstance());
    }

    protected CoordinatorEventProcessor buildCoordinatorEventProcessor(Clock clock) {
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
                clock);
    }

    /** Verifies the elected leader and ISR in memory and ZooKeeper. */
    protected void verifyIsr(TableBucket tb, int expectedLeader, List<Integer> expectedIsr)
            throws Exception {
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> fromCtx((ctx) -> ctx.getBucketLeaderAndIsr(tb)),
                        Duration.ofMinutes(1),
                        "leader not elected");
        LeaderAndIsr newLeaderAndIsrOfZk = zookeeperClient.getLeaderAndIsr(tb).get();
        assertThat(leaderAndIsr.leader())
                .isEqualTo(newLeaderAndIsrOfZk.leader())
                .isEqualTo(expectedLeader);
        assertThat(leaderAndIsr.isr())
                .isEqualTo(newLeaderAndIsrOfZk.isr())
                .hasSameElementsAs(expectedIsr);
    }

    /** Installs successful tablet server gateways for registered servers. */
    protected void initCoordinatorChannel() throws Exception {
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                Arrays.stream(zookeeperClient.getSortedTabletServerList())
                        .boxed()
                        .collect(Collectors.toSet()),
                Collections.emptySet());
    }

    /** Registers an additional tablet server in ZooKeeper. */
    protected void registerTabletServer(int serverId) throws Exception {
        zookeeperClient.registerTabletServer(
                serverId,
                new TabletServerRegistration(
                        "rack" + serverId,
                        Collections.singletonList(
                                new Endpoint("host" + serverId, 1001, DEFAULT_LISTENER_NAME)),
                        System.currentTimeMillis()));
    }

    /** Retries assertions against state read on the coordinator event thread. */
    protected void retryVerifyContext(Consumer<CoordinatorContext> verifyFunction) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    AccessContextEvent<Void> event =
                            new AccessContextEvent<>(
                                    ctx -> {
                                        verifyFunction.accept(ctx);
                                        return null;
                                    });
                    eventProcessor.getCoordinatorEventManager().put(event);
                    try {
                        event.getResultFuture().get(30, TimeUnit.SECONDS);
                    } catch (Throwable t) {
                        throw ExceptionUtils.stripExecutionException(t);
                    }
                });
    }

    /** Reads or updates state on the coordinator event thread. */
    protected <T> T fromCtx(Function<CoordinatorContext, T> retrieveFunction) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(retrieveFunction);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    /** Submits an ISR update through the coordinator event loop. */
    protected AdjustIsrResultForBucket submitAdjustIsr(
            TableBucket tableBucket, LeaderAndIsr newLeaderAndIsr) throws Exception {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new AdjustIsrReceivedEvent(
                                Collections.singletonMap(tableBucket, newLeaderAndIsr), response));
        return getAdjustIsrResponseData(response.get()).get(tableBucket);
    }

    /** Installs gateways that hold leader notifications until explicitly released. */
    protected void installBlockingNotifyGateways(
            ConcurrentLinkedDeque<PendingNotify> pendingTriggers) throws Exception {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int server : zookeeperClient.getSortedTabletServerList()) {
            TestingControlledNotifyGateway gateway =
                    new TestingControlledNotifyGateway(server, pendingTriggers);
            gateways.put(server, gateway);
        }
        testCoordinatorChannelManager.setGateways(gateways);
    }

    /** Releases queued notification responses. */
    protected static void drainPendingNotifyTriggers(
            ConcurrentLinkedDeque<PendingNotify> pendingTriggers) {
        PendingNotify trigger;
        while ((trigger = pendingTriggers.poll()) != null) {
            trigger.complete();
        }
    }

    /** Returns whether a server has a held notification response. */
    protected static boolean hasPendingNotifyTrigger(
            ConcurrentLinkedDeque<PendingNotify> pendingTriggers, int responseServerId) {
        for (PendingNotify trigger : pendingTriggers) {
            if (trigger.getResponseServerId() == responseServerId) {
                return true;
            }
        }
        return false;
    }

    /** Releases one held notification response from a server. */
    protected static void completePendingNotifyTrigger(
            ConcurrentLinkedDeque<PendingNotify> pendingTriggers, int responseServerId) {
        for (PendingNotify trigger : pendingTriggers) {
            if (trigger.getResponseServerId() == responseServerId) {
                assertThat(pendingTriggers.remove(trigger)).isTrue();
                trigger.complete();
                return;
            }
        }
        throw new AssertionError(
                "No pending NotifyLeaderAndIsr response for server " + responseServerId);
    }
}
