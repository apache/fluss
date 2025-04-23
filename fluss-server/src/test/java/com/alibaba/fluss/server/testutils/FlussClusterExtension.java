/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.fs.local.LocalFileSystem;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.utils.ServerRpcMessageUtils;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperTestUtils;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.RemoteLogManifestHandle;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.utils.FileUtils;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeStopBucketReplica;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toServerNode;
import static com.alibaba.fluss.server.zk.ZooKeeperTestUtils.createZooKeeperClient;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static com.alibaba.fluss.utils.function.FunctionUtils.uncheckedFunction;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Junit {@link Extension} which starts a Fluss Cluster.
 *
 * <p>Note: after each test, it'll always drop all the databases and tables.
 */
public final class FlussClusterExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    public static final String BUILTIN_DATABASE = "fluss";

    private final int initialNumOfTabletServers;
    private final String tabletServerListeners;
    private final String coordinatorServerListeners;

    private CoordinatorServer coordinatorServer;
    private ServerInfo coordinatorServerInfo;
    private TestingServer zooKeeperServer;
    private ZooKeeperClient zooKeeperClient;
    private RpcClient rpcClient;
    private MetadataManager metadataManager;

    private File tempDir;

    private final Map<Integer, TabletServer> tabletServers;
    private final Map<Integer, ServerInfo> tabletServerInfos;
    private final Configuration clusterConf;

    /** Creates a new {@link Builder} for {@link FlussClusterExtension}. */
    public static Builder builder() {
        return new Builder();
    }

    private FlussClusterExtension(
            int numOfTabletServers,
            String coordinatorServerListeners,
            String tabletServerListeners,
            Configuration clusterConf) {
        this.initialNumOfTabletServers = numOfTabletServers;
        this.tabletServers = new HashMap<>(numOfTabletServers);
        this.coordinatorServerListeners = coordinatorServerListeners;
        this.tabletServerListeners = tabletServerListeners;
        this.tabletServerInfos = new HashMap<>();
        this.clusterConf = clusterConf;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        start();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        close();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        // currently, do nothing
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        String defaultDb = BUILTIN_DATABASE;
        // TODO: we need to cleanup all zk nodes, including the assignments,
        //  but currently, we don't have a good way to do it
        if (metadataManager != null) {
            // drop all database and tables.
            List<String> databases = metadataManager.listDatabases();
            for (String database : databases) {
                if (!database.equals(defaultDb)) {
                    metadataManager.dropDatabase(database, true, true);
                    // delete the data dirs
                    for (int serverId : tabletServers.keySet()) {
                        String dataDir = getDataDir(serverId);
                        FileUtils.deleteDirectoryQuietly(Paths.get(dataDir, database).toFile());
                    }
                }
            }
            List<String> tables = metadataManager.listTables(defaultDb);
            for (String table : tables) {
                metadataManager.dropTable(TablePath.of(defaultDb, table), true);
            }
        }
    }

    public void start() throws Exception {
        tempDir = Files.createTempDirectory("fluss-testing-cluster").toFile();
        zooKeeperServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
        zooKeeperClient =
                createZooKeeperClient(zooKeeperServer.getConnectString(), NOPErrorHandler.INSTANCE);
        metadataManager = new MetadataManager(zooKeeperClient, clusterConf);
        Configuration conf = new Configuration();
        rpcClient =
                RpcClient.create(
                        conf,
                        new ClientMetricGroup(
                                MetricRegistry.create(conf, null), "fluss-cluster-extension"));
        startCoordinatorServer();
        startTabletServers();
        // wait coordinator knows all tablet servers to make cluster
        // have enough replication factor when creating table.
        waitUtilAllGatewayHasSameMetadata();
    }

    public void close() throws Exception {
        if (rpcClient != null) {
            rpcClient.close();
            rpcClient = null;
        }
        if (tempDir != null) {
            tempDir.delete();
            tempDir = null;
        }
        for (TabletServer tabletServer : tabletServers.values()) {
            tabletServer.close();
        }
        tabletServers.clear();
        tabletServerInfos.clear();
        if (coordinatorServer != null) {
            coordinatorServer.close();
            coordinatorServer = null;
        }
        if (zooKeeperClient != null) {
            zooKeeperClient.close();
            zooKeeperClient = null;
        }
        if (zooKeeperServer != null) {
            zooKeeperServer.close();
            zooKeeperServer = null;
        }
    }

    /** Start a coordinator server. start a new one if no coordinator server exists. */
    public void startCoordinatorServer() throws Exception {
        if (coordinatorServer == null) {
            // if no coordinator server exists, create a new coordinator server and start
            Configuration conf = new Configuration(clusterConf);
            conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS, zooKeeperServer.getConnectString());
            conf.setString(ConfigOptions.BIND_LISTENERS, coordinatorServerListeners);
            setRemoteDataDir(conf);
            coordinatorServer = new CoordinatorServer(conf);
            coordinatorServer.start();
            coordinatorServerInfo =
                    // TODO, Currently, we use 0 as coordinator server id.
                    new ServerInfo(
                            0,
                            null,
                            coordinatorServer.getRpcServer().getBindEndpoints(),
                            ServerType.COORDINATOR);
        } else {
            // start the existing coordinator server
            coordinatorServer.start();
            coordinatorServerInfo =
                    new ServerInfo(
                            0,
                            null,
                            coordinatorServer.getRpcServer().getBindEndpoints(),
                            ServerType.COORDINATOR);
        }
    }

    public void stopCoordinatorServer() throws Exception {
        coordinatorServer.close();
    }

    private void startTabletServers() throws Exception {
        // add tablet server to make generate assignment for table possible
        for (int i = 0; i < initialNumOfTabletServers; i++) {
            startTabletServer(i);
        }
    }

    /** Start a new tablet server. */
    public void startTabletServer(int serverId) throws Exception {
        startTabletServer(serverId, false);
    }

    public void startTabletServer(int serverId, boolean forceStartIfExists) throws Exception {
        if (tabletServers.containsKey(serverId)) {
            if (!forceStartIfExists) {
                throw new IllegalArgumentException(
                        "Tablet server " + serverId + " already exists.");
            }
        }
        startTabletServer(serverId, null);
    }

    private void startTabletServer(int serverId, @Nullable Configuration overwriteConfig)
            throws Exception {
        String dataDir = getDataDir(serverId);
        Configuration tabletServerConf = new Configuration(clusterConf);
        tabletServerConf.set(ConfigOptions.TABLET_SERVER_ID, serverId);
        tabletServerConf.set(ConfigOptions.TABLET_SERVER_RACK, "rack" + serverId);
        tabletServerConf.set(ConfigOptions.DATA_DIR, dataDir);
        tabletServerConf.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS, zooKeeperServer.getConnectString());
        tabletServerConf.setString(ConfigOptions.BIND_LISTENERS, tabletServerListeners);
        if (overwriteConfig != null) {
            tabletServerConf.addAll(overwriteConfig);
        }

        setRemoteDataDir(tabletServerConf);

        TabletServer tabletServer = new TabletServer(tabletServerConf);
        tabletServer.start();
        ServerInfo serverInfo =
                new ServerInfo(
                        serverId,
                        "rack" + serverId,
                        tabletServer.getRpcServer().getBindEndpoints(),
                        ServerType.TABLET_SERVER);

        tabletServers.put(serverId, tabletServer);
        tabletServerInfos.put(serverId, serverInfo);
    }

    public void restartTabletServer(int serverId, Configuration overwriteConfig) throws Exception {
        stopTabletServer(serverId);
        startTabletServer(serverId, overwriteConfig);
    }

    public void assertHasTabletServerNumber(int tabletServerNumber) {
        CoordinatorGateway coordinatorGateway = newCoordinatorClient();
        retry(
                Duration.ofMinutes(2),
                () ->
                        assertThat(
                                        coordinatorGateway
                                                .metadata(new MetadataRequest())
                                                .get()
                                                .getTabletServersCount())
                                .as("Tablet server number should be " + tabletServerNumber)
                                .isEqualTo(tabletServerNumber));
    }

    private String getDataDir(int serverId) {
        return tempDir.getAbsolutePath() + File.separator + "tablet-server-" + serverId;
    }

    private void setRemoteDataDir(Configuration conf) {
        conf.set(ConfigOptions.REMOTE_DATA_DIR, getRemoteDataDir());
    }

    public String getRemoteDataDir() {
        return LocalFileSystem.getLocalFsURI().getScheme()
                + "://"
                + tempDir.getAbsolutePath()
                + File.separator
                + "remote-data-dir";
    }

    /** Stop a tablet server. */
    public void stopTabletServer(int serverId) throws Exception {
        if (!tabletServers.containsKey(serverId)) {
            throw new IllegalArgumentException("Tablet server " + serverId + " does not exist.");
        }
        tabletServers.remove(serverId).close();
        tabletServerInfos.remove(serverId);
    }

    public Configuration getClientConfig() {
        return getClientConfig(null);
    }

    public String getBootstrapServers() {
        return String.join(",", getClientConfig().get(ConfigOptions.BOOTSTRAP_SERVERS));
    }

    public Configuration getClientConfig(@Nullable String listenerName) {
        Configuration flussConf = new Configuration();
        // now, just use the coordinator server as the bootstrap server
        flussConf.set(
                ConfigOptions.BOOTSTRAP_SERVERS,
                Collections.singletonList(
                        String.format(
                                "%s:%d",
                                getCoordinatorServerNode(listenerName).host(),
                                getCoordinatorServerNode(listenerName).port())));

        // set a small memory buffer for testing.
        flussConf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("2mb"));
        flussConf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("256kb"));
        flussConf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("1kb"));
        return flussConf;
    }

    public TabletServer getTabletServerById(int serverId) {
        return tabletServers.get(serverId);
    }

    public ServerInfo getCoordinatorServerInfo() {
        return coordinatorServerInfo;
    }

    public List<ServerInfo> getTabletServerInfos() {
        return new ArrayList<>(tabletServerInfos.values());
    }

    public ServerNode getCoordinatorServerNode() {
        return getCoordinatorServerNode(null);
    }

    public ServerNode getCoordinatorServerNode(@Nullable String listenerName) {
        Endpoint endpoint =
                listenerName != null
                        ? coordinatorServerInfo.endpoint(listenerName)
                        : coordinatorServerInfo.endpoints().get(0);
        return new ServerNode(
                coordinatorServerInfo.id(),
                endpoint.getHost(),
                endpoint.getPort(),
                ServerType.COORDINATOR);
    }

    public Set<TabletServer> getTabletServers() {
        return new HashSet<>(tabletServers.values());
    }

    public List<ServerNode> getTabletServerNodes() {
        return getTabletServerNodes(null);
    }

    public List<ServerNode> getTabletServerNodes(@Nullable String listenerName) {
        return tabletServerInfos.values().stream()
                .map(
                        node -> {
                            Endpoint endpoint =
                                    listenerName != null
                                            ? node.endpoint(listenerName)
                                            : node.endpoints().get(0);
                            return new ServerNode(
                                    node.id(),
                                    endpoint.getHost(),
                                    endpoint.getPort(),
                                    ServerType.TABLET_SERVER,
                                    node.rack());
                        })
                .collect(Collectors.toList());
    }

    public ZooKeeperClient getZooKeeperClient() {
        return zooKeeperClient;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public CoordinatorGateway newCoordinatorClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getCoordinatorServerNode, rpcClient, CoordinatorGateway.class);
    }

    public CoordinatorGateway newCoordinatorClient(String listenerName) {
        return GatewayClientProxy.createGatewayProxy(
                () -> getCoordinatorServerNode(listenerName), rpcClient, CoordinatorGateway.class);
    }

    public TabletServerGateway newTabletServerClientForNode(int serverId) {
        final ServerNode serverNode =
                getTabletServerNodes().stream()
                        .filter(n -> n.id() == serverId)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Tablet server " + serverId + " does not exist."));
        return newTabletServerClientForNode(serverNode);
    }

    private TabletServerGateway newTabletServerClientForNode(ServerNode serverNode) {
        return GatewayClientProxy.createGatewayProxy(
                () -> serverNode, rpcClient, TabletServerGateway.class);
    }

    /**
     * Wait until coordinator server and all the tablet servers have the same metadata. This method
     * needs to be called in advance for those ITCase which need to get metadata from server.
     */
    public void waitUtilAllGatewayHasSameMetadata() {
        for (AdminReadOnlyGateway gateway : collectAllRpcGateways()) {
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        MetadataResponse response = gateway.metadata(new MetadataRequest()).get();
                        assertThat(response.hasCoordinatorServer()).isTrue();
                        // check coordinator server node
                        ServerNode coordinatorNode =
                                toServerNode(
                                        response.getCoordinatorServer(), ServerType.COORDINATOR);
                        assertThat(coordinatorNode)
                                .isEqualTo(
                                        getCoordinatorServerNode(
                                                clusterConf.get(
                                                        ConfigOptions.INTERNAL_LISTENER_NAME)));
                        // check tablet server nodes
                        List<ServerNode> tsNodes =
                                response.getTabletServersList().stream()
                                        .map(n -> toServerNode(n, ServerType.TABLET_SERVER))
                                        .collect(Collectors.toList());
                        assertThat(tsNodes)
                                .containsExactlyInAnyOrderElementsOf(getTabletServerNodes());
                    });
        }
    }

    /** Wait until all the table assignments buckets are ready for table. */
    public void waitUtilTableReady(long tableId) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<TableAssignment> tableAssignmentOpt =
                            zkClient.getTableAssignment(tableId);
                    assertThat(tableAssignmentOpt).isPresent();
                    waitReplicaInAssignmentReady(zkClient, tableAssignmentOpt.get(), tableId, null);
                });
    }

    public void waitUtilTablePartitionReady(long tableId, long partitionId) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<PartitionAssignment> partitionAssignmentOpt =
                            zkClient.getPartitionAssignment(partitionId);
                    assertThat(partitionAssignmentOpt).isPresent();
                    waitReplicaInAssignmentReady(
                            zkClient, partitionAssignmentOpt.get(), tableId, partitionId);
                });
    }

    private void waitReplicaInAssignmentReady(
            ZooKeeperClient zkClient,
            TableAssignment tableAssignment,
            long tableId,
            Long partitionId)
            throws Exception {
        Set<Integer> buckets = tableAssignment.getBucketAssignments().keySet();
        for (int bucketId : buckets) {
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            Optional<LeaderAndIsr> leaderAndIsrOpt = zkClient.getLeaderAndIsr(tb);
            assertThat(leaderAndIsrOpt).isPresent();
            List<Integer> isr = leaderAndIsrOpt.get().isr();
            for (int replicaId : isr) {
                ReplicaManager replicaManager = getTabletServerById(replicaId).getReplicaManager();
                assertThat(replicaManager.getReplica(tb))
                        .isInstanceOf(ReplicaManager.OnlineReplica.class);
            }
        }
    }

    /** Wait until the input replica is kicked out of isr. */
    public void waitUtilReplicaShrinkFromIsr(TableBucket tableBucket, int replicaId) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsrOpt = zkClient.getLeaderAndIsr(tableBucket);
                    assertThat(leaderAndIsrOpt).isPresent();
                    List<Integer> isr = leaderAndIsrOpt.get().isr();
                    assertThat(isr.contains(replicaId)).isFalse();
                });
    }

    /** Wait until the input replica is expended into isr. */
    public void waitUtilReplicaExpandToIsr(TableBucket tableBucket, int replicaId) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsrOpt = zkClient.getLeaderAndIsr(tableBucket);
                    assertThat(leaderAndIsrOpt).isPresent();
                    List<Integer> isr = leaderAndIsrOpt.get().isr();
                    assertThat(isr.contains(replicaId)).isTrue();
                });
    }

    /** Wait until all the replicas are ready if we have multi replica for one table bucket. */
    public void waitUtilAllReplicaReady(TableBucket tableBucket) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsrOpt = zkClient.getLeaderAndIsr(tableBucket);
                    assertThat(leaderAndIsrOpt).isPresent();
                    LeaderAndIsr leaderAndIsr = leaderAndIsrOpt.get();
                    List<Integer> isr = leaderAndIsr.isr();
                    for (int replicaId : isr) {
                        ReplicaManager replicaManager =
                                getTabletServerById(replicaId).getReplicaManager();
                        assertThat(replicaManager.getReplica(tableBucket))
                                .isInstanceOf(ReplicaManager.OnlineReplica.class);
                    }

                    int leader = leaderAndIsr.leader();
                    ReplicaManager replicaManager = getTabletServerById(leader).getReplicaManager();
                    assertThat(replicaManager.getReplicaOrException(tableBucket).isLeader())
                            .isTrue();
                });
    }

    /**
     * Wait until some log segments copy to remote. This method can only ensure that there are at
     * least one log segment has been copied to remote, but it does not ensure that all log segments
     * have been copied to remote.
     */
    public void waitUtilSomeLogSegmentsCopyToRemote(TableBucket tableBucket) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    Optional<RemoteLogManifestHandle> remoteLogManifestHandle;
                    remoteLogManifestHandle = zkClient.getRemoteLogManifestHandle(tableBucket);
                    assertThat(remoteLogManifestHandle).isPresent();
                });
    }

    public CompletedSnapshot waitUtilSnapshotFinished(TableBucket tableBucket, long snapshotId) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        return waitValue(
                () -> {
                    Optional<BucketSnapshot> optSnapshot =
                            zkClient.getTableBucketSnapshot(tableBucket, snapshotId);
                    return optSnapshot
                            .map(BucketSnapshot::toCompletedSnapshotHandle)
                            .map(
                                    uncheckedFunction(
                                            CompletedSnapshotHandle::retrieveCompleteSnapshot));
                },
                Duration.ofMinutes(2),
                String.format(
                        "Fail to wait bucket %s snapshot %d finished", tableBucket, snapshotId));
    }

    public Replica waitAndGetLeaderReplica(TableBucket tableBucket) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        return waitValue(
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsrOpt = zkClient.getLeaderAndIsr(tableBucket);
                    if (!leaderAndIsrOpt.isPresent()) {
                        return Optional.empty();
                    } else {
                        int leader = leaderAndIsrOpt.get().leader();
                        return getReplica(tableBucket, leader, true);
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait leader replica ready");
    }

    public Replica waitAndGetFollowerReplica(TableBucket tableBucket, int replica) {
        return waitValue(
                () -> getReplica(tableBucket, replica, false),
                Duration.ofMinutes(1),
                "Fail to wait " + replica + " ready");
    }

    public void stopReplica(int tabletServerId, TableBucket tableBucket, int leaderEpoch)
            throws Exception {
        TabletServerGateway followerGateway = newTabletServerClientForNode(tabletServerId);
        // send stop replica request to the follower
        followerGateway
                .stopReplica(
                        new StopReplicaRequest()
                                .setCoordinatorEpoch(0)
                                .addAllStopReplicasReqs(
                                        Collections.singleton(
                                                makeStopBucketReplica(
                                                        tableBucket, false, leaderEpoch))))
                .get();
    }

    public void notifyLeaderAndIsr(
            int tabletServerId,
            TablePath tablePath,
            TableBucket tableBucket,
            LeaderAndIsr leaderAndIsr,
            List<Integer> replicas) {
        TabletServerGateway followerGateway = newTabletServerClientForNode(tabletServerId);
        PbNotifyLeaderAndIsrReqForBucket reqForBucket =
                makeNotifyBucketLeaderAndIsr(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(tablePath),
                                tableBucket,
                                replicas,
                                leaderAndIsr));
        NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest =
                ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest(
                        0, Collections.singletonList(reqForBucket));
        followerGateway.notifyLeaderAndIsr(notifyLeaderAndIsrRequest);
    }

    private Optional<Replica> getReplica(TableBucket tableBucket, int replica, boolean isLeader) {
        ReplicaManager replicaManager = getTabletServerById(replica).getReplicaManager();
        if (replicaManager.getReplica(tableBucket) instanceof ReplicaManager.OnlineReplica) {
            ReplicaManager.OnlineReplica onlineReplica =
                    (ReplicaManager.OnlineReplica) replicaManager.getReplica(tableBucket);
            if (onlineReplica.getReplica().isLeader() == isLeader) {
                return Optional.of(onlineReplica.getReplica());
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    public Map<String, Long> waitUntilPartitionAllReady(TablePath tablePath) {
        int preCreatePartitions = ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue();
        // wait util table partition is created
        return waitUntilPartitionsCreated(tablePath, preCreatePartitions);
    }

    public Map<String, Long> waitUntilPartitionAllReady(TablePath tablePath, int expectCount) {
        return waitUntilPartitionsCreated(tablePath, expectCount);
    }

    public Map<String, Long> waitUntilPartitionsCreated(TablePath tablePath, int expectCount) {
        return waitValue(
                () -> {
                    Map<String, Long> partitions =
                            zooKeeperClient.getPartitionNameAndIds(tablePath);
                    if (partitions.size() == expectCount) {
                        return Optional.of(partitions);
                    } else {
                        return Optional.empty();
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait " + expectCount + " partitions created");
    }

    public void waitUntilPartitionsDropped(TablePath tablePath, List<String> droppedPartitions) {
        waitUtil(
                () -> {
                    Map<String, Long> partitions =
                            zooKeeperClient.getPartitionNameAndIds(tablePath);
                    for (String droppedPartition : droppedPartitions) {
                        if (partitions.containsKey(droppedPartition)) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(1),
                "Fail to wait partitions dropped");
    }

    public int waitAndGetLeader(TableBucket tb) {
        return waitLeaderAndIsrReady(tb).leader();
    }

    public LeaderAndIsr waitLeaderAndIsrReady(TableBucket tb) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        return waitValue(
                () -> zkClient.getLeaderAndIsr(tb), Duration.ofMinutes(1), "leader is not ready");
    }

    private List<AdminReadOnlyGateway> collectAllRpcGateways() {
        String internalListenerName = clusterConf.get(ConfigOptions.INTERNAL_LISTENER_NAME);
        List<AdminReadOnlyGateway> rpcServiceBases = new ArrayList<>();
        rpcServiceBases.add(newCoordinatorClient(internalListenerName));
        rpcServiceBases.addAll(
                getTabletServerNodes(internalListenerName).stream()
                        .map(this::newTabletServerClientForNode)
                        .collect(Collectors.toList()));
        return rpcServiceBases;
    }

    public CoordinatorServer getCoordinatorServer() {
        return coordinatorServer;
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for {@link FlussClusterExtension}. */
    public static class Builder {
        private static final String DEFAULT_LISTENERS = "FLUSS://localhost:0";
        private int numOfTabletServers = 1;
        private String tabletServerListeners = DEFAULT_LISTENERS;
        private String coordinatorServerListeners = DEFAULT_LISTENERS;

        private final Configuration clusterConf = new Configuration();

        public Builder() {
            // reduce testing resources
            clusterConf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
            clusterConf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        }

        /** Sets the number of tablet servers. */
        public Builder setNumOfTabletServers(int numOfTabletServers) {
            this.numOfTabletServers = numOfTabletServers;
            return this;
        }

        /** Sets the base cluster configuration for TabletServer and CoordinatorServer. */
        public Builder setClusterConf(Configuration clusterConf) {
            clusterConf.toMap().forEach(this.clusterConf::setString);
            return this;
        }

        /** Sets the listeners of tablet servers. */
        public Builder setTabletServerListeners(String tabletServerListeners) {
            this.tabletServerListeners = tabletServerListeners;
            return this;
        }

        /** Sets the listeners of coordinator servers. */
        public Builder setCoordinatorServerListeners(String coordinatorServerListeners) {
            this.coordinatorServerListeners = coordinatorServerListeners;
            return this;
        }

        public FlussClusterExtension build() {
            return new FlussClusterExtension(
                    numOfTabletServers,
                    coordinatorServerListeners,
                    tabletServerListeners,
                    clusterConf);
        }
    }
}
