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

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.fs.local.LocalFileSystem;
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
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.coordinator.MetaDataManager;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperTestUtils;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.RemoteLogManifestHandle;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.NetUtils;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

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

import static com.alibaba.fluss.server.utils.RpcMessageUtils.toServerNode;
import static com.alibaba.fluss.server.zk.ZooKeeperTestUtils.createZooKeeperClient;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static com.alibaba.fluss.utils.NetUtils.getAvailablePort;
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

    private static final String HOST_ADDRESS = "127.0.0.1";

    private final int initialNumOfTabletServers;

    private CoordinatorServer coordinatorServer;
    private ServerNode coordinatorServerNode;
    private TestingServer zooKeeperServer;
    private ZooKeeperClient zooKeeperClient;
    private RpcClient rpcClient;
    private MetaDataManager metaDataManager;

    private File tempDir;

    private final Map<Integer, TabletServer> tabletServers;
    private final List<ServerNode> tabletServerNodes;
    private final Configuration clusterConf;

    /** Creates a new {@link Builder} for {@link FlussClusterExtension}. */
    public static Builder builder() {
        return new Builder();
    }

    private FlussClusterExtension(int numOfTabletServers, Configuration clusterConf) {
        this.initialNumOfTabletServers = numOfTabletServers;
        this.tabletServers = new HashMap<>(numOfTabletServers);
        this.tabletServerNodes = new ArrayList<>();
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
    public void afterEach(ExtensionContext extensionContext) {
        String defaultDb = BUILTIN_DATABASE;
        // TODO: we need to cleanup all zk nodes, including the assignments,
        //  but currently, we don't have a good way to do it
        if (metaDataManager != null) {
            // drop all database and tables
            List<String> databases = metaDataManager.listDatabases();
            for (String database : databases) {
                if (!database.equals(defaultDb)) {
                    metaDataManager.dropDatabase(database, true, true);
                    // delete the data dirs
                    for (int serverId : tabletServers.keySet()) {
                        String dataDir = getDataDir(serverId);
                        FileUtils.deleteDirectoryQuietly(Paths.get(dataDir, database).toFile());
                    }
                }
            }
            List<String> tables = metaDataManager.listTables(defaultDb);
            for (String table : tables) {
                metaDataManager.dropTable(TablePath.of(defaultDb, table), true);
            }
        }
    }

    public void start() throws Exception {
        tempDir = Files.createTempDirectory("fluss-testing-cluster").toFile();
        zooKeeperServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
        zooKeeperClient =
                createZooKeeperClient(zooKeeperServer.getConnectString(), NOPErrorHandler.INSTANCE);
        metaDataManager = new MetaDataManager(zooKeeperClient);
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
        tabletServerNodes.clear();
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
            try (NetUtils.Port availablePort = getAvailablePort()) {
                Configuration conf = new Configuration(clusterConf);
                conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS, zooKeeperServer.getConnectString());
                conf.setString(ConfigOptions.COORDINATOR_HOST, HOST_ADDRESS);
                conf.setString(
                        ConfigOptions.COORDINATOR_PORT, String.valueOf(availablePort.getPort()));
                setRemoteDataDir(conf);

                coordinatorServer = new CoordinatorServer(conf);
                coordinatorServer.start();
                coordinatorServerNode =
                        // we use -1 as coordinator server id
                        new ServerNode(
                                -1, HOST_ADDRESS, availablePort.getPort(), ServerType.COORDINATOR);
            }
        } else {
            // start the existing coordinator server
            coordinatorServer.start();
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
        if (tabletServers.containsKey(serverId)) {
            throw new IllegalArgumentException("Tablet server " + serverId + " already exists.");
        }
        String dataDir = getDataDir(serverId);
        final ServerNode serverNode;
        final TabletServer tabletServer;
        try (NetUtils.Port availablePort = getAvailablePort()) {
            Configuration tabletServerConf = new Configuration(clusterConf);
            tabletServerConf.set(ConfigOptions.TABLET_SERVER_HOST, HOST_ADDRESS);
            tabletServerConf.set(ConfigOptions.TABLET_SERVER_ID, serverId);
            tabletServerConf.set(
                    ConfigOptions.TABLET_SERVER_PORT, String.valueOf(availablePort.getPort()));
            tabletServerConf.set(ConfigOptions.DATA_DIR, dataDir);
            tabletServerConf.setString(
                    ConfigOptions.ZOOKEEPER_ADDRESS, zooKeeperServer.getConnectString());

            setRemoteDataDir(tabletServerConf);

            tabletServer = new TabletServer(tabletServerConf);
            tabletServer.start();
            serverNode =
                    new ServerNode(
                            serverId,
                            HOST_ADDRESS,
                            availablePort.getPort(),
                            ServerType.TABLET_SERVER);
        }

        tabletServers.put(serverId, tabletServer);
        tabletServerNodes.add(serverNode);
    }

    private String getDataDir(int serverId) {
        return tempDir.getAbsolutePath() + File.separator + "tablet-server-" + serverId;
    }

    private void setRemoteDataDir(Configuration conf) {
        String remoteDataDir =
                LocalFileSystem.getLocalFsURI().getScheme()
                        + "://"
                        + tempDir.getAbsolutePath()
                        + File.separator
                        + "remote-data-dir";
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
    }

    /** Stop a tablet server. */
    public void stopTabletServer(int serverId) throws Exception {
        if (!tabletServers.containsKey(serverId)) {
            throw new IllegalArgumentException("Tablet server " + serverId + " does not exist.");
        }
        tabletServers.remove(serverId).close();
        tabletServerNodes.removeIf(node -> node.id() == serverId);
    }

    public Configuration getClientConfig() {
        Configuration flussConf = new Configuration();
        // now, just use the coordinator server as the bootstrap server
        flussConf.set(
                ConfigOptions.BOOTSTRAP_SERVERS,
                Collections.singletonList(
                        String.format(
                                "%s:%d",
                                coordinatorServerNode.host(), coordinatorServerNode.port())));

        // set a small memory buffer for testing.
        flussConf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("2mb"));
        flussConf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("256kb"));
        flussConf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("1kb"));
        return flussConf;
    }

    public TabletServer getTabletServerById(int serverId) {
        return tabletServers.get(serverId);
    }

    public ServerNode getCoordinatorServerNode() {
        return coordinatorServerNode;
    }

    public Set<TabletServer> getTabletServers() {
        return new HashSet<>(tabletServers.values());
    }

    public List<ServerNode> getTabletServerNodes() {
        return tabletServerNodes;
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

    public TabletServerGateway newTabletServerClientForNode(int serverId) {
        final ServerNode serverNode =
                tabletServerNodes.stream()
                        .filter(n -> n.id() == serverId)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Tablet server " + serverId + " does not exist."));
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
                        assertThat(coordinatorNode).isEqualTo(getCoordinatorServerNode());
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
                        ReplicaManager replicaManager =
                                getTabletServerById(leader).getReplicaManager();
                        if (replicaManager.getReplica(tableBucket)
                                instanceof ReplicaManager.OnlineReplica) {
                            ReplicaManager.OnlineReplica onlineReplica =
                                    (ReplicaManager.OnlineReplica)
                                            replicaManager.getReplica(tableBucket);
                            if (onlineReplica.getReplica().isLeader()) {
                                return Optional.of(onlineReplica.getReplica());
                            } else {
                                return Optional.empty();
                            }
                        } else {
                            return Optional.empty();
                        }
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait leader replica ready");
    }

    public Map<String, Long> waitUtilPartitionAllReady(TablePath tablePath) {
        int preCreatePartitions = ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue();
        // wait util table partition is created
        return waitUntilPartitionsCreated(tablePath, preCreatePartitions);
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

    public int waitAndGetLeader(TableBucket tb) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofMinutes(1),
                        "leader is not ready");
        return leaderAndIsr.leader();
    }

    private List<AdminReadOnlyGateway> collectAllRpcGateways() {
        List<AdminReadOnlyGateway> rpcServiceBases = new ArrayList<>();
        rpcServiceBases.add(newCoordinatorClient());
        rpcServiceBases.addAll(
                getTabletServerNodes().stream()
                        .map(n -> newTabletServerClientForNode(n.id()))
                        .collect(Collectors.toList()));
        return rpcServiceBases;
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for {@link FlussClusterExtension}. */
    public static class Builder {
        private int numOfTabletServers = 1;
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

        public FlussClusterExtension build() {
            return new FlussClusterExtension(numOfTabletServers, clusterConf);
        }
    }
}
