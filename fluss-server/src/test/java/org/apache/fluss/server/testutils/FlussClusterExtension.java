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

package org.apache.fluss.server.testutils;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.authorizer.DefaultAuthorizer;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.rebalance.RebalanceManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.PeriodicSnapshotManager;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperTestUtils;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeStopBucketReplica;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toServerNode;
import static org.apache.fluss.server.zk.ZooKeeperTestUtils.createZooKeeperClient;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.fluss.utils.function.FunctionUtils.uncheckedFunction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
    private final Clock clock;

    /** Creates a new {@link Builder} for {@link FlussClusterExtension}. */
    public static Builder builder() {
        return new Builder();
    }

    private FlussClusterExtension(
            int numOfTabletServers,
            String coordinatorServerListeners,
            String tabletServerListeners,
            Configuration clusterConf,
            Clock clock) {
        this.initialNumOfTabletServers = numOfTabletServers;
        this.tabletServers = new HashMap<>(numOfTabletServers);
        this.coordinatorServerListeners = coordinatorServerListeners;
        this.tabletServerListeners = tabletServerListeners;
        this.tabletServerInfos = new HashMap<>();
        this.clusterConf = clusterConf;
        this.clock = clock;
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

        // TODO we need to drop these table by dropTable Event instead of manual clear table
        // metadata.
        for (TabletServer tabletServer : tabletServers.values()) {
            tabletServer.getMetadataCache().clearTableMetadata();
        }
    }

    public void start() throws Exception {
        tempDir = Files.createTempDirectory("fluss-testing-cluster").toFile();
        zooKeeperServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
        zooKeeperClient =
                createZooKeeperClient(zooKeeperServer.getConnectString(), NOPErrorHandler.INSTANCE);
        metadataManager =
                new MetadataManager(
                        zooKeeperClient,
                        clusterConf,
                        new LakeCatalogDynamicLoader(clusterConf, null, true));
        Configuration conf = new Configuration();
        rpcClient =
                RpcClient.create(
                        conf,
                        new ClientMetricGroup(
                                MetricRegistry.create(conf, null), "fluss-cluster-extension"),
                        false);
        startCoordinatorServer();
        startTabletServers();
        // wait coordinator knows all tablet servers to make cluster
        // have enough replication factor when creating table.
        waitUntilAllGatewayHasSameMetadata();
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
            conf.set(ConfigOptions.COORDINATOR_ID, 0);
            conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS, zooKeeperServer.getConnectString());
            conf.setString(ConfigOptions.BIND_LISTENERS, coordinatorServerListeners);
            setRemoteDataDir(conf);
            coordinatorServer = new CoordinatorServer(conf);
            coordinatorServer.start();
            waitUntilCoordinatorServerElected();
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
            waitUntilCoordinatorServerElected();
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

        TabletServer tabletServer = new TabletServer(tabletServerConf, clock);
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

    public RebalanceManager getRebalanceManager() {
        return coordinatorServer.getRebalanceManager();
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
     * Wait until coordinator server and all the tablet servers have the same metadata (Only need to
     * make sure same server info not to make sure table metadata). This method needs to be called
     * in advance for those ITCase which need to get metadata from server.
     */
    public void waitUntilAllGatewayHasSameMetadata() {
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
    public void waitUntilTableReady(long tableId) {
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

    /**
     * Wait until all authorization are synchronized to all tablet servers.
     *
     * @param aclBindings aclBindings to be synchronized.
     * @param exist whether aclBinding exist.
     */
    public void waitUntilAuthenticationSync(Collection<AclBinding> aclBindings, boolean exist) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Set<ServerBase> servers = new HashSet<>(getTabletServers());
                    servers.add(getCoordinatorServer());
                    servers.forEach(
                            ts -> {
                                Authorizer authorizer = ts.getAuthorizer();
                                assertThat(authorizer).isNotNull();
                                for (AclBinding aclBinding : aclBindings) {
                                    AccessControlEntry accessControlEntry =
                                            aclBinding.getAccessControlEntry();
                                    assertThat(
                                                    ((DefaultAuthorizer) authorizer)
                                                            .aclsAllowAccess(
                                                                    aclBinding.getResource(),
                                                                    accessControlEntry
                                                                            .getPrincipal(),
                                                                    accessControlEntry
                                                                            .getOperationType(),
                                                                    accessControlEntry.getHost()))
                                            .isEqualTo(exist);
                                }
                            });
                });
    }

    public void waitUntilTablePartitionReady(long tableId, long partitionId) {
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
    public void waitUntilReplicaShrinkFromIsr(TableBucket tableBucket, int replicaId) {
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
    public void waitUntilReplicaExpandToIsr(TableBucket tableBucket, int replicaId) {
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
    public void waitUntilAllReplicaReady(TableBucket tableBucket) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsrOpt = zkClient.getLeaderAndIsr(tableBucket);
                    assertThat(leaderAndIsrOpt).isPresent();
                    LeaderAndIsr leaderAndIsr = leaderAndIsrOpt.get();
                    List<Integer> isr = leaderAndIsr.isr();
                    for (int replicaId : isr) {
                        TabletServer tabletServer = getTabletServerById(replicaId);
                        ReplicaManager replicaManager = tabletServer.getReplicaManager();
                        assertThat(replicaManager.getReplica(tableBucket))
                                .isInstanceOf(ReplicaManager.OnlineReplica.class);

                        // check table metadata.
                        TabletServerMetadataCache serverMetadataCache =
                                tabletServer.getMetadataCache();
                        assertThat(serverMetadataCache.getTablePath(tableBucket.getTableId()))
                                .isPresent();
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
    public void waitUntilSomeLogSegmentsCopyToRemote(TableBucket tableBucket) {
        ZooKeeperClient zkClient = getZooKeeperClient();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    Optional<RemoteLogManifestHandle> remoteLogManifestHandle;
                    remoteLogManifestHandle = zkClient.getRemoteLogManifestHandle(tableBucket);
                    assertThat(remoteLogManifestHandle).isPresent();
                });
    }

    public void triggerAndWaitSnapshot(TablePath tablePath) throws Exception {
        Optional<TableRegistration> table = zooKeeperClient.getTable(tablePath);
        //noinspection SimplifyOptionalCallChains (Java 8 compatibility)
        if (!table.isPresent()) {
            throw new IllegalStateException("Table not found for table path " + tablePath);
        }

        TableRegistration tableRegistration = table.get();
        long tableId = tableRegistration.tableId;
        int bucketCount = tableRegistration.bucketCount;

        List<TableBucket> tableBuckets = new ArrayList<>();
        if (!tableRegistration.isPartitioned()) {
            for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                tableBuckets.add(new TableBucket(tableId, null, bucketId));
            }
        } else {
            Map<String, Long> partitions = zooKeeperClient.getPartitionNameAndIds(tablePath);
            for (Long partitionId : partitions.values()) {
                for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                    tableBuckets.add(new TableBucket(tableId, partitionId, bucketId));
                }
            }
        }

        // trigger and wait all snapshots finished
        triggerAndWaitSnapshots(tableBuckets);
    }

    public void triggerAndWaitSnapshots(Collection<TableBucket> tableBuckets) {
        Map<TableBucket, Long> snapshotMap = new HashMap<>();
        for (TableBucket tableBucket : tableBuckets) {
            Long snapshotId = triggerSnapshot(tableBucket);
            if (snapshotId != null) {
                snapshotMap.put(tableBucket, snapshotId);
            }
        }
        // wait all snapshots finished
        for (Map.Entry<TableBucket, Long> entry : snapshotMap.entrySet()) {
            waitUntilSnapshotFinished(entry.getKey(), entry.getValue());
        }
    }

    public CompletedSnapshot triggerAndWaitSnapshot(TableBucket tableBucket) {
        Long snapshotId = triggerSnapshot(tableBucket);
        if (snapshotId != null) {
            return waitUntilSnapshotFinished(tableBucket, snapshotId);
        } else {
            fail("No new snapshot triggered for table bucket " + tableBucket);
            return null;
        }
    }

    private Long triggerSnapshot(TableBucket tableBucket) {
        Long snapshotId = null;
        Long nextSnapshotId = null;
        for (TabletServer ts : tabletServers.values()) {
            ReplicaManager.HostedReplica replica = ts.getReplicaManager().getReplica(tableBucket);
            if (replica instanceof ReplicaManager.OnlineReplica) {
                Replica r = ((ReplicaManager.OnlineReplica) replica).getReplica();
                PeriodicSnapshotManager kvSnapshotManager = r.getKvSnapshotManager();
                if (r.isLeader() && kvSnapshotManager != null) {
                    snapshotId = kvSnapshotManager.currentSnapshotId();
                    kvSnapshotManager.triggerSnapshot();
                    nextSnapshotId = kvSnapshotManager.currentSnapshotId();
                    break;
                }
            }
        }

        if (snapshotId != null) {
            if (nextSnapshotId > snapshotId) {
                // only there is a new snapshot triggered, we return the snapshot id
                return snapshotId;
            } else {
                return null;
            }
        } else {
            fail("No KV snapshot manager found for table bucket " + tableBucket);
            return null;
        }
    }

    private CompletedSnapshot waitUntilSnapshotFinished(TableBucket tableBucket, long snapshotId) {
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
                Duration.ofSeconds(30),
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
        // wait until table partition is created
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
        waitUntil(
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

    public void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zooKeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofSeconds(10),
                "Fail to wait coordinator server elected");
        // Sleep 1 second to make sure coordinator server has been started and event processor
        // started.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for {@link FlussClusterExtension}. */
    public static class Builder {
        private static final String DEFAULT_LISTENERS = "FLUSS://localhost:0";
        private int numOfTabletServers = 1;
        private String tabletServerListeners = DEFAULT_LISTENERS;
        private String coordinatorServerListeners = DEFAULT_LISTENERS;
        private Clock clock = SystemClock.getInstance();

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

        /** Sets the clock of fluss cluster. */
        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public FlussClusterExtension build() {
            return new FlussClusterExtension(
                    numOfTabletServers,
                    coordinatorServerListeners,
                    tabletServerListeners,
                    clusterConf,
                    clock);
        }
    }
}
