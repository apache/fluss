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

package org.apache.fluss.server.zk;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.DefaultAuthorizer.VersionedAcls;
import org.apache.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.zk.ZkAsyncRequest.ZkGetChildrenRequest;
import org.apache.fluss.server.zk.ZkAsyncRequest.ZkGetDataRequest;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetChildrenResponse;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetDataResponse;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.ResourceAcl;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.ZkData.AclChangeNotificationNode;
import org.apache.fluss.server.zk.data.ZkData.BucketIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.BucketRemoteLogsZNode;
import org.apache.fluss.server.zk.data.ZkData.BucketSnapshotIdZNode;
import org.apache.fluss.server.zk.data.ZkData.BucketSnapshotsZNode;
import org.apache.fluss.server.zk.data.ZkData.ConfigEntityZNode;
import org.apache.fluss.server.zk.data.ZkData.CoordinatorZNode;
import org.apache.fluss.server.zk.data.ZkData.DatabaseZNode;
import org.apache.fluss.server.zk.data.ZkData.DatabasesZNode;
import org.apache.fluss.server.zk.data.ZkData.LakeTableZNode;
import org.apache.fluss.server.zk.data.ZkData.LeaderAndIsrZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionSequenceIdZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionsZNode;
import org.apache.fluss.server.zk.data.ZkData.ResourceAclNode;
import org.apache.fluss.server.zk.data.ZkData.SchemaZNode;
import org.apache.fluss.server.zk.data.ZkData.SchemasZNode;
import org.apache.fluss.server.zk.data.ZkData.ServerIdZNode;
import org.apache.fluss.server.zk.data.ZkData.ServerIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdZNode;
import org.apache.fluss.server.zk.data.ZkData.TableSequenceIdZNode;
import org.apache.fluss.server.zk.data.ZkData.TableZNode;
import org.apache.fluss.server.zk.data.ZkData.TablesZNode;
import org.apache.fluss.server.zk.data.ZkData.WriterIdZNode;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.BackgroundCallback;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.CuratorEvent;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.fromPartitionName;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * This class includes methods for write/read various metadata (leader address, tablet server
 * registration, table assignment, table, schema) in Zookeeper.
 */
@Internal
public class ZooKeeperClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class);
    public static final int UNKNOWN_VERSION = -2;
    private static final int MAX_BATCH_SIZE = 1024;
    private static final int DEFAULT_SCHEMA_ID = 1;

    private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private final CuratorFramework zkClient;
    private final ZkSequenceIDCounter tableIdCounter;
    private final ZkSequenceIDCounter partitionIdCounter;
    private final ZkSequenceIDCounter writerIdCounter;

    private final Semaphore inFlightRequests;

    public ZooKeeperClient(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Configuration configuration) {
        this.curatorFrameworkWrapper = curatorFrameworkWrapper;
        this.zkClient = curatorFrameworkWrapper.asCuratorFramework();
        this.tableIdCounter = new ZkSequenceIDCounter(zkClient, TableSequenceIdZNode.path());
        this.partitionIdCounter =
                new ZkSequenceIDCounter(zkClient, PartitionSequenceIdZNode.path());
        this.writerIdCounter = new ZkSequenceIDCounter(zkClient, WriterIdZNode.path());

        int maxInFlightRequests =
                configuration.getInt(ConfigOptions.ZOOKEEPER_MAX_INFLIGHT_REQUESTS);
        this.inFlightRequests = new Semaphore(maxInFlightRequests);
    }

    public Optional<byte[]> getOrEmpty(String path) throws Exception {
        try {
            return Optional.of(zkClient.getData().forPath(path));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Coordinator server
    // --------------------------------------------------------------------------------------------

    /** Register a coordinator leader server to ZK. */
    public void registerCoordinatorLeader(CoordinatorAddress coordinatorAddress) throws Exception {
        String path = CoordinatorZNode.path();
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, CoordinatorZNode.encode(coordinatorAddress));
        LOG.info("Registered leader {} at path {}.", coordinatorAddress, path);
    }

    /** Get the leader address registered in ZK. */
    public Optional<CoordinatorAddress> getCoordinatorAddress() throws Exception {
        Optional<byte[]> bytes = getOrEmpty(CoordinatorZNode.path());
        return bytes.map(CoordinatorZNode::decode);
    }

    // --------------------------------------------------------------------------------------------
    // Tablet server
    // --------------------------------------------------------------------------------------------

    /** Register a tablet server to ZK. */
    public void registerTabletServer(
            int tabletServerId, TabletServerRegistration tabletServerRegistration)
            throws Exception {
        String path = ServerIdZNode.path(tabletServerId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, ServerIdZNode.encode(tabletServerRegistration));
        LOG.info(
                "Registered tablet server {} at path {} with registration {}.",
                tabletServerId,
                path,
                tabletServerRegistration);
    }

    /** Get the tablet server registered in ZK. */
    public Optional<TabletServerRegistration> getTabletServer(int tabletServerId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(ServerIdZNode.path(tabletServerId));
        return bytes.map(ServerIdZNode::decode);
    }

    /** Get the tablet servers registered in ZK. */
    public Map<Integer, TabletServerRegistration> getTabletServers(int[] tabletServerIds)
            throws Exception {
        Map<String, Integer> path2IdMap =
                Arrays.stream(tabletServerIds)
                        .boxed()
                        .collect(toMap(ServerIdZNode::path, id -> id));

        List<ZkGetDataResponse> responses = getDataInBackground(path2IdMap.keySet());
        return processGetDataResponses(
                responses,
                response -> path2IdMap.get(response.getPath()),
                ServerIdZNode::decode,
                "tablet server registration");
    }

    /** Gets the list of sorted server Ids. */
    public int[] getSortedTabletServerList() throws Exception {
        List<String> tabletServers = getChildren(ServerIdsZNode.path());
        return tabletServers.stream().mapToInt(Integer::parseInt).sorted().toArray();
    }

    // --------------------------------------------------------------------------------------------
    // Tablet assignments
    // --------------------------------------------------------------------------------------------

    /** Register table assignment to ZK. */
    public void registerTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, TableIdZNode.encode(tableAssignment));
        LOG.info("Registered table assignment {} for table id {}.", tableAssignment, tableId);
    }

    /** Get the table assignment in ZK. */
    public Optional<TableAssignment> getTableAssignment(long tableId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(TableIdZNode.path(tableId));
        return bytes.map(
                data ->
                        // we'll put a laketable node under TableIdZNode,
                        // so it won't be Optional#empty
                        // but will with a zero-length array
                        data.length == 0 ? null : TableIdZNode.decode(data));
    }

    /** Get the tables assignments in ZK. */
    public Map<Long, TableAssignment> getTablesAssignments(Collection<Long> tableIds)
            throws Exception {
        Map<String, Long> path2TableIdMap =
                tableIds.stream().collect(toMap(TableIdZNode::path, id -> id));

        List<ZkGetDataResponse> responses = getDataInBackground(path2TableIdMap.keySet());
        return processGetDataResponses(
                responses,
                response -> path2TableIdMap.get(response.getPath()),
                TableIdZNode::decode,
                "table assignment");
    }

    /** Get the partition assignment in ZK. */
    public Optional<PartitionAssignment> getPartitionAssignment(long partitionId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(PartitionIdZNode.path(partitionId));
        return bytes.map(PartitionIdZNode::decode);
    }

    /** Get the partitions assignments in ZK. */
    public Map<Long, PartitionAssignment> getPartitionsAssignments(Collection<Long> partitionIds)
            throws Exception {
        Map<String, Long> path2PartitionIdMap =
                partitionIds.stream().collect(toMap(PartitionIdZNode::path, id -> id));

        List<ZkGetDataResponse> responses = getDataInBackground(path2PartitionIdMap.keySet());
        return processGetDataResponses(
                responses,
                response -> path2PartitionIdMap.get(response.getPath()),
                PartitionIdZNode::decode,
                "partition assignment");
    }

    public void updateTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.setData().forPath(path, TableIdZNode.encode(tableAssignment));
        LOG.info("Updated table assignment {} for table id {}.", tableAssignment, tableId);
    }

    public void deleteTableAssignment(long tableId) throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table assignment for table id {}.", tableId);
    }

    public void deletePartitionAssignment(long partitionId) throws Exception {
        String path = PartitionIdZNode.path(partitionId);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table assignment for partition id {}.", partitionId);
    }

    // --------------------------------------------------------------------------------------------
    // Table state
    // --------------------------------------------------------------------------------------------

    /** Register bucket LeaderAndIsr to ZK. */
    public void registerLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr)
            throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, LeaderAndIsrZNode.encode(leaderAndIsr));
        LOG.info("Registered {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
    }

    public void batchRegisterLeaderAndIsrForTablePartition(
            List<RegisterTableBucketLeadAndIsrInfo> registerList) throws Exception {
        if (registerList.isEmpty()) {
            return;
        }

        List<CuratorOp> ops = new ArrayList<>(registerList.size());
        // In transaction API, it is not allowed to use "creatingParentsIfNeeded()"
        // So we have to create parent dictionary in advance.
        RegisterTableBucketLeadAndIsrInfo firstInfo = registerList.get(0);
        String bucketsParentPath = BucketIdsZNode.path(firstInfo.getTableBucket());
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(bucketsParentPath);

        for (RegisterTableBucketLeadAndIsrInfo info : registerList) {
            LOG.info(
                    "Batch Register {} for bucket {} in Zookeeper.",
                    info.getLeaderAndIsr(),
                    info.getTableBucket());
            byte[] data = LeaderAndIsrZNode.encode(info.getLeaderAndIsr());
            // create direct parent node
            CuratorOp parentNodeCreate =
                    zkClient.transactionOp()
                            .create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(ZkData.BucketIdZNode.path(info.getTableBucket()));
            // create current node
            CuratorOp currentNodeCreate =
                    zkClient.transactionOp()
                            .create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(LeaderAndIsrZNode.path(info.getTableBucket()), data);
            ops.add(parentNodeCreate);
            ops.add(currentNodeCreate);
            if (ops.size() == MAX_BATCH_SIZE) {
                zkClient.transaction().forOperations(ops);
                ops.clear();
            }
        }
        if (!ops.isEmpty()) {
            zkClient.transaction().forOperations(ops);
        }
        LOG.info(
                "Batch registered leadAndIsr for tableId: {}, partitionId: {}, partitionName: {}  in Zookeeper.",
                firstInfo.getTableBucket().getTableId(),
                firstInfo.getTableBucket().getPartitionId(),
                firstInfo.getPartitionName());
    }

    /** Get the bucket LeaderAndIsr in ZK. */
    public Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(LeaderAndIsrZNode.path(tableBucket));
        return bytes.map(LeaderAndIsrZNode::decode);
    }

    /**
     * Get the LeaderAndIsr for each buckets in a batch async way. The returned map only contains
     * buckets those have leader and isr assigned (zk node created).
     */
    public Map<TableBucket, LeaderAndIsr> getLeaderAndIsrs(Collection<TableBucket> tableBuckets)
            throws Exception {
        Map<String, TableBucket> path2TableBucketMap =
                tableBuckets.stream().collect(toMap(LeaderAndIsrZNode::path, bucket -> bucket));

        List<ZkGetDataResponse> responses = getDataInBackground(path2TableBucketMap.keySet());
        return processGetDataResponses(
                responses,
                response -> path2TableBucketMap.get(response.getPath()),
                LeaderAndIsrZNode::decode,
                "leader and isr");
    }

    public void updateLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr)
            throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.setData().forPath(path, LeaderAndIsrZNode.encode(leaderAndIsr));
        LOG.info("Updated {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
    }

    public void batchUpdateLeaderAndIsr(Map<TableBucket, LeaderAndIsr> leaderAndIsrList)
            throws Exception {
        if (leaderAndIsrList.isEmpty()) {
            return;
        }

        List<CuratorOp> ops = new ArrayList<>(leaderAndIsrList.size());
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : leaderAndIsrList.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            LeaderAndIsr leaderAndIsr = entry.getValue();

            LOG.info("Batch Update {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
            String path = LeaderAndIsrZNode.path(tableBucket);
            byte[] data = LeaderAndIsrZNode.encode(leaderAndIsr);
            CuratorOp updateOp = zkClient.transactionOp().setData().forPath(path, data);
            ops.add(updateOp);
            if (ops.size() == MAX_BATCH_SIZE) {
                zkClient.transaction().forOperations(ops);
                ops.clear();
            }
        }
        if (!ops.isEmpty()) {
            zkClient.transaction().forOperations(ops);
        }
    }

    public void deleteLeaderAndIsr(TableBucket tableBucket) throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.delete().forPath(path);
        LOG.info("Deleted LeaderAndIsr for bucket {} in Zookeeper.", tableBucket);
    }

    // --------------------------------------------------------------------------------------------
    // Database
    // --------------------------------------------------------------------------------------------
    public void registerDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception {
        String path = DatabaseZNode.path(database);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, DatabaseZNode.encode(databaseRegistration));
        LOG.info("Registered database {}", database);
    }

    /** Get the database in ZK. */
    public Optional<DatabaseRegistration> getDatabase(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        Optional<byte[]> bytes = getOrEmpty(path);
        return bytes.map(DatabaseZNode::decode);
    }

    public void deleteDatabase(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public boolean databaseExists(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        return zkClient.checkExists().forPath(path) != null;
    }

    public List<String> listDatabases() throws Exception {
        return getChildren(DatabasesZNode.path());
    }

    public List<String> listTables(String databaseName) throws Exception {
        return getChildren(TablesZNode.path(databaseName));
    }

    // --------------------------------------------------------------------------------------------
    // Table
    // --------------------------------------------------------------------------------------------

    /** generate a table id . */
    public long getTableIdAndIncrement() throws Exception {
        return tableIdCounter.getAndIncrement();
    }

    public long getPartitionIdAndIncrement() throws Exception {
        return partitionIdCounter.getAndIncrement();
    }

    /** Register table to ZK metadata. */
    public void registerTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        registerTable(tablePath, tableRegistration, true);
    }

    /**
     * Register table to ZK metadata.
     *
     * @param needCreateNode when register a table to zk, whether need to create the node of the
     *     path. In the case that we first register the schema to a path which will be the children
     *     path of the path to store the table, then register the table, we won't need to create the
     *     node again.
     */
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        String path = TableZNode.path(tablePath);
        byte[] tableBytes = TableZNode.encode(tableRegistration);
        if (needCreateNode) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, tableBytes);
        } else {
            zkClient.setData().forPath(path, tableBytes);
        }
        LOG.info(
                "Registered table {} for database {}",
                tablePath.getTableName(),
                tablePath.getDatabaseName());
    }

    /** Get the table in ZK. */
    public Optional<TableRegistration> getTable(TablePath tablePath) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(TableZNode.path(tablePath));
        return bytes.map(TableZNode::decode);
    }

    /** Get the tables in ZK. */
    public Map<TablePath, TableRegistration> getTables(Collection<TablePath> tablePaths)
            throws Exception {
        Map<String, TablePath> path2TablePathMap =
                tablePaths.stream().collect(Collectors.toMap(TableZNode::path, path -> path));

        List<ZkGetDataResponse> responses = getDataInBackground(path2TablePathMap.keySet());
        return processGetDataResponses(
                responses,
                response -> path2TablePathMap.get(response.getPath()),
                TableZNode::decode,
                "tables registration");
    }

    /** Get the v1 schema for given tables in ZK. */
    public Map<TablePath, SchemaInfo> getV1Schemas(Collection<TablePath> tablePaths)
            throws Exception {
        Map<String, TablePath> path2TablePathMap =
                tablePaths.stream()
                        .collect(toMap(p -> SchemaZNode.path(p, DEFAULT_SCHEMA_ID), path -> path));
        List<ZkGetDataResponse> responses = getDataInBackground(path2TablePathMap.keySet());
        return processGetDataResponses(
                responses,
                resp -> path2TablePathMap.get(resp.getPath()),
                b -> new SchemaInfo(SchemaZNode.decode(b), DEFAULT_SCHEMA_ID),
                "schema");
    }

    /** Update the table in ZK. */
    public void updateTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        String path = TableZNode.path(tablePath);
        zkClient.setData().forPath(path, TableZNode.encode(tableRegistration));
        LOG.info(
                "Updated table {} for database {}",
                tablePath.getTableName(),
                tablePath.getDatabaseName());
    }

    /** Delete the table in ZK. */
    public void deleteTable(TablePath tablePath) throws Exception {
        String path = TableZNode.path(tablePath);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table {}.", tablePath);
    }

    public boolean tableExist(TablePath tablePath) throws Exception {
        String path = TableZNode.path(tablePath);
        Stat stat = zkClient.checkExists().forPath(path);
        // when we create a table, we will first create a node with
        // path 'table_path/schemas/schema_id' to store the schema, so we can't use the path of
        // table 'table_path' exist or not to check the table exist or not.
        return stat != null && stat.getDataLength() > 0;
    }

    /** Get the partitions of a table in ZK. */
    public Set<String> getPartitions(TablePath tablePath) throws Exception {
        String path = PartitionsZNode.path(tablePath);
        return new HashSet<>(getChildren(path));
    }

    /** Get the partitions of tables in ZK. */
    public Map<TablePath, List<String>> getPartitionsForTables(Collection<TablePath> tablePaths)
            throws Exception {
        Map<String, TablePath> path2TablePathMap =
                tablePaths.stream().collect(toMap(PartitionsZNode::path, path -> path));

        List<ZkGetChildrenResponse> responses = getChildrenInBackground(path2TablePathMap.keySet());
        return processGetChildrenResponses(
                responses,
                response -> path2TablePathMap.get(response.getPath()),
                "partitions for tables");
    }

    /** Get the partition and the id for the partitions of a table in ZK. */
    public Map<String, Long> getPartitionNameAndIds(TablePath tablePath) throws Exception {
        Map<String, Long> partitions = new HashMap<>();
        for (String partitionName : getPartitions(tablePath)) {
            Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
            optPartition.ifPresent(
                    partition -> partitions.put(partitionName, partition.getPartitionId()));
        }
        return partitions;
    }

    /** Get the partition and the id for the partitions of tables in ZK. */
    public Map<TablePath, Map<String, Long>> getPartitionNameAndIdsForTables(
            List<TablePath> tablePaths) throws Exception {
        Map<TablePath, Map<String, Long>> result = new HashMap<>();

        Map<TablePath, List<String>> tablePath2Partitions = getPartitionsForTables(tablePaths);

        // each TablePath has a list of partitions
        Map<String, TablePath> zkPath2TablePath = new HashMap<>();
        Map<String, String> zkPath2PartitionName = new HashMap<>();
        for (Map.Entry<TablePath, List<String>> entry : tablePath2Partitions.entrySet()) {
            TablePath tablePath = entry.getKey();
            List<String> partitions = entry.getValue();
            for (String partitionName : partitions) {
                zkPath2TablePath.put(PartitionZNode.path(tablePath, partitionName), tablePath);
                zkPath2PartitionName.put(
                        PartitionZNode.path(tablePath, partitionName), partitionName);
            }
        }

        List<ZkGetDataResponse> responses = getDataInBackground(zkPath2TablePath.keySet());
        for (ZkGetDataResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK) {
                String zkPath = response.getPath();
                TablePath tablePath = zkPath2TablePath.get(zkPath);
                String partitionName = zkPath2PartitionName.get(zkPath);
                long partitionId = PartitionZNode.decode(response.getData()).getPartitionId();
                result.computeIfAbsent(tablePath, k -> new HashMap<>())
                        .put(partitionName, partitionId);
            } else {
                LOG.warn(
                        "Failed to get data for path {}: {}",
                        response.getPath(),
                        response.getResultCode());
            }
        }
        return result;
    }

    /** Get the partition and the id for the partitions of a table in ZK by partition spec. */
    public Map<String, Long> getPartitionNameAndIds(
            TablePath tablePath,
            List<String> partitionKeys,
            ResolvedPartitionSpec partialPartitionSpec)
            throws Exception {
        Map<String, Long> partitions = new HashMap<>();

        for (String partitionName : getPartitions(tablePath)) {
            ResolvedPartitionSpec resolvedPartitionSpec =
                    fromPartitionName(partitionKeys, partitionName);
            boolean contains = resolvedPartitionSpec.contains(partialPartitionSpec);
            if (contains) {
                Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
                optPartition.ifPresent(
                        partition -> partitions.put(partitionName, partition.getPartitionId()));
            }
        }

        return partitions;
    }

    /** Get the id and name for the partitions of a table in ZK. */
    public Map<Long, String> getPartitionIdAndNames(TablePath tablePath) throws Exception {
        Map<Long, String> result = new HashMap<>();
        getPartitionIdAndPaths(Collections.singletonList(tablePath))
                .forEach(
                        (k, v) -> {
                            result.put(k, v.getPartitionName());
                        });
        return result;
    }

    /** Get the id and name for the partitions of tables in ZK. */
    public Map<Long, PhysicalTablePath> getPartitionIdAndPaths(Collection<TablePath> tablePath)
            throws Exception {
        // batch get partitions names for tables
        Map<TablePath, List<String>> partitionsForTables = getPartitionsForTables(tablePath);
        List<PhysicalTablePath> partitionPaths = new ArrayList<>();
        for (Map.Entry<TablePath, List<String>> entry : partitionsForTables.entrySet()) {
            for (String partitionName : entry.getValue()) {
                partitionPaths.add(PhysicalTablePath.of(entry.getKey(), partitionName));
            }
        }

        // batch get partitions ids
        Map<PhysicalTablePath, TablePartition> partitionIds = getPartitionIds(partitionPaths);
        Map<Long, PhysicalTablePath> partitionIdAndPaths = new HashMap<>();
        partitionIds.forEach(
                (partitionPath, tablePartition) -> {
                    if (tablePartition != null) {
                        partitionIdAndPaths.put(tablePartition.getPartitionId(), partitionPath);
                    }
                });

        return partitionIdAndPaths;
    }

    /** Get a partition of a table in ZK. */
    public Optional<TablePartition> getPartition(TablePath tablePath, String partitionName)
            throws Exception {
        String path = PartitionZNode.path(tablePath, partitionName);
        return getOrEmpty(path).map(PartitionZNode::decode);
    }

    /** Get partition id and table id for each partition in a batch async way. */
    public Map<PhysicalTablePath, TablePartition> getPartitionIds(
            Collection<PhysicalTablePath> partitionPaths) throws Exception {
        Map<String, PhysicalTablePath> path2PartitionPathMap =
                partitionPaths.stream()
                        .collect(
                                toMap(
                                        p ->
                                                PartitionZNode.path(
                                                        p.getTablePath(),
                                                        checkNotNull(p.getPartitionName())),
                                        path -> path));

        List<ZkGetDataResponse> responses = getDataInBackground(path2PartitionPathMap.keySet());
        return processGetDataResponses(
                responses,
                response -> path2PartitionPathMap.get(response.getPath()),
                PartitionZNode::decode,
                "partition");
    }

    /** Get partition num of a table in ZK. */
    public int getPartitionNumber(TablePath tablePath) throws Exception {
        String path = PartitionsZNode.path(tablePath);
        Stat stat = zkClient.checkExists().forPath(path);
        if (stat == null) {
            return 0;
        }
        return stat.getNumChildren();
    }

    /** Delete a partition for a table in ZK. */
    public void deletePartition(TablePath tablePath, String partitionName) throws Exception {
        String path = PartitionZNode.path(tablePath, partitionName);
        zkClient.delete().forPath(path);
    }

    /** Register partition assignment and metadata in transaction. */
    public void registerPartitionAssignmentAndMetadata(
            long partitionId,
            String partitionName,
            PartitionAssignment partitionAssignment,
            TablePath tablePath,
            long tableId)
            throws Exception {
        // Merge "registerPartitionAssignment()" and "registerPartition()"
        // into one transaction. This is to avoid the case that the partition assignment is
        // registered
        // but the partition metadata is not registered.

        // Create parent dictionary in advance.
        try {
            String tabletServerPartitionParentPath = ZkData.PartitionIdsZNode.path();
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(tabletServerPartitionParentPath);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
        try {
            String metadataPartitionParentPath = PartitionsZNode.path(tablePath);
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(metadataPartitionParentPath);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }

        List<CuratorOp> ops = new ArrayList<>(2);
        String tabletServerPartitionPath = PartitionIdZNode.path(partitionId);
        CuratorOp tabletServerPartitionNode =
                zkClient.transactionOp()
                        .create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(
                                tabletServerPartitionPath,
                                PartitionIdZNode.encode(partitionAssignment));

        String metadataPath = PartitionZNode.path(tablePath, partitionName);
        CuratorOp metadataPartitionNode =
                zkClient.transactionOp()
                        .create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(
                                metadataPath,
                                PartitionZNode.encode(new TablePartition(tableId, partitionId)));

        ops.add(tabletServerPartitionNode);
        ops.add(metadataPartitionNode);
        zkClient.transaction().forOperations(ops);
    }
    // --------------------------------------------------------------------------------------------
    // Schema
    // --------------------------------------------------------------------------------------------

    /** Register schema to ZK metadata and return the schema id. */
    public int registerSchema(TablePath tablePath, Schema schema) throws Exception {
        int currentSchemaId = getCurrentSchemaId(tablePath);
        // increase schema id.
        currentSchemaId++;
        String path = SchemaZNode.path(tablePath, currentSchemaId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, SchemaZNode.encode(schema));
        LOG.info("Registered new schema version {} for table {}.", currentSchemaId, tablePath);
        return currentSchemaId;
    }

    /** Get the specific schema by schema id in ZK metadata. */
    public Optional<SchemaInfo> getSchemaById(TablePath tablePath, int schemaId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(SchemaZNode.path(tablePath, schemaId));
        return bytes.map(b -> new SchemaInfo(SchemaZNode.decode(b), schemaId));
    }

    /** Gets the current schema id of the given table in ZK metadata. */
    public int getCurrentSchemaId(TablePath tablePath) throws Exception {
        Optional<Integer> currentSchemaId =
                getChildren(SchemasZNode.path(tablePath)).stream()
                        .map(Integer::parseInt)
                        .reduce(Math::max);
        return currentSchemaId.orElse(0);
    }

    // --------------------------------------------------------------------------------------------
    // Table Bucket snapshot
    // --------------------------------------------------------------------------------------------
    public void registerTableBucketSnapshot(TableBucket tableBucket, BucketSnapshot snapshot)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshot.getSnapshotId());
        zkClient.create()
                .creatingParentsIfNeeded()
                .forPath(path, BucketSnapshotIdZNode.encode(snapshot));
    }

    public void deleteTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshotId);
        zkClient.delete().forPath(path);
    }

    public OptionalLong getTableBucketLatestSnapshotId(TableBucket tableBucket) throws Exception {
        String path = BucketSnapshotsZNode.path(tableBucket);
        return getChildren(path).stream().mapToLong(Long::parseLong).max();
    }

    public Optional<BucketSnapshot> getTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshotId);
        return getOrEmpty(path).map(BucketSnapshotIdZNode::decode);
    }

    /** Get the latest snapshot of the table bucket. */
    public Optional<BucketSnapshot> getTableBucketLatestSnapshot(TableBucket tableBucket)
            throws Exception {
        OptionalLong latestSnapshotId = getTableBucketLatestSnapshotId(tableBucket);
        if (latestSnapshotId.isPresent()) {
            return getTableBucketSnapshot(tableBucket, latestSnapshotId.getAsLong());
        } else {
            return Optional.empty();
        }
    }

    public List<Tuple2<BucketSnapshot, Long>> getTableBucketAllSnapshotAndIds(
            TableBucket tableBucket) throws Exception {
        String path = BucketSnapshotsZNode.path(tableBucket);
        List<Tuple2<BucketSnapshot, Long>> snapshotAndIds = new ArrayList<>();
        for (String snapshotId : getChildren(path)) {
            long snapshotIdLong = Long.parseLong(snapshotId);
            Optional<BucketSnapshot> optionalTableBucketSnapshot =
                    getTableBucketSnapshot(tableBucket, snapshotIdLong);
            optionalTableBucketSnapshot.ifPresent(
                    snapshot -> snapshotAndIds.add(Tuple2.of(snapshot, snapshotIdLong)));
        }
        return snapshotAndIds;
    }

    /**
     * Get all the latest snapshot for the buckets of the table. If no any buckets found for the
     * table in zk, return empty. The key of the map is the bucket id, the value is the optional
     * latest snapshot, empty if there is no snapshot for the kv bucket.
     */
    public Map<Integer, Optional<BucketSnapshot>> getTableLatestBucketSnapshot(long tableId)
            throws Exception {
        Optional<TableAssignment> optTableAssignment = getTableAssignment(tableId);
        if (!optTableAssignment.isPresent()) {
            return Collections.emptyMap();
        } else {
            TableAssignment tableAssignment = optTableAssignment.get();
            return getBucketSnapshots(tableId, null, tableAssignment);
        }
    }

    public Map<Integer, Optional<BucketSnapshot>> getPartitionLatestBucketSnapshot(long partitionId)
            throws Exception {
        Optional<PartitionAssignment> optPartitionAssignment = getPartitionAssignment(partitionId);
        if (!optPartitionAssignment.isPresent()) {
            return Collections.emptyMap();
        } else {
            return getBucketSnapshots(
                    optPartitionAssignment.get().getTableId(),
                    partitionId,
                    optPartitionAssignment.get());
        }
    }

    private Map<Integer, Optional<BucketSnapshot>> getBucketSnapshots(
            long tableId, @Nullable Long partitionId, TableAssignment tableAssignment)
            throws Exception {
        Map<Integer, Optional<BucketSnapshot>> snapshots = new HashMap<>();
        // first, put as empty for all buckets
        for (Integer bucket : tableAssignment.getBuckets()) {
            snapshots.put(bucket, Optional.empty());
        }

        // get the bucket ids
        String bucketIdsPath =
                partitionId == null
                        ? BucketIdsZNode.pathOfTable(tableId)
                        : BucketIdsZNode.pathOfPartition(partitionId);
        // iterate all buckets
        for (String bucketIdStr : getChildren(bucketIdsPath)) {
            // get the bucket id
            int bucketId = Integer.parseInt(bucketIdStr);
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            // get the snapshot node for the bucket
            String bucketSnapshotPath = BucketSnapshotsZNode.path(tableBucket);
            // get all the snapshots for the bucket
            List<String> bucketSnapshots = getChildren(bucketSnapshotPath);

            Optional<Long> optLatestSnapshotId =
                    bucketSnapshots.stream().map(Long::parseLong).reduce(Math::max);
            Optional<BucketSnapshot> optTableBucketSnapshot = Optional.empty();
            if (optLatestSnapshotId.isPresent()) {
                optTableBucketSnapshot =
                        getTableBucketSnapshot(tableBucket, optLatestSnapshotId.get());
            }
            snapshots.put(bucketId, optTableBucketSnapshot);
        }
        return snapshots;
    }

    // --------------------------------------------------------------------------------------------
    // Writer
    // --------------------------------------------------------------------------------------------

    /** generate an unique id for writer. */
    public long getWriterIdAndIncrement() throws Exception {
        return writerIdCounter.getAndIncrement();
    }

    // --------------------------------------------------------------------------------------------
    // Remote log manifest handler
    // --------------------------------------------------------------------------------------------

    /**
     * Register or update the remote log manifest handle to zookeeper.
     *
     * <p>Note: If there is already a remote log manifest for the given table bucket, it will be
     * overwritten.
     */
    public void upsertRemoteLogManifestHandle(
            TableBucket tableBucket, RemoteLogManifestHandle remoteLogManifestHandle)
            throws Exception {
        String path = BucketRemoteLogsZNode.path(tableBucket);
        if (getRemoteLogManifestHandle(tableBucket).isPresent()) {
            zkClient.setData().forPath(path, BucketRemoteLogsZNode.encode(remoteLogManifestHandle));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, BucketRemoteLogsZNode.encode(remoteLogManifestHandle));
        }
    }

    public Optional<RemoteLogManifestHandle> getRemoteLogManifestHandle(TableBucket tableBucket)
            throws Exception {
        String path = BucketRemoteLogsZNode.path(tableBucket);
        return getOrEmpty(path).map(BucketRemoteLogsZNode::decode);
    }

    public void upsertLakeTableSnapshot(long tableId, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        String path = LakeTableZNode.path(tableId);
        Optional<LakeTableSnapshot> optLakeTableSnapshot = getLakeTableSnapshot(tableId);
        if (optLakeTableSnapshot.isPresent()) {
            // we need to merge current lake table snapshot with previous
            // since the current lake table snapshot request won't carry all
            // the bucket for the table. It will only carry the bucket that is written
            // after the previous commit
            LakeTableSnapshot previous = optLakeTableSnapshot.get();

            // merge log startup offset, current will override the previous
            Map<TableBucket, Long> bucketLogStartOffset =
                    new HashMap<>(previous.getBucketLogStartOffset());
            bucketLogStartOffset.putAll(lakeTableSnapshot.getBucketLogStartOffset());

            // merge log end offsets, current will override the previous
            Map<TableBucket, Long> bucketLogEndOffset =
                    new HashMap<>(previous.getBucketLogEndOffset());
            bucketLogEndOffset.putAll(lakeTableSnapshot.getBucketLogEndOffset());

            // merge max timestamp, current will override the previous
            Map<TableBucket, Long> bucketMaxTimestamp =
                    new HashMap<>(previous.getBucketMaxTimestamp());
            bucketMaxTimestamp.putAll(lakeTableSnapshot.getBucketMaxTimestamp());

            Map<Long, String> partitionNameById =
                    new HashMap<>(previous.getPartitionNameIdByPartitionId());
            partitionNameById.putAll(lakeTableSnapshot.getPartitionNameIdByPartitionId());

            lakeTableSnapshot =
                    new LakeTableSnapshot(
                            lakeTableSnapshot.getSnapshotId(),
                            lakeTableSnapshot.getTableId(),
                            bucketLogStartOffset,
                            bucketLogEndOffset,
                            bucketMaxTimestamp,
                            partitionNameById);
            zkClient.setData().forPath(path, LakeTableZNode.encode(lakeTableSnapshot));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, LakeTableZNode.encode(lakeTableSnapshot));
        }
    }

    public Optional<LakeTableSnapshot> getLakeTableSnapshot(long tableId) throws Exception {
        String path = LakeTableZNode.path(tableId);
        return getOrEmpty(path).map(LakeTableZNode::decode);
    }

    /**
     * Register or update the acl registration to zookeeper.
     *
     * <p>Note: If there is already an acl registration for the given resources and the acl version
     * is smaller than new one, it will be overwritten.(we need to compare the version before
     * upsert)
     *
     * @return the version of the acl node after upsert.
     */
    public int updateResourceAcl(
            Resource resource, Set<AccessControlEntry> accessControlEntries, int expectedVersion)
            throws Exception {
        String path = ResourceAclNode.path(resource);
        LOG.info("update acl node {} with value {}", resource, accessControlEntries);
        return zkClient.setData()
                .withVersion(expectedVersion)
                .forPath(path, ResourceAclNode.encode(new ResourceAcl(accessControlEntries)))
                .getVersion();
    }

    public void createResourceAcl(Resource resource, Set<AccessControlEntry> accessControlEntries)
            throws Exception {
        String path = ResourceAclNode.path(resource);
        LOG.info("insert acl node {} with value {}", resource, accessControlEntries);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, ResourceAclNode.encode(new ResourceAcl(accessControlEntries)));
    }

    /**
     * Retrieves the ACL (Access Control List) for a specific resource from ZooKeeper.
     *
     * @param resource the resource to query
     * @return an Optional containing the ResourceAcl if it exists, or empty if not found
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public VersionedAcls getResourceAclWithVersion(Resource resource) throws Exception {
        String path = ResourceAclNode.path(resource);
        try {
            Stat stat = new Stat();
            byte[] bytes = zkClient.getData().storingStatIn(stat).forPath(path);
            int zkVersion = stat.getVersion();
            Optional<ResourceAcl> resourceAcl =
                    Optional.ofNullable(bytes).map(ResourceAclNode::decode);

            return new VersionedAcls(
                    zkVersion,
                    resourceAcl.isPresent()
                            ? resourceAcl.get().getEntries()
                            : Collections.emptySet());
        } catch (KeeperException.NoNodeException e) {
            return new VersionedAcls(UNKNOWN_VERSION, Collections.emptySet());
        }
    }

    /**
     * Retrieves all resources of a specific type and their corresponding ACLs from ZooKeeper.
     *
     * @param resourceType the type of resource to query
     * @return a list of child node names representing resources of the given type
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public List<String> listResourcesByType(ResourceType resourceType) throws Exception {
        String path = ResourceAclNode.path(resourceType);
        return getChildren(path);
    }

    /**
     * Deletes the ACL (Access Control List) for a specific resource from ZooKeeper.
     *
     * @param resource the resource whose ACL should be deleted
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public void conditionalDeleteResourceAcl(Resource resource, int zkVersion) throws Exception {
        String path = ResourceAclNode.path(resource);
        zkClient.delete().withVersion(zkVersion).forPath(path);
    }

    public void insertAclChangeNotification(Resource resource) throws Exception {
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath(
                        AclChangeNotificationNode.pathPrefix(),
                        AclChangeNotificationNode.encode(resource));
        LOG.info("add acl change notification for resource {}  ", resource);
    }

    public Map<String, String> fetchEntityConfig() throws Exception {
        String path = ConfigEntityZNode.path();
        return getOrEmpty(path).map(ConfigEntityZNode::decode).orElse(new HashMap<>());
    }

    public void upsertServerEntityConfig(Map<String, String> configs) throws Exception {
        upsertEntityConfigs(configs);
    }

    public void upsertEntityConfigs(Map<String, String> configs) throws Exception {
        String path = ConfigEntityZNode.path();
        if (zkClient.checkExists().forPath(path) != null) {
            zkClient.setData().forPath(path, ConfigEntityZNode.encode(configs));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, ConfigEntityZNode.encode(configs));
        }

        LOG.info("upsert entity configs {}", configs);
        insertConfigChangeNotification();
    }

    public void insertConfigChangeNotification() throws Exception {
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath(
                        ZkData.ConfigEntityChangeNotificationSequenceZNode.pathPrefix(),
                        ZkData.ConfigEntityChangeNotificationSequenceZNode.encode());
    }

    // --------------------------------------------------------------------------------------------
    // Utils
    // --------------------------------------------------------------------------------------------

    /**
     * Gets all the child nodes at a given zk node path.
     *
     * @param path the path to list children
     * @return list of child node names
     */
    public List<String> getChildren(String path) throws Exception {
        try {
            return zkClient.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        }
    }

    /** Gets the data and stat of a given zk node path. */
    public Optional<Stat> getStat(String path) throws Exception {
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            return Optional.ofNullable(stat);
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    /** delete a path. */
    public void deletePath(String path) throws Exception {
        try {
            zkClient.delete().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
        }
    }

    public CuratorFramework getCuratorClient() {
        return zkClient;
    }

    // --------------------------------------------------------------------------------------------
    // Table and Partition Metadata
    // --------------------------------------------------------------------------------------------

    /**
     * Get bucket metadata for multiple tables in batch async way. The returned map only contains
     * tables those have assignments. If a table has no assignment yet (just been created), it will
     * not be included in the result map.
     */
    public Map<Long, List<BucketMetadata>> getBucketMetadataForTables(Collection<Long> tableIds)
            throws Exception {
        // The replica assignments map may not contain all tableIds
        Map<Long, List<BucketMetadata>> result = new HashMap<>();
        // initialize result with all table ids.
        tableIds.forEach(id -> result.put(id, new ArrayList<>()));
        Map<Long, TableAssignment> tablesAssignments = getTablesAssignments(tableIds);
        List<TableBucket> buckets = new ArrayList<>();
        tablesAssignments.forEach(
                (tableId, assignment) -> {
                    for (Integer bucketId : assignment.getBuckets()) {
                        buckets.add(new TableBucket(tableId, null, bucketId));
                    }
                });
        Map<TableBucket, LeaderAndIsr> leaderAndIsrs = getLeaderAndIsrs(buckets);
        // The LeaderAndIsr map may not contain all buckets, so we iterate on assignment buckets
        for (TableBucket bucket : buckets) {
            int bucketId = bucket.getBucket();
            long tableId = bucket.getTableId();
            // this might be null if the leader and isr not known yet.
            BucketMetadata bucketMetadata =
                    createBucketMetadata(
                            leaderAndIsrs,
                            bucket,
                            bucketId,
                            checkNotNull(tablesAssignments.get(tableId)));
            result.get(tableId).add(bucketMetadata);
        }
        return result;
    }

    /**
     * Get bucket metadata for multiple partitions in batch async way. The returned map only
     * contains partitions those have assignments. If a partition has no assignment yet (just been
     * created), it will not be included in the result map.
     */
    public Map<Long, List<BucketMetadata>> getBucketMetadataForPartitions(
            Collection<Long> partitionIds) throws Exception {
        // The replica assignments map may not contain all partitionIds
        Map<Long, PartitionAssignment> partitionsAssignments =
                getPartitionsAssignments(partitionIds);
        List<TableBucket> buckets = new ArrayList<>();
        partitionsAssignments.forEach(
                (partitionId, assignment) -> {
                    for (Integer bucketId : assignment.getBuckets()) {
                        buckets.add(
                                new TableBucket(assignment.getTableId(), partitionId, bucketId));
                    }
                });
        // The LeaderAndIsr map may not contain all buckets
        Map<TableBucket, LeaderAndIsr> leaderAndIsrs = getLeaderAndIsrs(buckets);
        Map<Long, List<BucketMetadata>> result = new HashMap<>();
        for (TableBucket bucket : buckets) {
            int bucketId = bucket.getBucket();
            long partitionId = checkNotNull(bucket.getPartitionId());
            BucketMetadata bucketMetadata =
                    createBucketMetadata(
                            leaderAndIsrs,
                            bucket,
                            bucketId,
                            checkNotNull(partitionsAssignments.get(partitionId)));
            result.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(bucketMetadata);
        }
        return result;
    }

    private BucketMetadata createBucketMetadata(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrs,
            TableBucket bucket,
            int bucketId,
            TableAssignment assignment) {
        // this might be null if the leader and isr not known yet.
        LeaderAndIsr leaderAndIsr = leaderAndIsrs.get(bucket);
        Integer leader = leaderAndIsr != null ? leaderAndIsr.leader() : null;
        Integer leaderEpoch = leaderAndIsr != null ? leaderAndIsr.leaderEpoch() : null;
        List<Integer> replicas = assignment.getBucketAssignments().get(bucketId).getReplicas();
        return new BucketMetadata(bucketId, leader, leaderEpoch, replicas);
    }

    /** Close the underlying ZooKeeperClient. */
    @Override
    public void close() {
        LOG.info("Closing...");
        if (curatorFrameworkWrapper != null) {
            curatorFrameworkWrapper.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // ZK batch async utils
    // -------------------------------------------------------------------------------------------

    /**
     * Send a pipelined sequence of requests and return a CompletableFuture for all their responses.
     *
     * <p>The watch flag on each outgoing request will be set if we've already registered a handler
     * for the path associated with the request.
     *
     * @param requests a sequence of requests to send
     * @param respCreator function to create response objects from curator events
     * @return CompletableFuture containing the responses for the requests
     */
    private <Resp extends ZkAsyncResponse, Req extends ZkAsyncRequest>
            CompletableFuture<List<Resp>> handleRequestInBackgroundAsync(
                    List<Req> requests, Function<CuratorEvent, Resp> respCreator) {
        if (requests == null || requests.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        CompletableFuture<List<Resp>> future = new CompletableFuture<>();
        BackgroundCallback callback = createBackgroundCallback(requests, respCreator, future);

        try {
            for (Req request : requests) {
                try {
                    inFlightRequests.acquire();
                    if (request instanceof ZkGetDataRequest) {
                        zkClient.getData().inBackground(callback).forPath(request.getPath());

                    } else if (request instanceof ZkGetChildrenRequest) {
                        zkClient.getChildren().inBackground(callback).forPath(request.getPath());

                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported request type: " + request.getClass());
                    }
                } catch (Exception e) {
                    inFlightRequests.release();
                    throw e;
                }
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Nonnull
    private <Resp extends ZkAsyncResponse, Req extends ZkAsyncRequest>
            BackgroundCallback createBackgroundCallback(
                    List<Req> requests,
                    Function<CuratorEvent, Resp> respCreator,
                    CompletableFuture<List<Resp>> future) {
        ArrayBlockingQueue<Resp> responseQueue = new ArrayBlockingQueue<>(requests.size());
        CountDownLatch countDownLatch = new CountDownLatch(requests.size());

        // Complete future when all responses received
        return (client, event) -> {
            try {
                Resp response = respCreator.apply(event);
                responseQueue.add(response);
                countDownLatch.countDown();

                // Complete future when all responses received
                if (countDownLatch.getCount() == 0) {
                    future.complete(new ArrayList<>(responseQueue));
                }
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                inFlightRequests.release();
            }
        };
    }

    /**
     * Gets the child nodes at given zk node paths in background.
     *
     * @param paths the paths to list children
     * @return list of async responses for each path
     * @throws Exception if there is an error during the operation
     */
    private List<ZkGetChildrenResponse> getChildrenInBackground(Collection<String> paths)
            throws Exception {
        List<ZkGetChildrenRequest> requests =
                paths.stream().map(ZkGetChildrenRequest::new).collect(Collectors.toList());
        return handleRequestInBackground(requests, ZkGetChildrenResponse::create);
    }

    /**
     * Gets the data of given zk node paths in background.
     *
     * @param paths the paths to fetch data
     * @return list of async responses for each path
     * @throws Exception if there is an error during the operation
     */
    private List<ZkGetDataResponse> getDataInBackground(Collection<String> paths) throws Exception {
        List<ZkGetDataRequest> requests =
                paths.stream().map(ZkGetDataRequest::new).collect(Collectors.toList());
        return handleRequestInBackground(requests, ZkGetDataResponse::create);
    }

    /**
     * Send a pipelined sequence of requests and wait for all of their responses synchronously in
     * background.
     *
     * <p>The watch flag on each outgoing request will be set if we've already registered a handler
     * for the path associated with the request.
     *
     * @param requests a sequence of requests to send and wait on.
     * @return the responses for the requests. If all requests have the same type, the responses
     *     will have the respective response type.
     */
    private <Resp extends ZkAsyncResponse, Req extends ZkAsyncRequest>
            List<Resp> handleRequestInBackground(
                    List<Req> requests, Function<CuratorEvent, Resp> respCreator) throws Exception {
        try {
            return handleRequestInBackgroundAsync(requests, respCreator).get();
        } catch (ExecutionException e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);
            if (cause instanceof Exception) {
                throw (Exception) cause;
            } else {
                throw new Exception("Async request handling failed", cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Exception("Request handling was interrupted", e);
        }
    }

    /**
     * Template method to process multiple ZooKeeper data responses with decoder.
     *
     * @param responses list of ZkGetDataResponse from ZooKeeper
     * @param keyExtractor function to extract key from response
     * @param decoder function to decode byte array to target type
     * @param operationName name of the operation for error messages
     * @param <K> the type of the result map key
     * @param <V> the type of the result map value
     * @return Map containing decoded results for successful responses
     */
    public static <K, V> Map<K, V> processGetDataResponses(
            List<ZkGetDataResponse> responses,
            Function<ZkGetDataResponse, K> keyExtractor,
            Function<byte[], V> decoder,
            String operationName) {
        Map<K, V> result = new HashMap<>();
        for (ZkGetDataResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK) {
                byte[] data = response.getData();
                if (data != null && data.length > 0) {
                    K key = keyExtractor.apply(response);
                    V value = decoder.apply(response.getData());
                    result.put(key, value);
                } else {
                    LOG.warn("Data is empty for path {}", response.getPath());
                }
            } else {
                LOG.warn(
                        "Failed to get {} for path {}: {}",
                        operationName,
                        response.getPath(),
                        response.getResultCode());
            }
        }
        return result;
    }

    /**
     * Template method to process multiple ZooKeeper children responses with decoder.
     *
     * @param responses list of ZkGetChildrenResponse from ZooKeeper
     * @param keyExtractor function to extract key from response
     * @param operationName name of the operation for error messages
     * @param <K> the type of the result map key
     * @return Map containing children lists for successful responses
     */
    public static <K> Map<K, List<String>> processGetChildrenResponses(
            List<ZkGetChildrenResponse> responses,
            Function<ZkGetChildrenResponse, K> keyExtractor,
            String operationName) {
        Map<K, List<String>> result = new HashMap<>();
        for (ZkGetChildrenResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK) {
                K key = keyExtractor.apply(response);
                result.put(key, response.getChildren());
            } else {
                LOG.warn(
                        "Failed to get {} for path {}: {}",
                        operationName,
                        response.getPath(),
                        response.getResultCode());
            }
        }
        return result;
    }
}
