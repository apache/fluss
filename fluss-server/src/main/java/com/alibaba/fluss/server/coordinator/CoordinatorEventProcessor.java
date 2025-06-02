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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FencedLeaderEpochException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidCoordinatorException;
import com.alibaba.fluss.exception.InvalidUpdateVersionException;
import com.alibaba.fluss.exception.UnknownTableOrBucketException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import com.alibaba.fluss.rpc.messages.PbCommitLakeTableSnapshotRespForTable;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.coordinator.event.AccessContextEvent;
import com.alibaba.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEvent;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEventManager;
import com.alibaba.fluss.server.coordinator.event.CreatePartitionEvent;
import com.alibaba.fluss.server.coordinator.event.CreateTableEvent;
import com.alibaba.fluss.server.coordinator.event.DeadTabletServerEvent;
import com.alibaba.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.DropPartitionEvent;
import com.alibaba.fluss.server.coordinator.event.DropTableEvent;
import com.alibaba.fluss.server.coordinator.event.EventProcessor;
import com.alibaba.fluss.server.coordinator.event.FencedCoordinatorEvent;
import com.alibaba.fluss.server.coordinator.event.NewTabletServerEvent;
import com.alibaba.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.watcher.TableChangeWatcher;
import com.alibaba.fluss.server.coordinator.event.watcher.TabletServerChangeWatcher;
import com.alibaba.fluss.server.coordinator.statemachine.ReplicaState;
import com.alibaba.fluss.server.coordinator.statemachine.ReplicaStateMachine;
import com.alibaba.fluss.server.coordinator.statemachine.TableBucketStateMachine;
import com.alibaba.fluss.server.entity.AdjustIsrResultForBucket;
import com.alibaba.fluss.server.entity.CommitLakeTableSnapshotData;
import com.alibaba.fluss.server.entity.CommitRemoteLogManifestData;
import com.alibaba.fluss.server.entity.DeleteReplicaResultForBucket;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotStore;
import com.alibaba.fluss.server.metadata.ClusterMetadataInfo;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metrics.group.CoordinatorMetricGroup;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.RemoteLogManifestHandle;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdsZNode;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.alibaba.fluss.server.coordinator.statemachine.BucketState.OfflineBucket;
import static com.alibaba.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.OfflineReplica;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionStarted;
import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionSuccessful;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeAdjustIsrResponse;
import static com.alibaba.fluss.utils.concurrent.FutureUtils.completeFromCallable;

/** An implementation for {@link EventProcessor}. */
@NotThreadSafe
public class CoordinatorEventProcessor implements EventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventProcessor.class);

    private final ZooKeeperClient zooKeeperClient;
    private final CoordinatorContext coordinatorContext;
    private final ReplicaStateMachine replicaStateMachine;
    private final TableBucketStateMachine tableBucketStateMachine;
    private final CoordinatorEventManager coordinatorEventManager;
    private final MetadataManager metadataManager;
    private final TableManager tableManager;
    private final AutoPartitionManager autoPartitionManager;
    private final LakeTableTieringManager lakeTableTieringManager;
    private final TableChangeWatcher tableChangeWatcher;
    private final CoordinatorChannelManager coordinatorChannelManager;
    private final TabletServerChangeWatcher tabletServerChangeWatcher;
    private final ServerMetadataCache serverMetadataCache;
    private final CoordinatorRequestBatch coordinatorRequestBatch;
    private final CoordinatorMetricGroup coordinatorMetricGroup;
    private final String internalListenerName;

    private final CompletedSnapshotStoreManager completedSnapshotStoreManager;

    // in normal case, it won't be null, but from I can see, it'll only be null in unit test
    // since the we won't register a coordinator node in zk.
    // todo: may remove the nullable in the future
    private @Nullable ServerInfo coordinatorServerInfo;

    // metrics
    private volatile int tabletServerCount;
    private volatile int offlineBucketCount;
    private volatile int tableCount;
    private volatile int bucketCount;
    private volatile int replicasToDeleteCount;

    public CoordinatorEventProcessor(
            ZooKeeperClient zooKeeperClient,
            ServerMetadataCache serverMetadataCache,
            CoordinatorChannelManager coordinatorChannelManager,
            AutoPartitionManager autoPartitionManager,
            LakeTableTieringManager lakeTableTieringManager,
            CoordinatorMetricGroup coordinatorMetricGroup,
            Configuration conf,
            ExecutorService ioExecutor) {
        this(
                zooKeeperClient,
                serverMetadataCache,
                coordinatorChannelManager,
                new CoordinatorContext(),
                autoPartitionManager,
                lakeTableTieringManager,
                coordinatorMetricGroup,
                conf,
                ioExecutor);
    }

    public CoordinatorEventProcessor(
            ZooKeeperClient zooKeeperClient,
            ServerMetadataCache serverMetadataCache,
            CoordinatorChannelManager coordinatorChannelManager,
            CoordinatorContext coordinatorContext,
            AutoPartitionManager autoPartitionManager,
            LakeTableTieringManager lakeTableTieringManager,
            CoordinatorMetricGroup coordinatorMetricGroup,
            Configuration conf,
            ExecutorService ioExecutor) {
        this.zooKeeperClient = zooKeeperClient;
        this.serverMetadataCache = serverMetadataCache;
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.coordinatorContext = coordinatorContext;
        this.coordinatorEventManager = new CoordinatorEventManager(this, coordinatorMetricGroup);
        this.replicaStateMachine =
                new ReplicaStateMachine(
                        coordinatorContext,
                        new CoordinatorRequestBatch(
                                coordinatorChannelManager, coordinatorEventManager),
                        zooKeeperClient);
        this.tableBucketStateMachine =
                new TableBucketStateMachine(
                        coordinatorContext,
                        new CoordinatorRequestBatch(
                                coordinatorChannelManager, coordinatorEventManager),
                        zooKeeperClient);
        this.metadataManager = new MetadataManager(zooKeeperClient, conf);

        this.tableManager =
                new TableManager(
                        metadataManager,
                        coordinatorContext,
                        replicaStateMachine,
                        tableBucketStateMachine,
                        new RemoteStorageCleaner(conf, ioExecutor));
        this.tableChangeWatcher = new TableChangeWatcher(zooKeeperClient, coordinatorEventManager);
        this.tabletServerChangeWatcher =
                new TabletServerChangeWatcher(zooKeeperClient, coordinatorEventManager);
        this.coordinatorRequestBatch =
                new CoordinatorRequestBatch(coordinatorChannelManager, coordinatorEventManager);
        this.completedSnapshotStoreManager =
                new CompletedSnapshotStoreManager(
                        conf.getInt(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS),
                        ioExecutor,
                        zooKeeperClient);
        this.autoPartitionManager = autoPartitionManager;
        this.lakeTableTieringManager = lakeTableTieringManager;
        this.coordinatorMetricGroup = coordinatorMetricGroup;
        this.internalListenerName = conf.getString(ConfigOptions.INTERNAL_LISTENER_NAME);
        registerMetrics();
    }

    private void registerMetrics() {
        coordinatorMetricGroup.gauge(MetricNames.ACTIVE_COORDINATOR_COUNT, () -> 1);
        coordinatorMetricGroup.gauge(
                MetricNames.ACTIVE_TABLET_SERVER_COUNT, () -> tabletServerCount);
        coordinatorMetricGroup.gauge(MetricNames.OFFLINE_BUCKET_COUNT, () -> offlineBucketCount);
        coordinatorMetricGroup.gauge(MetricNames.BUCKET_COUNT, () -> bucketCount);
        coordinatorMetricGroup.gauge(MetricNames.TABLE_COUNT, () -> tableCount);
        coordinatorMetricGroup.gauge(
                MetricNames.REPLICAS_TO_DELETE_COUNT, () -> replicasToDeleteCount);
    }

    public CoordinatorEventManager getCoordinatorEventManager() {
        return coordinatorEventManager;
    }

    public void startup() {
        coordinatorServerInfo = getCoordinatorServerInfo();
        // start watchers first so that we won't miss node in zk;
        tabletServerChangeWatcher.start();
        tableChangeWatcher.start();
        LOG.info("Initializing coordinator context.");
        try {
            initCoordinatorContext();
        } catch (Exception e) {
            throw new FlussRuntimeException("Fail to initialize coordinator context.", e);
        }

        // We need to send UpdateMetadataRequest after the coordinator context is initialized and
        // before the state machines in tableManager
        // are started. This is because tablet servers need to receive the list of live tablet
        // servers from UpdateMetadataRequest before
        // they can process the LeaderRequests that are generated by
        // replicaStateMachine.startup() and
        // partitionStateMachine.startup().
        LOG.info("Sending update metadata request.");
        updateServerMetadataCache(
                Optional.ofNullable(coordinatorServerInfo),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()));

        // start table manager
        tableManager.startup();
        updateMetrics();

        // start the event manager which will then process the event
        coordinatorEventManager.start();
    }

    public void shutdown() {
        // close the event manager
        coordinatorEventManager.close();
        onShutdown();
    }

    private ServerInfo getCoordinatorServerInfo() {
        try {
            return zooKeeperClient
                    .getCoordinatorAddress()
                    .map(
                            coordinatorAddress ->
                                    // TODO we set id to 0 as that CoordinatorServer don't support
                                    // HA, if we support HA, we need to set id to the config
                                    // CoordinatorServer id to avoid node drift.
                                    new ServerInfo(
                                            0,
                                            null, // For coordinatorServer, no rack info
                                            coordinatorAddress.getEndpoints(),
                                            ServerType.COORDINATOR))
                    .orElseGet(
                            () -> {
                                LOG.error("Coordinator server address is empty in zookeeper.");
                                return null;
                            });
        } catch (Exception e) {
            throw new FlussRuntimeException("Get coordinator address failed.", e);
        }
    }

    public int getCoordinatorEpoch() {
        return coordinatorContext.getCoordinatorEpoch();
    }

    private void initCoordinatorContext() throws Exception {
        long start = System.currentTimeMillis();
        // get all tablet server's
        int[] currentServers = zooKeeperClient.getSortedTabletServerList();
        List<ServerInfo> tabletServerInfos = new ArrayList<>();
        List<ServerNode> internalServerNodes = new ArrayList<>();
        for (int server : currentServers) {
            TabletServerRegistration registration = zooKeeperClient.getTabletServer(server).get();
            ServerInfo serverInfo =
                    new ServerInfo(
                            server,
                            registration.getRack(),
                            registration.getEndpoints(),
                            ServerType.TABLET_SERVER);
            // Get internal listener endpoint to send request to tablet server.
            Endpoint internalEndpoint = serverInfo.endpoint(internalListenerName);
            if (internalEndpoint == null) {
                LOG.error(
                        "Can not find endpoint for listener name {} for tablet server {}",
                        internalListenerName,
                        serverInfo);
                continue;
            }
            tabletServerInfos.add(serverInfo);
            internalServerNodes.add(
                    new ServerNode(
                            server,
                            internalEndpoint.getHost(),
                            internalEndpoint.getPort(),
                            ServerType.TABLET_SERVER));
        }

        coordinatorContext.setLiveTabletServers(tabletServerInfos);
        // init tablet server channels
        coordinatorChannelManager.startup(internalServerNodes);

        // load all tables
        List<TableInfo> autoPartitionTables = new ArrayList<>();
        List<Tuple2<TableInfo, Long>> lakeTables = new ArrayList<>();
        for (String database : metadataManager.listDatabases()) {
            for (String tableName : metadataManager.listTables(database)) {
                TablePath tablePath = TablePath.of(database, tableName);
                TableInfo tableInfo = metadataManager.getTable(tablePath);
                coordinatorContext.putTablePath(tableInfo.getTableId(), tablePath);
                coordinatorContext.putTableInfo(tableInfo);
                if (tableInfo.getTableConfig().isDataLakeEnabled()) {
                    // always set to current time,
                    // todo: should get from the last lake snapshot
                    lakeTables.add(Tuple2.of(tableInfo, System.currentTimeMillis()));
                }
                if (tableInfo.isPartitioned()) {
                    Map<String, Long> partitions =
                            zooKeeperClient.getPartitionNameAndIds(tablePath);
                    for (Map.Entry<String, Long> partition : partitions.entrySet()) {
                        // put partition info to coordinator context
                        coordinatorContext.putPartition(partition.getValue(), partition.getKey());
                    }
                    // if the table is auto partition, put the partitions info
                    if (tableInfo
                            .getTableConfig()
                            .getAutoPartitionStrategy()
                            .isAutoPartitionEnabled()) {
                        autoPartitionTables.add(tableInfo);
                    }
                }
            }
        }
        autoPartitionManager.initAutoPartitionTables(autoPartitionTables);
        lakeTableTieringManager.initWithLakeTables(lakeTables);

        // load all assignment
        loadTableAssignment();
        loadPartitionAssignment();
        long end = System.currentTimeMillis();
        LOG.info("Current total {} tables in the cluster.", coordinatorContext.allTables().size());
        LOG.info(
                "Detect tables {} to be deleted after initializing coordinator context. ",
                coordinatorContext.getTablesToBeDeleted());
        LOG.info(
                "Detect partition {} to be deleted after initializing coordinator context. ",
                coordinatorContext.getPartitionsToBeDeleted());
        LOG.info("End initializing coordinator context, cost {}ms", end - start);
    }

    private void loadTableAssignment() throws Exception {
        List<String> assignmentTables = zooKeeperClient.getChildren(TableIdsZNode.path());
        Set<Long> deletedTables = new HashSet<>();
        for (String tableIdStr : assignmentTables) {
            long tableId = Long.parseLong(tableIdStr);
            // if table id not in current coordinator context,
            // we'll consider it as deleted
            if (!coordinatorContext.containsTableId(tableId)) {
                deletedTables.add(tableId);
            }
            Optional<TableAssignment> optAssignment = zooKeeperClient.getTableAssignment(tableId);
            if (optAssignment.isPresent()) {
                TableAssignment tableAssignment = optAssignment.get();
                loadAssignment(tableId, tableAssignment, null);
            } else {
                LOG.warn(
                        "Can't get the assignment for table {} with id {}.",
                        coordinatorContext.getTablePathById(tableId),
                        tableId);
            }
        }
        coordinatorContext.queueTableDeletion(deletedTables);
    }

    private void loadPartitionAssignment() throws Exception {
        // load all assignment
        List<String> partitionAssignmentNodes =
                zooKeeperClient.getChildren(PartitionIdsZNode.path());
        Set<TablePartition> deletedPartitions = new HashSet<>();
        for (String partitionIdStr : partitionAssignmentNodes) {
            long partitionId = Long.parseLong(partitionIdStr);
            Optional<PartitionAssignment> optAssignment =
                    zooKeeperClient.getPartitionAssignment(partitionId);
            if (!optAssignment.isPresent()) {
                LOG.warn("Can't get the assignment for table partition {}.", partitionId);
                continue;
            }
            PartitionAssignment partitionAssignment = optAssignment.get();
            long tableId = partitionAssignment.getTableId();
            // partition id doesn't exist in coordinator context, consider it as deleted
            if (!coordinatorContext.containsPartitionId(partitionId)) {
                deletedPartitions.add(new TablePartition(tableId, partitionId));
            }
            loadAssignment(tableId, optAssignment.get(), partitionId);
        }
        coordinatorContext.queuePartitionDeletion(deletedPartitions);
    }

    private void loadAssignment(
            long tableId, TableAssignment tableAssignment, @Nullable Long partitionId)
            throws Exception {
        for (Map.Entry<Integer, BucketAssignment> entry :
                tableAssignment.getBucketAssignments().entrySet()) {
            int bucketId = entry.getKey();
            BucketAssignment bucketAssignment = entry.getValue();
            // put the assignment information to context
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            coordinatorContext.updateBucketReplicaAssignment(
                    tableBucket, bucketAssignment.getReplicas());
            Optional<LeaderAndIsr> optLeaderAndIsr = zooKeeperClient.getLeaderAndIsr(tableBucket);
            // update bucket LeaderAndIsr info
            optLeaderAndIsr.ifPresent(
                    leaderAndIsr ->
                            coordinatorContext.putBucketLeaderAndIsr(tableBucket, leaderAndIsr));
        }
    }

    private void onShutdown() {
        // first shutdown table manager
        tableManager.shutdown();

        // then reset coordinatorContext
        coordinatorContext.resetContext();

        // then stop watchers
        tableChangeWatcher.stop();
        tabletServerChangeWatcher.stop();
    }

    @Override
    public void process(CoordinatorEvent event) {
        try {
            if (event instanceof CreateTableEvent) {
                processCreateTable((CreateTableEvent) event);
            } else if (event instanceof CreatePartitionEvent) {
                processCreatePartition((CreatePartitionEvent) event);
            } else if (event instanceof DropTableEvent) {
                processDropTable((DropTableEvent) event);
            } else if (event instanceof DropPartitionEvent) {
                processDropPartition((DropPartitionEvent) event);
            } else if (event instanceof NotifyLeaderAndIsrResponseReceivedEvent) {
                processNotifyLeaderAndIsrResponseReceivedEvent(
                        (NotifyLeaderAndIsrResponseReceivedEvent) event);
            } else if (event instanceof DeleteReplicaResponseReceivedEvent) {
                processDeleteReplicaResponseReceived((DeleteReplicaResponseReceivedEvent) event);
            } else if (event instanceof NewTabletServerEvent) {
                processNewTabletServer((NewTabletServerEvent) event);
            } else if (event instanceof DeadTabletServerEvent) {
                processDeadTabletServer((DeadTabletServerEvent) event);
            } else if (event instanceof AdjustIsrReceivedEvent) {
                AdjustIsrReceivedEvent adjustIsrReceivedEvent = (AdjustIsrReceivedEvent) event;
                CompletableFuture<AdjustIsrResponse> callback =
                        adjustIsrReceivedEvent.getRespCallback();
                completeFromCallable(
                        callback,
                        () ->
                                makeAdjustIsrResponse(
                                        tryProcessAdjustIsr(
                                                adjustIsrReceivedEvent.getLeaderAndIsrMap())));
            } else if (event instanceof CommitKvSnapshotEvent) {
                CommitKvSnapshotEvent commitKvSnapshotEvent = (CommitKvSnapshotEvent) event;
                CompletableFuture<CommitKvSnapshotResponse> callback =
                        commitKvSnapshotEvent.getRespCallback();
                completeFromCallable(
                        callback, () -> tryProcessCommitKvSnapshot(commitKvSnapshotEvent));
            } else if (event instanceof CommitRemoteLogManifestEvent) {
                CommitRemoteLogManifestEvent commitRemoteLogManifestEvent =
                        (CommitRemoteLogManifestEvent) event;
                completeFromCallable(
                        commitRemoteLogManifestEvent.getRespCallback(),
                        () -> tryProcessCommitRemoteLogManifest(commitRemoteLogManifestEvent));
            } else if (event instanceof CommitLakeTableSnapshotEvent) {
                CommitLakeTableSnapshotEvent commitLakeTableSnapshotEvent =
                        (CommitLakeTableSnapshotEvent) event;
                completeFromCallable(
                        commitLakeTableSnapshotEvent.getRespCallback(),
                        () -> tryProcessCommitLakeTableSnapshot(commitLakeTableSnapshotEvent));
            } else if (event instanceof AccessContextEvent) {
                AccessContextEvent<?> accessContextEvent = (AccessContextEvent<?>) event;
                processAccessContext(accessContextEvent);
            } else {
                LOG.warn("Unknown event type: {}", event.getClass().getName());
            }
        } finally {
            updateMetrics();
        }
    }

    private void updateMetrics() {
        tabletServerCount = coordinatorContext.getLiveTabletServers().size();
        tableCount = coordinatorContext.allTables().size();
        bucketCount = coordinatorContext.bucketLeaderAndIsr().size();
        offlineBucketCount = coordinatorContext.getOfflineBucketCount();

        int replicasToDeletes = 0;
        // for replica in partitions to be deleted
        for (TablePartition tablePartition : coordinatorContext.getPartitionsToBeDeleted()) {
            for (TableBucketReplica replica :
                    coordinatorContext.getAllReplicasForPartition(
                            tablePartition.getTableId(), tablePartition.getPartitionId())) {
                replicasToDeletes =
                        isReplicaToDelete(replica) ? replicasToDeletes + 1 : replicasToDeletes;
            }
        }
        // for replica in tables to be deleted
        for (long tableId : coordinatorContext.getTablesToBeDeleted()) {
            for (TableBucketReplica replica : coordinatorContext.getAllReplicasForTable(tableId)) {
                replicasToDeletes =
                        isReplicaToDelete(replica) ? replicasToDeletes + 1 : replicasToDeletes;
            }
        }
        this.replicasToDeleteCount = replicasToDeletes;
    }

    private boolean isReplicaToDelete(TableBucketReplica replica) {
        ReplicaState replicaState = coordinatorContext.getReplicaState(replica);
        return replicaState != null && replicaState != ReplicaDeletionSuccessful;
    }

    private void processCreateTable(CreateTableEvent createTableEvent) {
        long tableId = createTableEvent.getTableInfo().getTableId();
        // skip the table if it already exists
        if (coordinatorContext.containsTableId(tableId)) {
            return;
        }
        TableInfo tableInfo = createTableEvent.getTableInfo();
        coordinatorContext.putTableInfo(tableInfo);
        tableManager.onCreateNewTable(
                tableInfo.getTablePath(),
                tableInfo.getTableId(),
                createTableEvent.getTableAssignment());
        if (createTableEvent.isAutoPartitionTable()) {
            autoPartitionManager.addAutoPartitionTable(tableInfo, true);
        }
        if (tableInfo.getTableConfig().isDataLakeEnabled()) {
            lakeTableTieringManager.addNewLakeTable(tableInfo);
        }
    }

    private void processCreatePartition(CreatePartitionEvent createPartitionEvent) {
        long partitionId = createPartitionEvent.getPartitionId();
        // skip the partition if it already exists
        if (coordinatorContext.containsPartitionId(partitionId)) {
            return;
        }

        long tableId = createPartitionEvent.getTableId();
        String partitionName = createPartitionEvent.getPartitionName();
        tableManager.onCreateNewPartition(
                createPartitionEvent.getTablePath(),
                tableId,
                createPartitionEvent.getPartitionId(),
                partitionName,
                createPartitionEvent.getPartitionAssignment());
        autoPartitionManager.addPartition(tableId, partitionName);
    }

    private void processDropTable(DropTableEvent dropTableEvent) {
        // If this is a primary key table, drop the kv snapshot store.
        TableInfo dropTableInfo = coordinatorContext.getTableInfoById(dropTableEvent.getTableId());
        if (dropTableInfo.hasPrimaryKey()) {
            Set<TableBucket> deleteTableBuckets =
                    coordinatorContext.getAllBucketsForTable(dropTableEvent.getTableId());
            completedSnapshotStoreManager.removeCompletedSnapshotStoreByTableBuckets(
                    deleteTableBuckets);
        }

        coordinatorContext.queueTableDeletion(Collections.singleton(dropTableEvent.getTableId()));
        tableManager.onDeleteTable(dropTableEvent.getTableId());
        if (dropTableEvent.isAutoPartitionTable()) {
            autoPartitionManager.removeAutoPartitionTable(dropTableEvent.getTableId());
        }
        if (dropTableEvent.isDataLakeEnabled()) {
            lakeTableTieringManager.removeLakeTable(dropTableEvent.getTableId());
        }
    }

    private void processDropPartition(DropPartitionEvent dropPartitionEvent) {
        long tableId = dropPartitionEvent.getTableId();
        TablePartition tablePartition =
                new TablePartition(tableId, dropPartitionEvent.getPartitionId());

        // If this is a primary key table partition, drop the kv snapshot store.
        TableInfo dropTableInfo = coordinatorContext.getTableInfoById(tableId);
        if (dropTableInfo.hasPrimaryKey()) {
            Set<TableBucket> deleteTableBuckets =
                    coordinatorContext.getAllBucketsForPartition(
                            tableId, dropPartitionEvent.getPartitionId());
            completedSnapshotStoreManager.removeCompletedSnapshotStoreByTableBuckets(
                    deleteTableBuckets);
        }

        coordinatorContext.queuePartitionDeletion(Collections.singleton(tablePartition));
        tableManager.onDeletePartition(tableId, dropPartitionEvent.getPartitionId());
        autoPartitionManager.removePartition(tableId, dropPartitionEvent.getPartitionName());
    }

    private void processDeleteReplicaResponseReceived(
            DeleteReplicaResponseReceivedEvent deleteReplicaResponseReceivedEvent) {
        List<DeleteReplicaResultForBucket> deleteReplicaResultForBuckets =
                deleteReplicaResponseReceivedEvent.getDeleteReplicaResults();

        Set<TableBucketReplica> failDeletedReplicas = new HashSet<>();
        Set<TableBucketReplica> successDeletedReplicas = new HashSet<>();
        for (DeleteReplicaResultForBucket deleteReplicaResultForBucket :
                deleteReplicaResultForBuckets) {
            TableBucketReplica tableBucketReplica =
                    deleteReplicaResultForBucket.getTableBucketReplica();
            if (deleteReplicaResultForBucket.succeeded()) {
                successDeletedReplicas.add(tableBucketReplica);
            } else {
                failDeletedReplicas.add(tableBucketReplica);
            }
        }
        // clear the fail deleted number for the success deleted replicas
        coordinatorContext.clearFailDeleteNumbers(successDeletedReplicas);

        // pick up the replicas to retry delete and replicas that considered as success delete
        Tuple2<Set<TableBucketReplica>, Set<TableBucketReplica>>
                retryDeleteAndSuccessDeleteReplicas =
                        coordinatorContext.retryDeleteAndSuccessDeleteReplicas(failDeletedReplicas);

        // transmit to deletion started for retry delete replicas
        replicaStateMachine.handleStateChanges(
                retryDeleteAndSuccessDeleteReplicas.f0, ReplicaDeletionStarted);

        // add all the replicas that considered as success delete to success deleted replicas
        successDeletedReplicas.addAll(retryDeleteAndSuccessDeleteReplicas.f1);
        // transmit to deletion successful for success deleted replicas
        replicaStateMachine.handleStateChanges(successDeletedReplicas, ReplicaDeletionSuccessful);
        // if any success deletion, we can resume
        if (!successDeletedReplicas.isEmpty()) {
            tableManager.resumeDeletions();
        }
    }

    private void processNotifyLeaderAndIsrResponseReceivedEvent(
            NotifyLeaderAndIsrResponseReceivedEvent notifyLeaderAndIsrResponseReceivedEvent) {
        // get the server that receives the response
        int serverId = notifyLeaderAndIsrResponseReceivedEvent.getResponseServerId();
        Set<TableBucketReplica> offlineReplicas = new HashSet<>();
        // get all the results for each bucket
        List<NotifyLeaderAndIsrResultForBucket> notifyLeaderAndIsrResultForBuckets =
                notifyLeaderAndIsrResponseReceivedEvent.getNotifyLeaderAndIsrResultForBuckets();
        for (NotifyLeaderAndIsrResultForBucket notifyLeaderAndIsrResultForBucket :
                notifyLeaderAndIsrResultForBuckets) {
            // if the error code is not none, we will consider it as offline
            if (notifyLeaderAndIsrResultForBucket.failed()) {
                offlineReplicas.add(
                        new TableBucketReplica(
                                notifyLeaderAndIsrResultForBucket.getTableBucket(), serverId));
            }
        }
        if (!offlineReplicas.isEmpty()) {
            // trigger replicas to offline
            onReplicaBecomeOffline(offlineReplicas);
        }
    }

    private void onReplicaBecomeOffline(Set<TableBucketReplica> offlineReplicas) {
        LOG.info("The replica {} become offline.", offlineReplicas);
        for (TableBucketReplica offlineReplica : offlineReplicas) {
            coordinatorContext.addOfflineBucketInServer(
                    offlineReplica.getTableBucket(), offlineReplica.getReplica());
        }

        Set<TableBucket> bucketWithOfflineLeader = new HashSet<>();
        // for the offline replicas, if the bucket's leader is equal to the offline replica,
        // we consider it as offline
        for (TableBucketReplica offlineReplica : offlineReplicas) {
            coordinatorContext
                    .getBucketLeaderAndIsr(offlineReplica.getTableBucket())
                    .ifPresent(
                            leaderAndIsr -> {
                                if (leaderAndIsr.leader() == offlineReplica.getReplica()) {
                                    bucketWithOfflineLeader.add(offlineReplica.getTableBucket());
                                }
                            });
        }
        // for the bucket with offline leader, we set it to offline and
        // then try to transmit to Online
        // set it to offline as the leader replica fail
        tableBucketStateMachine.handleStateChange(bucketWithOfflineLeader, OfflineBucket);
        // try to change it to online again, which may trigger re-election
        tableBucketStateMachine.handleStateChange(bucketWithOfflineLeader, OnlineBucket);

        // for all the offline replicas, do nothing other than set it to offline currently like
        // kafka, todo: but we may need to select another tablet server to put
        // replica
        replicaStateMachine.handleStateChanges(offlineReplicas, OfflineReplica);
    }

    private void processNewTabletServer(NewTabletServerEvent newTabletServerEvent) {
        // NOTE: we won't need to detect bounced tablet servers like Kafka as we won't
        // miss the event of tablet server un-register and register again since we can
        // listener the children created and deleted in zk node.

        // Also, Kafka use broker epoch to make it can reject the LeaderAndIsrRequest,
        // UpdateMetadataRequest and StopReplicaRequest
        // whose epoch < current broker epoch.
        // See more in KIP-380 & https://github.com/apache/kafka/pull/5821
        // but for the case of StopReplicaRequest in Fluss, although we will send
        // stop replica after tablet server is controlled shutdown, but we will detect
        // it start when it bounce and send start replica request again. It seems not a
        // problem in Fluss;
        // TODO: revisit here to see whether we really need epoch for tablet server like kafka
        // when we finish the logic of tablet server
        ServerInfo serverInfo = newTabletServerEvent.getServerInfo();
        int tabletServerId = serverInfo.id();
        if (coordinatorContext.getLiveTabletServers().containsKey(serverInfo.id())) {
            // if the dead server is already in live servers, return directly
            // it may happen during coordinator server initiation, the watcher watch a new tablet
            // server register event and put it to event manager, but after that, the coordinator
            // server read
            // all tablet server nodes registered which contain the tablet server a; in this case,
            // we can ignore it.
            return;
        }

        // process new tablet server
        LOG.info("New tablet server callback for tablet server {}", tabletServerId);

        coordinatorContext.removeOfflineBucketInServer(tabletServerId);
        coordinatorContext.addLiveTabletServer(serverInfo);

        ServerNode serverNode = serverInfo.nodeOrThrow(internalListenerName);
        coordinatorChannelManager.addTabletServer(serverNode);

        // update server metadata cache.
        updateServerMetadataCache(
                Optional.ofNullable(coordinatorServerInfo),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()));

        // when a new tablet server comes up, we need to get all replicas of the server
        // and transmit them to online
        Set<TableBucketReplica> replicas =
                coordinatorContext.replicasOnTabletServer(tabletServerId).stream()
                        .filter(
                                // don't consider replicas to be deleted
                                tableBucketReplica ->
                                        !coordinatorContext.isToBeDeleted(
                                                tableBucketReplica.getTableBucket()))
                        .collect(Collectors.toSet());

        replicaStateMachine.handleStateChanges(replicas, OnlineReplica);

        // when a new tablet server comes up, we trigger leader election for all new
        // and offline partitions to see if those tablet servers become leaders for some/all
        // of those
        tableBucketStateMachine.triggerOnlineBucketStateChange();
    }

    private void processDeadTabletServer(DeadTabletServerEvent deadTabletServerEvent) {
        int tabletServerId = deadTabletServerEvent.getServerId();
        if (!coordinatorContext.getLiveTabletServers().containsKey(tabletServerId)) {
            // if the dead server is already not in live servers, return directly
            // it may happen during coordinator server initiation, the watcher watch a new tablet
            // server unregister event, but the coordinator server also don't read it from zk and
            // haven't init to coordinator context
            return;
        }
        // process dead tablet server
        LOG.info("Tablet server failure callback for {}.", tabletServerId);
        coordinatorContext.removeOfflineBucketInServer(tabletServerId);
        coordinatorContext.removeLiveTabletServer(tabletServerId);
        coordinatorChannelManager.removeTabletServer(tabletServerId);

        updateServerMetadataCache(
                Optional.ofNullable(coordinatorServerInfo),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()));

        TableBucketStateMachine tableBucketStateMachine = tableManager.getTableBucketStateMachine();
        // get all table bucket whose leader is in this server and it not to be deleted
        Set<TableBucket> bucketsWithOfflineLeader =
                coordinatorContext.getBucketsWithLeaderIn(tabletServerId).stream()
                        .filter(
                                // don't consider buckets to be deleted
                                tableBucket -> !coordinatorContext.isToBeDeleted(tableBucket))
                        .collect(Collectors.toSet());
        // trigger offline state for all the table buckets whose current leader
        // is the failed tablet server
        tableBucketStateMachine.handleStateChange(bucketsWithOfflineLeader, OfflineBucket);

        // trigger online state changes for offline or new buckets
        tableBucketStateMachine.triggerOnlineBucketStateChange();

        // get all replicas in this server and is not to be deleted
        Set<TableBucketReplica> replicas =
                coordinatorContext.replicasOnTabletServer(tabletServerId).stream()
                        .filter(
                                // don't consider replicas to be deleted
                                tableBucketReplica ->
                                        !coordinatorContext.isToBeDeleted(
                                                tableBucketReplica.getTableBucket()))
                        .collect(Collectors.toSet());

        // trigger OfflineReplica state change for those newly offline replicas
        replicaStateMachine.handleStateChanges(replicas, OfflineReplica);
    }

    private List<AdjustIsrResultForBucket> tryProcessAdjustIsr(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrList) {
        // TODO verify leader epoch.

        List<AdjustIsrResultForBucket> result = new ArrayList<>();
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : leaderAndIsrList.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            LeaderAndIsr tryAdjustLeaderAndIsr = entry.getValue();

            try {
                validateLeaderAndIsr(tableBucket, tryAdjustLeaderAndIsr);
            } catch (Exception e) {
                result.add(new AdjustIsrResultForBucket(tableBucket, ApiError.fromThrowable(e)));
                continue;
            }

            // Do the updates in ZK.
            LeaderAndIsr currentLeaderAndIsr =
                    coordinatorContext
                            .getBucketLeaderAndIsr(tableBucket)
                            .orElseThrow(
                                    () ->
                                            new FlussRuntimeException(
                                                    "Leader not found for table bucket "
                                                            + tableBucket));
            LeaderAndIsr newLeaderAndIsr =
                    new LeaderAndIsr(
                            // the leaderEpoch in request has been validated to be equal to current
                            // leaderEpoch, which means the leader is still the same, so we use
                            // leader and leaderEpoch in currentLeaderAndIsr.
                            currentLeaderAndIsr.leader(),
                            currentLeaderAndIsr.leaderEpoch(),
                            // TODO: reject the request if there is a replica in ISR is not online,
                            //  see KIP-841.
                            tryAdjustLeaderAndIsr.isr(),
                            coordinatorContext.getCoordinatorEpoch(),
                            currentLeaderAndIsr.bucketEpoch() + 1);
            try {
                zooKeeperClient.updateLeaderAndIsr(tableBucket, newLeaderAndIsr);
            } catch (Exception e) {
                LOG.error("Error when register leader and isr.", e);
                result.add(new AdjustIsrResultForBucket(tableBucket, ApiError.fromThrowable(e)));
            }

            // update coordinator leader and isr cache.
            coordinatorContext.putBucketLeaderAndIsr(tableBucket, newLeaderAndIsr);

            // TODO update metadata for all alive tablet servers.

            // Successful return.
            result.add(new AdjustIsrResultForBucket(tableBucket, newLeaderAndIsr));
        }
        return result;
    }

    /**
     * Validate the new leader and isr.
     *
     * @param tableBucket table bucket
     * @param newLeaderAndIsr new leader and isr
     */
    private void validateLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr newLeaderAndIsr) {
        if (coordinatorContext.getTablePathById(tableBucket.getTableId()) == null) {
            throw new UnknownTableOrBucketException("Unknown table id " + tableBucket.getTableId());
        }

        Optional<LeaderAndIsr> leaderAndIsrOpt =
                coordinatorContext.getBucketLeaderAndIsr(tableBucket);
        if (!leaderAndIsrOpt.isPresent()) {
            throw new UnknownTableOrBucketException("Unknown table or bucket " + tableBucket);
        } else {
            LeaderAndIsr currentLeaderAndIsr = leaderAndIsrOpt.get();
            if (newLeaderAndIsr.leaderEpoch() > currentLeaderAndIsr.leaderEpoch()
                    || newLeaderAndIsr.bucketEpoch() > currentLeaderAndIsr.bucketEpoch()
                    || newLeaderAndIsr.coordinatorEpoch()
                            > coordinatorContext.getCoordinatorEpoch()) {
                // If the replica leader has a higher replica epoch, then it is likely
                // that this node is no longer the active coordinator.
                throw new InvalidCoordinatorException(
                        "The coordinator is no longer the active coordinator.");
            } else if (newLeaderAndIsr.leaderEpoch() < currentLeaderAndIsr.leaderEpoch()) {
                throw new FencedLeaderEpochException(
                        "The request leader epoch in adjust isr request is lower than current leader epoch in coordinator.");
            } else if (newLeaderAndIsr.bucketEpoch() < currentLeaderAndIsr.bucketEpoch()) {
                // If the replica leader has a lower bucket epoch, then it is likely
                // that this node is not the leader.
                throw new InvalidUpdateVersionException(
                        "The request bucket epoch in adjust isr request is lower than current bucket epoch in coordinator.");
            }
        }
    }

    private CommitKvSnapshotResponse tryProcessCommitKvSnapshot(CommitKvSnapshotEvent event)
            throws Exception {
        // validate
        validateFencedEvent(event);

        TableBucket tb = event.getTableBucket();
        CompletedSnapshot completedSnapshot =
                event.getAddCompletedSnapshotData().getCompletedSnapshot();
        // add completed snapshot
        CompletedSnapshotStore completedSnapshotStore =
                completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(tb);
        completedSnapshotStore.add(completedSnapshot);

        // send notify snapshot request to all replicas.
        // TODO: this should be moved after sending AddCompletedSnapshotResponse
        coordinatorRequestBatch.newBatch();
        coordinatorContext
                .getBucketLeaderAndIsr(tb)
                .ifPresent(
                        leaderAndIsr ->
                                coordinatorRequestBatch
                                        .addNotifyKvSnapshotOffsetRequestForTabletServers(
                                                coordinatorContext.getFollowers(
                                                        tb, leaderAndIsr.leader()),
                                                tb,
                                                completedSnapshot.getLogOffset()));
        coordinatorRequestBatch.sendNotifyKvSnapshotOffsetRequest(
                coordinatorContext.getCoordinatorEpoch());
        return new CommitKvSnapshotResponse();
    }

    private CommitRemoteLogManifestResponse tryProcessCommitRemoteLogManifest(
            CommitRemoteLogManifestEvent event) {
        CommitRemoteLogManifestData manifestData = event.getCommitRemoteLogManifestData();
        CommitRemoteLogManifestResponse response = new CommitRemoteLogManifestResponse();
        TableBucket tb = event.getTableBucket();
        try {
            validateFencedEvent(event);
            // do commit remote log manifest snapshot path to zk.
            zooKeeperClient.upsertRemoteLogManifestHandle(
                    tb,
                    new RemoteLogManifestHandle(
                            manifestData.getRemoteLogManifestPath(),
                            manifestData.getRemoteLogEndOffset()));
        } catch (Exception e) {
            LOG.error(
                    "Error when commit remote log manifest, the leader need to revert the commit.",
                    e);
            response.setCommitSuccess(false);
            return response;
        }

        response.setCommitSuccess(true);
        // send notify remote log offsets request to all replicas.
        coordinatorRequestBatch.newBatch();
        coordinatorContext
                .getBucketLeaderAndIsr(tb)
                .ifPresent(
                        leaderAndIsr ->
                                coordinatorRequestBatch
                                        .addNotifyRemoteLogOffsetsRequestForTabletServers(
                                                coordinatorContext.getFollowers(
                                                        tb, leaderAndIsr.leader()),
                                                tb,
                                                manifestData.getRemoteLogStartOffset(),
                                                manifestData.getRemoteLogEndOffset()));
        coordinatorRequestBatch.sendNotifyRemoteLogOffsetsRequest(
                coordinatorContext.getCoordinatorEpoch());
        return response;
    }

    private <T> void processAccessContext(AccessContextEvent<T> event) {
        try {
            T result = event.getAccessFunction().apply(coordinatorContext);
            event.getResultFuture().complete(result);
        } catch (Throwable t) {
            event.getResultFuture().completeExceptionally(t);
        }
    }

    private CommitLakeTableSnapshotResponse tryProcessCommitLakeTableSnapshot(
            CommitLakeTableSnapshotEvent commitLakeTableSnapshotEvent) {
        CommitLakeTableSnapshotData commitLakeTableSnapshotData =
                commitLakeTableSnapshotEvent.getCommitLakeTableSnapshotData();
        CommitLakeTableSnapshotResponse response = new CommitLakeTableSnapshotResponse();
        Map<Long, LakeTableSnapshot> lakeTableSnapshots =
                commitLakeTableSnapshotData.getLakeTableSnapshot();
        for (Map.Entry<Long, LakeTableSnapshot> lakeTableSnapshotEntry :
                lakeTableSnapshots.entrySet()) {
            Long tableId = lakeTableSnapshotEntry.getKey();

            PbCommitLakeTableSnapshotRespForTable tableResp = response.addTableResp();
            tableResp.setTableId(tableId);

            try {
                zooKeeperClient.upsertLakeTableSnapshot(tableId, lakeTableSnapshotEntry.getValue());
            } catch (Exception e) {
                ApiError error = ApiError.fromThrowable(e);
                tableResp.setError(error.error().code(), error.message());
            }
        }

        // send notify lakehouse data request to all replicas.
        coordinatorRequestBatch.newBatch();
        for (Map.Entry<Long, LakeTableSnapshot> lakeTableSnapshotEntry :
                lakeTableSnapshots.entrySet()) {
            LakeTableSnapshot lakeTableSnapshot = lakeTableSnapshotEntry.getValue();
            for (Map.Entry<TableBucket, Long> bucketLogEndOffsetEntry :
                    lakeTableSnapshot.getBucketLogEndOffset().entrySet()) {
                TableBucket tb = bucketLogEndOffsetEntry.getKey();
                coordinatorContext
                        .getBucketLeaderAndIsr(bucketLogEndOffsetEntry.getKey())
                        .ifPresent(
                                leaderAndIsr ->
                                        coordinatorRequestBatch
                                                .addNotifyLakeTableOffsetRequestForTableServers(
                                                        coordinatorContext.getAssignment(tb),
                                                        tb,
                                                        lakeTableSnapshot));
            }
        }
        coordinatorRequestBatch.sendNotifyLakeTableOffsetRequest(
                coordinatorContext.getCoordinatorEpoch());
        return response;
    }

    private void validateFencedEvent(FencedCoordinatorEvent event) {
        TableBucket tb = event.getTableBucket();
        if (coordinatorContext.getTablePathById(tb.getTableId()) == null) {
            throw new UnknownTableOrBucketException("Unknown table id " + tb.getTableId());
        }
        Optional<LeaderAndIsr> leaderAndIsrOpt = coordinatorContext.getBucketLeaderAndIsr(tb);
        if (!leaderAndIsrOpt.isPresent()) {
            throw new UnknownTableOrBucketException("Unknown table or bucket " + tb);
        }

        LeaderAndIsr currentLeaderAndIsr = leaderAndIsrOpt.get();

        // todo: It will still happen that the request (with a ex-coordinator epoch) is send to a
        // ex-coordinator.
        // we may need to leverage zk to valid it while put data into zk using CAS like Kafka.
        int coordinatorEpoch = event.getCoordinatorEpoch();
        int bucketLeaderEpoch = event.getBucketLeaderEpoch();
        if (bucketLeaderEpoch > currentLeaderAndIsr.bucketEpoch()
                || coordinatorEpoch > coordinatorContext.getCoordinatorEpoch()) {
            // If the replica leader has a higher replica epoch,
            // or the request has a higher coordinator epoch,
            // then it is likely that this node is no longer the active coordinator.
            throw new InvalidCoordinatorException(
                    "The coordinator is no longer the active coordinator.");
        }

        if (bucketLeaderEpoch < currentLeaderAndIsr.leaderEpoch()) {
            throw new FencedLeaderEpochException(
                    "The request leader epoch in coordinator event: "
                            + event.getClass().getSimpleName()
                            + " is lower than current leader epoch in coordinator.");
        }

        if (tb.getPartitionId() != null) {
            if (!coordinatorContext.containsPartitionId(tb.getPartitionId())) {
                throw new UnknownTableOrBucketException("Unknown partition bucket: " + tb);
            }
        } else {
            if (!coordinatorContext.containsTableId(tb.getTableId())) {
                throw new UnknownTableOrBucketException("Unknown table id " + tb.getTableId());
            }
        }
    }

    /** Update metadata cache for coordinator server and all remote tablet servers. */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void updateServerMetadataCache(
            Optional<ServerInfo> coordinatorServer, Set<ServerInfo> aliveTabletServers) {
        // 1. update local metadata cache.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadataInfo(coordinatorServer, aliveTabletServers));

        // 2. send update metadata request to all alive tablet servers
        coordinatorRequestBatch.newBatch();
        Set<Integer> serverIds =
                aliveTabletServers.stream().map(ServerInfo::id).collect(Collectors.toSet());
        coordinatorRequestBatch.addUpdateMetadataRequestForTabletServers(
                serverIds, coordinatorServer, aliveTabletServers);
        coordinatorRequestBatch.sendUpdateMetadataRequest();
    }

    @VisibleForTesting
    CompletedSnapshotStoreManager completedSnapshotStoreManager() {
        return completedSnapshotStoreManager;
    }
}
