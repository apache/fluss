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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.IneligibleReplicaException;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.exception.InvalidUpdateVersionException;
import org.apache.fluss.exception.ServerNotExistException;
import org.apache.fluss.exception.ServerTagAlreadyExistException;
import org.apache.fluss.exception.ServerTagNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TabletServerNotAvailableException;
import org.apache.fluss.exception.UnknownServerException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AddServerTagResponse;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.CommitKvSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.rpc.messages.ControlledShutdownResponse;
import org.apache.fluss.rpc.messages.PbCommitLakeTableSnapshotRespForTable;
import org.apache.fluss.rpc.messages.RemoveServerTagResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AddServerTagEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import org.apache.fluss.server.coordinator.event.ControlledShutdownEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.coordinator.event.CreatePartitionEvent;
import org.apache.fluss.server.coordinator.event.CreateTableEvent;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.EventProcessor;
import org.apache.fluss.server.coordinator.event.FencedCoordinatorEvent;
import org.apache.fluss.server.coordinator.event.NewTabletServerEvent;
import org.apache.fluss.server.coordinator.event.NotifyKvSnapshotOffsetEvent;
import org.apache.fluss.server.coordinator.event.NotifyLakeTableOffsetEvent;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.RemoveServerTagEvent;
import org.apache.fluss.server.coordinator.event.SchemaChangeEvent;
import org.apache.fluss.server.coordinator.event.watcher.TableChangeWatcher;
import org.apache.fluss.server.coordinator.event.watcher.TabletServerChangeWatcher;
import org.apache.fluss.server.coordinator.statemachine.ReplicaStateMachine;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine;
import org.apache.fluss.server.entity.AdjustIsrResultForBucket;
import org.apache.fluss.server.entity.CommitLakeTableSnapshotData;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.entity.DeleteReplicaResultForBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.ServerTags;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdsZNode;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableHelper;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.fluss.server.coordinator.statemachine.BucketState.OfflineBucket;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElectionStrategy.CONTROLLED_SHUTDOWN_ELECTION;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OfflineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionStarted;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionSuccessful;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeAdjustIsrResponse;
import static org.apache.fluss.utils.concurrent.FutureUtils.completeFromCallable;

/** An implementation for {@link EventProcessor}. */
@NotThreadSafe
public class CoordinatorEventProcessor implements EventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventProcessor.class);

    private final ZooKeeperClient zooKeeperClient;
    private final ExecutorService ioExecutor;
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
    private final CoordinatorMetadataCache serverMetadataCache;
    private final CoordinatorRequestBatch coordinatorRequestBatch;
    private final CoordinatorMetricGroup coordinatorMetricGroup;
    private final String internalListenerName;

    private final CompletedSnapshotStoreManager completedSnapshotStoreManager;
    private final LakeTableHelper lakeTableHelper;

    public CoordinatorEventProcessor(
            ZooKeeperClient zooKeeperClient,
            CoordinatorMetadataCache serverMetadataCache,
            CoordinatorChannelManager coordinatorChannelManager,
            CoordinatorContext coordinatorContext,
            AutoPartitionManager autoPartitionManager,
            LakeTableTieringManager lakeTableTieringManager,
            CoordinatorMetricGroup coordinatorMetricGroup,
            Configuration conf,
            ExecutorService ioExecutor,
            MetadataManager metadataManager) {
        this.zooKeeperClient = zooKeeperClient;
        this.serverMetadataCache = serverMetadataCache;
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.coordinatorContext = coordinatorContext;
        this.coordinatorEventManager = new CoordinatorEventManager(this, coordinatorMetricGroup);
        this.replicaStateMachine =
                new ReplicaStateMachine(
                        coordinatorContext,
                        new CoordinatorRequestBatch(
                                coordinatorChannelManager,
                                coordinatorEventManager,
                                coordinatorContext),
                        zooKeeperClient);
        this.tableBucketStateMachine =
                new TableBucketStateMachine(
                        coordinatorContext,
                        new CoordinatorRequestBatch(
                                coordinatorChannelManager,
                                coordinatorEventManager,
                                coordinatorContext),
                        zooKeeperClient);
        this.metadataManager = metadataManager;

        this.tableManager =
                new TableManager(
                        metadataManager,
                        coordinatorContext,
                        replicaStateMachine,
                        tableBucketStateMachine,
                        new RemoteStorageCleaner(conf, ioExecutor),
                        ioExecutor);
        this.tableChangeWatcher = new TableChangeWatcher(zooKeeperClient, coordinatorEventManager);
        this.tabletServerChangeWatcher =
                new TabletServerChangeWatcher(zooKeeperClient, coordinatorEventManager);
        this.coordinatorRequestBatch =
                new CoordinatorRequestBatch(
                        coordinatorChannelManager, coordinatorEventManager, coordinatorContext);
        this.completedSnapshotStoreManager =
                new CompletedSnapshotStoreManager(
                        conf.getInt(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS),
                        ioExecutor,
                        zooKeeperClient,
                        coordinatorMetricGroup);
        this.autoPartitionManager = autoPartitionManager;
        this.lakeTableTieringManager = lakeTableTieringManager;
        this.coordinatorMetricGroup = coordinatorMetricGroup;
        this.internalListenerName = conf.getString(ConfigOptions.INTERNAL_LISTENER_NAME);
        this.ioExecutor = ioExecutor;
        this.lakeTableHelper =
                new LakeTableHelper(zooKeeperClient, conf.getString(ConfigOptions.REMOTE_DATA_DIR));
    }

    public CoordinatorEventManager getCoordinatorEventManager() {
        return coordinatorEventManager;
    }

    public void startup() {
        coordinatorContext.setCoordinatorServerInfo(getCoordinatorServerInfo());
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
        // before the state machines in tableManager are started. This is because tablet servers
        // need to receive the list of live tablet servers from UpdateMetadataRequest before they
        // can process the LeaderRequests that are generated by replicaStateMachine.startup() and
        // partitionStateMachine.startup().
        // update coordinator metadata cache when CoordinatorServer start.
        HashSet<ServerInfo> tabletServerInfoList =
                new HashSet<>(coordinatorContext.getLiveTabletServers().values());
        serverMetadataCache.updateMetadata(
                coordinatorContext.getCoordinatorServerInfo(),
                tabletServerInfoList,
                coordinatorContext.getServerTags());
        updateTabletServerMetadataCacheWhenStartup(tabletServerInfoList);

        // start table manager
        tableManager.startup();

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
                                throw new FlussRuntimeException(
                                        "Coordinator server address is empty in zookeeper.");
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

        long start4loadTabletServer = System.currentTimeMillis();
        Map<Integer, TabletServerRegistration> tabletServerRegistrations =
                zooKeeperClient.getTabletServers(currentServers);
        for (int server : currentServers) {
            TabletServerRegistration registration = tabletServerRegistrations.get(server);
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
        LOG.info(
                "Load tablet servers success in {}ms when initializing coordinator context.",
                System.currentTimeMillis() - start4loadTabletServer);

        // init tablet server channels
        coordinatorChannelManager.startup(internalServerNodes);

        // load server tags.
        zooKeeperClient
                .getServerTags()
                .ifPresent(tags -> coordinatorContext.initSeverTags(tags.getServerTags()));

        // load all tables
        long start4loadTables = System.currentTimeMillis();
        List<TableInfo> autoPartitionTables = new ArrayList<>();
        List<Tuple2<TableInfo, Long>> lakeTables = new ArrayList<>();
        Set<TablePath> tablePathSet = new HashSet<>();
        for (String database : metadataManager.listDatabases()) {
            for (String tableName : metadataManager.listTables(database)) {
                tablePathSet.add(TablePath.of(database, tableName));
            }
        }
        Map<TablePath, TableInfo> tablePath2TableInfoMap = metadataManager.getTables(tablePathSet);
        List<TablePath> partitionedTablePathList =
                tablePath2TableInfoMap.entrySet().stream()
                        .filter(entry -> entry.getValue().isPartitioned())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        Map<TablePath, Map<String, Long>> tablePathMap =
                zooKeeperClient.getPartitionNameAndIdsForTables(partitionedTablePathList);
        for (TablePath tablePath : tablePathSet) {
            TableInfo tableInfo = tablePath2TableInfoMap.get(tablePath);
            coordinatorContext.putTablePath(tableInfo.getTableId(), tablePath);
            coordinatorContext.putTableInfo(tableInfo);
            if (tableInfo.getTableConfig().isDataLakeEnabled()) {
                // always set to current time,
                // todo: should get from the last lake snapshot
                lakeTables.add(Tuple2.of(tableInfo, System.currentTimeMillis()));
            }
            if (tableInfo.isPartitioned()) {
                Map<String, Long> partitions = tablePathMap.get(tablePath);
                if (partitions != null) {
                    for (Map.Entry<String, Long> partition : partitions.entrySet()) {
                        // put partition info to coordinator context
                        coordinatorContext.putPartition(
                                partition.getValue(),
                                PhysicalTablePath.of(tableInfo.getTablePath(), partition.getKey()));
                    }
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
        LOG.info(
                "Load tables success in {}ms when initializing coordinator context.",
                System.currentTimeMillis() - start4loadTables);

        autoPartitionManager.initAutoPartitionTables(autoPartitionTables);
        lakeTableTieringManager.initWithLakeTables(lakeTables);

        // load all assignment
        long start4loadAssignment = System.currentTimeMillis();
        loadTableAssignment();
        loadPartitionAssignment();
        LOG.info(
                "Load table and partition assignment success in {}ms when initializing coordinator context.",
                System.currentTimeMillis() - start4loadAssignment);

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
        List<Long> tableIds =
                assignmentTables.stream().map(Long::parseLong).collect(Collectors.toList());
        Map<Long, TableAssignment> tableId2tableAssignmentMap =
                zooKeeperClient.getTablesAssignments(tableIds);
        for (Long tableId : tableIds) {
            // if table id not in current coordinator context,
            // we'll consider it as deleted
            if (!coordinatorContext.containsTableId(tableId)) {
                deletedTables.add(tableId);
            }
            TableAssignment assignment = tableId2tableAssignmentMap.get(tableId);
            if (assignment != null) {
                loadAssignment(tableId, assignment, null);
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
        List<Long> partitionAssignmentNodes =
                zooKeeperClient.getChildren(PartitionIdsZNode.path()).stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList());
        Set<TablePartition> deletedPartitions = new HashSet<>();
        Map<Long, PartitionAssignment> partitionId2partitionAssignmentMap =
                zooKeeperClient.getPartitionsAssignments(partitionAssignmentNodes);
        for (Long partitionId : partitionAssignmentNodes) {
            PartitionAssignment assignment = partitionId2partitionAssignmentMap.get(partitionId);
            if (assignment == null) {
                LOG.warn("Can't get the assignment for table partition {}.", partitionId);
                continue;
            }
            long tableId = assignment.getTableId();
            // partition id doesn't exist in coordinator context, consider it as deleted
            if (!coordinatorContext.containsPartitionId(partitionId)) {
                deletedPartitions.add(new TablePartition(tableId, partitionId));
            }
            loadAssignment(tableId, assignment, partitionId);
        }
        coordinatorContext.queuePartitionDeletion(deletedPartitions);
    }

    private void loadAssignment(
            long tableId, TableAssignment tableAssignment, @Nullable Long partitionId)
            throws Exception {
        Set<TableBucket> tableBucketSet = new HashSet<>();
        for (Map.Entry<Integer, BucketAssignment> entry :
                tableAssignment.getBucketAssignments().entrySet()) {
            int bucketId = entry.getKey();
            BucketAssignment bucketAssignment = entry.getValue();
            // put the assignment information to context
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            tableBucketSet.add(tableBucket);
            coordinatorContext.updateBucketReplicaAssignment(
                    tableBucket, bucketAssignment.getReplicas());
        }
        Map<TableBucket, LeaderAndIsr> leaderAndIsrMap =
                zooKeeperClient.getLeaderAndIsrs(tableBucketSet);
        for (TableBucket tableBucket : tableBucketSet) {
            LeaderAndIsr leaderAndIsr = leaderAndIsrMap.get(tableBucket);
            // update bucket LeaderAndIsr info
            if (leaderAndIsr != null) {
                coordinatorContext.putBucketLeaderAndIsr(tableBucket, leaderAndIsr);
            }
        }

        // register table/bucket metrics when initialing context.
        TablePath tablePath = coordinatorContext.getTablePathById(tableId);
        if (tablePath != null) {
            coordinatorMetricGroup.addTableBucketMetricGroup(
                    PhysicalTablePath.of(
                            tablePath,
                            partitionId == null
                                    ? null
                                    : coordinatorContext.getPartitionName(partitionId)),
                    tableId,
                    partitionId,
                    tableAssignment.getBucketAssignments().keySet());
        }
    }

    private void onShutdown() {
        // first shutdown table manager
        tableManager.shutdown();

        // then stop watchers
        tableChangeWatcher.stop();
        tabletServerChangeWatcher.stop();
    }

    @Override
    public void process(CoordinatorEvent event) {
        if (event instanceof CreateTableEvent) {
            processCreateTable((CreateTableEvent) event);
        } else if (event instanceof CreatePartitionEvent) {
            processCreatePartition((CreatePartitionEvent) event);
        } else if (event instanceof DropTableEvent) {
            processDropTable((DropTableEvent) event);
        } else if (event instanceof DropPartitionEvent) {
            processDropPartition((DropPartitionEvent) event);
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            processSchemaChange(schemaChangeEvent);
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
            tryProcessCommitKvSnapshot(
                    commitKvSnapshotEvent, commitKvSnapshotEvent.getRespCallback());
        } else if (event instanceof NotifyKvSnapshotOffsetEvent) {
            processNotifyKvSnapshotOffsetEvent((NotifyKvSnapshotOffsetEvent) event);
        } else if (event instanceof NotifyLakeTableOffsetEvent) {
            processNotifyLakeTableOffsetEvent((NotifyLakeTableOffsetEvent) event);
        } else if (event instanceof CommitRemoteLogManifestEvent) {
            CommitRemoteLogManifestEvent commitRemoteLogManifestEvent =
                    (CommitRemoteLogManifestEvent) event;
            completeFromCallable(
                    commitRemoteLogManifestEvent.getRespCallback(),
                    () -> tryProcessCommitRemoteLogManifest(commitRemoteLogManifestEvent));
        } else if (event instanceof CommitLakeTableSnapshotEvent) {
            CommitLakeTableSnapshotEvent commitLakeTableSnapshotEvent =
                    (CommitLakeTableSnapshotEvent) event;
            tryProcessCommitLakeTableSnapshot(
                    commitLakeTableSnapshotEvent, commitLakeTableSnapshotEvent.getRespCallback());
        } else if (event instanceof ControlledShutdownEvent) {
            ControlledShutdownEvent controlledShutdownEvent = (ControlledShutdownEvent) event;
            completeFromCallable(
                    controlledShutdownEvent.getRespCallback(),
                    () -> tryProcessControlledShutdown(controlledShutdownEvent));
        } else if (event instanceof AddServerTagEvent) {
            AddServerTagEvent addServerTagEvent = (AddServerTagEvent) event;
            completeFromCallable(
                    addServerTagEvent.getRespCallback(),
                    () -> processAddServerTag(addServerTagEvent));
        } else if (event instanceof RemoveServerTagEvent) {
            RemoveServerTagEvent removeServerTagEvent = (RemoveServerTagEvent) event;
            completeFromCallable(
                    removeServerTagEvent.getRespCallback(),
                    () -> processRemoveServerTag(removeServerTagEvent));
        } else if (event instanceof AccessContextEvent) {
            AccessContextEvent<?> accessContextEvent = (AccessContextEvent<?>) event;
            processAccessContext(accessContextEvent);
        } else {
            LOG.warn("Unknown event type: {}", event.getClass().getName());
        }
    }

    private void processCreateTable(CreateTableEvent createTableEvent) {
        long tableId = createTableEvent.getTableInfo().getTableId();
        // skip the table if it already exists
        if (coordinatorContext.containsTableId(tableId)) {
            return;
        }
        TableInfo tableInfo = createTableEvent.getTableInfo();
        TablePath tablePath = tableInfo.getTablePath();
        coordinatorContext.putTableInfo(tableInfo);
        TableAssignment tableAssignment = createTableEvent.getTableAssignment();
        tableManager.onCreateNewTable(tablePath, tableInfo.getTableId(), tableAssignment);
        if (createTableEvent.isAutoPartitionTable()) {
            autoPartitionManager.addAutoPartitionTable(tableInfo, true);
        }
        if (tableInfo.getTableConfig().isDataLakeEnabled()) {
            lakeTableTieringManager.addNewLakeTable(tableInfo);
        }

        if (!tableInfo.isPartitioned()) {
            Set<TableBucket> tableBuckets = new HashSet<>();
            tableAssignment
                    .getBucketAssignments()
                    .keySet()
                    .forEach(bucketId -> tableBuckets.add(new TableBucket(tableId, bucketId)));
            updateTabletServerMetadataCache(
                    new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                    null,
                    null,
                    tableBuckets);

            // register table metrics.
            coordinatorMetricGroup.addTableBucketMetricGroup(
                    PhysicalTablePath.of(tablePath),
                    tableId,
                    null,
                    tableAssignment.getBucketAssignments().keySet());

        } else {
            updateTabletServerMetadataCache(
                    new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                    tableId,
                    null,
                    Collections.emptySet());
        }
    }

    private void processSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        TablePath tablePath = schemaChangeEvent.getTablePath();
        SchemaInfo schemaInfo = schemaChangeEvent.getSchemaInfo();
        long tableId = coordinatorContext.getTableIdByPath(tablePath);

        // if table is created, will create schema first. In this case, schema id will be included
        // in CreateTableEvent.
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return;
        }

        TableInfo oldTableInfo = coordinatorContext.getTableInfoById(tableId);
        if (oldTableInfo.getSchemaId() == schemaInfo.getSchemaId()) {
            return;
        }

        coordinatorContext.putTableInfo(
                new TableInfo(
                        tablePath,
                        tableId,
                        schemaInfo.getSchemaId(),
                        schemaInfo.getSchema(),
                        oldTableInfo.getBucketKeys(),
                        oldTableInfo.getPartitionKeys(),
                        oldTableInfo.getNumBuckets(),
                        oldTableInfo.getProperties(),
                        oldTableInfo.getCustomProperties(),
                        oldTableInfo.getComment().orElse(null),
                        oldTableInfo.getCreatedTime(),
                        System.currentTimeMillis()));

        updateTabletServerMetadataCache(
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                tableId,
                null,
                null);
    }

    private void processCreatePartition(CreatePartitionEvent createPartitionEvent) {
        long partitionId = createPartitionEvent.getPartitionId();
        // skip the partition if it already exists
        if (coordinatorContext.containsPartitionId(partitionId)) {
            return;
        }

        long tableId = createPartitionEvent.getTableId();
        TablePath tablePath = createPartitionEvent.getTablePath();
        String partitionName = createPartitionEvent.getPartitionName();
        PartitionAssignment partitionAssignment = createPartitionEvent.getPartitionAssignment();
        tableManager.onCreateNewPartition(
                tablePath,
                tableId,
                createPartitionEvent.getPartitionId(),
                partitionName,
                partitionAssignment);
        autoPartitionManager.addPartition(tableId, partitionName);

        Set<TableBucket> tableBuckets = new HashSet<>();
        partitionAssignment
                .getBucketAssignments()
                .keySet()
                .forEach(
                        bucketId ->
                                tableBuckets.add(new TableBucket(tableId, partitionId, bucketId)));

        // register partition metrics.
        coordinatorMetricGroup.addTableBucketMetricGroup(
                PhysicalTablePath.of(tablePath, partitionName),
                tableId,
                partitionId,
                partitionAssignment.getBucketAssignments().keySet());

        updateTabletServerMetadataCache(
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                null,
                null,
                tableBuckets);
    }

    private void processDropTable(DropTableEvent dropTableEvent) {
        // If this is a primary key table, drop the kv snapshot store.
        long tableId = dropTableEvent.getTableId();
        TableInfo dropTableInfo = coordinatorContext.getTableInfoById(tableId);
        if (dropTableInfo.hasPrimaryKey()) {
            Set<TableBucket> deleteTableBuckets = coordinatorContext.getAllBucketsForTable(tableId);
            completedSnapshotStoreManager.removeCompletedSnapshotStoreByTableBuckets(
                    deleteTableBuckets);
        }

        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));
        tableManager.onDeleteTable(tableId);
        if (dropTableEvent.isAutoPartitionTable()) {
            autoPartitionManager.removeAutoPartitionTable(tableId);
        }
        if (dropTableEvent.isDataLakeEnabled()) {
            lakeTableTieringManager.removeLakeTable(tableId);
        }

        // send update metadata request.
        updateTabletServerMetadataCache(
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                tableId,
                null,
                Collections.emptySet());

        // remove table metrics.
        coordinatorMetricGroup.removeTableMetricGroup(dropTableInfo.getTablePath(), tableId);
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

        // send update metadata request.
        updateTabletServerMetadataCache(
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                tableId,
                tablePartition.getPartitionId(),
                Collections.emptySet());

        // remove partition metrics.
        coordinatorMetricGroup.removeTablePartitionMetricsGroup(
                dropTableInfo.getTablePath(), tableId, tablePartition.getPartitionId());
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
        // listen the children created and deleted in zk node.

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

        // update coordinatorServer metadata cache for the new added table server.
        serverMetadataCache.updateMetadata(
                coordinatorContext.getCoordinatorServerInfo(),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                coordinatorContext.getServerTags());
        // update server info for all tablet servers.
        updateTabletServerMetadataCache(
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                null,
                null,
                Collections.emptySet());
        // update table info for the new added table server.
        updateTabletServerMetadataCache(
                Collections.singleton(serverInfo),
                null,
                null,
                coordinatorContext.bucketLeaderAndIsr().keySet());

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
        coordinatorContext.shuttingDownTabletServers().remove(tabletServerId);
        coordinatorChannelManager.removeTabletServer(tabletServerId);

        // Here, we will first update alive tabletServer info for all tabletServers and
        // coordinatorServer metadata. The purpose of this approach is to prevent the scenario where
        // NotifyLeaderAndIsrRequest gets sent before UpdateMetadataRequest, which could cause the
        // leader to incorrectly adjust isr.
        Set<ServerInfo> serverInfos =
                new HashSet<>(coordinatorContext.getLiveTabletServers().values());
        // update coordinatorServer metadata cache.
        serverMetadataCache.updateMetadata(
                coordinatorContext.getCoordinatorServerInfo(),
                serverInfos,
                coordinatorContext.getServerTags());
        updateTabletServerMetadataCache(serverInfos, null, null, Collections.emptySet());

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

        // update tabletServer metadata cache by send updateMetadata request.
        updateTabletServerMetadataCache(serverInfos, null, null, bucketsWithOfflineLeader);
    }

    private AddServerTagResponse processAddServerTag(AddServerTagEvent event) {
        AddServerTagResponse addServerTagResponse = new AddServerTagResponse();
        List<Integer> serverIds = event.getServerIds();
        ServerTag serverTag = event.getServerTag();

        // Verify that dose serverTag exist for input serverIds. If any of them exists for one
        // serverId and the server ta isg different, an ServerNotExistException error will be thrown
        // and none of them will be written to coordinatorContext and zk.
        Map<Integer, ServerInfo> liveTabletServers = coordinatorContext.getLiveTabletServers();
        for (Integer serverId : serverIds) {
            if (!liveTabletServers.containsKey(serverId)) {
                throw new ServerNotExistException(
                        String.format(
                                "Server %s not exists when trying to add server tag.", serverId));
            }

            Optional<ServerTag> existServerTagOpt = coordinatorContext.getServerTag(serverId);
            if (existServerTagOpt.isPresent() && existServerTagOpt.get() != serverTag) {
                throw new ServerTagAlreadyExistException(
                        String.format(
                                "Server tag %s already exists for server %s. However you want to set it to %s, "
                                        + "please remove the server tag first.",
                                existServerTagOpt.get(), serverId, serverTag));
            }
        }

        // First register to zk, and then update coordinatorContext.
        Map<Integer, ServerTag> serverTags = coordinatorContext.getServerTags();
        for (Integer serverId : serverIds) {
            serverTags.put(serverId, serverTag);
        }

        try {
            if (zooKeeperClient.getServerTags().isPresent()) {
                zooKeeperClient.updateServerTags(new ServerTags(serverTags));
            } else {
                zooKeeperClient.registerServerTags(new ServerTags(serverTags));
            }
        } catch (Exception e) {
            LOG.error("Error when add server tags to zookeeper.", e);
            throw new UnknownServerException("Error when add server tags to zookeeper.", e);
        }

        // Then update coordinatorContext.
        serverIds.forEach(serverId -> coordinatorContext.putServerTag(serverId, serverTag));
        // update coordinatorServer metadata cache for the new added serverTag
        serverMetadataCache.updateMetadata(
                coordinatorContext.getCoordinatorServerInfo(),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                coordinatorContext.getServerTags());

        return addServerTagResponse;
    }

    private RemoveServerTagResponse processRemoveServerTag(RemoveServerTagEvent event) {
        RemoveServerTagResponse removeServerTagResponse = new RemoveServerTagResponse();
        List<Integer> serverIds = event.getServerIds();
        ServerTag serverTag = event.getServerTag();

        // Verify that dose serverTag not exist for input serverIds. If the server tag does not
        // exist for any one of the tabletServers, throw an error and none of them will be removed
        // form coordinatorContext and zk.
        Map<Integer, ServerInfo> liveTabletServers = coordinatorContext.getLiveTabletServers();
        for (Integer serverId : serverIds) {
            if (!liveTabletServers.containsKey(serverId)) {
                throw new ServerNotExistException(
                        String.format(
                                "Server %s not exists when trying to removing server tag.",
                                serverId));
            }

            Optional<ServerTag> existServerTagOpt = coordinatorContext.getServerTag(serverId);
            if (existServerTagOpt.isPresent() && existServerTagOpt.get() != serverTag) {
                throw new ServerTagNotExistException(
                        String.format(
                                "Server tag %s not exists for server %s, the current server tag of this server is %s.",
                                serverTag, serverId, existServerTagOpt.get()));
            }
        }

        // First register to zk, and then update coordinatorContext.
        Map<Integer, ServerTag> serverTags = coordinatorContext.getServerTags();
        for (Integer serverId : serverIds) {
            serverTags.remove(serverId);
        }

        try {
            if (serverTags.isEmpty()) {
                zooKeeperClient.deleteServerTags();
            } else {
                zooKeeperClient.updateServerTags(new ServerTags(serverTags));
            }
        } catch (Exception e) {
            LOG.error("Error when remove server tags from zookeeper.", e);
            throw new UnknownServerException("Error when remove server tags from zookeeper.", e);
        }

        // Then update coordinatorContext.
        serverIds.forEach(coordinatorContext::removeServerTag);
        // update coordinatorServer metadata cache for the new removed serverTag
        serverMetadataCache.updateMetadata(
                coordinatorContext.getCoordinatorServerInfo(),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                coordinatorContext.getServerTags());

        return removeServerTagResponse;
    }

    private List<AdjustIsrResultForBucket> tryProcessAdjustIsr(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrList) {
        // TODO verify leader epoch.

        List<AdjustIsrResultForBucket> result = new ArrayList<>();
        Map<TableBucket, LeaderAndIsr> newLeaderAndIsrList = new HashMap<>();
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
            newLeaderAndIsrList.put(tableBucket, newLeaderAndIsr);
        }

        try {
            zooKeeperClient.batchUpdateLeaderAndIsr(newLeaderAndIsrList);
            newLeaderAndIsrList.forEach(
                    (tableBucket, newLeaderAndIsr) ->
                            result.add(new AdjustIsrResultForBucket(tableBucket, newLeaderAndIsr)));
        } catch (Exception batchException) {
            LOG.error("Error when batch update leader and isr. Try one by one.", batchException);

            for (Map.Entry<TableBucket, LeaderAndIsr> entry : newLeaderAndIsrList.entrySet()) {
                TableBucket tableBucket = entry.getKey();
                LeaderAndIsr newLeaderAndIsr = entry.getValue();
                try {
                    zooKeeperClient.updateLeaderAndIsr(tableBucket, newLeaderAndIsr);
                } catch (Exception e) {
                    LOG.error("Error when register leader and isr.", e);
                    result.add(
                            new AdjustIsrResultForBucket(tableBucket, ApiError.fromThrowable(e)));
                }
                // Successful return.
                result.add(new AdjustIsrResultForBucket(tableBucket, newLeaderAndIsr));
            }
        }

        // update coordinator leader and isr cache.
        newLeaderAndIsrList.forEach(coordinatorContext::putBucketLeaderAndIsr);

        // TODO update metadata for all alive tablet servers.

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
            } else {
                // Check if the new ISR are all ineligible replicas (doesn't contain any shutting
                // down tabletServers).
                Set<Integer> ineligibleReplicas = new HashSet<>(newLeaderAndIsr.isr());
                ineligibleReplicas.removeAll(coordinatorContext.liveTabletServerSet());
                if (!ineligibleReplicas.isEmpty()) {
                    String errorMsg =
                            String.format(
                                    "Rejecting adjustIsr request for table bucket %s because it "
                                            + "specified ineligible replicas %s in the new ISR %s",
                                    tableBucket, ineligibleReplicas, newLeaderAndIsr.isr());
                    LOG.info(errorMsg);
                    throw new IneligibleReplicaException(errorMsg);
                }
            }
        }
    }

    private void tryProcessCommitKvSnapshot(
            CommitKvSnapshotEvent event, CompletableFuture<CommitKvSnapshotResponse> callback) {
        // validate
        try {
            validateFencedEvent(event);
        } catch (Exception e) {
            callback.completeExceptionally(e);
            return;
        }
        // commit the kv snapshot asynchronously
        TableBucket tb = event.getTableBucket();
        TablePath tablePath = coordinatorContext.getTablePathById(tb.getTableId());
        ioExecutor.execute(
                () -> {
                    try {
                        CompletedSnapshot completedSnapshot =
                                event.getAddCompletedSnapshotData().getCompletedSnapshot();
                        // add completed snapshot
                        CompletedSnapshotStore completedSnapshotStore =
                                completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(
                                        tablePath, tb);
                        // this involves IO operation (ZK), so we do it in ioExecutor
                        completedSnapshotStore.add(completedSnapshot);
                        coordinatorEventManager.put(
                                new NotifyKvSnapshotOffsetEvent(
                                        tb, completedSnapshot.getLogOffset()));
                        callback.complete(new CommitKvSnapshotResponse());
                    } catch (Exception e) {
                        callback.completeExceptionally(e);
                    }
                });
    }

    private void processNotifyKvSnapshotOffsetEvent(NotifyKvSnapshotOffsetEvent event) {
        TableBucket tb = event.getTableBucket();
        long logOffset = event.getLogOffset();
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
                                                logOffset));
        coordinatorRequestBatch.sendNotifyKvSnapshotOffsetRequest(
                coordinatorContext.getCoordinatorEpoch());
    }

    private void processNotifyLakeTableOffsetEvent(NotifyLakeTableOffsetEvent event) {
        Map<Long, LakeTableSnapshot> lakeTableSnapshots = event.getLakeTableSnapshots();
        Map<Long, Map<TableBucket, Long>> tableMaxTieredTimestamps =
                event.getTableMaxTieredTimestamps();
        coordinatorRequestBatch.newBatch();
        for (Map.Entry<Long, LakeTableSnapshot> lakeTableSnapshotEntry :
                lakeTableSnapshots.entrySet()) {
            LakeTableSnapshot lakeTableSnapshot = lakeTableSnapshotEntry.getValue();
            Map<TableBucket, Long> tableBucketMaxTieredTimestamps =
                    tableMaxTieredTimestamps.getOrDefault(
                            lakeTableSnapshotEntry.getKey(), Collections.emptyMap());
            for (TableBucket tb : lakeTableSnapshot.getBucketLogEndOffset().keySet()) {
                coordinatorContext
                        .getBucketLeaderAndIsr(tb)
                        .ifPresent(
                                leaderAndIsr ->
                                        coordinatorRequestBatch
                                                .addNotifyLakeTableOffsetRequestForTableServers(
                                                        coordinatorContext.getAssignment(tb),
                                                        tb,
                                                        lakeTableSnapshot,
                                                        tableBucketMaxTieredTimestamps.get(tb)));
            }
        }
        coordinatorRequestBatch.sendNotifyLakeTableOffsetRequest(
                coordinatorContext.getCoordinatorEpoch());
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

    private void tryProcessCommitLakeTableSnapshot(
            CommitLakeTableSnapshotEvent commitLakeTableSnapshotEvent,
            CompletableFuture<CommitLakeTableSnapshotResponse> callback) {
        CommitLakeTableSnapshotData commitLakeTableSnapshotData =
                commitLakeTableSnapshotEvent.getCommitLakeTableSnapshotData();
        if (commitLakeTableSnapshotData.getLakeTableSnapshotMetadatas().isEmpty()) {
            handleCommitLakeTableSnapshotV1(commitLakeTableSnapshotEvent, callback);
        } else {
            handleCommitLakeTableSnapshotV2(commitLakeTableSnapshotEvent, callback);
        }
    }

    private void handleCommitLakeTableSnapshotV1(
            CommitLakeTableSnapshotEvent commitLakeTableSnapshotEvent,
            CompletableFuture<CommitLakeTableSnapshotResponse> callback) {
        // commit the lake table snapshot asynchronously
        CommitLakeTableSnapshotData commitLakeTableSnapshotData =
                commitLakeTableSnapshotEvent.getCommitLakeTableSnapshotData();
        Map<Long, LakeTableSnapshot> lakeTableSnapshots =
                commitLakeTableSnapshotData.getLakeTableSnapshot();
        Map<Long, TablePath> tablePathById = new HashMap<>();
        for (Map.Entry<Long, LakeTableSnapshot> lakeTableSnapshotEntry :
                lakeTableSnapshots.entrySet()) {
            Long tableId = lakeTableSnapshotEntry.getKey();
            TablePath tablePath = coordinatorContext.getTablePathById(tableId);
            if (tablePath != null) {
                tablePathById.put(tableId, tablePath);
            }
        }

        ioExecutor.execute(
                () -> {
                    try {
                        CommitLakeTableSnapshotResponse response =
                                new CommitLakeTableSnapshotResponse();
                        Set<Long> failedTableIds = new HashSet<>();
                        for (Map.Entry<Long, LakeTableSnapshot> lakeTableSnapshotEntry :
                                lakeTableSnapshots.entrySet()) {
                            Long tableId = lakeTableSnapshotEntry.getKey();

                            PbCommitLakeTableSnapshotRespForTable tableResp =
                                    response.addTableResp();
                            tableResp.setTableId(tableId);

                            try {
                                TablePath tablePath = tablePathById.get(tableId);
                                if (tablePath == null) {
                                    throw new TableNotExistException(
                                            "Table "
                                                    + tableId
                                                    + " not found in coordinator context.");
                                }

                                // this involves IO operation (ZK), so we do it in ioExecutor
                                lakeTableHelper.registerLakeTableSnapshotV1(
                                        tableId, lakeTableSnapshotEntry.getValue());
                            } catch (Exception e) {
                                failedTableIds.add(tableId);
                                ApiError error = ApiError.fromThrowable(e);
                                tableResp.setError(error.error().code(), error.message());
                            }
                        }

                        // remove failed tables
                        Map<Long, LakeTableSnapshot> commitlakeTableSnapshots =
                                commitLakeTableSnapshotData.getLakeTableSnapshot();
                        commitlakeTableSnapshots.keySet().removeAll(failedTableIds);
                        Map<Long, Map<TableBucket, Long>> tableMaxTieredTimestamps =
                                commitLakeTableSnapshotData.getTableMaxTieredTimestamps();
                        tableMaxTieredTimestamps.keySet().removeAll(failedTableIds);

                        coordinatorEventManager.put(
                                new NotifyLakeTableOffsetEvent(
                                        commitlakeTableSnapshots, tableMaxTieredTimestamps));
                        callback.complete(response);
                    } catch (Exception e) {
                        callback.completeExceptionally(e);
                    }
                });
    }

    private void handleCommitLakeTableSnapshotV2(
            CommitLakeTableSnapshotEvent commitLakeTableSnapshotEvent,
            CompletableFuture<CommitLakeTableSnapshotResponse> callback) {
        CommitLakeTableSnapshotData commitLakeTableSnapshotData =
                commitLakeTableSnapshotEvent.getCommitLakeTableSnapshotData();
        Map<Long, LakeTable.LakeSnapshotMetadata> lakeSnapshotMetadatas =
                commitLakeTableSnapshotData.getLakeTableSnapshotMetadatas();
        ioExecutor.execute(
                () -> {
                    try {
                        CommitLakeTableSnapshotResponse response =
                                new CommitLakeTableSnapshotResponse();
                        Set<Long> failedTableIds = new HashSet<>();
                        for (Map.Entry<Long, LakeTable.LakeSnapshotMetadata>
                                lakeSnapshotMetadataEntry : lakeSnapshotMetadatas.entrySet()) {
                            PbCommitLakeTableSnapshotRespForTable tableResp =
                                    response.addTableResp();
                            long tableId = lakeSnapshotMetadataEntry.getKey();
                            tableResp.setTableId(tableId);
                            try {
                                lakeTableHelper.registerLakeTableSnapshotV2(
                                        tableId, lakeSnapshotMetadataEntry.getValue());
                            } catch (Exception e) {
                                failedTableIds.add(tableId);
                                ApiError error = ApiError.fromThrowable(e);
                                tableResp.setError(error.error().code(), error.message());
                            }
                        }
                        // remove failed tables
                        Map<Long, LakeTableSnapshot> lakeTableSnapshots =
                                commitLakeTableSnapshotData.getLakeTableSnapshot();
                        lakeTableSnapshots.keySet().removeAll(failedTableIds);
                        Map<Long, Map<TableBucket, Long>> tableMaxTieredTimestamps =
                                commitLakeTableSnapshotData.getTableMaxTieredTimestamps();
                        tableMaxTieredTimestamps.keySet().removeAll(failedTableIds);

                        coordinatorEventManager.put(
                                new NotifyLakeTableOffsetEvent(
                                        lakeTableSnapshots, tableMaxTieredTimestamps));
                        callback.complete(response);
                    } catch (Exception e) {
                        callback.completeExceptionally(e);
                    }
                });
    }

    private ControlledShutdownResponse tryProcessControlledShutdown(
            ControlledShutdownEvent controlledShutdownEvent) {
        ControlledShutdownResponse response = new ControlledShutdownResponse();

        // TODO here we need to check tabletServerEpoch, avoid to receive controlled shutdown
        // request from an old tabletServer. Trace by https://github.com/alibaba/fluss/issues/1153
        int tabletServerEpoch = controlledShutdownEvent.getTabletServerEpoch();

        int tabletServerId = controlledShutdownEvent.getTabletServerId();
        LOG.info(
                "Try to process controlled shutdown for tabletServer: {} of tabletServer epoch: {}",
                controlledShutdownEvent.getTabletServerId(),
                tabletServerEpoch);

        if (!coordinatorContext.liveOrShuttingDownTabletServers().contains(tabletServerId)) {
            throw new TabletServerNotAvailableException(
                    "TabletServer" + tabletServerId + " is not available.");
        }

        coordinatorContext.shuttingDownTabletServers().add(tabletServerId);
        LOG.debug(
                "All shutting down tabletServers: {}",
                coordinatorContext.shuttingDownTabletServers());
        LOG.debug("All live tabletServers: {}", coordinatorContext.liveTabletServerSet());

        List<TableBucketReplica> replicasToActOn =
                coordinatorContext.replicasOnTabletServer(tabletServerId).stream()
                        .filter(
                                replica -> {
                                    TableBucket tableBucket = replica.getTableBucket();
                                    return !coordinatorContext.getAssignment(tableBucket).isEmpty()
                                            && coordinatorContext
                                                    .getBucketLeaderAndIsr(tableBucket)
                                                    .isPresent()
                                            && !coordinatorContext.isToBeDeleted(tableBucket);
                                })
                        .collect(Collectors.toList());

        Set<TableBucket> bucketsLedByServer = new HashSet<>();
        Set<TableBucketReplica> replicasFollowedByServer = new HashSet<>();
        for (TableBucketReplica replica : replicasToActOn) {
            TableBucket tableBucket = replica.getTableBucket();
            if (replica.getReplica()
                    == coordinatorContext.getBucketLeaderAndIsr(tableBucket).get().leader()) {
                bucketsLedByServer.add(tableBucket);
            } else {
                replicasFollowedByServer.add(replica);
            }
        }

        tableBucketStateMachine.handleStateChange(
                bucketsLedByServer, OnlineBucket, CONTROLLED_SHUTDOWN_ELECTION);

        // TODO need send stop request to the leader?

        // If the tabletServer is a follower, updates the isr in ZK and notifies the current leader.
        replicaStateMachine.handleStateChanges(replicasFollowedByServer, OfflineReplica);

        // Return the list of buckets that are still being managed by the controlled shutdown
        // tabletServer after leader migration.
        response.addAllRemainingLeaderBuckets(
                coordinatorContext.getBucketsWithLeaderIn(tabletServerId).stream()
                        .map(ServerRpcMessageUtils::fromTableBucket)
                        .collect(Collectors.toList()));
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

    /** Update metadata cache for all remote tablet servers when coordinator startup. */
    private void updateTabletServerMetadataCacheWhenStartup(Set<ServerInfo> aliveTabletServers) {
        coordinatorRequestBatch.newBatch();
        Set<Integer> serverIds =
                aliveTabletServers.stream().map(ServerInfo::id).collect(Collectors.toSet());

        Set<Long> tablesToBeDeleted = coordinatorContext.getTablesToBeDeleted();

        tablesToBeDeleted.forEach(
                tableId ->
                        coordinatorRequestBatch.addUpdateMetadataRequestForTabletServers(
                                serverIds, tableId, null, Collections.emptySet()));

        Set<TablePartition> partitionsToBeDeleted = coordinatorContext.getPartitionsToBeDeleted();
        partitionsToBeDeleted.forEach(
                tablePartition ->
                        coordinatorRequestBatch.addUpdateMetadataRequestForTabletServers(
                                serverIds,
                                tablePartition.getTableId(),
                                tablePartition.getPartitionId(),
                                Collections.emptySet()));

        Set<TableBucket> tableBuckets = new HashSet<>();
        coordinatorContext
                .bucketLeaderAndIsr()
                .forEach(
                        (tableBucket, leaderAndIsr) -> {
                            if (!coordinatorContext.isToBeDeleted(tableBucket)) {
                                tableBuckets.add(tableBucket);
                            }
                        });
        coordinatorRequestBatch.addUpdateMetadataRequestForTabletServers(
                serverIds, null, null, tableBuckets);

        coordinatorRequestBatch.sendUpdateMetadataRequest();
    }

    /** Update metadata cache for all remote tablet servers. */
    private void updateTabletServerMetadataCache(
            Set<ServerInfo> aliveTabletServers,
            @Nullable Long tableId,
            @Nullable Long partitionId,
            Set<TableBucket> tableBuckets) {
        coordinatorRequestBatch.newBatch();
        Set<Integer> serverIds =
                aliveTabletServers.stream().map(ServerInfo::id).collect(Collectors.toSet());
        coordinatorRequestBatch.addUpdateMetadataRequestForTabletServers(
                serverIds, tableId, partitionId, tableBuckets);
        coordinatorRequestBatch.sendUpdateMetadataRequest();
    }

    @VisibleForTesting
    CompletedSnapshotStoreManager completedSnapshotStoreManager() {
        return completedSnapshotStoreManager;
    }
}
