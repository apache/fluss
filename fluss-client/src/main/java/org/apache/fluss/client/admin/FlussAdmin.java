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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AddServerTagRequest;
import org.apache.fluss.rpc.messages.AlterClusterConfigsRequest;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.CancelRebalanceRequest;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.DatabaseExistsRequest;
import org.apache.fluss.rpc.messages.DatabaseExistsResponse;
import org.apache.fluss.rpc.messages.DescribeClusterConfigsRequest;
import org.apache.fluss.rpc.messages.DropAclsRequest;
import org.apache.fluss.rpc.messages.DropDatabaseRequest;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.GetDatabaseInfoRequest;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.ListAclsRequest;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListRebalanceProgressRequest;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.messages.PbPartitionSpec;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.RebalanceRequest;
import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.rpc.messages.RemoveServerTagRequest;
import org.apache.fluss.rpc.messages.TableExistsRequest;
import org.apache.fluss.rpc.messages.TableExistsResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeAlterTableRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeCreatePartitionRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeDropPartitionRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeListOffsetsRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makePbPartitionSpec;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.toConfigEntries;
import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toAclBindings;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclBindingFilters;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclFilter;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The default implementation of {@link Admin}.
 *
 * <p>This class is thread-safe. The API of this class is evolving, see {@link Admin} for details.
 */
public class FlussAdmin implements Admin {

    private final AdminGateway gateway;
    private final AdminReadOnlyGateway readOnlyGateway;
    private final MetadataUpdater metadataUpdater;

    public FlussAdmin(RpcClient client, MetadataUpdater metadataUpdater) {
        this.gateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, client, AdminGateway.class);
        this.readOnlyGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getRandomTabletServer, client, AdminGateway.class);
        this.metadataUpdater = metadataUpdater;
    }

    @Override
    public CompletableFuture<List<ServerNode>> getServerNodes() {
        CompletableFuture<List<ServerNode>> future = new CompletableFuture<>();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        List<ServerNode> serverNodeList = new ArrayList<>();
                        Cluster cluster =
                                sendMetadataRequestAndRebuildCluster(
                                        readOnlyGateway,
                                        false,
                                        metadataUpdater.getCluster(),
                                        null,
                                        null,
                                        null);
                        serverNodeList.add(cluster.getCoordinatorServer());
                        serverNodeList.addAll(cluster.getAliveTabletServerList());
                        future.complete(serverNodeList);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath) {
        GetTableSchemaRequest request = new GetTableSchemaRequest();
        // requesting the latest schema of the given table by not setting schema id
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return readOnlyGateway
                .getTableSchema(request)
                .thenApply(
                        r ->
                                new SchemaInfo(
                                        Schema.fromJsonBytes(r.getSchemaJson()), r.getSchemaId()));
    }

    @Override
    public CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath, int schemaId) {
        GetTableSchemaRequest request = new GetTableSchemaRequest();
        // requesting the latest schema of the given table by not setting schema id
        request.setSchemaId(schemaId)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return readOnlyGateway
                .getTableSchema(request)
                .thenApply(
                        r ->
                                new SchemaInfo(
                                        Schema.fromJsonBytes(r.getSchemaJson()), r.getSchemaId()));
    }

    @Override
    public CompletableFuture<Void> createDatabase(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists) {
        TablePath.validateDatabaseName(databaseName);
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setDatabaseJson(databaseDescriptor.toJsonBytes())
                .setDatabaseName(databaseName)
                .setIgnoreIfExists(ignoreIfExists);
        return gateway.createDatabase(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<DatabaseInfo> getDatabaseInfo(String databaseName) {
        GetDatabaseInfoRequest request = new GetDatabaseInfoRequest();
        request.setDatabaseName(databaseName);
        return readOnlyGateway
                .getDatabaseInfo(request)
                .thenApply(
                        r ->
                                new DatabaseInfo(
                                        databaseName,
                                        DatabaseDescriptor.fromJsonBytes(r.getDatabaseJson()),
                                        r.getCreatedTime(),
                                        r.getModifiedTime()));
    }

    @Override
    public CompletableFuture<Void> dropDatabase(
            String databaseName, boolean ignoreIfNotExists, boolean cascade) {
        DropDatabaseRequest request = new DropDatabaseRequest();

        request.setIgnoreIfNotExists(ignoreIfNotExists)
                .setCascade(cascade)
                .setDatabaseName(databaseName);
        return gateway.dropDatabase(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Boolean> databaseExists(String databaseName) {
        DatabaseExistsRequest request = new DatabaseExistsRequest();
        request.setDatabaseName(databaseName);
        return readOnlyGateway.databaseExists(request).thenApply(DatabaseExistsResponse::isExists);
    }

    @Override
    public CompletableFuture<List<String>> listDatabases() {
        ListDatabasesRequest request = new ListDatabasesRequest();
        return readOnlyGateway
                .listDatabases(request)
                .thenApply(ListDatabasesResponse::getDatabaseNamesList);
    }

    @Override
    public CompletableFuture<Void> createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists) {
        tablePath.validate();
        CreateTableRequest request = new CreateTableRequest();
        request.setTableJson(tableDescriptor.toJsonBytes())
                .setIgnoreIfExists(ignoreIfExists)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.createTable(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> alterTable(
            TablePath tablePath, List<TableChange> tableChanges, boolean ignoreIfNotExists) {
        tablePath.validate();
        AlterTableRequest request =
                makeAlterTableRequest(tablePath, tableChanges, ignoreIfNotExists);
        return gateway.alterTable(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<TableInfo> getTableInfo(TablePath tablePath) {
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return readOnlyGateway
                .getTableInfo(request)
                .thenApply(
                        r ->
                                TableInfo.of(
                                        tablePath,
                                        r.getTableId(),
                                        r.getSchemaId(),
                                        TableDescriptor.fromJsonBytes(r.getTableJson()),
                                        r.getCreatedTime(),
                                        r.getModifiedTime()));
    }

    @Override
    public CompletableFuture<Void> dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
        DropTableRequest request = new DropTableRequest();
        request.setIgnoreIfNotExists(ignoreIfNotExists)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.dropTable(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Boolean> tableExists(TablePath tablePath) {
        TableExistsRequest request = new TableExistsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return readOnlyGateway.tableExists(request).thenApply(TableExistsResponse::isExists);
    }

    @Override
    public CompletableFuture<List<String>> listTables(String databaseName) {
        ListTablesRequest request = new ListTablesRequest();
        request.setDatabaseName(databaseName);
        return readOnlyGateway.listTables(request).thenApply(ListTablesResponse::getTableNamesList);
    }

    @Override
    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(TablePath tablePath) {
        return listPartitionInfos(tablePath, null);
    }

    @Override
    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(
            TablePath tablePath, PartitionSpec partitionSpec) {
        ListPartitionInfosRequest request = new ListPartitionInfosRequest();
        request.setTablePath(
                new PbTablePath()
                        .setDatabaseName(tablePath.getDatabaseName())
                        .setTableName(tablePath.getTableName()));

        if (partitionSpec != null) {
            PbPartitionSpec pbPartitionSpec = makePbPartitionSpec(partitionSpec);
            request.setPartialPartitionSpec(pbPartitionSpec);
        }
        return readOnlyGateway
                .listPartitionInfos(request)
                .thenApply(ClientRpcMessageUtils::toPartitionInfos);
    }

    @Override
    public CompletableFuture<Void> createPartition(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfExists) {
        return gateway.createPartition(
                        makeCreatePartitionRequest(tablePath, partitionSpec, ignoreIfExists))
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> dropPartition(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        return gateway.dropPartition(
                        makeDropPartitionRequest(tablePath, partitionSpec, ignoreIfNotExists))
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath) {
        GetLatestKvSnapshotsRequest request = new GetLatestKvSnapshotsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return readOnlyGateway
                .getLatestKvSnapshots(request)
                .thenApply(ClientRpcMessageUtils::toKvSnapshots);
    }

    @Override
    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(
            TablePath tablePath, String partitionName) {
        checkNotNull(partitionName, "partitionName");
        GetLatestKvSnapshotsRequest request = new GetLatestKvSnapshotsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        request.setPartitionName(partitionName);
        return readOnlyGateway
                .getLatestKvSnapshots(request)
                .thenApply(ClientRpcMessageUtils::toKvSnapshots);
    }

    @Override
    public CompletableFuture<KvSnapshotMetadata> getKvSnapshotMetadata(
            TableBucket bucket, long snapshotId) {
        GetKvSnapshotMetadataRequest request = new GetKvSnapshotMetadataRequest();
        if (bucket.getPartitionId() != null) {
            request.setPartitionId(bucket.getPartitionId());
        }
        request.setTableId(bucket.getTableId())
                .setBucketId(bucket.getBucket())
                .setSnapshotId(snapshotId);
        return readOnlyGateway
                .getKvSnapshotMetadata(request)
                .thenApply(ClientRpcMessageUtils::toKvSnapshotMetadata);
    }

    @Override
    public CompletableFuture<LakeSnapshot> getLatestLakeSnapshot(TablePath tablePath) {
        GetLatestLakeSnapshotRequest request = new GetLatestLakeSnapshotRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        return readOnlyGateway
                .getLatestLakeSnapshot(request)
                .thenApply(ClientRpcMessageUtils::toLakeTableSnapshotInfo);
    }

    @Override
    public ListOffsetsResult listOffsets(
            TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec) {
        return listOffsets(PhysicalTablePath.of(tablePath), buckets, offsetSpec);
    }

    @Override
    public ListOffsetsResult listOffsets(
            TablePath tablePath,
            String partitionName,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec) {
        return listOffsets(PhysicalTablePath.of(tablePath, partitionName), buckets, offsetSpec);
    }

    private ListOffsetsResult listOffsets(
            PhysicalTablePath physicalTablePath,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec) {
        Long partitionId = null;
        metadataUpdater.updateTableOrPartitionMetadata(physicalTablePath.getTablePath(), null);
        TableInfo tableInfo = getTableInfo(physicalTablePath.getTablePath()).join();

        // if partition name is not null, we need to check and update partition metadata
        if (physicalTablePath.getPartitionName() != null) {
            metadataUpdater.updatePhysicalTableMetadata(Collections.singleton(physicalTablePath));
            partitionId = metadataUpdater.getPartitionIdOrElseThrow(physicalTablePath);
        }
        Map<Integer, ListOffsetsRequest> requestMap =
                prepareListOffsetsRequests(
                        metadataUpdater,
                        tableInfo.getTableId(),
                        partitionId,
                        buckets,
                        offsetSpec,
                        tableInfo.getTablePath());
        Map<Integer, CompletableFuture<Long>> bucketToOffsetMap = MapUtils.newConcurrentHashMap();
        for (int bucket : buckets) {
            bucketToOffsetMap.put(bucket, new CompletableFuture<>());
        }

        sendListOffsetsRequest(metadataUpdater, requestMap, bucketToOffsetMap);
        return new ListOffsetsResult(bucketToOffsetMap);
    }

    @Override
    public CompletableFuture<Collection<AclBinding>> listAcls(AclBindingFilter aclBindingFilter) {
        CompletableFuture<Collection<AclBinding>> future = new CompletableFuture<>();
        ListAclsRequest listAclsRequest =
                new ListAclsRequest().setAclFilter(toPbAclFilter(aclBindingFilter));

        gateway.listAcls(listAclsRequest)
                .whenComplete(
                        (r, t) -> {
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(toAclBindings(r.getAclsList()));
                            }
                        });
        return future;
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> aclBindings) {
        CreateAclsResult result = new CreateAclsResult(aclBindings);
        CreateAclsRequest createAclsRequest =
                new CreateAclsRequest().addAllAcls(toPbAclInfos(aclBindings));

        gateway.createAcls(createAclsRequest)
                .whenComplete(
                        (r, t) -> {
                            if (t != null) {
                                result.completeExceptionally(t);
                            } else {
                                result.complete(r.getAclResList());
                            }
                        });
        return result;
    }

    @Override
    public DropAclsResult dropAcls(Collection<AclBindingFilter> filters) {
        DropAclsResult result = new DropAclsResult(filters);
        DropAclsRequest dropAclsRequest =
                new DropAclsRequest()
                        .addAllAclFilters(toPbAclBindingFilters(result.values().keySet()));
        gateway.dropAcls(dropAclsRequest)
                .whenComplete(
                        (r, t) -> {
                            if (t != null) {
                                result.completeExceptionally(t);
                            } else {
                                result.complete(r.getFilterResultsList());
                            }
                        });
        return result;
    }

    @Override
    public CompletableFuture<Collection<ConfigEntry>> describeClusterConfigs() {
        CompletableFuture<Collection<ConfigEntry>> future = new CompletableFuture<>();
        DescribeClusterConfigsRequest request = new DescribeClusterConfigsRequest();
        gateway.describeClusterConfigs(request)
                .whenComplete(
                        (r, t) -> {
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(toConfigEntries(r.getConfigsList()));
                            }
                        });
        return future;
    }

    @Override
    public CompletableFuture<Void> alterClusterConfigs(Collection<AlterConfig> configs) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        AlterClusterConfigsRequest request = new AlterClusterConfigsRequest();
        for (AlterConfig alterConfig : configs) {
            PbAlterConfig pBAlterConfig =
                    request.addAlterConfig()
                            .setConfigKey(alterConfig.key())
                            .setOpType(alterConfig.opType().value);
            if (alterConfig.value() != null) {
                pBAlterConfig.setConfigValue(alterConfig.value());
            }
        }
        gateway.alterClusterConfigs(request)
                .whenComplete(
                        (r, t) -> {
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(null);
                            }
                        });

        return future;
    }

    @Override
    public CompletableFuture<Void> addServerTag(List<Integer> tabletServers, ServerTag serverTag) {
        AddServerTagRequest request = new AddServerTagRequest().setServerTag(serverTag.value);
        tabletServers.forEach(request::addServerId);
        return gateway.addServerTag(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> removeServerTag(
            List<Integer> tabletServers, ServerTag serverTag) {
        RemoveServerTagRequest request = new RemoveServerTagRequest().setServerTag(serverTag.value);
        tabletServers.forEach(request::addServerId);
        return gateway.removeServerTag(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<String> rebalance(List<GoalType> priorityGoals) {
        RebalanceRequest request = new RebalanceRequest();
        priorityGoals.forEach(goal -> request.addGoal(goal.value));
        return gateway.rebalance(request).thenApply(RebalanceResponse::getRebalanceId);
    }

    @Override
    public CompletableFuture<Optional<RebalanceProgress>> listRebalanceProgress(
            @Nullable String rebalanceId) {
        ListRebalanceProgressRequest request = new ListRebalanceProgressRequest();

        if (rebalanceId != null) {
            request.setRebalanceId(rebalanceId);
        }

        return gateway.listRebalanceProgress(request)
                .thenApply(ClientRpcMessageUtils::toRebalanceProgress);
    }

    @Override
    public CompletableFuture<Void> cancelRebalance(@Nullable String rebalanceId) {
        CancelRebalanceRequest request = new CancelRebalanceRequest();

        if (rebalanceId != null) {
            request.setRebalanceId(rebalanceId);
        }

        return gateway.cancelRebalance(request).thenApply(r -> null);
    }

    @Override
    public void close() {
        // nothing to do yet
    }

    private static Map<Integer, ListOffsetsRequest> prepareListOffsetsRequests(
            MetadataUpdater metadataUpdater,
            long tableId,
            @Nullable Long partitionId,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec,
            TablePath tablePath) {
        Map<Integer, List<Integer>> nodeForBucketList = new HashMap<>();
        for (Integer bucketId : buckets) {
            int leader =
                    metadataUpdater.leaderFor(
                            tablePath, new TableBucket(tableId, partitionId, bucketId));
            nodeForBucketList.computeIfAbsent(leader, k -> new ArrayList<>()).add(bucketId);
        }

        Map<Integer, ListOffsetsRequest> listOffsetsRequests = new HashMap<>();
        nodeForBucketList.forEach(
                (leader, ids) ->
                        listOffsetsRequests.put(
                                leader,
                                makeListOffsetsRequest(tableId, partitionId, ids, offsetSpec)));
        return listOffsetsRequests;
    }

    private static void sendListOffsetsRequest(
            MetadataUpdater metadataUpdater,
            Map<Integer, ListOffsetsRequest> leaderToRequestMap,
            Map<Integer, CompletableFuture<Long>> bucketToOffsetMap) {
        leaderToRequestMap.forEach(
                (leader, request) -> {
                    TabletServerGateway gateway =
                            metadataUpdater.newTabletServerClientForNode(leader);
                    if (gateway == null) {
                        throw new LeaderNotAvailableException(
                                "Server " + leader + " is not found in metadata cache.");
                    } else {
                        gateway.listOffsets(request)
                                .thenAccept(
                                        r -> {
                                            for (PbListOffsetsRespForBucket resp :
                                                    r.getBucketsRespsList()) {
                                                if (resp.hasErrorCode()) {
                                                    bucketToOffsetMap
                                                            .get(resp.getBucketId())
                                                            .completeExceptionally(
                                                                    ApiError.fromErrorMessage(resp)
                                                                            .exception());
                                                } else {
                                                    bucketToOffsetMap
                                                            .get(resp.getBucketId())
                                                            .complete(resp.getOffset());
                                                }
                                            }
                                        });
                    }
                });
    }

    @VisibleForTesting
    public AdminGateway getAdminGateway() {
        return gateway;
    }

    @VisibleForTesting
    public AdminReadOnlyGateway getAdminReadOnlyGateway() {
        return readOnlyGateway;
    }
}
