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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.client.metadata.KvSnapshotMetadata;
import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.utils.ClientRpcMessageUtils;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.CreateAclsRequest;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.DropAclsRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.GetDatabaseInfoRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetTableInfoRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.ListAclsRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.PbListOffsetsRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPartitionSpec;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.utils.MapUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeCreatePartitionRequest;
import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeDropPartitionRequest;
import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeListOffsetsRequest;
import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makePbPartitionSpec;
import static com.alibaba.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBindings;
import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toPbAclBindingFilters;
import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toPbAclFilter;
import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * The default implementation of {@link Admin}.
 *
 * <p>This class is thread-safe. The API of this class is evolving, see {@link Admin} for details.
 */
public class FlussAdmin implements Admin {

    private final AdminGateway gateway;
    private final AdminReadOnlyGateway readOnlyGateway;
    private final MetadataUpdater metadataUpdater;
    private final RpcClient client;

    public FlussAdmin(RpcClient client, MetadataUpdater metadataUpdater) {
        this.gateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, client, AdminGateway.class);
        this.readOnlyGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getRandomTabletServer, client, AdminGateway.class);
        this.metadataUpdater = metadataUpdater;
        this.client = client;
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
        long tableId = metadataUpdater.getTableId(physicalTablePath.getTablePath());
        // if partition name is not null, we need to check and update partition metadata
        if (physicalTablePath.getPartitionName() != null) {
            metadataUpdater.updatePhysicalTableMetadata(Collections.singleton(physicalTablePath));
            partitionId = metadataUpdater.getPartitionIdOrElseThrow(physicalTablePath);
        }
        Map<Integer, ListOffsetsRequest> requestMap =
                prepareListOffsetsRequests(
                        metadataUpdater, tableId, partitionId, buckets, offsetSpec);
        Map<Integer, CompletableFuture<Long>> bucketToOffsetMap = MapUtils.newConcurrentHashMap();
        for (int bucket : buckets) {
            bucketToOffsetMap.put(bucket, new CompletableFuture<>());
        }

        sendListOffsetsRequest(metadataUpdater, client, requestMap, bucketToOffsetMap);
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
    public void close() {
        // nothing to do yet
    }

    private static Map<Integer, ListOffsetsRequest> prepareListOffsetsRequests(
            MetadataUpdater metadataUpdater,
            long tableId,
            @Nullable Long partitionId,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec) {
        Map<Integer, List<Integer>> nodeForBucketList = new HashMap<>();
        for (Integer bucketId : buckets) {
            int leader = metadataUpdater.leaderFor(new TableBucket(tableId, partitionId, bucketId));
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
            RpcClient client,
            Map<Integer, ListOffsetsRequest> leaderToRequestMap,
            Map<Integer, CompletableFuture<Long>> bucketToOffsetMap) {
        leaderToRequestMap.forEach(
                (leader, request) -> {
                    TabletServerGateway gateway =
                            GatewayClientProxy.createGatewayProxy(
                                    () -> metadataUpdater.getTabletServer(leader),
                                    client,
                                    TabletServerGateway.class);
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
                });
    }
}
