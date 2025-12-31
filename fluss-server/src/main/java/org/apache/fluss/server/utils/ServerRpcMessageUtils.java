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

package org.apache.fluss.server.utils;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ColumnPositionType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.FileChannelChunk;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.LimitScanResultForBucket;
import org.apache.fluss.rpc.entity.ListOffsetsResultForBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PrefixLookupResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.AdjustIsrRequest;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.CommitKvSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.CreateAclsResponse;
import org.apache.fluss.rpc.messages.DropAclsResponse;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.InitWriterResponse;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListAclsResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.PbAclInfo;
import org.apache.fluss.rpc.messages.PbAddColumn;
import org.apache.fluss.rpc.messages.PbAdjustIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbAdjustIsrReqForTable;
import org.apache.fluss.rpc.messages.PbAdjustIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbAdjustIsrRespForTable;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbBucketOffset;
import org.apache.fluss.rpc.messages.PbCreateAclRespInfo;
import org.apache.fluss.rpc.messages.PbDescribeConfig;
import org.apache.fluss.rpc.messages.PbDropAclsFilterResult;
import org.apache.fluss.rpc.messages.PbDropAclsMatchingAcl;
import org.apache.fluss.rpc.messages.PbDropColumn;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.messages.PbKeyValue;
import org.apache.fluss.rpc.messages.PbKvSnapshot;
import org.apache.fluss.rpc.messages.PbLakeSnapshotForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotMetadata;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbModifyColumn;
import org.apache.fluss.rpc.messages.PbNotifyLakeTableOffsetReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbPartitionSpec;
import org.apache.fluss.rpc.messages.PbPhysicalTablePath;
import org.apache.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PbRemoteLogSegment;
import org.apache.fluss.rpc.messages.PbRemotePathAndLocalFile;
import org.apache.fluss.rpc.messages.PbRenameColumn;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaRespForBucket;
import org.apache.fluss.rpc.messages.PbTableBucket;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.PbValue;
import org.apache.fluss.rpc.messages.PbValueList;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.server.authorizer.AclCreateResult;
import org.apache.fluss.server.authorizer.AclDeleteResult;
import org.apache.fluss.server.entity.AdjustIsrResultForBucket;
import org.apache.fluss.server.entity.CommitLakeTableSnapshotData;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.LakeBucketOffset;
import org.apache.fluss.server.entity.NotifyKvSnapshotOffsetData;
import org.apache.fluss.server.entity.NotifyLakeTableOffsetData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.entity.NotifyRemoteLogOffsetsData;
import org.apache.fluss.server.entity.StopReplicaData;
import org.apache.fluss.server.entity.StopReplicaResultForBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.json.DataTypeJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toByteBuffer;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfo;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response.
 */
public class ServerRpcMessageUtils {

    public static TablePath toTablePath(PbTablePath pbTablePath) {
        return new TablePath(pbTablePath.getDatabaseName(), pbTablePath.getTableName());
    }

    public static PhysicalTablePath toPhysicalTablePath(PbPhysicalTablePath pbPhysicalPath) {
        return PhysicalTablePath.of(
                pbPhysicalPath.getDatabaseName(),
                pbPhysicalPath.getTableName(),
                pbPhysicalPath.hasPartitionName() ? pbPhysicalPath.getPartitionName() : null);
    }

    public static PbPhysicalTablePath fromPhysicalTablePath(PhysicalTablePath physicalPath) {
        PbPhysicalTablePath pbPath =
                new PbPhysicalTablePath()
                        .setDatabaseName(physicalPath.getDatabaseName())
                        .setTableName(physicalPath.getTableName());
        if (physicalPath.getPartitionName() != null) {
            pbPath.setPartitionName(physicalPath.getPartitionName());
        }
        return pbPath;
    }

    public static PbTablePath fromTablePath(TablePath tablePath) {
        return new PbTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
    }

    public static TableBucket toTableBucket(PbTableBucket protoTableBucket) {
        return new TableBucket(
                protoTableBucket.getTableId(),
                protoTableBucket.hasPartitionId() ? protoTableBucket.getPartitionId() : null,
                protoTableBucket.getBucketId());
    }

    public static PbTableBucket fromTableBucket(TableBucket tableBucket) {
        PbTableBucket pbTableBucket =
                new PbTableBucket()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket());
        if (tableBucket.getPartitionId() != null) {
            pbTableBucket.setPartitionId(tableBucket.getPartitionId());
        }
        return pbTableBucket;
    }

    public static ServerNode toServerNode(PbServerNode pbServerNode, ServerType serverType) {
        return new ServerNode(
                pbServerNode.getNodeId(),
                pbServerNode.getHost(),
                pbServerNode.getPort(),
                serverType,
                pbServerNode.hasRack() ? pbServerNode.getRack() : null);
    }

    public static TableChange toTableChange(PbAlterConfig pbAlterConfig) {
        AlterConfigOpType opType = AlterConfigOpType.from(pbAlterConfig.getOpType());
        switch (opType) {
            case SET: // SET_OPTION
                return TableChange.set(
                        pbAlterConfig.getConfigKey(), pbAlterConfig.getConfigValue());
            case DELETE: // RESET_OPTION
                return TableChange.reset(pbAlterConfig.getConfigKey());
            case APPEND:
            case SUBTRACT:
            default:
                throw new IllegalArgumentException(
                        "Unsupported alter configs op type " + pbAlterConfig.getOpType());
        }
    }

    public static List<TableChange> toAlterTableConfigChanges(List<PbAlterConfig> alterConfigs) {
        return alterConfigs.stream()
                .filter(Objects::nonNull)
                .map(ServerRpcMessageUtils::toTableChange)
                .collect(Collectors.toList());
    }

    public static List<TableChange> toAlterTableSchemaChanges(AlterTableRequest request) {
        List<TableChange> alterTableSchemaChanges = new ArrayList<>();
        alterTableSchemaChanges.addAll(toAddColumns(request.getAddColumnsList()));
        alterTableSchemaChanges.addAll(toDropColumns(request.getDropColumnsList()));
        alterTableSchemaChanges.addAll(toRenameColumns(request.getRenameColumnsList()));
        alterTableSchemaChanges.addAll(toModifyColumns(request.getModifyColumnsList()));
        return alterTableSchemaChanges;
    }

    public static List<TableChange> toAddColumns(List<PbAddColumn> addColumns) {
        return addColumns.stream()
                .filter(Objects::nonNull)
                .map(
                        pbAddColumn ->
                                TableChange.addColumn(
                                        pbAddColumn.getColumnName(),
                                        JsonSerdeUtils.readValue(
                                                pbAddColumn.getDataTypeJson(),
                                                DataTypeJsonSerde.INSTANCE),
                                        pbAddColumn.hasComment() ? pbAddColumn.getComment() : null,
                                        toColumnPosition(pbAddColumn.getColumnPositionType())))
                .collect(Collectors.toList());
    }

    public static List<TableChange.SchemaChange> toDropColumns(List<PbDropColumn> dropColumns) {
        return dropColumns.stream()
                .filter(Objects::nonNull)
                .map(pbDropColumn -> TableChange.dropColumn(pbDropColumn.getColumnName()))
                .collect(Collectors.toList());
    }

    public static List<TableChange.SchemaChange> toRenameColumns(
            List<PbRenameColumn> alterColumns) {
        return alterColumns.stream()
                .filter(Objects::nonNull)
                .map(
                        pbRenameColumn ->
                                TableChange.renameColumn(
                                        pbRenameColumn.getOldColumnName(),
                                        pbRenameColumn.getNewColumnName()))
                .collect(Collectors.toList());
    }

    public static List<TableChange.SchemaChange> toModifyColumns(
            List<PbModifyColumn> modifyColumns) {
        return modifyColumns.stream()
                .filter(Objects::nonNull)
                .map(
                        pbModifyColumn ->
                                TableChange.modifyColumn(
                                        pbModifyColumn.getColumnName(),
                                        JsonSerdeUtils.readValue(
                                                pbModifyColumn.getDataTypeJson(),
                                                DataTypeJsonSerde.INSTANCE),
                                        pbModifyColumn.hasComment()
                                                ? pbModifyColumn.getComment()
                                                : null,
                                        pbModifyColumn.hasColumnPositionType()
                                                ? toColumnPosition(
                                                        pbModifyColumn.getColumnPositionType())
                                                : null))
                .collect(Collectors.toList());
    }

    private static TableChange.ColumnPosition toColumnPosition(int columnPositionType) {
        ColumnPositionType opType = ColumnPositionType.from(columnPositionType);
        switch (opType) {
            case LAST:
                return TableChange.ColumnPosition.last();
            default:
                throw new IllegalArgumentException("Unsupported column position type " + opType);
        }
    }

    public static MetadataResponse buildMetadataResponse(
            @Nullable ServerNode coordinatorServer,
            Set<ServerNode> aliveTabletServers,
            List<TableMetadata> tableMetadataList,
            List<PartitionMetadata> partitionMetadataList) {
        MetadataResponse metadataResponse = new MetadataResponse();

        if (coordinatorServer != null) {
            metadataResponse
                    .setCoordinatorServer()
                    .setNodeId(coordinatorServer.id())
                    .setHost(coordinatorServer.host())
                    .setPort(coordinatorServer.port());
        }

        List<PbServerNode> pbServerNodeList = new ArrayList<>();
        for (ServerNode serverNode : aliveTabletServers) {
            PbServerNode pbServerNode =
                    new PbServerNode()
                            .setNodeId(serverNode.id())
                            .setHost(serverNode.host())
                            .setPort(serverNode.port());
            if (serverNode.rack() != null) {
                pbServerNode.setRack(serverNode.rack());
            }
            pbServerNodeList.add(pbServerNode);
        }

        List<PbTableMetadata> pbTableMetadataList = new ArrayList<>();
        tableMetadataList.forEach(
                tableMetadata -> pbTableMetadataList.add(toPbTableMetadata(tableMetadata)));

        List<PbPartitionMetadata> pbPartitionMetadataList = new ArrayList<>();
        partitionMetadataList.forEach(
                partitionMetadata ->
                        pbPartitionMetadataList.add(toPbPartitionMetadata(partitionMetadata)));

        metadataResponse.addAllTabletServers(pbServerNodeList);
        metadataResponse.addAllTableMetadatas(pbTableMetadataList);
        metadataResponse.addAllPartitionMetadatas(pbPartitionMetadataList);
        return metadataResponse;
    }

    public static UpdateMetadataRequest makeUpdateMetadataRequest(
            @Nullable ServerInfo coordinatorServer,
            Set<ServerInfo> aliveTableServers,
            List<TableMetadata> tableMetadataList,
            List<PartitionMetadata> partitionMetadataList) {
        UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest();
        Set<PbServerNode> aliveTableServerNodes = new HashSet<>();
        for (ServerInfo serverInfo : aliveTableServers) {
            List<Endpoint> endpoints = serverInfo.endpoints();
            PbServerNode pbTabletServerNode =
                    new PbServerNode()
                            .setNodeId(serverInfo.id())
                            .setListeners(Endpoint.toListenersString(endpoints))
                            // for backward compatibility for versions <= 0.6
                            .setHost(endpoints.get(0).getHost())
                            .setPort(endpoints.get(0).getPort());
            if (serverInfo.rack() != null) {
                pbTabletServerNode.setRack(serverInfo.rack());
            }
            aliveTableServerNodes.add(pbTabletServerNode);
        }
        updateMetadataRequest.addAllTabletServers(aliveTableServerNodes);

        if (coordinatorServer != null) {
            updateMetadataRequest
                    .setCoordinatorServer()
                    .setNodeId(coordinatorServer.id())
                    .setListeners(Endpoint.toListenersString(coordinatorServer.endpoints()))
                    // for backward compatibility for versions <= 0.6
                    .setHost(coordinatorServer.endpoints().get(0).getHost())
                    .setPort(coordinatorServer.endpoints().get(0).getPort());
        }

        List<PbTableMetadata> pbTableMetadataList = new ArrayList<>();
        tableMetadataList.forEach(
                tableMetadata -> pbTableMetadataList.add(toPbTableMetadata(tableMetadata)));

        List<PbPartitionMetadata> pbPartitionMetadataList = new ArrayList<>();
        partitionMetadataList.forEach(
                partitionMetadata ->
                        pbPartitionMetadataList.add(toPbPartitionMetadata(partitionMetadata)));
        updateMetadataRequest.addAllTableMetadatas(pbTableMetadataList);
        updateMetadataRequest.addAllPartitionMetadatas(pbPartitionMetadataList);

        return updateMetadataRequest;
    }

    public static ClusterMetadata getUpdateMetadataRequestData(UpdateMetadataRequest request) {
        ServerInfo coordinatorServer = null;
        if (request.hasCoordinatorServer()) {
            PbServerNode pbCoordinatorServer = request.getCoordinatorServer();
            List<Endpoint> endpoints =
                    pbCoordinatorServer.hasListeners()
                            ? Endpoint.fromListenersString(pbCoordinatorServer.getListeners())
                            // backward compatible with old version that doesn't have listeners
                            : Collections.singletonList(
                                    new Endpoint(
                                            pbCoordinatorServer.getHost(),
                                            pbCoordinatorServer.getPort(),
                                            // TODO: maybe use internal listener name from conf
                                            ConfigOptions.INTERNAL_LISTENER_NAME.defaultValue()));
            coordinatorServer =
                    new ServerInfo(
                            pbCoordinatorServer.getNodeId(),
                            pbCoordinatorServer.hasRack() ? pbCoordinatorServer.getRack() : null,
                            endpoints,
                            ServerType.COORDINATOR);
        }

        Set<ServerInfo> aliveTabletServers = new HashSet<>();
        for (PbServerNode tabletServer : request.getTabletServersList()) {
            List<Endpoint> endpoints =
                    tabletServer.hasListeners()
                            ? Endpoint.fromListenersString(tabletServer.getListeners())
                            // backward compatible with old version that doesn't have listeners
                            : Collections.singletonList(
                                    new Endpoint(
                                            tabletServer.getHost(),
                                            tabletServer.getPort(),
                                            // TODO: maybe use internal listener name from conf
                                            ConfigOptions.INTERNAL_LISTENER_NAME.defaultValue()));
            aliveTabletServers.add(
                    new ServerInfo(
                            tabletServer.getNodeId(),
                            tabletServer.hasRack() ? tabletServer.getRack() : null,
                            endpoints,
                            ServerType.TABLET_SERVER));
        }

        List<TableMetadata> tableMetadataList = new ArrayList<>();
        request.getTableMetadatasList()
                .forEach(tableMetadata -> tableMetadataList.add(toTableMetaData(tableMetadata)));

        List<PartitionMetadata> partitionMetadataList = new ArrayList<>();
        request.getPartitionMetadatasList()
                .forEach(
                        partitionMetadata ->
                                partitionMetadataList.add(toPartitionMetadata(partitionMetadata)));

        return new ClusterMetadata(
                coordinatorServer, aliveTabletServers, tableMetadataList, partitionMetadataList);
    }

    private static PbTableMetadata toPbTableMetadata(TableMetadata tableMetadata) {
        TableInfo tableInfo = tableMetadata.getTableInfo();
        PbTableMetadata pbTableMetadata =
                new PbTableMetadata()
                        .setTableId(tableInfo.getTableId())
                        .setSchemaId(tableInfo.getSchemaId())
                        .setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                        .setCreatedTime(tableInfo.getCreatedTime())
                        .setModifiedTime(tableInfo.getModifiedTime());
        TablePath tablePath = tableInfo.getTablePath();
        pbTableMetadata
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        pbTableMetadata.addAllBucketMetadatas(
                toPbBucketMetadata(tableMetadata.getBucketMetadataList()));
        return pbTableMetadata;
    }

    private static PbPartitionMetadata toPbPartitionMetadata(PartitionMetadata partitionMetadata) {
        PbPartitionMetadata pbPartitionMetadata =
                new PbPartitionMetadata()
                        .setTableId(partitionMetadata.getTableId())
                        .setPartitionId(partitionMetadata.getPartitionId())
                        .setPartitionName(partitionMetadata.getPartitionName());
        pbPartitionMetadata.addAllBucketMetadatas(
                toPbBucketMetadata(partitionMetadata.getBucketMetadataList()));
        return pbPartitionMetadata;
    }

    private static List<PbBucketMetadata> toPbBucketMetadata(
            List<BucketMetadata> bucketMetadataList) {
        List<PbBucketMetadata> pbBucketMetadataList = new ArrayList<>();
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            PbBucketMetadata pbBucketMetadata =
                    new PbBucketMetadata().setBucketId(bucketMetadata.getBucketId());

            OptionalInt leaderEpochOpt = bucketMetadata.getLeaderEpoch();
            if (leaderEpochOpt.isPresent()) {
                pbBucketMetadata.setLeaderEpoch(leaderEpochOpt.getAsInt());
            }

            OptionalInt leaderId = bucketMetadata.getLeaderId();
            if (leaderId.isPresent()) {
                pbBucketMetadata.setLeaderId(leaderId.getAsInt());
            }

            for (Integer replica : bucketMetadata.getReplicas()) {
                pbBucketMetadata.addReplicaId(replica);
            }

            pbBucketMetadataList.add(pbBucketMetadata);
        }
        return pbBucketMetadataList;
    }

    private static TableMetadata toTableMetaData(PbTableMetadata pbTableMetadata) {
        TablePath tablePath = toTablePath(pbTableMetadata.getTablePath());
        long tableId = pbTableMetadata.getTableId();
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        tableId,
                        pbTableMetadata.getSchemaId(),
                        TableDescriptor.fromJsonBytes(pbTableMetadata.getTableJson()),
                        pbTableMetadata.getCreatedTime(),
                        pbTableMetadata.getModifiedTime());

        List<BucketMetadata> bucketMetadata = new ArrayList<>();
        for (PbBucketMetadata pbBucketMetadata : pbTableMetadata.getBucketMetadatasList()) {
            bucketMetadata.add(toBucketMetadata(pbBucketMetadata));
        }

        return new TableMetadata(tableInfo, bucketMetadata);
    }

    private static BucketMetadata toBucketMetadata(PbBucketMetadata pbBucketMetadata) {
        return new BucketMetadata(
                pbBucketMetadata.getBucketId(),
                pbBucketMetadata.hasLeaderId() ? pbBucketMetadata.getLeaderId() : null,
                pbBucketMetadata.hasLeaderEpoch() ? pbBucketMetadata.getLeaderEpoch() : null,
                Arrays.stream(pbBucketMetadata.getReplicaIds())
                        .boxed()
                        .collect(Collectors.toList()));
    }

    private static PartitionMetadata toPartitionMetadata(PbPartitionMetadata pbPartitionMetadata) {
        return new PartitionMetadata(
                pbPartitionMetadata.getTableId(),
                pbPartitionMetadata.getPartitionName(),
                pbPartitionMetadata.getPartitionId(),
                pbPartitionMetadata.getBucketMetadatasList().stream()
                        .map(ServerRpcMessageUtils::toBucketMetadata)
                        .collect(Collectors.toList()));
    }

    public static NotifyLeaderAndIsrRequest makeNotifyLeaderAndIsrRequest(
            int coordinatorEpoch, Collection<PbNotifyLeaderAndIsrReqForBucket> notifyLeaders) {
        return new NotifyLeaderAndIsrRequest()
                .setCoordinatorEpoch(coordinatorEpoch)
                .addAllNotifyBucketsLeaderReqs(notifyLeaders);
    }

    public static PbNotifyLeaderAndIsrReqForBucket makeNotifyBucketLeaderAndIsr(
            NotifyLeaderAndIsrData notifyLeaderAndIsrData) {
        PbNotifyLeaderAndIsrReqForBucket reqForBucket =
                new PbNotifyLeaderAndIsrReqForBucket()
                        .setLeader(notifyLeaderAndIsrData.getLeader())
                        .setLeaderEpoch(notifyLeaderAndIsrData.getLeaderEpoch())
                        .setBucketEpoch(notifyLeaderAndIsrData.getBucketEpoch());

        TableBucket tb = notifyLeaderAndIsrData.getTableBucket();
        PbTableBucket pbTableBucket =
                reqForBucket
                        .setTableBucket()
                        .setTableId(tb.getTableId())
                        .setBucketId(tb.getBucket());
        if (tb.getPartitionId() != null) {
            pbTableBucket.setPartitionId(tb.getPartitionId());
        }

        PhysicalTablePath physicalTablePath = notifyLeaderAndIsrData.getPhysicalTablePath();
        reqForBucket
                .setPhysicalTablePath(fromPhysicalTablePath(physicalTablePath))
                .setReplicas(notifyLeaderAndIsrData.getReplicasArray())
                .setIsrs(notifyLeaderAndIsrData.getIsrArray());

        return reqForBucket;
    }

    public static List<NotifyLeaderAndIsrData> getNotifyLeaderAndIsrRequestData(
            NotifyLeaderAndIsrRequest request) {
        List<NotifyLeaderAndIsrData> notifyLeaderAndIsrDataList = new ArrayList<>();
        for (PbNotifyLeaderAndIsrReqForBucket reqForBucket :
                request.getNotifyBucketsLeaderReqsList()) {
            List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < reqForBucket.getReplicasCount(); i++) {
                replicas.add(reqForBucket.getReplicaAt(i));
            }

            List<Integer> isr = new ArrayList<>();
            for (int i = 0; i < reqForBucket.getIsrsCount(); i++) {
                isr.add(reqForBucket.getIsrAt(i));
            }

            PbTableBucket pbTableBucket = reqForBucket.getTableBucket();
            notifyLeaderAndIsrDataList.add(
                    new NotifyLeaderAndIsrData(
                            toPhysicalTablePath(reqForBucket.getPhysicalTablePath()),
                            toTableBucket(pbTableBucket),
                            replicas,
                            new LeaderAndIsr(
                                    reqForBucket.getLeader(),
                                    reqForBucket.getLeaderEpoch(),
                                    isr,
                                    request.getCoordinatorEpoch(),
                                    reqForBucket.getBucketEpoch())));
        }
        return notifyLeaderAndIsrDataList;
    }

    public static NotifyLeaderAndIsrResponse makeNotifyLeaderAndIsrResponse(
            List<NotifyLeaderAndIsrResultForBucket> bucketsResult) {
        NotifyLeaderAndIsrResponse notifyLeaderAndIsrResponse = new NotifyLeaderAndIsrResponse();
        List<PbNotifyLeaderAndIsrRespForBucket> respForBuckets = new ArrayList<>();
        for (NotifyLeaderAndIsrResultForBucket bucketResult : bucketsResult) {
            PbNotifyLeaderAndIsrRespForBucket respForBucket =
                    new PbNotifyLeaderAndIsrRespForBucket();
            TableBucket tableBucket = bucketResult.getTableBucket();
            PbTableBucket pbTableBucket =
                    respForBucket
                            .setTableBucket()
                            .setTableId(tableBucket.getTableId())
                            .setBucketId(tableBucket.getBucket());
            if (tableBucket.getPartitionId() != null) {
                pbTableBucket.setPartitionId(tableBucket.getPartitionId());
            }
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            }
            respForBuckets.add(respForBucket);
        }
        notifyLeaderAndIsrResponse.addAllNotifyBucketsLeaderResps(respForBuckets);
        return notifyLeaderAndIsrResponse;
    }

    public static List<NotifyLeaderAndIsrResultForBucket> getNotifyLeaderAndIsrResponseData(
            NotifyLeaderAndIsrResponse response) {
        List<NotifyLeaderAndIsrResultForBucket> notifyLeaderAndIsrResultForBuckets =
                new ArrayList<>();
        for (PbNotifyLeaderAndIsrRespForBucket protoNotifyLeaderRespForBucket :
                response.getNotifyBucketsLeaderRespsList()) {
            TableBucket tableBucket =
                    toTableBucket(protoNotifyLeaderRespForBucket.getTableBucket());
            // construct the result for notify bucket leader and isr
            NotifyLeaderAndIsrResultForBucket notifyLeaderAndIsrResultForBucket =
                    protoNotifyLeaderRespForBucket.hasErrorCode()
                            ? new NotifyLeaderAndIsrResultForBucket(
                                    tableBucket,
                                    ApiError.fromErrorMessage(protoNotifyLeaderRespForBucket))
                            : new NotifyLeaderAndIsrResultForBucket(tableBucket);
            notifyLeaderAndIsrResultForBuckets.add(notifyLeaderAndIsrResultForBucket);
        }
        return notifyLeaderAndIsrResultForBuckets;
    }

    public static PbStopReplicaReqForBucket makeStopBucketReplica(
            TableBucket tableBucket, boolean isDelete, int leaderEpoch) {
        PbStopReplicaReqForBucket stopBucketReplicaRequest = new PbStopReplicaReqForBucket();
        PbTableBucket pbTableBucket =
                stopBucketReplicaRequest
                        .setDelete(isDelete)
                        .setLeaderEpoch(leaderEpoch)
                        .setTableBucket()
                        .setBucketId(tableBucket.getBucket())
                        .setTableId(tableBucket.getTableId());
        if (tableBucket.getPartitionId() != null) {
            pbTableBucket.setPartitionId(tableBucket.getPartitionId());
        }
        return stopBucketReplicaRequest;
    }

    public static List<StopReplicaData> getStopReplicaData(StopReplicaRequest request) {
        List<StopReplicaData> stopReplicaDataList = new ArrayList<>();
        for (PbStopReplicaReqForBucket reqForBucket : request.getStopReplicasReqsList()) {
            PbTableBucket tableBucket = reqForBucket.getTableBucket();
            stopReplicaDataList.add(
                    new StopReplicaData(
                            toTableBucket(tableBucket),
                            reqForBucket.isDelete(),
                            request.getCoordinatorEpoch(),
                            reqForBucket.getLeaderEpoch()));
        }

        return stopReplicaDataList;
    }

    public static StopReplicaResponse makeStopReplicaResponse(
            List<StopReplicaResultForBucket> resultForBuckets) {
        StopReplicaResponse stopReplicaResponse = new StopReplicaResponse();
        List<PbStopReplicaRespForBucket> stopReplicaRespForBucketList = new ArrayList<>();
        for (StopReplicaResultForBucket bucketResult : resultForBuckets) {
            PbStopReplicaRespForBucket respForBucket = new PbStopReplicaRespForBucket();
            PbTableBucket pbTableBucket =
                    respForBucket
                            .setTableBucket()
                            .setTableId(bucketResult.getTableId())
                            .setBucketId(bucketResult.getBucketId());
            if (bucketResult.getTableBucket().getPartitionId() != null) {
                pbTableBucket.setPartitionId(bucketResult.getTableBucket().getPartitionId());
            }
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            }
            stopReplicaRespForBucketList.add(respForBucket);
        }
        stopReplicaResponse.addAllStopReplicasResps(stopReplicaRespForBucketList);
        return stopReplicaResponse;
    }

    public static Map<TableBucket, MemoryLogRecords> getProduceLogData(
            ProduceLogRequest produceRequest) {
        long tableId = produceRequest.getTableId();
        Map<TableBucket, MemoryLogRecords> produceEntryData = new HashMap<>();
        for (PbProduceLogReqForBucket produceLogReqForBucket :
                produceRequest.getBucketsReqsList()) {
            ByteBuffer recordBuffer = toByteBuffer(produceLogReqForBucket.getRecordsSlice());
            MemoryLogRecords logRecords = MemoryLogRecords.pointToByteBuffer(recordBuffer);
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            produceLogReqForBucket.hasPartitionId()
                                    ? produceLogReqForBucket.getPartitionId()
                                    : null,
                            produceLogReqForBucket.getBucketId());
            produceEntryData.put(tb, logRecords);
        }
        return produceEntryData;
    }

    public static ProduceLogResponse makeProduceLogResponse(
            Collection<ProduceLogResultForBucket> appendLogResultForBucketList) {
        ProduceLogResponse produceResponse = new ProduceLogResponse();
        List<PbProduceLogRespForBucket> produceLogRespForBucketList = new ArrayList<>();
        for (ProduceLogResultForBucket bucketResult : appendLogResultForBucketList) {
            PbProduceLogRespForBucket producedBucket =
                    new PbProduceLogRespForBucket().setBucketId(bucketResult.getBucketId());
            TableBucket tableBucket = bucketResult.getTableBucket();
            if (tableBucket.getPartitionId() != null) {
                producedBucket.setPartitionId(tableBucket.getPartitionId());
            }

            if (bucketResult.failed()) {
                producedBucket.setError(
                        bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                producedBucket.setBaseOffset(bucketResult.getBaseOffset());
            }
            produceLogRespForBucketList.add(producedBucket);
        }
        produceResponse.addAllBucketsResps(produceLogRespForBucketList);
        return produceResponse;
    }

    public static Map<TableBucket, FetchReqInfo> getFetchLogData(FetchLogRequest request) {
        Map<TableBucket, FetchReqInfo> fetchDataMap = new HashMap<>();
        for (PbFetchLogReqForTable fetchLogReqForTable : request.getTablesReqsList()) {
            long tableId = fetchLogReqForTable.getTableId();
            final int[] projectionFields;
            if (fetchLogReqForTable.isProjectionPushdownEnabled()) {
                projectionFields = fetchLogReqForTable.getProjectedFields();
            } else {
                projectionFields = null;
            }

            List<PbFetchLogReqForBucket> bucketsReqsList = fetchLogReqForTable.getBucketsReqsList();
            for (PbFetchLogReqForBucket fetchLogReqForBucket : bucketsReqsList) {
                int bucketId = fetchLogReqForBucket.getBucketId();
                fetchDataMap.put(
                        new TableBucket(
                                tableId,
                                fetchLogReqForBucket.hasPartitionId()
                                        ? fetchLogReqForBucket.getPartitionId()
                                        : null,
                                bucketId),
                        new FetchReqInfo(
                                tableId,
                                fetchLogReqForBucket.getFetchOffset(),
                                fetchLogReqForBucket.getMaxFetchBytes(),
                                projectionFields));
            }
        }

        return fetchDataMap;
    }

    public static FetchLogResponse makeFetchLogResponse(
            Map<TableBucket, FetchLogResultForBucket> fetchLogResult,
            Map<TableBucket, FetchLogResultForBucket> fetchLogErrors) {
        return makeFetchLogResponse(mergeResponse(fetchLogResult, fetchLogErrors));
    }

    public static FetchLogResponse makeFetchLogResponse(
            Map<TableBucket, FetchLogResultForBucket> fetchLogResult) {
        Map<Long, List<PbFetchLogRespForBucket>> fetchLogRespMap = new HashMap<>();
        for (Map.Entry<TableBucket, FetchLogResultForBucket> entry : fetchLogResult.entrySet()) {
            TableBucket tb = entry.getKey();
            FetchLogResultForBucket bucketResult = entry.getValue();
            PbFetchLogRespForBucket fetchLogRespForBucket =
                    new PbFetchLogRespForBucket().setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                fetchLogRespForBucket.setPartitionId(tb.getPartitionId());
            }
            if (bucketResult.failed()) {
                fetchLogRespForBucket.setError(
                        bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                fetchLogRespForBucket
                        .setHighWatermark(bucketResult.getHighWatermark())
                        // TODO: set log start offset here if we support log clean.
                        .setLogStartOffset(0L);

                if (bucketResult.fetchFromRemote()) {
                    // set remote log fetch info.
                    RemoteLogFetchInfo rlfInfo = bucketResult.remoteLogFetchInfo();
                    checkNotNull(rlfInfo, "Remote log fetch info is null.");
                    List<PbRemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
                    for (RemoteLogSegment logSegment : rlfInfo.remoteLogSegmentList()) {
                        PbRemoteLogSegment pbRemoteLogSegment =
                                new PbRemoteLogSegment()
                                        .setRemoteLogStartOffset(logSegment.remoteLogStartOffset())
                                        .setRemoteLogSegmentId(
                                                logSegment.remoteLogSegmentId().toString())
                                        .setRemoteLogEndOffset(logSegment.remoteLogEndOffset())
                                        .setSegmentSizeInBytes(logSegment.segmentSizeInBytes())
                                        .setMaxTimestamp(logSegment.maxTimestamp());
                        remoteLogSegmentList.add(pbRemoteLogSegment);
                    }
                    fetchLogRespForBucket
                            .setRemoteLogFetchInfo()
                            .setRemoteLogTabletDir(rlfInfo.remoteLogTabletDir())
                            .addAllRemoteLogSegments(remoteLogSegmentList)
                            .setFirstStartPos(rlfInfo.firstStartPos());
                    if (rlfInfo.partitionName() != null) {
                        fetchLogRespForBucket
                                .setRemoteLogFetchInfo()
                                .setPartitionName(rlfInfo.partitionName());
                    }
                } else {
                    // set records
                    LogRecords records = bucketResult.recordsOrEmpty();
                    if (records instanceof FileLogRecords) {
                        FileChannelChunk chunk = ((FileLogRecords) records).toChunk();
                        // zero-copy optimization for file channel
                        fetchLogRespForBucket.setRecords(
                                chunk.getFileChannel(), chunk.getPosition(), chunk.getSize());
                    } else if (records instanceof MemoryLogRecords) {
                        // this should never happen, but we still support fetch memory log records.
                        if (records == MemoryLogRecords.EMPTY) {
                            fetchLogRespForBucket.setRecords(new byte[0]);
                        } else {
                            MemoryLogRecords logRecords = (MemoryLogRecords) records;
                            fetchLogRespForBucket.setRecords(
                                    logRecords.getMemorySegment(),
                                    logRecords.getPosition(),
                                    logRecords.sizeInBytes());
                        }
                    } else if (records instanceof BytesViewLogRecords) {
                        // zero-copy for project push down.
                        fetchLogRespForBucket.setRecordsBytesView(
                                ((BytesViewLogRecords) records).getBytesView());
                    } else {
                        throw new UnsupportedOperationException(
                                "Not supported log records type: " + records.getClass().getName());
                    }
                }
            }
            if (fetchLogRespMap.containsKey(tb.getTableId())) {
                fetchLogRespMap.get(tb.getTableId()).add(fetchLogRespForBucket);
            } else {
                List<PbFetchLogRespForBucket> fetchLogRespForBuckets = new ArrayList<>();
                fetchLogRespForBuckets.add(fetchLogRespForBucket);
                fetchLogRespMap.put(tb.getTableId(), fetchLogRespForBuckets);
            }
        }

        List<PbFetchLogRespForTable> fetchLogRespForTables = new ArrayList<>();
        for (Map.Entry<Long, List<PbFetchLogRespForBucket>> entry : fetchLogRespMap.entrySet()) {
            PbFetchLogRespForTable fetchLogRespForTable = new PbFetchLogRespForTable();
            fetchLogRespForTable.setTableId(entry.getKey());
            fetchLogRespForTable.addAllBucketsResps(entry.getValue());
            fetchLogRespForTables.add(fetchLogRespForTable);
        }

        FetchLogResponse fetchLogResponse = new FetchLogResponse();
        fetchLogResponse.addAllTablesResps(fetchLogRespForTables);
        return fetchLogResponse;
    }

    public static Map<TableBucket, KvRecordBatch> getPutKvData(PutKvRequest putKvRequest) {
        long tableId = putKvRequest.getTableId();
        Map<TableBucket, KvRecordBatch> produceEntryData = new HashMap<>();
        for (PbPutKvReqForBucket putKvReqForBucket : putKvRequest.getBucketsReqsList()) {
            ByteBuffer recordsBuffer = toByteBuffer(putKvReqForBucket.getRecordsSlice());
            DefaultKvRecordBatch kvRecords = DefaultKvRecordBatch.pointToByteBuffer(recordsBuffer);
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            putKvReqForBucket.hasPartitionId()
                                    ? putKvReqForBucket.getPartitionId()
                                    : null,
                            putKvReqForBucket.getBucketId());
            produceEntryData.put(tb, kvRecords);
        }
        return produceEntryData;
    }

    public static Map<TableBucket, List<byte[]>> toLookupData(LookupRequest lookupRequest) {
        long tableId = lookupRequest.getTableId();
        Map<TableBucket, List<byte[]>> lookupEntryData = new HashMap<>();
        for (PbLookupReqForBucket lookupReqForBucket : lookupRequest.getBucketsReqsList()) {
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            lookupReqForBucket.hasPartitionId()
                                    ? lookupReqForBucket.getPartitionId()
                                    : null,
                            lookupReqForBucket.getBucketId());
            List<byte[]> keys = new ArrayList<>(lookupReqForBucket.getKeysCount());
            for (int i = 0; i < lookupReqForBucket.getKeysCount(); i++) {
                keys.add(lookupReqForBucket.getKeyAt(i));
            }
            lookupEntryData.put(tb, keys);
        }
        return lookupEntryData;
    }

    public static Map<TableBucket, List<byte[]>> toPrefixLookupData(
            PrefixLookupRequest prefixLookupRequest) {
        long tableId = prefixLookupRequest.getTableId();
        Map<TableBucket, List<byte[]>> lookupEntryData = new HashMap<>();
        for (PbPrefixLookupReqForBucket lookupReqForBucket :
                prefixLookupRequest.getBucketsReqsList()) {
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            lookupReqForBucket.hasPartitionId()
                                    ? lookupReqForBucket.getPartitionId()
                                    : null,
                            lookupReqForBucket.getBucketId());
            List<byte[]> keys = new ArrayList<>(lookupReqForBucket.getKeysCount());
            for (int i = 0; i < lookupReqForBucket.getKeysCount(); i++) {
                keys.add(lookupReqForBucket.getKeyAt(i));
            }
            lookupEntryData.put(tb, keys);
        }
        return lookupEntryData;
    }

    public static @Nullable int[] getTargetColumns(PutKvRequest putKvRequest) {
        int[] targetColumns = putKvRequest.getTargetColumns();
        return targetColumns.length == 0 ? null : targetColumns;
    }

    public static PutKvResponse makePutKvResponse(Collection<PutKvResultForBucket> kvPutResult) {
        PutKvResponse putKvResponse = new PutKvResponse();
        List<PbPutKvRespForBucket> putKvRespForBucketList = new ArrayList<>();
        for (PutKvResultForBucket bucketResult : kvPutResult) {
            PbPutKvRespForBucket putKvBucket =
                    new PbPutKvRespForBucket().setBucketId(bucketResult.getBucketId());
            TableBucket tableBucket = bucketResult.getTableBucket();
            if (tableBucket.getPartitionId() != null) {
                putKvBucket.setPartitionId(tableBucket.getPartitionId());
            }

            if (bucketResult.failed()) {
                putKvBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            }
            putKvRespForBucketList.add(putKvBucket);
        }
        putKvResponse.addAllBucketsResps(putKvRespForBucketList);
        return putKvResponse;
    }

    public static LimitScanResponse makeLimitScanResponse(LimitScanResultForBucket bucketResult) {
        LimitScanResponse limitScanResponse = new LimitScanResponse();

        if (bucketResult.failed()) {
            limitScanResponse.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
        } else {

            Boolean isLogTable = bucketResult.isLogTable();
            if (isLogTable != null) {
                limitScanResponse.setIsLogTable(isLogTable);
            }

            DefaultValueRecordBatch valueRecords = bucketResult.getValues();
            if (valueRecords != null) {
                limitScanResponse.setRecords(
                        valueRecords.getSegment(),
                        valueRecords.getPosition(),
                        valueRecords.sizeInBytes());
            }

            LogRecords logRecords = bucketResult.getRecords();
            if (logRecords != null) {
                // TODO: code below is duplicated with FetchLogResponse, we should refactor it.
                if (logRecords instanceof FileLogRecords) {
                    FileChannelChunk chunk = ((FileLogRecords) logRecords).toChunk();
                    // zero-copy optimization for file channel
                    limitScanResponse.setRecords(
                            chunk.getFileChannel(), chunk.getPosition(), chunk.getSize());
                } else if (logRecords instanceof MemoryLogRecords) {
                    // this should never happen, but we still support fetch memory log records.
                    if (logRecords == MemoryLogRecords.EMPTY) {
                        limitScanResponse.setRecords(new byte[0]);
                    } else {
                        MemoryLogRecords records = (MemoryLogRecords) logRecords;
                        limitScanResponse.setRecords(
                                records.getMemorySegment(),
                                records.getPosition(),
                                records.sizeInBytes());
                    }
                } else if (logRecords instanceof BytesViewLogRecords) {
                    // zero-copy for project push down.
                    limitScanResponse.setRecordsBytesView(
                            ((BytesViewLogRecords) logRecords).getBytesView());
                } else {
                    throw new UnsupportedOperationException(
                            "Not supported log records type: " + logRecords.getClass().getName());
                }
            }
        }

        return limitScanResponse;
    }

    public static LookupResponse makeLookupResponse(
            Map<TableBucket, LookupResultForBucket> lookupResult,
            Map<TableBucket, LookupResultForBucket> lookupError) {
        return makeLookupResponse(mergeResponse(lookupResult, lookupError));
    }

    public static LookupResponse makeLookupResponse(
            Map<TableBucket, LookupResultForBucket> lookupResult) {
        LookupResponse lookupResponse = new LookupResponse();
        for (Map.Entry<TableBucket, LookupResultForBucket> entry : lookupResult.entrySet()) {
            TableBucket tb = entry.getKey();
            LookupResultForBucket bucketResult = entry.getValue();
            PbLookupRespForBucket lookupRespForBucket = lookupResponse.addBucketsResp();
            lookupRespForBucket.setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                lookupRespForBucket.setPartitionId(tb.getPartitionId());
            }
            if (bucketResult.failed()) {
                lookupRespForBucket.setError(
                        bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                for (byte[] value : bucketResult.lookupValues()) {
                    PbValue pbValue = lookupRespForBucket.addValue();
                    if (value != null) {
                        pbValue.setValues(value);
                    }
                }
            }
        }
        return lookupResponse;
    }

    public static PrefixLookupResponse makePrefixLookupResponse(
            Map<TableBucket, PrefixLookupResultForBucket> prefixLookupResult,
            Map<TableBucket, PrefixLookupResultForBucket> prefixLookupErrors) {
        return makePrefixLookupResponse(mergeResponse(prefixLookupResult, prefixLookupErrors));
    }

    public static PrefixLookupResponse makePrefixLookupResponse(
            Map<TableBucket, PrefixLookupResultForBucket> prefixLookupResult) {
        PrefixLookupResponse prefixLookupResponse = new PrefixLookupResponse();
        List<PbPrefixLookupRespForBucket> resultForAll = new ArrayList<>();
        for (Map.Entry<TableBucket, PrefixLookupResultForBucket> entry :
                prefixLookupResult.entrySet()) {
            PbPrefixLookupRespForBucket respForBucket = new PbPrefixLookupRespForBucket();
            TableBucket tb = entry.getKey();
            respForBucket.setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                respForBucket.setPartitionId(tb.getPartitionId());
            }

            PrefixLookupResultForBucket bucketResult = entry.getValue();
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                List<PbValueList> keyResultList = new ArrayList<>();
                for (List<byte[]> res : bucketResult.prefixLookupValues()) {
                    PbValueList pbValueList = new PbValueList();
                    for (byte[] bytes : res) {
                        pbValueList.addValue(bytes);
                    }
                    keyResultList.add(pbValueList);
                }
                respForBucket.addAllValueLists(keyResultList);
            }
            resultForAll.add(respForBucket);
        }
        prefixLookupResponse.addAllBucketsResps(resultForAll);
        return prefixLookupResponse;
    }

    public static AdjustIsrRequest makeAdjustIsrRequest(
            int serverId, Map<TableBucket, LeaderAndIsr> leaderAndIsrMap) {
        // group by table id.
        Map<Long, List<PbAdjustIsrReqForBucket>> reqForBucketByTableId = new HashMap<>();
        leaderAndIsrMap.forEach(
                ((tb, leaderAndIsr) -> {
                    PbAdjustIsrReqForBucket reqForBucket =
                            new PbAdjustIsrReqForBucket()
                                    .setBucketId(tb.getBucket())
                                    .setBucketEpoch(leaderAndIsr.bucketEpoch())
                                    .setCoordinatorEpoch(leaderAndIsr.coordinatorEpoch())
                                    .setLeaderEpoch(leaderAndIsr.leaderEpoch());
                    if (tb.getPartitionId() != null) {
                        reqForBucket.setPartitionId(tb.getPartitionId());
                    }
                    leaderAndIsr.isr().forEach(reqForBucket::addNewIsr);
                    if (reqForBucketByTableId.containsKey(tb.getTableId())) {
                        reqForBucketByTableId.get(tb.getTableId()).add(reqForBucket);
                    } else {
                        List<PbAdjustIsrReqForBucket> list = new ArrayList<>();
                        list.add(reqForBucket);
                        reqForBucketByTableId.put(tb.getTableId(), list);
                    }
                }));

        // make request.
        AdjustIsrRequest adjustIsrRequest = new AdjustIsrRequest().setServerId(serverId);
        List<PbAdjustIsrReqForTable> tablesReqs = new ArrayList<>();
        for (Map.Entry<Long, List<PbAdjustIsrReqForBucket>> entry :
                reqForBucketByTableId.entrySet()) {
            PbAdjustIsrReqForTable reqForTable =
                    new PbAdjustIsrReqForTable().setTableId(entry.getKey());
            reqForTable.addAllBucketsReqs(entry.getValue());
            tablesReqs.add(reqForTable);
        }
        adjustIsrRequest.addAllTablesReqs(tablesReqs);
        return adjustIsrRequest;
    }

    public static Map<TableBucket, LeaderAndIsr> getAdjustIsrData(AdjustIsrRequest request) {
        int leaderId = request.getServerId();
        Map<TableBucket, LeaderAndIsr> leaderAndIsrMap = new HashMap<>();
        for (PbAdjustIsrReqForTable reqForTable : request.getTablesReqsList()) {
            long tableId = reqForTable.getTableId();
            List<PbAdjustIsrReqForBucket> bucketsReqsList = reqForTable.getBucketsReqsList();
            for (PbAdjustIsrReqForBucket reqForBucket : bucketsReqsList) {
                TableBucket tb =
                        new TableBucket(
                                tableId,
                                reqForBucket.hasPartitionId()
                                        ? reqForBucket.getPartitionId()
                                        : null,
                                reqForBucket.getBucketId());
                List<Integer> newIsr = new ArrayList<>();
                for (int i = 0; i < reqForBucket.getNewIsrsCount(); i++) {
                    newIsr.add(reqForBucket.getNewIsrAt(i));
                }
                leaderAndIsrMap.put(
                        tb,
                        new LeaderAndIsr(
                                leaderId,
                                reqForBucket.getLeaderEpoch(),
                                newIsr,
                                reqForBucket.getCoordinatorEpoch(),
                                reqForBucket.getBucketEpoch()));
            }
        }
        return leaderAndIsrMap;
    }

    public static AdjustIsrResponse makeAdjustIsrResponse(
            List<AdjustIsrResultForBucket> resultForBuckets) {
        Map<Long, List<PbAdjustIsrRespForBucket>> respMap = new HashMap<>();
        for (AdjustIsrResultForBucket bucketResult : resultForBuckets) {
            TableBucket tb = bucketResult.getTableBucket();
            PbAdjustIsrRespForBucket respForBucket =
                    new PbAdjustIsrRespForBucket().setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                respForBucket.setPartitionId(tb.getPartitionId());
            }
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                LeaderAndIsr leaderAndIsr = bucketResult.leaderAndIsr();
                respForBucket
                        .setLeaderId(leaderAndIsr.leader())
                        .setLeaderEpoch(leaderAndIsr.leaderEpoch())
                        .setCoordinatorEpoch(leaderAndIsr.coordinatorEpoch())
                        .setBucketEpoch(leaderAndIsr.bucketEpoch())
                        .setIsrs(leaderAndIsr.isrArray());
            }

            if (respMap.containsKey(tb.getTableId())) {
                respMap.get(tb.getTableId()).add(respForBucket);
            } else {
                List<PbAdjustIsrRespForBucket> list = new ArrayList<>();
                list.add(respForBucket);
                respMap.put(tb.getTableId(), list);
            }
        }

        AdjustIsrResponse adjustIsrResponse = new AdjustIsrResponse();
        List<PbAdjustIsrRespForTable> respForTables = new ArrayList<>();
        for (Map.Entry<Long, List<PbAdjustIsrRespForBucket>> entry : respMap.entrySet()) {
            PbAdjustIsrRespForTable respForTable = new PbAdjustIsrRespForTable();
            respForTable.setTableId(entry.getKey());
            respForTable.addAllBucketsResps(entry.getValue());
            respForTables.add(respForTable);
        }
        adjustIsrResponse.addAllTablesResps(respForTables);
        return adjustIsrResponse;
    }

    public static Map<TableBucket, AdjustIsrResultForBucket> getAdjustIsrResponseData(
            AdjustIsrResponse response) {
        Map<TableBucket, AdjustIsrResultForBucket> adjustIsrResult = new HashMap<>();
        for (PbAdjustIsrRespForTable respForTable : response.getTablesRespsList()) {
            long tableId = respForTable.getTableId();
            for (PbAdjustIsrRespForBucket respForBucket : respForTable.getBucketsRespsList()) {
                TableBucket tb =
                        new TableBucket(
                                tableId,
                                respForBucket.hasPartitionId()
                                        ? respForBucket.getPartitionId()
                                        : null,
                                respForBucket.getBucketId());
                if (respForBucket.hasErrorCode()) {
                    adjustIsrResult.put(
                            tb,
                            new AdjustIsrResultForBucket(
                                    tb, ApiError.fromErrorMessage(respForBucket)));
                    continue;
                }

                List<Integer> isr = new ArrayList<>();
                for (int i = 0; i < respForBucket.getIsrsCount(); i++) {
                    isr.add(respForBucket.getIsrAt(i));
                }
                adjustIsrResult.put(
                        tb,
                        new AdjustIsrResultForBucket(
                                tb,
                                new LeaderAndIsr(
                                        respForBucket.getLeaderId(),
                                        respForBucket.getLeaderEpoch(),
                                        isr,
                                        respForBucket.getCoordinatorEpoch(),
                                        respForBucket.getBucketEpoch())));
            }
        }
        return adjustIsrResult;
    }

    public static Set<TableBucket> getListOffsetsData(ListOffsetsRequest request) {
        Set<TableBucket> tableBuckets = new HashSet<>();
        long tableId = request.getTableId();
        Long partitionId = request.hasPartitionId() ? request.getPartitionId() : null;
        for (int i = 0; i < request.getBucketIdsCount(); i++) {
            tableBuckets.add(new TableBucket(tableId, partitionId, request.getBucketIdAt(i)));
        }
        return tableBuckets;
    }

    public static ListOffsetsResponse makeListOffsetsResponse(
            List<ListOffsetsResultForBucket> results) {
        ListOffsetsResponse listOffsetsResponse = new ListOffsetsResponse();
        List<PbListOffsetsRespForBucket> respForBucketList = new ArrayList<>();
        for (ListOffsetsResultForBucket result : results) {
            PbListOffsetsRespForBucket respForBucket =
                    new PbListOffsetsRespForBucket().setBucketId(result.getBucketId());
            if (result.failed()) {
                respForBucket.setError(result.getErrorCode(), result.getErrorMessage());
            } else {
                respForBucket.setOffset(result.getOffset());
            }
            respForBucketList.add(respForBucket);
        }
        listOffsetsResponse.addAllBucketsResps(respForBucketList);
        return listOffsetsResponse;
    }

    public static ListOffsetsRequest makeListOffsetsRequest(
            int followerServerId,
            int offsetType,
            long tableId,
            @Nullable Long partitionId,
            int bucketId) {
        ListOffsetsRequest listOffsetsRequest = new ListOffsetsRequest();
        listOffsetsRequest
                .setFollowerServerId(followerServerId)
                .setOffsetType(offsetType)
                .setTableId(tableId)
                .setBucketIds(new int[] {bucketId});
        if (partitionId != null) {
            listOffsetsRequest.setPartitionId(partitionId);
        }
        return listOffsetsRequest;
    }

    public static CommitKvSnapshotRequest makeCommitKvSnapshotRequest(
            CompletedSnapshot completedSnapshot, int coordinatorEpoch, int bucketLeaderEpoch) {
        CommitKvSnapshotRequest request = new CommitKvSnapshotRequest();
        byte[] completedSnapshotBytes = CompletedSnapshotJsonSerde.toJson(completedSnapshot);
        request.setCompletedSnapshot(completedSnapshotBytes)
                .setCoordinatorEpoch(coordinatorEpoch)
                .setBucketLeaderEpoch(bucketLeaderEpoch);
        return request;
    }

    public static GetLatestKvSnapshotsResponse makeGetLatestKvSnapshotsResponse(
            long tableId,
            @Nullable Long partitionId,
            Map<Integer, Optional<BucketSnapshot>> latestSnapshots,
            int numBuckets) {
        List<PbKvSnapshot> pbSnapshots = makePbKvSnapshots(latestSnapshots, numBuckets);
        GetLatestKvSnapshotsResponse response =
                new GetLatestKvSnapshotsResponse()
                        .setTableId(tableId)
                        .addAllLatestSnapshots(pbSnapshots);
        if (partitionId != null) {
            response.setPartitionId(partitionId);
        }
        return response;
    }

    private static List<PbKvSnapshot> makePbKvSnapshots(
            Map<Integer, Optional<BucketSnapshot>> snapshots, int numBuckets) {
        List<PbKvSnapshot> result = new ArrayList<>();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            Optional<BucketSnapshot> snapshot = snapshots.get(bucket);
            PbKvSnapshot pbKvSnapshot = new PbKvSnapshot().setBucketId(bucket);
            if (snapshot != null && snapshot.isPresent()) {
                pbKvSnapshot
                        .setSnapshotId(snapshot.get().getSnapshotId())
                        .setLogOffset(snapshot.get().getLogOffset());
            }
            result.add(pbKvSnapshot);
        }
        return result;
    }

    public static GetKvSnapshotMetadataResponse makeKvSnapshotMetadataResponse(
            CompletedSnapshot completedSnapshot) {
        return new GetKvSnapshotMetadataResponse()
                .setLogOffset(completedSnapshot.getLogOffset())
                .addAllSnapshotFiles(toPbSnapshotFileHandles(completedSnapshot));
    }

    public static InitWriterResponse makeInitWriterResponse(long writerId) {
        return new InitWriterResponse().setWriterId(writerId);
    }

    private static List<PbRemotePathAndLocalFile> toPbSnapshotFileHandles(
            CompletedSnapshot completedSnapshot) {
        KvSnapshotHandle kvSnapshotHandle = completedSnapshot.getKvSnapshotHandle();
        return Stream.concat(
                        kvSnapshotHandle.getPrivateFileHandles().stream(),
                        kvSnapshotHandle.getSharedKvFileHandles().stream())
                .map(
                        kvFileHandleAndLocalPath -> {
                            // get the remote file path
                            String filePath =
                                    kvFileHandleAndLocalPath.getKvFileHandle().getFilePath();
                            // get the local name for the file
                            String localPath = kvFileHandleAndLocalPath.getLocalPath();
                            return new PbRemotePathAndLocalFile()
                                    .setRemotePath(filePath)
                                    .setLocalFileName(localPath);
                        })
                .collect(Collectors.toList());
    }

    public static GetFileSystemSecurityTokenResponse toGetFileSystemSecurityTokenResponse(
            String filesystemSchema, ObtainedSecurityToken obtainedSecurityToken) {
        GetFileSystemSecurityTokenResponse getFileSystemSecurityTokenResponse =
                new GetFileSystemSecurityTokenResponse()
                        .setToken(obtainedSecurityToken.getToken())
                        .setSchema(filesystemSchema);

        obtainedSecurityToken
                .getValidUntil()
                .ifPresent(getFileSystemSecurityTokenResponse::setExpirationTime);

        List<PbKeyValue> pbKeyValues =
                new ArrayList<>(obtainedSecurityToken.getAdditionInfos().size());
        for (Map.Entry<String, String> entry :
                obtainedSecurityToken.getAdditionInfos().entrySet()) {
            pbKeyValues.add(new PbKeyValue().setKey(entry.getKey()).setValue(entry.getValue()));
        }
        getFileSystemSecurityTokenResponse.addAllAdditionInfos(pbKeyValues);
        return getFileSystemSecurityTokenResponse;
    }

    public static CommitRemoteLogManifestData getCommitRemoteLogManifestData(
            CommitRemoteLogManifestRequest request) {
        return new CommitRemoteLogManifestData(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                new FsPath(request.getRemoteLogManifestPath()),
                request.getRemoteLogStartOffset(),
                request.getRemoteLogEndOffset(),
                request.getCoordinatorEpoch(),
                request.getBucketLeaderEpoch());
    }

    public static CommitRemoteLogManifestRequest makeCommitRemoteLogManifestRequest(
            CommitRemoteLogManifestData commitRemoteLogManifestData) {
        CommitRemoteLogManifestRequest request = new CommitRemoteLogManifestRequest();
        TableBucket tb = commitRemoteLogManifestData.getTableBucket();
        if (tb.getPartitionId() != null) {
            request.setPartitionId(tb.getPartitionId());
        }
        request.setTableId(tb.getTableId())
                .setBucketId(tb.getBucket())
                .setRemoteLogManifestPath(
                        commitRemoteLogManifestData.getRemoteLogManifestPath().toString())
                .setRemoteLogStartOffset(commitRemoteLogManifestData.getRemoteLogStartOffset())
                .setRemoteLogEndOffset(commitRemoteLogManifestData.getRemoteLogEndOffset())
                .setCoordinatorEpoch(commitRemoteLogManifestData.getCoordinatorEpoch())
                .setBucketLeaderEpoch(commitRemoteLogManifestData.getBucketLeaderEpoch());
        return request;
    }

    public static NotifyRemoteLogOffsetsRequest makeNotifyRemoteLogOffsetsRequest(
            TableBucket tableBucket, long remoteLogStartOffset, long remoteLogEndOffset) {
        NotifyRemoteLogOffsetsRequest request = new NotifyRemoteLogOffsetsRequest();
        if (tableBucket.getPartitionId() != null) {
            request.setPartitionId(tableBucket.getPartitionId());
        }
        request.setTableId(tableBucket.getTableId())
                .setBucketId(tableBucket.getBucket())
                .setRemoteStartOffset(remoteLogStartOffset)
                .setRemoteEndOffset(remoteLogEndOffset);
        return request;
    }

    public static NotifyRemoteLogOffsetsData getNotifyRemoteLogOffsetsData(
            NotifyRemoteLogOffsetsRequest request) {
        return new NotifyRemoteLogOffsetsData(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                request.getRemoteStartOffset(),
                request.getRemoteEndOffset(),
                request.getCoordinatorEpoch());
    }

    public static NotifyKvSnapshotOffsetData getNotifySnapshotOffsetData(
            NotifyKvSnapshotOffsetRequest request) {
        return new NotifyKvSnapshotOffsetData(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                request.getMinRetainOffset(),
                request.getCoordinatorEpoch());
    }

    public static NotifyKvSnapshotOffsetRequest makeNotifyKvSnapshotOffsetRequest(
            TableBucket tableBucket, long minRetainOffset) {
        NotifyKvSnapshotOffsetRequest request = new NotifyKvSnapshotOffsetRequest();
        if (tableBucket.getPartitionId() != null) {
            request.setPartitionId(tableBucket.getPartitionId());
        }
        request.setTableId(tableBucket.getTableId())
                .setBucketId(tableBucket.getBucket())
                .setMinRetainOffset(minRetainOffset);
        return request;
    }

    public static ListPartitionInfosResponse toListPartitionInfosResponse(
            List<String> partitionKeys, Map<String, Long> partitionNameAndIds) {
        ListPartitionInfosResponse listPartitionsResponse = new ListPartitionInfosResponse();
        for (Map.Entry<String, Long> partitionNameAndId : partitionNameAndIds.entrySet()) {
            ResolvedPartitionSpec spec =
                    ResolvedPartitionSpec.fromPartitionName(
                            partitionKeys, partitionNameAndId.getKey());
            listPartitionsResponse
                    .addPartitionsInfo()
                    .setPartitionId(partitionNameAndId.getValue())
                    .setPartitionSpec(makePbPartitionSpec(spec));
        }
        return listPartitionsResponse;
    }

    public static PbPartitionSpec makePbPartitionSpec(ResolvedPartitionSpec spec) {
        PbPartitionSpec pbPartitionSpec = new PbPartitionSpec();
        for (int i = 0; i < spec.getPartitionKeys().size(); i++) {
            pbPartitionSpec
                    .addPartitionKeyValue()
                    .setKey(spec.getPartitionKeys().get(i))
                    .setValue(spec.getPartitionValues().get(i));
        }
        return pbPartitionSpec;
    }

    public static CommitLakeTableSnapshotData getCommitLakeTableSnapshotData(
            CommitLakeTableSnapshotRequest request) {
        // handle rpc before 0.9
        Map<Long, LakeTableSnapshot> lakeTableInfoByTableId = new HashMap<>();
        Map<Long, Map<TableBucket, Long>> tableBucketsMaxTimestamp = new HashMap<>();
        for (PbLakeTableSnapshotInfo pbLakeTableSnapshotInfo : request.getTablesReqsList()) {
            long tableId = pbLakeTableSnapshotInfo.getTableId();
            long snapshotId = pbLakeTableSnapshotInfo.getSnapshotId();
            Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
            Map<TableBucket, Long> bucketLogMaxTimestamp = new HashMap<>();
            for (PbLakeTableOffsetForBucket lakeTableOffsetForBucket :
                    pbLakeTableSnapshotInfo.getBucketsReqsList()) {
                Long partitionId =
                        lakeTableOffsetForBucket.hasPartitionId()
                                ? lakeTableOffsetForBucket.getPartitionId()
                                : null;
                int bucketId = lakeTableOffsetForBucket.getBucketId();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                Long logEndOffset =
                        lakeTableOffsetForBucket.hasLogEndOffset()
                                ? lakeTableOffsetForBucket.getLogEndOffset()
                                : null;
                if (lakeTableOffsetForBucket.hasMaxTimestamp()) {
                    bucketLogMaxTimestamp.put(
                            tableBucket, lakeTableOffsetForBucket.getMaxTimestamp());
                }
                bucketLogEndOffset.put(tableBucket, logEndOffset);
            }
            LakeTableSnapshot lakeTableSnapshot =
                    new LakeTableSnapshot(snapshotId, bucketLogEndOffset);
            lakeTableInfoByTableId.put(tableId, lakeTableSnapshot);
            tableBucketsMaxTimestamp.put(tableId, bucketLogMaxTimestamp);
        }

        // handle rpc since 0.9
        Map<Long, LakeTable.LakeSnapshotMetadata> lakeSnapshotMetadatas = new HashMap<>();
        for (PbLakeTableSnapshotMetadata pbLakeTableSnapshotMetadata :
                request.getLakeTableSnapshotMetadatasList()) {
            lakeSnapshotMetadatas.put(
                    pbLakeTableSnapshotMetadata.getTableId(),
                    new LakeTable.LakeSnapshotMetadata(
                            pbLakeTableSnapshotMetadata.getSnapshotId(),
                            new FsPath(
                                    pbLakeTableSnapshotMetadata.getTieredBucketOffsetsFilePath()),
                            pbLakeTableSnapshotMetadata.hasReadableBucketOffsetsFilePath()
                                    ? new FsPath(
                                            pbLakeTableSnapshotMetadata
                                                    .getReadableBucketOffsetsFilePath())
                                    : null));
        }
        return new CommitLakeTableSnapshotData(
                lakeTableInfoByTableId, tableBucketsMaxTimestamp, lakeSnapshotMetadatas);
    }

    public static TableBucketOffsets toTableBucketOffsets(PbTableOffsets pbTableOffsets) {
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        long tableId = pbTableOffsets.getTableId();
        for (PbBucketOffset pbBucketOffset : pbTableOffsets.getBucketOffsetsList()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tableId,
                            pbBucketOffset.hasPartitionId()
                                    ? pbBucketOffset.getPartitionId()
                                    : null,
                            pbBucketOffset.getBucketId());
            bucketOffsets.put(tableBucket, pbBucketOffset.getLogEndOffset());
        }
        return new TableBucketOffsets(tableId, bucketOffsets);
    }

    public static PbNotifyLakeTableOffsetReqForBucket makeNotifyLakeTableOffsetForBucket(
            TableBucket tableBucket,
            LakeTableSnapshot lakeTableSnapshot,
            @Nullable Long maxTimestamp) {
        PbNotifyLakeTableOffsetReqForBucket reqForBucket =
                new PbNotifyLakeTableOffsetReqForBucket();
        if (tableBucket.getPartitionId() != null) {
            reqForBucket.setPartitionId(tableBucket.getPartitionId());
        }
        reqForBucket
                .setTableId(tableBucket.getTableId())
                .setBucketId(tableBucket.getBucket())
                .setSnapshotId(lakeTableSnapshot.getSnapshotId());

        lakeTableSnapshot.getLogEndOffset(tableBucket).ifPresent(reqForBucket::setLogEndOffset);

        if (maxTimestamp != null) {
            reqForBucket.setMaxTimestamp(maxTimestamp);
        }

        return reqForBucket;
    }

    public static NotifyLakeTableOffsetData getNotifyLakeTableOffset(
            NotifyLakeTableOffsetRequest notifyLakeTableOffsetRequest) {
        Map<TableBucket, LakeBucketOffset> lakeBucketOffsetMap = new HashMap<>();
        for (PbNotifyLakeTableOffsetReqForBucket reqForBucket :
                notifyLakeTableOffsetRequest.getNotifyBucketsReqsList()) {
            long tableId = reqForBucket.getTableId();
            Long partitionId = reqForBucket.hasPartitionId() ? reqForBucket.getPartitionId() : null;
            int bucket = reqForBucket.getBucketId();

            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);

            long snapshotId = reqForBucket.getSnapshotId();
            Long logStartOffset =
                    reqForBucket.hasLogStartOffset() ? reqForBucket.getLogStartOffset() : null;
            Long logEndOffset =
                    reqForBucket.hasLogEndOffset() ? reqForBucket.getLogEndOffset() : null;
            Long maxTimestamp =
                    reqForBucket.hasMaxTimestamp() ? reqForBucket.getMaxTimestamp() : null;
            lakeBucketOffsetMap.put(
                    tableBucket,
                    new LakeBucketOffset(snapshotId, logStartOffset, logEndOffset, maxTimestamp));
        }

        return new NotifyLakeTableOffsetData(
                notifyLakeTableOffsetRequest.getCoordinatorEpoch(), lakeBucketOffsetMap);
    }

    public static GetLatestLakeSnapshotResponse makeGetLatestLakeSnapshotResponse(
            long tableId, LakeTableSnapshot lakeTableSnapshot) {
        GetLatestLakeSnapshotResponse getLakeTableSnapshotResponse =
                new GetLatestLakeSnapshotResponse();

        getLakeTableSnapshotResponse.setTableId(tableId);
        getLakeTableSnapshotResponse.setSnapshotId(lakeTableSnapshot.getSnapshotId());

        for (Map.Entry<TableBucket, Long> logEndLogOffsetEntry :
                lakeTableSnapshot.getBucketLogEndOffset().entrySet()) {
            PbLakeSnapshotForBucket pbLakeSnapshotForBucket =
                    getLakeTableSnapshotResponse.addBucketSnapshot();
            TableBucket tableBucket = logEndLogOffsetEntry.getKey();
            pbLakeSnapshotForBucket
                    .setBucketId(tableBucket.getBucket())
                    .setLogOffset(logEndLogOffsetEntry.getValue());
            if (tableBucket.getPartitionId() != null) {
                pbLakeSnapshotForBucket.setPartitionId(tableBucket.getPartitionId());
            }
        }

        return getLakeTableSnapshotResponse;
    }

    public static PartitionSpec getPartitionSpec(PbPartitionSpec pbPartitionSpec) {
        Map<String, String> partitionKeyAndValues = new HashMap<>();
        for (int i = 0; i < pbPartitionSpec.getPartitionKeyValuesCount(); i++) {
            PbKeyValue pbKeyValue = pbPartitionSpec.getPartitionKeyValueAt(i);
            partitionKeyAndValues.put(pbKeyValue.getKey(), pbKeyValue.getValue());
        }
        return new PartitionSpec(partitionKeyAndValues);
    }

    public static ListAclsResponse makeListAclsResponse(Collection<AclBinding> aclBindings) {
        ListAclsResponse listAclsResponse = new ListAclsResponse();
        for (AclBinding aclBinding : aclBindings) {
            PbAclInfo aclInfo = listAclsResponse.addAcl();
            aclInfo.setResourceName(aclBinding.getResource().getName())
                    .setResourceType(aclBinding.getResource().getType().getCode())
                    .setPrincipalName(aclBinding.getAccessControlEntry().getPrincipal().getName())
                    .setPrincipalType(aclBinding.getAccessControlEntry().getPrincipal().getType())
                    .setHost(aclBinding.getAccessControlEntry().getHost())
                    .setOperationType(
                            aclBinding.getAccessControlEntry().getOperationType().getCode())
                    .setPermissionType(
                            aclBinding.getAccessControlEntry().getPermissionType().getCode());
        }
        return listAclsResponse;
    }

    public static CreateAclsResponse makeCreateAclsResponse(
            List<AclCreateResult> aclCreateResults) {
        List<PbCreateAclRespInfo> pbAclRespInfos = new ArrayList<>();

        for (AclCreateResult result : aclCreateResults) {
            PbCreateAclRespInfo pbAclRespInfo = new PbCreateAclRespInfo();
            pbAclRespInfo.setAcl(toPbAclInfo(result.getAclBinding()));
            if (result.exception().isPresent()) {
                ApiError apiError = ApiError.fromThrowable(result.exception().get());
                pbAclRespInfos.add(
                        pbAclRespInfo
                                .setErrorCode(apiError.error().code())
                                .setErrorMessage(apiError.message()));
            } else {
                pbAclRespInfos.add(pbAclRespInfo.setErrorCode(Errors.NONE.code()));
            }
        }
        return new CreateAclsResponse().addAllAclRes(pbAclRespInfos);
    }

    public static DropAclsResponse makeDropAclsResponse(List<AclDeleteResult> aclDeleteResults) {
        List<PbDropAclsFilterResult> dropAclsFilterResults = new ArrayList<>();

        for (AclDeleteResult result : aclDeleteResults) {
            if (result.error().isPresent()) {
                ApiError apiError = result.error().get();
                dropAclsFilterResults.add(
                        new PbDropAclsFilterResult()
                                .setErrorCode(apiError.error().code())
                                .setErrorMessage(apiError.message()));
                continue;
            }

            Collection<AclDeleteResult.AclBindingDeleteResult> aclBindingDeleteResults =
                    result.aclBindingDeleteResults();
            List<PbDropAclsMatchingAcl> dropAclsMatchingAcls = new ArrayList<>();
            for (AclDeleteResult.AclBindingDeleteResult aclBindingDeleteResult :
                    aclBindingDeleteResults) {
                PbDropAclsMatchingAcl dropAclsMatchingAcl = new PbDropAclsMatchingAcl();
                dropAclsMatchingAcl.setAcl(toPbAclInfo(aclBindingDeleteResult.aclBinding()));
                if (aclBindingDeleteResult.error().isPresent()) {
                    ApiError apiError = aclBindingDeleteResult.error().get();
                    dropAclsMatchingAcl.setError(apiError.error().code(), apiError.message());
                }
                dropAclsMatchingAcls.add(dropAclsMatchingAcl);
            }
            dropAclsFilterResults.add(
                    new PbDropAclsFilterResult().addAllMatchingAcls(dropAclsMatchingAcls));
        }
        return new DropAclsResponse().addAllFilterResults(dropAclsFilterResults);
    }

    public static LakeTieringHeartbeatResponse makeLakeTieringHeartbeatResponse(
            int coordinatorEpoch) {
        return new LakeTieringHeartbeatResponse().setCoordinatorEpoch(coordinatorEpoch);
    }

    public static List<PbDescribeConfig> toPbConfigEntries(List<ConfigEntry> describeConfigs) {
        return describeConfigs.stream()
                .map(
                        configEntry -> {
                            PbDescribeConfig pbDescribeConfig =
                                    new PbDescribeConfig()
                                            .setConfigKey(configEntry.key())
                                            .setConfigSource(configEntry.source().name());
                            if (configEntry.value() != null) {
                                pbDescribeConfig.setConfigValue(configEntry.value());
                            }
                            return pbDescribeConfig;
                        })
                .collect(Collectors.toList());
    }

    private static <T> Map<TableBucket, T> mergeResponse(
            Map<TableBucket, T> response, Map<TableBucket, T> errors) {
        if (errors.isEmpty()) {
            return response;
        }
        Map<TableBucket, T> result = new HashMap<>(response.size() + errors.size());
        result.putAll(response);
        result.putAll(errors);
        return result;
    }

    private static <T> Collection<T> mergeResponse(Collection<T> response, Collection<T> errors) {
        if (errors.isEmpty()) {
            return response;
        }
        Collection<T> result = new ArrayList<>(response.size() + errors.size());
        result.addAll(response);
        result.addAll(errors);
        return result;
    }
}
