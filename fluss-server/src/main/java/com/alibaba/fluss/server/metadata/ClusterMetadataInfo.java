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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbBucketMetadata;
import com.alibaba.fluss.rpc.messages.PbPartitionMetadata;
import com.alibaba.fluss.rpc.messages.PbServerNode;
import com.alibaba.fluss.rpc.messages.PbTableMetadata;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This entity used to describe the cluster metadata info, including coordinator server address,
 * alive tablets servers and {@link TableMetadataInfo} list, which can be used to build {@link
 * MetadataResponse} or convert from {@link UpdateMetadataRequest}.
 */
public class ClusterMetadataInfo {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<ServerNode> coordinatorServer;

    private final Set<ServerNode> aliveTabletServers;
    private final List<TableMetadataInfo> tableMetadataInfos;
    private final List<PartitionMetadataInfo> partitionMetadataInfos;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public ClusterMetadataInfo(
            Optional<ServerNode> coordinatorServer, Set<ServerNode> aliveTabletServers) {
        this(
                coordinatorServer,
                aliveTabletServers,
                Collections.emptyList(),
                Collections.emptyList());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public ClusterMetadataInfo(
            Optional<ServerNode> coordinatorServer,
            Set<ServerNode> aliveTabletServers,
            List<TableMetadataInfo> tableMetadataInfos,
            List<PartitionMetadataInfo> partitionMetadataInfos) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServers = aliveTabletServers;
        this.tableMetadataInfos = tableMetadataInfos;
        this.partitionMetadataInfos = partitionMetadataInfos;
    }

    public Optional<ServerNode> getCoordinatorServer() {
        return coordinatorServer;
    }

    public Set<ServerNode> getAliveTabletServers() {
        return aliveTabletServers;
    }

    public MetadataResponse toMetadataResponse() {
        MetadataResponse metadataResponse = new MetadataResponse();

        if (coordinatorServer.isPresent()) {
            ServerNode server = coordinatorServer.get();
            metadataResponse
                    .setCoordinatorServer()
                    .setNodeId(server.id())
                    .setHost(server.host())
                    .setPort(server.port());
        }

        List<PbServerNode> serverNodeList = new ArrayList<>();
        for (ServerNode serverNode : aliveTabletServers) {
            serverNodeList.add(
                    new PbServerNode()
                            .setNodeId(serverNode.id())
                            .setHost(serverNode.host())
                            .setPort(serverNode.port()));
        }

        List<PbTableMetadata> tableMetadatas = new ArrayList<>();
        for (TableMetadataInfo tableMetadataInfo : tableMetadataInfos) {
            TableInfo tableInfo = tableMetadataInfo.getTableInfo();
            PbTableMetadata tableMetadata =
                    new PbTableMetadata()
                            .setTableId(tableInfo.getTableId())
                            .setSchemaId(tableInfo.getSchemaId())
                            .setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                            .setCreatedTime(tableInfo.getCreatedTime())
                            .setModifiedTime(tableInfo.getModifiedTime());
            TablePath tablePath = tableInfo.getTablePath();
            tableMetadata
                    .setTablePath()
                    .setDatabaseName(tablePath.getDatabaseName())
                    .setTableName(tablePath.getTableName());
            tableMetadata.addAllBucketMetadatas(
                    toPbTableBucketMetadata(tableMetadataInfo.getBucketLocations()));

            tableMetadatas.add(tableMetadata);
        }

        List<PbPartitionMetadata> partitionMetadatas = new ArrayList<>();
        for (PartitionMetadataInfo partitionMetadataInfo : partitionMetadataInfos) {
            PbPartitionMetadata pbPartitionMetadata =
                    new PbPartitionMetadata()
                            .setTableId(partitionMetadataInfo.getTableId())
                            .setPartitionId(partitionMetadataInfo.getPartitionId())
                            .setPartitionName(partitionMetadataInfo.getPartitionName());
            pbPartitionMetadata.addAllBucketMetadatas(
                    toPbTableBucketMetadata(partitionMetadataInfo.getBucketLocations()));
            partitionMetadatas.add(pbPartitionMetadata);
        }

        metadataResponse.addAllTabletServers(serverNodeList);
        metadataResponse.addAllTableMetadatas(tableMetadatas);
        metadataResponse.addAllPartitionMetadatas(partitionMetadatas);
        return metadataResponse;
    }

    private List<PbBucketMetadata> toPbTableBucketMetadata(List<BucketLocation> bucketLocations) {
        List<PbBucketMetadata> bucketMetadata = new ArrayList<>();
        for (BucketLocation bucketLocation : bucketLocations) {
            PbBucketMetadata tableBucketMetadata =
                    new PbBucketMetadata().setBucketId(bucketLocation.getBucketId());
            if (bucketLocation.getLeader() != null) {
                tableBucketMetadata.setLeaderId(bucketLocation.getLeader().id());
            }

            for (ServerNode replica : bucketLocation.getReplicas()) {
                tableBucketMetadata.addReplicaId(replica.id());
            }

            bucketMetadata.add(tableBucketMetadata);
        }
        return bucketMetadata;
    }

    public static ClusterMetadataInfo fromUpdateMetadataRequest(UpdateMetadataRequest request) {
        Optional<ServerNode> coordinatorServer = Optional.empty();
        if (request.hasCoordinatorServer()) {
            PbServerNode pbCoordinatorServer = request.getCoordinatorServer();
            coordinatorServer =
                    Optional.of(
                            new ServerNode(
                                    pbCoordinatorServer.getNodeId(),
                                    pbCoordinatorServer.getHost(),
                                    pbCoordinatorServer.getPort(),
                                    ServerType.COORDINATOR));
        }

        Set<ServerNode> aliveTabletServers = new HashSet<>();
        for (PbServerNode tabletServer : request.getTabletServersList()) {
            aliveTabletServers.add(
                    new ServerNode(
                            tabletServer.getNodeId(),
                            tabletServer.getHost(),
                            tabletServer.getPort(),
                            ServerType.TABLET_SERVER));
        }
        return new ClusterMetadataInfo(coordinatorServer, aliveTabletServers);
    }
}
