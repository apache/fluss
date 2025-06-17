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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This entity used to describe the cluster metadata, including coordinator server address, alive
 * tablets servers and {@link TableMetadata} list or {@link PartitionMetadata}, which can be used to
 * build {@link MetadataResponse} or convert from {@link UpdateMetadataRequest}.
 */
public class ClusterMetadata {

    private final @Nullable ServerInfo coordinatorServer;
    private final Set<ServerInfo> aliveTabletServers;
    private final List<TableMetadata> tableMetadataList;
    private final List<PartitionMetadata> partitionMetadataList;

    @VisibleForTesting
    public ClusterMetadata(
            @Nullable ServerInfo coordinatorServer, Set<ServerInfo> aliveTabletServers) {
        this(
                coordinatorServer,
                aliveTabletServers,
                Collections.emptyList(),
                Collections.emptyList());
    }

    public ClusterMetadata(
            @Nullable ServerInfo coordinatorServer,
            Set<ServerInfo> aliveTabletServers,
            List<TableMetadata> tableMetadataList,
            List<PartitionMetadata> partitionMetadataList) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServers = aliveTabletServers;
        this.tableMetadataList = tableMetadataList;
        this.partitionMetadataList = partitionMetadataList;
    }

    public @Nullable ServerInfo getCoordinatorServer() {
        return coordinatorServer;
    }

    public Set<ServerInfo> getAliveTabletServers() {
        return aliveTabletServers;
    }

    public List<TableMetadata> getTableMetadataList() {
        return tableMetadataList;
    }

    public List<PartitionMetadata> getPartitionMetadataList() {
        return partitionMetadataList;
    }
}
