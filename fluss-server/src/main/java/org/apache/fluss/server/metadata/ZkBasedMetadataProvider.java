/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.metadata;

import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * An abstract implementation of {@link MetadataProvider} that provides common functionality for
 * fetching table and partition metadata from ZooKeeper.
 */
public abstract class ZkBasedMetadataProvider implements MetadataProvider {

    private final ZooKeeperClient zkClient;
    private final MetadataManager metadataManager;

    protected ZkBasedMetadataProvider(ZooKeeperClient zkClient, MetadataManager metadataManager) {
        this.zkClient = zkClient;
        this.metadataManager = metadataManager;
    }

    @Override
    public List<TableMetadata> getTablesMetadataFromZK(Collection<TablePath> tablePaths) {
        try {
            Map<TablePath, TableInfo> path2Info = metadataManager.getTables(tablePaths);
            Map<Long, TableInfo> id2Info =
                    path2Info.values().stream()
                            .collect(Collectors.toMap(TableInfo::getTableId, info -> info));
            // Fetch all table metadata from ZooKeeper
            Map<Long, List<BucketMetadata>> metadataForTables =
                    zkClient.getBucketMetadataForTables(id2Info.keySet());
            // we should iterate on the input tablePaths to guarantee the result contains all tables
            List<TableMetadata> result = new ArrayList<>();
            for (TablePath path : tablePaths) {
                TableInfo tableInfo = path2Info.get(path);
                if (tableInfo == null) {
                    throw new TableNotExistException("Table '" + path + "' does not exist.");
                }
                // the metadata maybe null when the table assignment not generated yet.
                List<BucketMetadata> metadata = metadataForTables.get(tableInfo.getTableId());
                TableMetadata tableMetadata = new TableMetadata(tableInfo, metadata);
                result.add(tableMetadata);
            }
            return result;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to fetch table metadata from ZooKeeper", e);
        }
    }

    @Override
    public List<PartitionMetadata> getPartitionsMetadataFromZK(
            Collection<PhysicalTablePath> partitionPaths) {
        try {
            Map<Long, String> partitionId2PartitionName = new HashMap<>();
            Map<Long, Long> partitionId2TableId = new HashMap<>();
            Map<PhysicalTablePath, TablePartition> partitionIds =
                    zkClient.getPartitionIds(partitionPaths);
            for (PhysicalTablePath partitionPath : partitionPaths) {
                TablePartition partition = partitionIds.get(partitionPath);
                if (partition == null) {
                    throw new PartitionNotExistException(
                            "Table partition '" + partitionPath + "' does not exist.");
                }
                partitionId2PartitionName.put(
                        partition.getPartitionId(), checkNotNull(partitionPath.getPartitionName()));
                partitionId2TableId.put(partition.getPartitionId(), partition.getTableId());
            }
            List<PartitionMetadata> result = new ArrayList<>();
            // Fetch all table metadata from ZooKeeper
            zkClient.getBucketMetadataForPartitions(partitionId2TableId.keySet())
                    .forEach(
                            (partitionId, bucketMetadataList) -> {
                                long tableId = partitionId2TableId.get(partitionId);
                                String partitionName = partitionId2PartitionName.get(partitionId);
                                PartitionMetadata partitionMetadata =
                                        new PartitionMetadata(
                                                tableId,
                                                partitionName,
                                                partitionId,
                                                bucketMetadataList);
                                result.add(partitionMetadata);
                            });
            return result;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to fetch partition metadata from ZooKeeper", e);
        }
    }
}
