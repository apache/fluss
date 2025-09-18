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

package org.apache.fluss.server.metadata;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Tablet server-side implementation of the MetadataProvider interface.
 *
 * <p>This provider serves metadata requests from tablet servers, utilizing a local metadata cache
 * for efficient access and ZooKeeper as the authoritative data source. It implements a cache-aside
 * pattern where metadata is first checked in the local cache, and if not found, retrieved from
 * ZooKeeper and then cached locally for future access.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li>Local caching - Uses {@link TabletServerMetadataCache} for fast metadata access
 *   <li>Cache updates - Automatically updates local cache when fetching from ZooKeeper
 *   <li>Optimized for tablet server workloads - Frequent metadata lookups with good locality
 * </ul>
 *
 * <p>This implementation is particularly suited for tablet servers that need to frequently access
 * metadata for partition management, leader election participation, and client request handling,
 * while maintaining consistency with the coordinator's view of metadata.
 */
public class TabletServerMetadataProvider implements MetadataProvider {

    private final ZooKeeperClient zkClient;

    private final TabletServerMetadataCache metadataCache;

    private final MetadataManager metadataManager;

    /**
     * Creates a new TabletServerMetadataProvider.
     *
     * @param zkClient the ZooKeeper client for accessing distributed metadata
     * @param metadataCache the local metadata cache for efficient metadata access
     * @param metadataManager the metadata manager for table information
     */
    public TabletServerMetadataProvider(
            ZooKeeperClient zkClient,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager) {
        this.zkClient = zkClient;
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
    }

    @Override
    public Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath) {
        return metadataCache.getTableMetadata(tablePath);
    }

    @Override
    public CompletableFuture<TableMetadata> getTableMetadataFromZk(TablePath tablePath) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        return ZooKeeperClient.getTableMetadataFromZkAsync(
                        zkClient, tablePath, tableInfo.getTableId(), tableInfo.isPartitioned())
                .thenApply(
                        bucketMetadataList -> {
                            TableMetadata tableMetadata =
                                    new TableMetadata(tableInfo, bucketMetadataList);
                            // Update local cache after successfully fetching from ZooKeeper
                            metadataCache.updateTableMetadata(tableMetadata);
                            return tableMetadata;
                        });
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId) {
        return metadataCache.getPhysicalTablePath(partitionId);
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadataFromCache(
            PhysicalTablePath physicalTablePath) {
        return metadataCache.getPartitionMetadata(physicalTablePath);
    }

    @Override
    public CompletableFuture<PartitionMetadata> getPartitionMetadataFromZk(
            PhysicalTablePath physicalTablePath) {
        return ZooKeeperClient.getPartitionMetadataFromZkAsync(physicalTablePath, zkClient)
                .thenApply(
                        partitionMetadata -> {
                            // Update local cache after successfully fetching from ZooKeeper
                            metadataCache.updatePartitionMetadata(partitionMetadata);
                            return partitionMetadata;
                        });
    }
}
