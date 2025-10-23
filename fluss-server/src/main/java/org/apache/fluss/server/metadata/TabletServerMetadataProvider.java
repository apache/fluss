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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

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
public class TabletServerMetadataProvider extends ZkBasedMetadataProvider {

    private final TabletServerMetadataCache metadataCache;

    /**
     * Creates a new TabletServerMetadataProvider.
     *
     * @param zkClient the ZooKeeper client for accessing distributed metadata
     * @param metadataManager the metadata manager for table information
     * @param metadataCache the local metadata cache for efficient metadata access
     */
    public TabletServerMetadataProvider(
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            TabletServerMetadataCache metadataCache) {
        super(zkClient, metadataManager);
        this.metadataCache = metadataCache;
    }

    @Override
    public Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath) {
        return metadataCache.getTableMetadata(tablePath);
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadataFromCache(
            PhysicalTablePath physicalTablePath) {
        return metadataCache.getPartitionMetadata(physicalTablePath);
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId) {
        return metadataCache.getPhysicalTablePath(partitionId);
    }

    @Override
    public List<TableMetadata> getTablesMetadataFromZK(Collection<TablePath> tablePaths) {
        List<TableMetadata> result = super.getTablesMetadataFromZK(tablePaths);
        // Update local cache after successfully fetching from ZooKeeper
        result.forEach(metadataCache::updateTableMetadata);
        return result;
    }

    @Override
    public List<PartitionMetadata> getPartitionsMetadataFromZK(
            Collection<PhysicalTablePath> partitionPaths) {
        List<PartitionMetadata> result = super.getPartitionsMetadataFromZK(partitionPaths);
        // Update local cache after successfully fetching from ZooKeeper
        result.forEach(metadataCache::updatePartitionMetadata);
        return result;
    }
}
