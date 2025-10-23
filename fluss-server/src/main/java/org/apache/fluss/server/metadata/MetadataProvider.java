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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Interface for providing metadata information about tables and partitions from various sources.
 * This provider supports retrieving metadata from both local cache and ZooKeeper, allowing for
 * flexible metadata access patterns with different consistency and performance characteristics.
 */
public interface MetadataProvider {

    /**
     * Retrieves table metadata from local cache.
     *
     * @param tablePath the path identifying the table
     * @return an Optional containing the table metadata if found in cache, empty otherwise
     */
    Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath);

    /**
     * Retrieves partition metadata from local cache.
     *
     * @param physicalTablePath the physical path identifying the table partition
     * @return an Optional containing the partition metadata if found in cache, empty otherwise
     */
    Optional<PartitionMetadata> getPartitionMetadataFromCache(PhysicalTablePath physicalTablePath);

    /**
     * Retrieves the physical table path from local cache using partition ID.
     *
     * @param partitionId the partition identifier
     * @return an Optional containing the physical table path if found in cache, empty otherwise
     */
    Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId);

    /**
     * Retrieves a batch of table metadata from ZooKeeper.
     *
     * @param tablePaths the path identifying the table
     * @return a CompletableFuture containing the table metadata from ZooKeeper
     */
    List<TableMetadata> getTablesMetadataFromZK(Collection<TablePath> tablePaths);

    /**
     * Retrieves a batch of partition metadata from ZooKeeper.
     *
     * @param partitionPaths the physical path identifying the table partition
     * @return an Optional containing the partition metadata if found in cache, empty otherwise
     */
    List<PartitionMetadata> getPartitionsMetadataFromZK(
            Collection<PhysicalTablePath> partitionPaths);
}
