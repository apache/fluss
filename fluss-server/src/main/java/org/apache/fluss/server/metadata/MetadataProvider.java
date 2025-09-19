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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
     * Retrieves table metadata from ZooKeeper asynchronously.
     *
     * @param tablePath the path identifying the table
     * @return a CompletableFuture containing the table metadata from ZooKeeper
     */
    CompletableFuture<TableMetadata> getTableMetadataFromZk(TablePath tablePath);

    /**
     * Retrieves the physical table path from local cache using partition ID.
     *
     * @param partitionId the partition identifier
     * @return an Optional containing the physical table path if found in cache, empty otherwise
     */
    Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId);

    /**
     * Retrieves partition metadata from local cache.
     *
     * @param physicalTablePath the physical path identifying the table partition
     * @return an Optional containing the partition metadata if found in cache, empty otherwise
     */
    Optional<PartitionMetadata> getPartitionMetadataFromCache(PhysicalTablePath physicalTablePath);

    /**
     * Retrieves partition metadata from ZooKeeper asynchronously.
     *
     * @param physicalTablePath the physical path identifying the table partition
     * @return a CompletableFuture containing the partition metadata from ZooKeeper
     */
    CompletableFuture<PartitionMetadata> getPartitionMetadataFromZk(
            PhysicalTablePath physicalTablePath);
}
