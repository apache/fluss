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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Coordinator-side implementation of the MetadataProvider interface.
 *
 * <p>This provider serves metadata requests from the coordinator server, utilizing both local
 * coordinator context cache and ZooKeeper as data sources. It provides efficient metadata access
 * for coordinator operations such as partition assignment, leader election, and table management.
 *
 * <p>The provider leverages:
 *
 * <ul>
 *   <li>CoordinatorContext - for cached metadata and partition assignments
 *   <li>MetadataManager - for table information management
 *   <li>ZooKeeperClient - for authoritative metadata retrieval from ZK
 * </ul>
 *
 * <p>This implementation is optimized for coordinator usage patterns where table and partition
 * metadata is frequently accessed for making assignment decisions and handling client requests.
 */
public class CoordinatorMetadataProvider extends ZkBasedMetadataProvider {

    private final CoordinatorContext ctx;

    /**
     * Creates a new CoordinatorMetadataProvider.
     *
     * @param zkClient the ZooKeeper client for accessing distributed metadata
     * @param metadataManager the metadata manager for table information
     * @param ctx the coordinator context providing cached metadata and assignments
     */
    public CoordinatorMetadataProvider(
            ZooKeeperClient zkClient, MetadataManager metadataManager, CoordinatorContext ctx) {
        super(zkClient, metadataManager);
        this.ctx = ctx;
    }

    @Override
    public Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath) {
        long tableId = ctx.getTableIdByPath(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return Optional.empty();
        }
        TableInfo tableInfo = ctx.getTableInfoById(tableId);

        List<BucketMetadata> bucketMetadataList =
                getBucketMetadataFromContext(ctx, tableId, null, ctx.getTableAssignment(tableId));
        return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadataFromCache(
            PhysicalTablePath partitionPath) {
        TablePath tablePath = partitionPath.getTablePath();
        String partitionName = partitionPath.getPartitionName();
        long tableId = ctx.getTableIdByPath(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return Optional.empty();
        }
        Optional<Long> partitionIdOpt = ctx.getPartitionId(partitionPath);
        if (!partitionIdOpt.isPresent()) {
            return Optional.empty();
        }
        long partitionId = partitionIdOpt.get();
        List<BucketMetadata> bucketMetadataList =
                getBucketMetadataFromContext(
                        ctx,
                        tableId,
                        partitionId,
                        ctx.getPartitionAssignment(new TablePartition(tableId, partitionId)));
        return Optional.of(
                new PartitionMetadata(tableId, partitionName, partitionId, bucketMetadataList));
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId) {
        return ctx.getPhysicalTablePath(partitionId);
    }

    /**
     * Constructs bucket metadata list from coordinator context information.
     *
     * <p>This method builds a complete list of bucket metadata by combining assignment information
     * from the provided table assignment map with leader and epoch data from the coordinator
     * context. Each bucket's metadata includes its ID, current leader server, leader epoch, and
     * replica assignments.
     *
     * @param ctx the coordinator context containing leader and epoch information
     * @param tableId the table identifier
     * @param partitionId the partition identifier, null for non-partitioned tables
     * @param tableAssigment the assignment map from bucket ID to list of replica server IDs
     * @return a list of bucket metadata objects containing complete bucket information
     */
    private static List<BucketMetadata> getBucketMetadataFromContext(
            CoordinatorContext ctx,
            long tableId,
            @Nullable Long partitionId,
            Map<Integer, List<Integer>> tableAssigment) {
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        tableAssigment.forEach(
                (bucketId, serverIds) -> {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                    Optional<LeaderAndIsr> optLeaderAndIsr = ctx.getBucketLeaderAndIsr(tableBucket);
                    Integer leader = optLeaderAndIsr.map(LeaderAndIsr::leader).orElse(null);
                    BucketMetadata bucketMetadata =
                            new BucketMetadata(
                                    bucketId,
                                    leader,
                                    ctx.getBucketLeaderEpoch(tableBucket),
                                    serverIds);
                    bucketMetadataList.add(bucketMetadata);
                });
        return bucketMetadataList;
    }
}
