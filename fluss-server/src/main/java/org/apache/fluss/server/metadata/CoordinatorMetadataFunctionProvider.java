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
import org.apache.fluss.server.RpcServiceBase;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The coordinator metadata function provider. */
public class CoordinatorMetadataFunctionProvider implements MetadataFunctionProvider {

    private final ZooKeeperClient zkClient;

    private final CoordinatorMetadataCache metadataCache;

    private final CoordinatorContext ctx;

    private final MetadataManager metadataManager;

    public CoordinatorMetadataFunctionProvider(
            ZooKeeperClient zkClient,
            CoordinatorMetadataCache metadataCache,
            CoordinatorContext ctx,
            MetadataManager metadataManager) {
        this.zkClient = zkClient;
        this.metadataCache = metadataCache;
        this.ctx = ctx;
        this.metadataManager = metadataManager;
    }

    @Override
    public Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        long tableId = ctx.getTableIdByPath(tablePath);
        List<BucketMetadata> bucketMetadataList;
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return Optional.empty();
        }
        bucketMetadataList =
                getBucketMetadataFromContext(ctx, tableId, null, ctx.getTableAssignment(tableId));
        return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
    }

    @Override
    public CompletableFuture<TableMetadata> getTableMetadataFromZk(TablePath tablePath) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        return RpcServiceBase.getTableMetadataFromZkAsync(
                        zkClient, tablePath, tableInfo.getTableId(), tableInfo.isPartitioned())
                .thenApply(bucketMetadata -> new TableMetadata(tableInfo, bucketMetadata));
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId) {
        return ctx.getPhysicalTablePath(partitionId);
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadataFromCache(
            PhysicalTablePath partitionPath) {
        TablePath tablePath =
                new TablePath(partitionPath.getDatabaseName(), partitionPath.getTableName());
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
    public CompletableFuture<PartitionMetadata> getPartitionMetadataFromZk(
            PhysicalTablePath partitionPath) {
        return RpcServiceBase.getPartitionMetadataFromZkAsync(partitionPath, zkClient);
    }

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
