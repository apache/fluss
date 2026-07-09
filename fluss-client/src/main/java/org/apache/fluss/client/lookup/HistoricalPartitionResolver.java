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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.PartitionUtils.toHistoricalPartitionSpec;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Resolves original partitions to their historical system partition ids.
 *
 * <p>Historical lookup requests still carry the original partition name, but the lookup RPC must be
 * sent to the generated historical system partition. This resolver bridges that gap on the client
 * side.
 */
@Internal
class HistoricalPartitionResolver {

    private final MetadataUpdater metadataUpdater;
    private final Admin admin;
    private final ConcurrentHashMap<HistoricalPartitionKey, CompletableFuture<Long>>
            inflightResolves;

    HistoricalPartitionResolver(MetadataUpdater metadataUpdater, Admin admin) {
        this.metadataUpdater = checkNotNull(metadataUpdater, "metadataUpdater must not be null.");
        this.admin = checkNotNull(admin, "admin must not be null.");
        this.inflightResolves = new ConcurrentHashMap<>();
    }

    CompletableFuture<Long> resolveHistoricalPartitionId(
            TableInfo tableInfo, String originalPartitionName) {
        HistoricalPartitionKey key =
                new HistoricalPartitionKey(
                        tableInfo.getTableId(), tableInfo.getTablePath(), originalPartitionName);
        // Multiple lookups for the same original partition can be issued concurrently. Coalesce
        // them so only one metadata refresh/create path runs for a given partition.
        CompletableFuture<Long> result = new CompletableFuture<>();
        CompletableFuture<Long> previous = inflightResolves.putIfAbsent(key, result);
        if (previous != null) {
            return previous;
        }

        resolveHistoricalPartitionIdInternal(tableInfo, originalPartitionName)
                .whenComplete(
                        (partitionId, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                            } else {
                                result.complete(partitionId);
                            }
                            inflightResolves.remove(key, result);
                        });
        return result;
    }

    private CompletableFuture<Long> resolveHistoricalPartitionIdInternal(
            TableInfo tableInfo, String originalPartitionName) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        ResolvedPartitionSpec historicalPartitionSpec;
        PhysicalTablePath historicalPartitionPath;
        try {
            // The server-side historical lookup path is keyed by the historical system partition,
            // not by the original partition that existed before retention cleanup.
            historicalPartitionSpec = toHistoricalPartitionSpec(tableInfo, originalPartitionName);
            historicalPartitionPath =
                    PhysicalTablePath.of(
                            tableInfo.getTablePath(), historicalPartitionSpec.getPartitionName());

            // Prefer cached metadata, then refresh once before creating the system partition.
            Long partitionId = getCachedPartitionId(historicalPartitionPath);
            if (partitionId != null) {
                result.complete(partitionId);
                return result;
            }

            tryRefreshHistoricalPartition(historicalPartitionPath);
            partitionId = getCachedPartitionId(historicalPartitionPath);
            if (partitionId != null) {
                result.complete(partitionId);
                return result;
            }
        } catch (Throwable t) {
            result.completeExceptionally(t);
            return result;
        }

        // If the historical system partition still does not exist, create it idempotently and
        // refresh metadata again so the caller can route the lookup request by partition id.
        admin.createPartition(
                        tableInfo.getTablePath(), historicalPartitionSpec.toPartitionSpec(), true)
                .whenComplete(
                        (ignored, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                                return;
                            }
                            try {
                                tryRefreshHistoricalPartition(historicalPartitionPath);
                                Long partitionId = getCachedPartitionId(historicalPartitionPath);
                                if (partitionId == null) {
                                    throw new PartitionNotExistException(
                                            "Historical partition "
                                                    + historicalPartitionPath
                                                    + " does not exist after creation.");
                                }
                                result.complete(partitionId);
                            } catch (Throwable t) {
                                result.completeExceptionally(t);
                            }
                        });
        return result;
    }

    private @Nullable Long getCachedPartitionId(PhysicalTablePath historicalPartitionPath) {
        return metadataUpdater.getPartitionId(historicalPartitionPath).orElse(null);
    }

    private void tryRefreshHistoricalPartition(PhysicalTablePath historicalPartitionPath) {
        try {
            metadataUpdater.checkAndUpdatePartitionMetadata(historicalPartitionPath);
        } catch (PartitionNotExistException ignored) {
            // The caller will create the historical partition if it still cannot be found.
        }
    }

    private static final class HistoricalPartitionKey {
        private final long tableId;
        private final TablePath tablePath;
        private final String originalPartitionName;

        private HistoricalPartitionKey(
                long tableId, TablePath tablePath, String originalPartitionName) {
            this.tableId = tableId;
            this.tablePath = tablePath;
            this.originalPartitionName = originalPartitionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HistoricalPartitionKey that = (HistoricalPartitionKey) o;
            return tableId == that.tableId
                    && Objects.equals(tablePath, that.tablePath)
                    && Objects.equals(originalPartitionName, that.originalPartitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, tablePath, originalPartitionName);
        }
    }
}
