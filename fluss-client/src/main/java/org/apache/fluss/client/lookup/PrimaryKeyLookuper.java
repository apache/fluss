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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.PartitionUtils.isPastAutoPartition;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** An implementation of {@link Lookuper} that lookups by primary key. */
@NotThreadSafe
class PrimaryKeyLookuper extends AbstractLookuper implements Lookuper {

    private final KeyEncoder primaryKeyEncoder;

    /**
     * Extract bucket key from lookup key row, use {@link #primaryKeyEncoder} if is default bucket
     * key (bucket key = physical primary key).
     */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;
    private final boolean insertIfNotExists;

    /**
     * Missing past partition names already routed to the historical system partition.
     *
     * <p>The historical partition option is fixed for the lifetime of the lookuper. Existing lookup
     * jobs that need historical partition data must be restarted after changing the option so newly
     * created lookupers use the latest table configuration. Missing current and future partitions
     * are not cached because the coordinator may create them later.
     */
    // TODO: Introduce list_historical_partitions or list_all_partitions to initialize this set
    // eagerly and avoid exception-driven discovery during bootstrap.
    private final Set<String> confirmedHistoricalPartitions;

    /** a getter to extract partition from lookup key row, null when it's not a partitioned. */
    private @Nullable final PartitionGetter partitionGetter;

    public PrimaryKeyLookuper(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            boolean insertIfNotExists) {
        super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.numBuckets = tableInfo.getNumBuckets();
        this.insertIfNotExists = insertIfNotExists;
        this.confirmedHistoricalPartitions = new HashSet<>();

        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(tableInfo.getPrimaryKeys());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        this.primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        lookupRowType,
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        this.bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        lookupRowType,
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);

        this.bucketingFunction = BucketingFunction.of(lakeFormat);

        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        // encoding the key row using a compacted way consisted with how the key is encoded when put
        // a row
        byte[] pkBytes = primaryKeyEncoder.encodeKey(lookupKey);
        byte[] bkBytes =
                bucketKeyEncoder == primaryKeyEncoder
                        ? pkBytes
                        : bucketKeyEncoder.encodeKey(lookupKey);
        int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
        Long partitionId = null;
        String originalPartitionName = null;
        int effectiveNumBuckets = numBuckets;
        if (partitionGetter != null) {
            originalPartitionName = partitionGetter.getPartition(lookupKey);
            if (confirmedHistoricalPartitions.contains(originalPartitionName)) {
                return historicalLookup(bucketId, pkBytes, originalPartitionName);
            }
            try {
                partitionId =
                        getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
                if (partitionId != null) {
                    effectiveNumBuckets =
                            resolvePartitionBucketCount(
                                    new TablePartition(tableInfo.getTableId(), partitionId),
                                    numBuckets);
                }
            } catch (PartitionNotExistException e) {
                return mayFallbackToHistoricalLookup(bucketId, pkBytes, originalPartitionName);
            }
        }

        // A partition created before ALTER bucket.num keeps its own layout, so re-route by the
        // partition's actual count. The historical lookups above keep the table-level bucketId
        // because the historical partition is resolved on its own path.
        if (effectiveNumBuckets != numBuckets) {
            bucketId = bucketingFunction.bucketing(bkBytes, effectiveNumBuckets);
        }
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupBucket(
                tableBucket,
                pkBytes,
                insertIfNotExists,
                false,
                originalPartitionName,
                effectiveNumBuckets);
    }

    /**
     * Falls back to historical lookup when the normal partition is missing and fallback is enabled.
     */
    private CompletableFuture<LookupResult> mayFallbackToHistoricalLookup(
            int bucketId, byte[] keyBytes, String originalPartitionName) {
        // Clear the stale normal-partition route before deciding whether to fall back so that a
        // partition created later can be discovered by the next lookup.
        metadataUpdater.invalidPhysicalTableBucketAndPartitionMeta(
                Collections.singleton(
                        PhysicalTablePath.of(tableInfo.getTablePath(), originalPartitionName)));
        if (!tableInfo.getTableConfig().isHistoricalPartitionEnabled()) {
            return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
        }
        // A missing current or future partition may still be created by auto-partitioning. Do not
        // cache it as historical, otherwise this lookuper would keep bypassing the normal partition
        // after it is created.
        if (!isPastAutoPartition(
                originalPartitionName,
                tableInfo.getTableConfig().getAutoPartitionStrategy(),
                Instant.now())) {
            return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
        }
        confirmedHistoricalPartitions.add(originalPartitionName);
        return historicalLookup(bucketId, keyBytes, originalPartitionName);
    }

    private CompletableFuture<LookupResult> historicalLookup(
            int bucketId, byte[] keyBytes, String originalPartitionName) {
        if (insertIfNotExists) {
            return completedExceptionally(
                    new UnsupportedOperationException(
                            "Lookup with insertIfNotExists is not supported for historical partition lookup."));
        }
        // TODO: Support historical lookup on rescaled tables. The lake data of the original
        //  partition is laid out with the bucket count that partition was tiered with, but that
        //  partition is already dropped from Fluss, so neither the table-level count nor the
        //  historical partition's own count is guaranteed to match it. Only the lake metadata
        //  still knows it (Paimon exposes it as DataSplit#totalBuckets), so the server has to
        //  resolve it and route to the bucket itself. Until then, reject the lookup instead of
        //  silently reading the wrong bucket.
        if (tableInfo.getBucketLayoutEpoch() > 0) {
            return completedExceptionally(
                    new UnsupportedOperationException(
                            String.format(
                                    "Historical partition lookup is not supported on table %s because its "
                                            + "'bucket.num' has been altered, so the bucket layout of already "
                                            + "tiered partitions can no longer be determined by the client.",
                                    tableInfo.getTablePath())));
        }
        PhysicalTablePath historicalPartitionPath =
                PhysicalTablePath.of(tableInfo.getTablePath(), HISTORICAL_PARTITION_VALUE);
        try {
            if (!metadataUpdater.checkAndUpdatePartitionMetadata(historicalPartitionPath)) {
                throw new PartitionNotExistException(
                        "Historical partition " + historicalPartitionPath + " does not exist.");
            }
            Long historicalPartitionId =
                    metadataUpdater.getPartitionIdOrElseThrow(historicalPartitionPath);
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), historicalPartitionId, bucketId);
            return lookupBucket(
                    tableBucket, keyBytes, false, true, originalPartitionName, numBuckets);
        } catch (Throwable t) {
            return completedExceptionally(t);
        }
    }

    private CompletableFuture<LookupResult> lookupBucket(
            TableBucket tableBucket,
            byte[] keyBytes,
            boolean insertIfNotExists,
            boolean historicalLookup,
            @Nullable String originalPartitionName,
            int bucketCount) {
        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        lookupClient
                .lookup(
                        tableInfo.getTablePath(),
                        tableBucket,
                        keyBytes,
                        insertIfNotExists,
                        historicalLookup ? originalPartitionName : null,
                        bucketCount)
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                // Only a missing normal partition with a captured original name can
                                // fall back. An already historical lookup must propagate its
                                // failure; falling back again would repeatedly issue the same
                                // historical lookup.
                                if (!(error instanceof PartitionNotExistException)
                                        || historicalLookup
                                        || originalPartitionName == null) {
                                    lookupFuture.completeExceptionally(error);
                                    return;
                                }

                                mayFallbackToHistoricalLookup(
                                                tableBucket.getBucket(),
                                                keyBytes,
                                                originalPartitionName)
                                        .whenComplete(
                                                (historicalResult, historicalError) -> {
                                                    if (historicalError != null) {
                                                        lookupFuture.completeExceptionally(
                                                                historicalError);
                                                    } else {
                                                        lookupFuture.complete(historicalResult);
                                                    }
                                                });
                            } else {
                                handleLookupResponse(
                                        result == null
                                                ? Collections.emptyList()
                                                : Collections.singletonList(result),
                                        lookupFuture);
                            }
                        });
        return lookupFuture;
    }

    private static CompletableFuture<LookupResult> completedExceptionally(Throwable throwable) {
        CompletableFuture<LookupResult> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}
