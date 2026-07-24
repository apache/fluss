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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
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
import static org.apache.fluss.utils.PartitionUtils.isHistoricalLookupCandidatePartition;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
    private final Admin admin;
    private final HistoricalPartitionResolver historicalPartitionResolver;

    /**
     * Partition names already confirmed as expired and eligible for historical lookup.
     *
     * <p>Expired partitions cannot be recreated or written in the current lifecycle, so this
     * decision remains valid for the lifetime of the lookuper.
     */
    private final Set<String> confirmedHistoricalPartitions;

    /** a getter to extract partition from lookup key row, null when it's not a partitioned. */
    private @Nullable final PartitionGetter partitionGetter;

    public PrimaryKeyLookuper(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            boolean insertIfNotExists,
            Admin admin,
            HistoricalPartitionResolver historicalPartitionResolver) {
        super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.numBuckets = tableInfo.getNumBuckets();
        this.insertIfNotExists = insertIfNotExists;
        this.admin = checkNotNull(admin, "admin must not be null.");
        this.historicalPartitionResolver =
                checkNotNull(
                        historicalPartitionResolver,
                        "historicalPartitionResolver must not be null.");
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
        if (partitionGetter != null) {
            String originalPartitionName = partitionGetter.getPartition(lookupKey);
            if (confirmedHistoricalPartitions.contains(originalPartitionName)) {
                return historicalLookup(bucketId, pkBytes, originalPartitionName, lookupKey);
            }
            try {
                partitionId =
                        getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                return mayFallbackToHistoricalLookup(
                        bucketId, pkBytes, originalPartitionName, lookupKey);
            }
        }

        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupBucket(tableBucket, pkBytes, insertIfNotExists, null, lookupKey);
    }

    /** Falls back to historical lookup if the lookup key belongs to a historical partition. */
    private CompletableFuture<LookupResult> mayFallbackToHistoricalLookup(
            int bucketId, byte[] keyBytes, String originalPartitionName, InternalRow lookupKey) {
        // The lookuper keeps tableInfo from creation time, but retention can be changed later.
        // Fetch the latest table metadata before classifying a missing partition as historical and
        // verify that the table path still refers to the same table.
        return admin.getTableInfo(tableInfo.getTablePath())
                .thenCompose(
                        latestTableInfo -> {
                            if (latestTableInfo.getTableId() != tableInfo.getTableId()) {
                                return completedExceptionally(
                                        new TableNotExistException(
                                                String.format(
                                                        "Table %s with id %d does not exist.",
                                                        tableInfo.getTablePath(),
                                                        tableInfo.getTableId())));
                            }
                            if (!isHistoricalLookupCandidatePartition(
                                    latestTableInfo, originalPartitionName, Instant.now())) {
                                return CompletableFuture.completedFuture(
                                        new LookupResult(Collections.emptyList()));
                            }
                            metadataUpdater.invalidPhysicalTableBucketAndPartitionMeta(
                                    Collections.singleton(
                                            PhysicalTablePath.of(
                                                    tableInfo.getTablePath(),
                                                    originalPartitionName)));
                            confirmedHistoricalPartitions.add(originalPartitionName);
                            return historicalLookup(
                                    bucketId, keyBytes, originalPartitionName, lookupKey);
                        });
    }

    private CompletableFuture<LookupResult> historicalLookup(
            int bucketId, byte[] keyBytes, String originalPartitionName, InternalRow lookupKey) {
        if (insertIfNotExists) {
            return completedExceptionally(
                    new UnsupportedOperationException(
                            "Lookup with insertIfNotExists is not supported for historical partition lookup."));
        }
        return historicalPartitionResolver
                .resolveHistoricalPartitionId(tableInfo, originalPartitionName)
                .thenCompose(
                        historicalPartitionId -> {
                            TableBucket tableBucket =
                                    new TableBucket(
                                            tableInfo.getTableId(),
                                            historicalPartitionId,
                                            bucketId);
                            return lookupBucket(
                                    tableBucket, keyBytes, false, originalPartitionName, lookupKey);
                        });
    }

    private CompletableFuture<LookupResult> lookupBucket(
            TableBucket tableBucket,
            byte[] keyBytes,
            boolean insertIfNotExists,
            @Nullable String originalPartitionName,
            InternalRow lookupKey) {
        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        lookupClient
                .lookup(
                        tableInfo.getTablePath(),
                        tableBucket,
                        keyBytes,
                        insertIfNotExists,
                        originalPartitionName)
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                // A historical lookup already carries the original partition name.
                                // Propagate its failure instead of falling back again.
                                if (!(error instanceof PartitionNotExistException)
                                        || originalPartitionName != null) {
                                    lookupFuture.completeExceptionally(error);
                                    return;
                                }

                                // The cached normal partition was deleted. Re-evaluate the routing
                                // using the lookup key.
                                String partitionName = partitionGetter.getPartition(lookupKey);
                                mayFallbackToHistoricalLookup(
                                                tableBucket.getBucket(),
                                                keyBytes,
                                                partitionName,
                                                lookupKey)
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
