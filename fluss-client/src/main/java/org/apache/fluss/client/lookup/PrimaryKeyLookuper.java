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
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;
import static org.apache.fluss.utils.PartitionUtils.isHistoricalLookupCandidatePartition;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An implementation of {@link Lookuper} that lookups by primary key. */
@NotThreadSafe
class PrimaryKeyLookuper extends AbstractLookuper implements Lookuper {

    private final KeyEncoder primaryKeyEncoder;
    private final KeyEncoder paimonPrimaryKeyEncoder;

    /**
     * Extract bucket key from lookup key row, use {@link #primaryKeyEncoder} if is default bucket
     * key (bucket key = physical primary key).
     */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;
    private final boolean insertIfNotExists;
    private final HistoricalPartitionResolver historicalPartitionResolver;

    /** a getter to extract partition from lookup key row, null when it's not a partitioned. */
    private @Nullable final PartitionGetter partitionGetter;

    public PrimaryKeyLookuper(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            boolean insertIfNotExists,
            HistoricalPartitionResolver historicalPartitionResolver) {
        super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.numBuckets = tableInfo.getNumBuckets();
        this.insertIfNotExists = insertIfNotExists;
        this.historicalPartitionResolver =
                checkNotNull(
                        historicalPartitionResolver,
                        "historicalPartitionResolver must not be null.");

        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(tableInfo.getPrimaryKeys());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        this.primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        lookupRowType,
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        this.paimonPrimaryKeyEncoder =
                new PaimonKeyEncoder(lookupRowType, tableInfo.getPhysicalPrimaryKeys());
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
        Long partitionId = null;
        String originalPartitionName = null;
        if (partitionGetter != null) {
            originalPartitionName = partitionGetter.getPartition(lookupKey);
            try {
                partitionId =
                        getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                if (isHistoricalLookupCandidatePartition(
                        tableInfo, originalPartitionName, Instant.now())) {
                    if (insertIfNotExists) {
                        return completedExceptionally(
                                new UnsupportedOperationException(
                                        "Lookup with insertIfNotExists is not supported for historical partition lookup."));
                    }
                    int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
                    return historicalLookup(
                            bucketId,
                            paimonPrimaryKeyEncoder.encodeKey(lookupKey),
                            originalPartitionName);
                }
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }

        int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupBucket(tableBucket, pkBytes, insertIfNotExists, null);
    }

    private CompletableFuture<LookupResult> historicalLookup(
            int bucketId, byte[] keyBytes, String originalPartitionName) {
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
                                    tableBucket, keyBytes, false, originalPartitionName);
                        });
    }

    private CompletableFuture<LookupResult> lookupBucket(
            TableBucket tableBucket,
            byte[] keyBytes,
            boolean insertIfNotExists,
            @Nullable String partitionName) {
        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        lookupClient
                .lookup(
                        tableInfo.getTablePath(),
                        tableBucket,
                        keyBytes,
                        insertIfNotExists,
                        partitionName)
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                lookupFuture.completeExceptionally(error);
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
