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
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;
import static org.apache.fluss.utils.PartitionUtils.buildHistoricalPartitionName;
import static org.apache.fluss.utils.PartitionUtils.isExpiredPartition;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An implementation of {@link Lookuper} that lookups by primary key. */
@NotThreadSafe
class PrimaryKeyLookuper extends AbstractLookuper implements Lookuper {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyLookuper.class);

    private final KeyEncoder primaryKeyEncoder;

    /**
     * Extract bucket key from lookup key row, use {@link #primaryKeyEncoder} if is default bucket
     * key (bucket key = physical primary key).
     */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;
    private final boolean insertIfNotExists;

    /** A getter to extract partition from lookup key row, null when it's not a partitioned. */
    @Nullable private final PartitionGetter partitionGetter;

    /** Partition keys for expired partition detection, null when not partitioned. */
    @Nullable private final List<String> partitionKeys;

    /** Auto partition strategy for expired partition detection, null when not partitioned. */
    @Nullable private final AutoPartitionStrategy autoPartitionStrategy;

    /** Whether the table has data lake enabled (needed for expired partition detection). */
    private final boolean isDataLakeEnabled;

    /** Admin for creating __historical__ partition when needed. */
    private final Admin admin;

    public PrimaryKeyLookuper(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            Admin admin,
            boolean insertIfNotExists) {
        super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.numBuckets = tableInfo.getNumBuckets();
        this.insertIfNotExists = insertIfNotExists;
        this.admin = admin;

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
                tableInfo.isDefaultBucketKey()
                        ? primaryKeyEncoder
                        : KeyEncoder.ofBucketKeyEncoder(
                                lookupRowType, tableInfo.getBucketKeys(), lakeFormat);

        this.bucketingFunction = BucketingFunction.of(lakeFormat);

        if (tableInfo.isPartitioned()) {
            this.partitionGetter = new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys());
            this.partitionKeys = tableInfo.getPartitionKeys();
            this.autoPartitionStrategy = tableInfo.getTableConfig().getAutoPartitionStrategy();
            this.isDataLakeEnabled = tableInfo.getTableConfig().getDataLakeFormat().isPresent();
        } else {
            this.partitionGetter = null;
            this.partitionKeys = null;
            this.autoPartitionStrategy = null;
            this.isDataLakeEnabled = false;
        }
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
            String partitionName = partitionGetter.getPartition(lookupKey);
            try {
                partitionId =
                        getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                if (!isExpiredPartition(
                        partitionName,
                        partitionKeys,
                        checkNotNull(autoPartitionStrategy),
                        isDataLakeEnabled)) {
                    return CompletableFuture.completedFuture(
                            new LookupResult(Collections.emptyList()));
                }
                originalPartitionName = partitionName;
                partitionId = resolveHistoricalPartitionId(partitionName);
                if (partitionId == null) {
                    return CompletableFuture.completedFuture(
                            new LookupResult(Collections.emptyList()));
                }
            }
        }

        int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        lookupClient
                .lookup(
                        tableInfo.getTablePath(),
                        tableBucket,
                        pkBytes,
                        insertIfNotExists,
                        originalPartitionName)
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

    /**
     * Resolves the partition ID for the __historical__ partition corresponding to the given expired
     * partition. Creates the __historical__ partition if it doesn't exist yet (e.g., the data was
     * tiered to the lake before the partition was dropped by TTL).
     *
     * @return the partition ID, or null if the historical partition could not be resolved
     */
    @Nullable
    private Long resolveHistoricalPartitionId(String partitionName) {
        String historicalName =
                buildHistoricalPartitionName(partitionName, partitionKeys, autoPartitionStrategy);
        PhysicalTablePath historicalPath =
                PhysicalTablePath.of(tableInfo.getTablePath(), historicalName);

        metadataUpdater.checkAndUpdatePartitionMetadata(historicalPath);
        Long partitionId = metadataUpdater.getPartitionId(historicalPath).orElse(null);
        if (partitionId != null) {
            return partitionId;
        }

        // __historical__ doesn't exist yet — create it so the server can do lake fallback
        LOG.info(
                "Creating historical partition {} for expired partition lookup {}",
                historicalPath,
                partitionName);
        try {
            ResolvedPartitionSpec spec =
                    ResolvedPartitionSpec.fromPartitionName(partitionKeys, historicalName);
            admin.createPartition(tableInfo.getTablePath(), spec.toPartitionSpec(), true).get();
        } catch (Exception e) {
            LOG.warn("Failed to create historical partition {}", historicalPath, e);
            return null;
        }

        metadataUpdater.checkAndUpdatePartitionMetadata(historicalPath);
        return metadataUpdater.getPartitionId(historicalPath).orElse(null);
    }
}
