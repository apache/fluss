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
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;

/**
 * An implementation of {@link Lookuper} that lookups by prefix key. A prefix key is a prefix subset
 * of the primary key.
 */
@NotThreadSafe
class PrefixKeyLookuper extends AbstractLookuper {

    /** Extract bucket key from prefix lookup key row. */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /**
     * a getter to extract partition from prefix lookup key row, null when it's not a partitioned.
     */
    private @Nullable final PartitionGetter partitionGetter;

    public PrefixKeyLookuper(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            List<String> lookupColumnNames) {
        super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        // sanity check
        validatePrefixLookup(tableInfo, lookupColumnNames);
        this.numBuckets = tableInfo.getNumBuckets();
        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(lookupColumnNames);
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        this.bucketKeyEncoder = KeyEncoder.of(lookupRowType, tableInfo.getBucketKeys(), lakeFormat);
        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;
    }

    private void validatePrefixLookup(TableInfo tableInfo, List<String> lookupColumns) {
        // verify is primary key table
        if (!tableInfo.hasPrimaryKey()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Log table %s doesn't support prefix lookup",
                            tableInfo.getTablePath()));
        }

        // verify the bucket keys are the prefix subset of physical primary keys
        List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        for (int i = 0; i < bucketKeys.size(); i++) {
            if (!bucketKeys.get(i).equals(physicalPrimaryKeys.get(i))) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not perform prefix lookup on table '%s', "
                                        + "because the bucket keys %s is not a prefix subset of the "
                                        + "physical primary keys %s (excluded partition fields if present).",
                                tableInfo.getTablePath(), bucketKeys, physicalPrimaryKeys));
            }
        }

        // verify the lookup columns must contain all partition fields if this is partitioned table
        if (tableInfo.isPartitioned()) {
            List<String> partitionKeys = tableInfo.getPartitionKeys();
            Set<String> lookupColumnsSet = new HashSet<>(lookupColumns);
            if (!lookupColumnsSet.containsAll(partitionKeys)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not perform prefix lookup on table '%s', "
                                        + "because the lookup columns %s must contain all partition fields %s.",
                                tableInfo.getTablePath(), lookupColumns, partitionKeys));
            }
        }

        // verify the lookup columns must contain all bucket keys **in order**
        List<String> physicalLookupColumns = new ArrayList<>(lookupColumns);
        physicalLookupColumns.removeAll(tableInfo.getPartitionKeys());
        if (!physicalLookupColumns.equals(bucketKeys)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not perform prefix lookup on table '%s', "
                                    + "because the lookup columns %s must contain all bucket keys %s in order.",
                            tableInfo.getTablePath(), lookupColumns, bucketKeys));
        }
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow prefixKey) {
        byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(prefixKey);
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);

        Long partitionId = null;
        if (partitionGetter != null) {
            try {
                partitionId =
                        getPartitionId(
                                prefixKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }

        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        lookupClient
                .prefixLookup(tableBucket, bucketKeyBytes)
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                lookupFuture.completeExceptionally(
                                        new RuntimeException(
                                                "Failed to perform prefix lookup for table: "
                                                        + tableInfo.getTablePath(),
                                                error));
                            } else {
                                handleLookupResponse(result, lookupFuture);
                            }
                        });
        return lookupFuture;
    }
}
