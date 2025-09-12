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
import org.apache.fluss.client.table.getter.PartialPartitionGetter;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.client.utils.ClientUtils;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;

/**
 * An implementation of {@link Lookuper} that lookups by prefix key. A prefix key is a prefix subset
 * of the primary key.
 */
class PrefixKeyLookuper implements Lookuper {

    private final TableInfo tableInfo;

    private final MetadataUpdater metadataUpdater;

    private final LookupClient lookupClient;

    /** Extract bucket key from prefix lookup key row. */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /**
     * a getter to extract partition from prefix lookup key row, null when it's not a partitioned or
     * when lookup keys don't contain all partition columns.
     */
    private @Nullable final PartitionGetter partitionGetter;

    /** a partial partition getter to extract available partition values from lookup key row. */
    private @Nullable final PartialPartitionGetter partialPartitionGetter;

    /** Decode the lookup bytes to result row. */
    private final ValueDecoder kvValueDecoder;

    public PrefixKeyLookuper(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            List<String> lookupColumnNames) {
        // sanity check
        validatePrefixLookup(tableInfo, lookupColumnNames);
        // initialization
        this.tableInfo = tableInfo;
        this.numBuckets = tableInfo.getNumBuckets();
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(lookupColumnNames);
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        this.bucketKeyEncoder = KeyEncoder.of(lookupRowType, tableInfo.getBucketKeys(), lakeFormat);
        this.bucketingFunction = BucketingFunction.of(lakeFormat);

        if (tableInfo.isPartitioned()) {
            List<String> partitionKeys = tableInfo.getPartitionKeys();
            Set<String> lookupColumnSet = new HashSet<>(lookupColumnNames);
            Set<String> availablePartitionKeys =
                    partitionKeys.stream()
                            .filter(lookupColumnSet::contains)
                            .collect(Collectors.toSet());

            if (availablePartitionKeys.size() == partitionKeys.size()) {
                // All partition keys are available, use traditional PartitionGetter
                this.partitionGetter = new PartitionGetter(lookupRowType, partitionKeys);
                this.partialPartitionGetter = null;
            } else if (availablePartitionKeys.size() > 0) {
                // Some partition keys are available, use PartialPartitionGetter
                this.partitionGetter = null;
                List<String> availablePartitionKeyList =
                        partitionKeys.stream()
                                .filter(availablePartitionKeys::contains)
                                .collect(Collectors.toList());
                this.partialPartitionGetter =
                        new PartialPartitionGetter(lookupRowType, availablePartitionKeyList);
            } else {
                // No partition keys available
                this.partitionGetter = null;
                this.partialPartitionGetter = null;
            }
        } else {
            this.partitionGetter = null;
            this.partialPartitionGetter = null;
        }

        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                tableInfo.getRowType().getChildren().toArray(new DataType[0])));
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

        // NOTE: Relaxed constraint - no longer require lookup columns to contain all partition
        // fields
        // This allows cross-partition prefix lookups with performance considerations

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

        // Unified lookup process:
        // Step 1: Determine target partitions based on prefixKey
        // Step 2: Perform lookup on the target partition list
        // Single partition lookup is a special case where the partition list contains only one
        // element
        return getTargetPartitions(prefixKey)
                .thenCompose(
                        partitionIds -> {
                            if (partitionIds.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        new LookupResult(Collections.emptyList()));
                            }

                            // Create parallel lookup tasks for all target partitions
                            List<CompletableFuture<List<byte[]>>> lookupFutures =
                                    partitionIds.stream()
                                            .map(
                                                    partitionId -> {
                                                        TableBucket tableBucket =
                                                                new TableBucket(
                                                                        tableInfo.getTableId(),
                                                                        partitionId,
                                                                        bucketId);
                                                        return lookupClient.prefixLookup(
                                                                tableBucket, bucketKeyBytes);
                                                    })
                                            .collect(Collectors.toList());

                            // Combine all lookup results
                            return CompletableFuture.allOf(
                                            lookupFutures.toArray(new CompletableFuture[0]))
                                    .thenApply(
                                            v -> {
                                                List<byte[]> allResults = new ArrayList<>();
                                                for (CompletableFuture<List<byte[]>> future :
                                                        lookupFutures) {
                                                    try {
                                                        allResults.addAll(future.get());
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(
                                                                "Error combining lookup results",
                                                                e);
                                                    }
                                                }
                                                return decodeResults(allResults);
                                            });
                        });
    }

    private CompletableFuture<List<Long>> getTargetPartitions(InternalRow prefixKey) {
        if (!tableInfo.isPartitioned()) {
            // Non-partitioned table: return list with single null partition ID
            return CompletableFuture.completedFuture(Collections.singletonList(null));
        }

        if (partitionGetter != null) {
            // All partition keys are available: single partition lookup
            try {
                Long partitionId =
                        getPartitionId(
                                prefixKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
                return CompletableFuture.completedFuture(Collections.singletonList(partitionId));
            } catch (PartitionNotExistException e) {
                // Partition does not exist, return empty list
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
        }

        // Multi-partition case
        return CompletableFuture.supplyAsync(
                () ->
                        ClientUtils.getPartitionIds(
                                tableInfo.getTablePath(),
                                metadataUpdater,
                                prefixKey,
                                partialPartitionGetter));
    }

    private LookupResult decodeResults(List<byte[]> resultBytes) {
        List<InternalRow> rowList = new ArrayList<>(resultBytes.size());
        for (byte[] valueBytes : resultBytes) {
            if (valueBytes == null) {
                continue;
            }
            rowList.add(kvValueDecoder.decodeValue(valueBytes).row);
        }
        return new LookupResult(rowList);
    }
}
