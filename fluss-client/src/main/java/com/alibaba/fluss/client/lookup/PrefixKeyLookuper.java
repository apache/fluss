/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.client.lakehouse.LakeTableBucketAssigner;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.getter.PartitionGetter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.decode.RowDecoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.client.utils.ClientUtils.getBucketId;
import static com.alibaba.fluss.client.utils.ClientUtils.getPartitionId;

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

    private final boolean isDataLakeEnable;

    private final int numBuckets;

    private final LakeTableBucketAssigner lakeTableBucketAssigner;

    /**
     * a getter to extract partition from prefix lookup key row, null when it's not a partitioned.
     */
    private @Nullable final PartitionGetter partitionGetter;

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
        this.bucketKeyEncoder =
                KeyEncoder.createKeyEncoder(lookupRowType, tableInfo.getBucketKeys());
        this.isDataLakeEnable = tableInfo.getTableConfig().isDataLakeEnabled();
        this.lakeTableBucketAssigner =
                new LakeTableBucketAssigner(lookupRowType, tableInfo.getBucketKeys(), numBuckets);
        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;
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
        byte[] bucketKeyBytes = bucketKeyEncoder.encode(prefixKey);
        int bucketId =
                getBucketId(
                        bucketKeyBytes,
                        prefixKey,
                        lakeTableBucketAssigner,
                        isDataLakeEnable,
                        numBuckets,
                        metadataUpdater);

        Long partitionId = null;
        if (partitionGetter != null) {
            partitionId =
                    getPartitionId(
                            prefixKey, partitionGetter, tableInfo.getTablePath(), metadataUpdater);
        }

        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupClient
                .prefixLookup(tableBucket, bucketKeyBytes)
                .thenApply(
                        result -> {
                            List<InternalRow> rowList = new ArrayList<>(result.size());
                            for (byte[] valueBytes : result) {
                                if (valueBytes == null) {
                                    continue;
                                }
                                rowList.add(kvValueDecoder.decodeValue(valueBytes).row);
                            }
                            return new LookupResult(rowList);
                        });
    }
}
