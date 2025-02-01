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
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.decode.RowDecoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.client.utils.ClientUtils.getBucketId;
import static com.alibaba.fluss.client.utils.ClientUtils.getPartitionId;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** An implementation of {@link Lookuper} that lookups by primary key. */
class PrimaryKeyLookuper implements Lookuper {

    private final TableInfo tableInfo;

    private final MetadataUpdater metadataUpdater;

    private final LookupClient lookupClient;

    private final KeyEncoder primaryKeyEncoder;

    /**
     * Extract bucket key from lookup key row, use {@link #primaryKeyEncoder} if is default bucket
     * key (bucket key = physical primary key).
     */
    private final KeyEncoder bucketKeyEncoder;

    private final boolean isDataLakeEnable;

    private final int numBuckets;

    private final LakeTableBucketAssigner lakeTableBucketAssigner;

    /** a getter to extract partition from lookup key row, null when it's not a partitioned. */
    private @Nullable final PartitionGetter partitionGetter;

    /** Decode the lookup bytes to result row. */
    private final ValueDecoder kvValueDecoder;

    public PrimaryKeyLookuper(
            TableInfo tableInfo, MetadataUpdater metadataUpdater, LookupClient lookupClient) {
        checkArgument(
                tableInfo.getTableDescriptor().hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.tableInfo = tableInfo;
        this.numBuckets =
                tableInfo.getTableDescriptor().getTableDistribution().get().getBucketCount().get();
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;

        TableDescriptor tableDescriptor = tableInfo.getTableDescriptor();
        Schema schema = tableDescriptor.getSchema();
        RowType primaryKeyRowType = schema.toRowType().project(schema.getPrimaryKeyIndexes());
        this.primaryKeyEncoder =
                KeyEncoder.createKeyEncoder(
                        primaryKeyRowType,
                        primaryKeyRowType.getFieldNames(),
                        tableDescriptor.getPartitionKeys());
        if (tableDescriptor.isDefaultBucketKey()) {
            this.bucketKeyEncoder = primaryKeyEncoder;
        } else {
            // bucket key doesn't contain partition key, so no need exclude partition keys
            this.bucketKeyEncoder =
                    new KeyEncoder(primaryKeyRowType, tableDescriptor.getBucketKeyIndexes());
        }
        this.isDataLakeEnable = tableInfo.getTableDescriptor().isDataLakeEnabled();
        this.lakeTableBucketAssigner =
                new LakeTableBucketAssigner(
                        primaryKeyRowType, tableDescriptor.getBucketKey(), numBuckets);
        this.partitionGetter =
                tableDescriptor.isPartitioned()
                        ? new PartitionGetter(primaryKeyRowType, tableDescriptor.getPartitionKeys())
                        : null;
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableDescriptor.getKvFormat(),
                                schema.toRowType().getChildren().toArray(new DataType[0])));
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        // encoding the key row using a compacted way consisted with how the key is encoded when put
        // a row
        byte[] pkBytes = primaryKeyEncoder.encode(lookupKey);
        byte[] bkBytes =
                bucketKeyEncoder == primaryKeyEncoder
                        ? pkBytes
                        : bucketKeyEncoder.encode(lookupKey);
        Long partitionId =
                partitionGetter == null
                        ? null
                        : getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
        int bucketId =
                getBucketId(
                        bkBytes,
                        lookupKey,
                        lakeTableBucketAssigner,
                        isDataLakeEnable,
                        numBuckets,
                        metadataUpdater);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupClient
                .lookup(tableBucket, pkBytes)
                .thenApply(
                        valueBytes -> {
                            InternalRow row =
                                    valueBytes == null
                                            ? null
                                            : kvValueDecoder.decodeValue(valueBytes).row;
                            return new LookupResult(row);
                        });
    }
}
