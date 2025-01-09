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

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.lakehouse.LakeTableBucketAssigner;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.getter.PartitionGetter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.ValueDecoder;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.client.utils.ClientUtils.getBucketId;
import static com.alibaba.fluss.client.utils.ClientUtils.getPartitionId;

/**
 * The default impl of {@link PrefixLookuper}.
 *
 * @since 0.6
 */
@PublicEvolving
public class FlussPrefixLookuper implements PrefixLookuper {

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

    public FlussPrefixLookuper(
            TableInfo tableInfo,
            int numBuckets,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            KeyEncoder bucketKeyEncoder,
            LakeTableBucketAssigner lakeTableBucketAssigner,
            @Nullable PartitionGetter partitionGetter,
            ValueDecoder kvValueDecoder) {
        this.tableInfo = tableInfo;
        this.numBuckets = numBuckets;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.isDataLakeEnable = tableInfo.getTableDescriptor().isDataLakeEnabled();
        this.lakeTableBucketAssigner = lakeTableBucketAssigner;
        this.bucketKeyEncoder = bucketKeyEncoder;
        this.partitionGetter = partitionGetter;
        this.kvValueDecoder = kvValueDecoder;
    }

    @Override
    public CompletableFuture<PrefixLookupResult> prefixLookup(InternalRow prefixKey) {
        byte[] prefixKeyBytes = bucketKeyEncoder.encode(prefixKey);
        int bucketId =
                getBucketId(
                        prefixKeyBytes,
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
                .prefixLookup(tableBucket, prefixKeyBytes)
                .thenApply(
                        result -> {
                            List<InternalRow> rowList = new ArrayList<>();
                            for (byte[] valueBytes : result) {
                                rowList.add(
                                        valueBytes == null
                                                ? null
                                                : kvValueDecoder.decodeValue(valueBytes).row);
                            }
                            return new PrefixLookupResult(rowList);
                        });
    }
}
