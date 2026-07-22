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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.MathUtils;
import org.apache.fluss.utils.MurmurHashUtils;

import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle.InputDataPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.UnsafeUtils.BYTE_ARRAY_BASE_OFFSET;

/**
 * Partitions the lookup-join probe stream by Fluss bucket, consistent with the client-side
 * bucketing used by {@code PrimaryKeyLookuper}/{@code PrefixKeyLookuper} (bucket key encoding +
 * {@link BucketingFunction}). Rows mapping to the same Fluss bucket are always routed to the same
 * partition.
 *
 * <p>For partitioned tables the client routes by {@code (partitionId, bucketId)}: the same bucket
 * id in different partitions is a different tablet. The partition keys therefore join the bucket
 * key in the channel computation, so rows targeting the same tablet stay co-located while different
 * partitions are spread across subtasks instead of collapsing onto one.
 */
public class FlussLookupInputPartitioner implements InputDataPartitioner {

    private static final long serialVersionUID = 1L;

    private final LookupNormalizer normalizer;
    // Flink row type of the normalized lookup key row (the expected lookup keys in Fluss key order,
    // i.e. the full primary key for a primary-key lookup, or the bucket keys + partition keys for a
    // prefix lookup).
    private final RowType keyFlinkRowType;
    private final List<String> bucketKeyNames;
    // Partition-key field names within the normalized lookup key; empty for non-partitioned tables.
    private final List<String> partitionKeyNames;
    @Nullable private final DataLakeFormat lakeFormat;
    private final int numBuckets;

    private transient KeyEncoder bucketKeyEncoder;
    // null when the table is not partitioned.
    @Nullable private transient KeyEncoder partitionKeyEncoder;
    private transient BucketingFunction bucketingFunction;
    private transient FlinkAsFlussRow reuseRow;

    /**
     * Creates a partitioner consistent with Fluss client-side bucket routing.
     *
     * @param normalizer normalizes Flink lookup keys into Fluss lookup-key order
     * @param keyFlinkRowType row type of the normalized lookup key
     * @param bucketKeyNames bucket-key field names within the normalized lookup key
     * @param partitionKeyNames partition-key field names within the normalized lookup key; empty
     *     for non-partitioned tables
     * @param lakeFormat optional lake format that defines key encoding and bucketing behavior
     * @param numBuckets positive number of buckets in the Fluss table
     */
    public FlussLookupInputPartitioner(
            LookupNormalizer normalizer,
            RowType keyFlinkRowType,
            List<String> bucketKeyNames,
            List<String> partitionKeyNames,
            @Nullable DataLakeFormat lakeFormat,
            int numBuckets) {
        this.normalizer = normalizer;
        this.keyFlinkRowType = keyFlinkRowType;
        this.bucketKeyNames = bucketKeyNames;
        this.partitionKeyNames = partitionKeyNames;
        this.lakeFormat = lakeFormat;
        checkArgument(numBuckets > 0, "numBuckets must be positive, but was %s.", numBuckets);
        this.numBuckets = numBuckets;
    }

    private void ensureInitialized() {
        if (bucketKeyEncoder == null) {
            org.apache.fluss.types.RowType flussKeyType =
                    FlinkConversions.toFlussRowType(keyFlinkRowType);
            // bucketing uses the bucket-key encoder consistent with the client's bucket routing
            bucketKeyEncoder =
                    KeyEncoder.ofBucketKeyEncoder(flussKeyType, bucketKeyNames, lakeFormat);
            if (!partitionKeyNames.isEmpty()) {
                partitionKeyEncoder =
                        KeyEncoder.ofBucketKeyEncoder(flussKeyType, partitionKeyNames, lakeFormat);
            }
            bucketingFunction = BucketingFunction.of(lakeFormat);
            reuseRow = new FlinkAsFlussRow();
        }
    }

    @Override
    public int partition(RowData joinKeys, int numPartitions) {
        // Null lookup keys cannot match, but LEFT lookup joins still need to reach the operator.
        for (int i = 0; i < joinKeys.getArity(); i++) {
            if (joinKeys.isNullAt(i)) {
                return 0;
            }
        }
        ensureInitialized();
        // normalize the projected join keys into the Fluss key order
        RowData normalizedKey = normalizer.normalizeLookupKey(joinKeys);
        InternalRow flussKeyRow = reuseRow.replace(normalizedKey);
        byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(flussKeyRow);
        // BucketingFunction always returns a non-negative bucket id.
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);
        if (partitionKeyEncoder == null) {
            return bucketId % numPartitions;
        }
        // Route by (partition, bucket) so different partitions of the same bucket id are spread
        // across subtasks while rows targeting the same tablet stay co-located. Mix in the bucket
        // id using the same Murmur hash family the client bucketing uses (FlussBucketingFunction).
        byte[] partitionKeyBytes = partitionKeyEncoder.encodeKey(flussKeyRow);
        int partitionHash =
                MurmurHashUtils.hashUnsafeBytes(
                        partitionKeyBytes, BYTE_ARRAY_BASE_OFFSET, partitionKeyBytes.length);
        // murmurHash returns a non-negative int, so the modulo is always a valid channel.
        return MathUtils.murmurHash(partitionHash * 31 + bucketId) % numPartitions;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }
}
