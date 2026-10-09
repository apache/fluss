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
import org.apache.fluss.flink.adapter.SupportsLookupCustomShuffleAdapter.InputDataPartitionerAdapter;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.MathUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Partitions the lookup-join probe stream by Fluss bucket, consistent with the client-side
 * bucketing used by {@code PrimaryKeyLookuper}/{@code PrefixKeyLookuper} (bucket-key encoding +
 * {@link BucketingFunction}).
 *
 * <p>Partitioned and non-partitioned tables use the same strategy. When the bucket and subtask
 * counts do not evenly divide each other, weighted logical slots balance the expected load across
 * subtasks. This keeps every lookup key on a stable subtask while bounding each bucket's RPC
 * fan-out. Existing bucket-affinity mappings are preserved when either count is an exact multiple
 * of the other.
 */
public class FlussLookupInputPartitioner implements InputDataPartitionerAdapter {

    private static final long serialVersionUID = 1L;

    private final LookupNormalizer normalizer;
    // Flink row type of the normalized lookup key row (the expected lookup keys in Fluss key order,
    // i.e. the full primary key for a primary-key lookup, or the bucket keys + partition keys for a
    // prefix lookup).
    private final RowType keyFlinkRowType;
    private final List<String> bucketKeyNames;
    @Nullable private final DataLakeFormat lakeFormat;
    private final int numBuckets;

    private transient KeyEncoder bucketKeyEncoder;
    private transient KeyEncoder lookupKeyEncoder;
    private transient BucketingFunction bucketingFunction;
    private transient FlinkAsFlussRow reuseRow;

    /**
     * Creates a partitioner consistent with Fluss client-side bucket routing.
     *
     * @param normalizer normalizes Flink lookup keys into Fluss lookup-key order
     * @param keyFlinkRowType row type of the normalized lookup key
     * @param bucketKeyNames bucket-key field names within the normalized lookup key
     * @param lakeFormat optional lake format that defines key encoding and bucketing behavior
     * @param numBuckets positive number of buckets in the Fluss table
     */
    public FlussLookupInputPartitioner(
            LookupNormalizer normalizer,
            RowType keyFlinkRowType,
            List<String> bucketKeyNames,
            @Nullable DataLakeFormat lakeFormat,
            int numBuckets) {
        this.normalizer = checkNotNull(normalizer, "normalizer must not be null.");
        this.keyFlinkRowType = checkNotNull(keyFlinkRowType, "keyFlinkRowType must not be null.");
        checkNotNull(bucketKeyNames, "bucketKeyNames must not be null.");
        checkArgument(
                !bucketKeyNames.isEmpty()
                        && keyFlinkRowType.getFieldNames().containsAll(bucketKeyNames),
                "Illegal bucket keys: %s, must be a non-empty subset of lookup keys: %s.",
                bucketKeyNames,
                keyFlinkRowType.getFieldNames());
        this.bucketKeyNames = new ArrayList<>(bucketKeyNames);
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
            lookupKeyEncoder =
                    CompactedKeyEncoder.createKeyEncoder(
                            flussKeyType, keyFlinkRowType.getFieldNames());
            bucketingFunction = BucketingFunction.of(lakeFormat);
            reuseRow = new FlinkAsFlussRow();
        }
    }

    @Override
    public int partition(RowData joinKeys, int numPartitions) {
        checkArgument(
                numPartitions > 0, "numPartitions must be positive, but was %s.", numPartitions);
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
        if (numBuckets >= numPartitions && numBuckets % numPartitions == 0) {
            return bucketId % numPartitions;
        }

        byte[] lookupKeyBytes = lookupKeyEncoder.encodeKey(flussKeyRow);
        // Do not derive this hash from the bucket hash. The low bits of that hash determine the
        // bucket id, so reusing it can make some logical slots unreachable.
        int lookupKeyHash = MathUtils.murmurHash(Arrays.hashCode(lookupKeyBytes));
        if (numPartitions > numBuckets && numPartitions % numBuckets == 0) {
            // Preserve the original disjoint round-robin assignment when every bucket owns the
            // same number of subtasks.
            int candidateCount = numPartitions / numBuckets;
            return (lookupKeyHash % candidateCount) * numBuckets + bucketId;
        }

        // Represent the assignment with LCM(numBuckets, numPartitions) logical slots without
        // materializing them. Each bucket owns numPartitions / gcd consecutive slots and each
        // subtask owns numBuckets / gcd consecutive slots. A uniform hash within a bucket therefore
        // gives every subtask the same expected number of slots, while a bucket can only fan out to
        // the subtasks whose slot ranges overlap its own range.
        int gcd = greatestCommonDivisor(numBuckets, numPartitions);
        int slotsPerBucket = numPartitions / gcd;
        int slotsPerSubtask = numBuckets / gcd;
        int slotWithinBucket = lookupKeyHash % slotsPerBucket;
        long logicalSlot = (long) bucketId * slotsPerBucket + slotWithinBucket;
        return (int) (logicalSlot / slotsPerSubtask);
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    private static int greatestCommonDivisor(int first, int second) {
        while (second != 0) {
            int remainder = first % second;
            first = second;
            second = remainder;
        }
        return first;
    }
}
