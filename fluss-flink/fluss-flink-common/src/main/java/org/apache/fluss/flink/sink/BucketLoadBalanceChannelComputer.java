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

package org.apache.fluss.flink.sink;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.MathUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

/**
 * {@link ChannelComputer} for {@link
 * org.apache.fluss.flink.sink.shuffle.DistributionMode#BUCKET_LOAD_BALANCE}.
 *
 * <p>Distributes records by bucket key with even load across all downstream channels. Unlike {@link
 * FlinkRowDataChannelComputer} (BUCKET mode) which maps each bucket to a single channel, this
 * computer uses an LCM-based logical slot assignment so that every channel receives traffic from
 * every bucket.
 *
 * <p>The core routing algorithm ({@link #routeByBucket}) is shared between the sink (this class)
 * and the source-side lookup-join partitioner ({@code FlussLookupInputPartitioner}).
 *
 * <p>Records with the same bucket key always route to the same channel.
 *
 * @param <InputT> the type of records
 */
@Internal
public class BucketLoadBalanceChannelComputer<InputT> implements ChannelComputer<InputT> {

    private static final long serialVersionUID = 1L;

    private final @Nullable DataLakeFormat lakeFormat;
    private final int numBucket;
    private final RowType flussRowType;
    private final List<String> bucketKeys;
    private final FlussSerializationSchema<InputT> serializationSchema;

    private transient int numChannels;
    private transient BucketingFunction bucketingFunction;
    private transient KeyEncoder bucketKeyEncoder;

    public BucketLoadBalanceChannelComputer(
            RowType flussRowType,
            List<String> bucketKeys,
            @Nullable DataLakeFormat lakeFormat,
            int numBucket,
            FlussSerializationSchema<InputT> serializationSchema) {
        this.flussRowType = flussRowType;
        this.bucketKeys = bucketKeys;
        this.lakeFormat = lakeFormat;
        this.numBucket = numBucket;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.bucketKeyEncoder = KeyEncoder.ofBucketKeyEncoder(flussRowType, bucketKeys, lakeFormat);

        try {
            this.serializationSchema.open(new SerializerInitContextImpl(flussRowType, false));
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }

    @Override
    public int channel(InputT record) {
        try {
            RowWithOp rowWithOp = serializationSchema.serialize(record);
            InternalRow row = rowWithOp.getRow();
            byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(row);
            return routeByBucket(
                    bucketKeyBytes, bucketKeyBytes, numBucket, numChannels, bucketingFunction);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to serialize record of type '%s'"
                                    + " in BucketLoadBalanceChannelComputer: %s",
                            record != null ? record.getClass().getName() : "null", e.getMessage()),
                    e);
        }
    }

    @Override
    public String toString() {
        return "BUCKET_LOAD_BALANCE";
    }

    // ------------------------------------------------------------------------------------------
    //  Shared routing logic
    // ------------------------------------------------------------------------------------------

    /**
     * Routes a record to a downstream channel using bucket-ID + LCM-based slot assignment.
     *
     * <p>This is the core routing algorithm used by both the sink-side {@code
     * BucketLoadBalanceChannelComputer} and the source-side lookup-join partitioner ({@code
     * FlussLookupInputPartitioner}). Callers provide two separate byte arrays so that different
     * hash keys can be used for the intra-bucket slot selection without affecting bucket-ID
     * computation.
     *
     * <ul>
     *   <li>Sink usage: both {@code bucketKeyBytes} and {@code hashKeyBytes} are the encoded bucket
     *       key — records with the same bucket key always land on the same subtask.
     *   <li>Lookup-join usage: {@code bucketKeyBytes} comes from the bucket-key encoder while
     *       {@code hashKeyBytes} comes from the full lookup-key encoder, giving a different hash
     *       domain within each bucket so that different lookup keys in the same bucket are spread
     *       across the bucket's subtask range.
     * </ul>
     *
     * <p>When the bucket count is evenly divisible by the channel count the hash is skipped
     * entirely and the result is simply {@code bucketId % numChannels}, matching the behaviour of
     * the original {@code BUCKET} distribution mode.
     *
     * @param bucketKeyBytes encoded bucket key (determines bucket ID)
     * @param hashKeyBytes encoded key used for the intra-bucket slot hash
     * @param numBuckets total number of buckets
     * @param numChannels number of downstream channels (subtask parallelism)
     * @param bucketingFunction the bucketing function for computing the bucket ID
     * @return channel index in {@code [0, numChannels)}
     */
    public static int routeByBucket(
            byte[] bucketKeyBytes,
            byte[] hashKeyBytes,
            int numBuckets,
            int numChannels,
            BucketingFunction bucketingFunction) {
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);

        // Fast-path: when bucket count is an exact multiple of channel count, every bucket
        // maps to a single channel and no hash is needed.
        if (numBuckets >= numChannels && numBuckets % numChannels == 0) {
            return bucketId % numChannels;
        }

        int recordHash = MathUtils.murmurHash(Arrays.hashCode(hashKeyBytes));
        return ChannelComputer.select(bucketId, numBuckets, numChannels, recordHash);
    }
}
