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
import org.apache.fluss.flink.shuffle.BucketLoadBalanceRouter;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

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
 * <p>The routing algorithm is shared with the source-side lookup-join partitioner ({@code
 * FlussLookupInputPartitioner}); see {@link BucketLoadBalanceRouter}.
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
    private transient BucketLoadBalanceRouter bucketLoadBalanceRouter;

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
        this.bucketLoadBalanceRouter =
                new BucketLoadBalanceRouter(
                        KeyEncoder.ofBucketKeyEncoder(flussRowType, bucketKeys, lakeFormat),
                        null,
                        BucketingFunction.of(lakeFormat),
                        numBucket);

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
            return bucketLoadBalanceRouter.route(row, numChannels);
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
}
