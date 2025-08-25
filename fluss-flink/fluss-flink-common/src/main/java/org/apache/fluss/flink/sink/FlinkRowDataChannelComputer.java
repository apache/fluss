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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** {@link ChannelComputer} for flink {@link RowData}. */
public class FlinkRowDataChannelComputer<InputT> implements ChannelComputer<InputT> {

    private static final long serialVersionUID = 1L;

    private final @Nullable DataLakeFormat lakeFormat;
    private final int numBucket;
    private final RowType flussRowType;
    private final List<String> bucketKeys;
    private final List<String> partitionKeys;
    private final FlussSerializationSchema<InputT> serializationSchema;

    private transient int numChannels;
    private transient BucketingFunction bucketingFunction;
    private transient KeyEncoder bucketKeyEncoder;
    private transient boolean combineShuffleWithPartitionName;
    private transient @Nullable PartitionGetter partitionGetter;

    public FlinkRowDataChannelComputer(
            RowType flussRowType,
            List<String> bucketKeys,
            List<String> partitionKeys,
            @Nullable DataLakeFormat lakeFormat,
            int numBucket,
            FlussSerializationSchema<InputT> serializationSchema) {
        this.flussRowType = flussRowType;
        this.bucketKeys = bucketKeys;
        this.partitionKeys = partitionKeys;
        this.lakeFormat = lakeFormat;
        this.numBucket = numBucket;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.bucketKeyEncoder = KeyEncoder.of(flussRowType, bucketKeys, lakeFormat);
        if (partitionKeys.isEmpty()) {
            this.partitionGetter = null;
        } else {
            this.partitionGetter = new PartitionGetter(flussRowType, partitionKeys);
        }

        // Only when partition keys exist and the Flink job parallelism and the bucket number are
        // not divisible, then we need to include the partition name as part of the shuffle key.
        // This approach can help avoid the possible data skew. For example, if bucket number is 3
        // and task parallelism is 2, it is highly possible that data shuffle becomes uneven. For
        // instance, in task1, it might have 'partition0-bucket0', 'partition1-bucket0',
        // 'partition0-bucket2', and 'partition1-bucket2', whereas in task2, it would only have
        // 'partition0-bucket1' and 'partition1-bucket1'. As partition number increases, this
        // situation becomes even more severe.
        this.combineShuffleWithPartitionName =
                partitionGetter != null && numBucket % numChannels != 0;

        try {
            this.serializationSchema.open(new SerializerInitContextImpl(flussRowType));
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }

    @Override
    public int channel(InputT record) {
        try {
            RowWithOp rowWithOp = serializationSchema.serialize(record);
            InternalRow row = rowWithOp.getRow();

            int bucketId = bucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), numBucket);
            if (!combineShuffleWithPartitionName) {
                return ChannelComputer.select(bucketId, numChannels);
            } else {
                checkNotNull(partitionGetter, "partitionGetter is null");
                String partitionName = partitionGetter.getPartition(row);
                return ChannelComputer.select(partitionName, bucketId, numChannels);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to serialize record of type '%s' in FlinkRowDataChannelComputer: %s",
                            record != null ? record.getClass().getName() : "null", e.getMessage()),
                    e);
        }
    }

    @Override
    public String toString() {
        return "BUCKET_SHUFFLE";
    }

    @VisibleForTesting
    boolean isCombineShuffleWithPartitionName() {
        return combineShuffleWithPartitionName;
    }
}
