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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;
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
    private final @Nullable PartitionBucketCountResolver partitionBucketCountResolver;

    private transient int numChannels;
    private transient BucketingFunction bucketingFunction;
    private transient KeyEncoder bucketKeyEncoder;
    private transient @Nullable PartitionGetter partitionGetter;

    public FlinkRowDataChannelComputer(
            RowType flussRowType,
            List<String> bucketKeys,
            List<String> partitionKeys,
            @Nullable DataLakeFormat lakeFormat,
            TablePath tablePath,
            Configuration flussConfig,
            int numBucket,
            FlussSerializationSchema<InputT> serializationSchema) {
        this.flussRowType = flussRowType;
        this.bucketKeys = bucketKeys;
        this.partitionKeys = partitionKeys;
        this.lakeFormat = lakeFormat;
        this.numBucket = numBucket;
        // Non-partitioned tables fix the bucket count to the table-level value; partitioned
        // tables resolve the actual per-partition count at runtime.
        this.partitionBucketCountResolver =
                partitionKeys.isEmpty()
                        ? null
                        : new PartitionBucketCountResolver(tablePath, flussConfig, numBucket);
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.bucketKeyEncoder = KeyEncoder.ofBucketKeyEncoder(flussRowType, bucketKeys, lakeFormat);
        if (partitionKeys.isEmpty()) {
            this.partitionGetter = null;
        } else {
            this.partitionGetter = new PartitionGetter(flussRowType, partitionKeys);
        }

        try {
            // no need to read real database, thus assume to deserialize the fluss row as same as
            // flink table type.
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

            if (partitionBucketCountResolver == null) {
                // Non-partitioned table: the bucket count is fixed to the table-level value.
                int bucketId =
                        bucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), numBucket);
                return ChannelComputer.select(bucketId, numChannels);
            }

            checkNotNull(partitionGetter, "partitionGetter is null");
            String partitionName = partitionGetter.getPartition(row);
            // Resolve the partition's actual bucket count Sharding with the stale table-level
            // value would scatter the records of one bucket across multiple writer subtasks
            // and eventually break sink recovery.
            int bucketCount = partitionBucketCountResolver.bucketCountOf(partitionName);
            int bucketId =
                    bucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), bucketCount);
            if (ChannelComputer.shouldCombinePartitionInSharding(true, bucketCount, numChannels)) {
                return ChannelComputer.select(partitionName, bucketId, numChannels);
            }
            return ChannelComputer.select(bucketId, numChannels);
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
        return "BUCKET";
    }
}
