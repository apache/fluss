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

package com.alibaba.fluss.flink.source.state;

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** A serializer for {@link SourceEnumeratorState}. */
public class FlussSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<SourceEnumeratorState> {

    public static final FlussSourceEnumeratorStateSerializer INSTANCE =
            new FlussSourceEnumeratorStateSerializer();

    private static final int VERSION_0 = 0;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = VERSION_0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SourceEnumeratorState state) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        // write assigned buckets
        out.writeInt(state.getAssignedBuckets().size());
        for (TableBucket tableBucket : state.getAssignedBuckets()) {
            out.writeLong(tableBucket.getTableId());

            // write partition
            // if partition is not null
            if (tableBucket.getPartitionId() != null) {
                out.writeBoolean(true);
                out.writeLong(tableBucket.getPartitionId());
            } else {
                out.writeBoolean(false);
            }

            out.writeInt(tableBucket.getBucket());
        }
        // write assigned partitions
        out.writeInt(state.getAssignedPartitions().size());
        for (Map.Entry<Long, String> entry : state.getAssignedPartitions().entrySet()) {
            out.writeLong(entry.getKey());
            out.writeUTF(entry.getValue());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public SourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_0) {
            throw new IOException("Unknown version or corrupt state: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        // deserialize assigned buckets
        int assignedBucketsSize = in.readInt();
        Set<TableBucket> assignedBuckets = new HashSet<>(assignedBucketsSize);
        for (int i = 0; i < assignedBucketsSize; i++) {
            // read partition
            long tableId = in.readLong();
            Long partition = null;
            if (in.readBoolean()) {
                partition = in.readLong();
            }

            int bucket = in.readInt();
            assignedBuckets.add(new TableBucket(tableId, partition, bucket));
        }

        // deserialize assigned partitions
        int assignedPartitionsSize = in.readInt();
        Map<Long, String> assignedPartitions = new HashMap<>(assignedPartitionsSize);
        for (int i = 0; i < assignedPartitionsSize; i++) {
            long partitionId = in.readLong();
            String partition = in.readUTF();
            assignedPartitions.put(partitionId, partition);
        }
        return new SourceEnumeratorState(assignedBuckets, assignedPartitions);
    }
}
