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

package org.apache.fluss.flink.source.state;

import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A serializer for {@link SourceEnumeratorState}. */
public class FlussSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<SourceEnumeratorState> {

    @Nullable private final LakeSource<LakeSplit> lakeSource;

    private static final int VERSION_0 = 0;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = VERSION_0;

    public FlussSourceEnumeratorStateSerializer(LakeSource<LakeSplit> lakeSource) {
        this.lakeSource = lakeSource;
    }

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
        // write remain lake snapshot splits
        out.writeInt(state.getRemainingLakeSnapshotSplits().size());
        for (LakeSnapshotSplit split : state.getRemainingLakeSnapshotSplits()) {
            byte[] serializeBytes =
                    checkNotNull(lakeSource).getSplitSerializer().serialize(split.getLakeSplit());
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
            out.writeUTF(split.getPartitionName());
            // write partition
            // if partition is not null
            if (split.getTableBucket().getPartitionId() != null) {
                out.writeBoolean(true);
                out.writeLong(split.getTableBucket().getPartitionId());
            } else {
                out.writeBoolean(false);
            }
            out.writeLong(split.getTableBucket().getTableId());
            out.writeInt(split.getTableBucket().getBucket());
        }
        // write table buckets offset
        out.writeInt(state.getTableBucketsOffset().size());
        for (Map.Entry<TableBucket, Long> entry : state.getTableBucketsOffset().entrySet()) {
            out.writeLong(entry.getKey().getTableId());
            // write partition
            // if partition is not null
            if (entry.getKey().getPartitionId() != null) {
                out.writeBoolean(true);
                out.writeLong(entry.getKey().getPartitionId());
            } else {
                out.writeBoolean(false);
            }
            out.writeInt(entry.getKey().getBucket());
            out.writeLong(entry.getValue());
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

        // deserialize remain lake snapshot splits
        int remainLakeSnapshotSplitsSize = in.readInt();
        List<LakeSnapshotSplit> remainLakeSnapshotSplits =
                new ArrayList<>(remainLakeSnapshotSplitsSize);
        for (int i = 0; i < remainLakeSnapshotSplitsSize; i++) {
            int splitSize = in.readInt();
            byte[] splitBytes = new byte[splitSize];
            in.readFully(splitBytes);
            LakeSplit split =
                    checkNotNull(lakeSource).getSplitSerializer().deserialize(0, splitBytes);
            String partitionName = in.readUTF();
            Long partition = null;
            if (in.readBoolean()) {
                partition = in.readLong();
            }
            long tableId = in.readLong();
            int bucket = in.readInt();
            remainLakeSnapshotSplits.add(
                    new LakeSnapshotSplit(
                            new TableBucket(tableId, partition, bucket), partitionName, split));
        }

        // deserialize table buckets offset
        int tableBucketsOffsetSize = in.readInt();
        Map<TableBucket, Long> tableBucketsOffset = new HashMap<>(tableBucketsOffsetSize);
        for (int i = 0; i < tableBucketsOffsetSize; i++) {
            long tableId = in.readLong();
            Long partition = null;
            if (in.readBoolean()) {
                partition = in.readLong();
            }
            int bucket = in.readInt();
            long offset = in.readLong();
            tableBucketsOffset.put(new TableBucket(tableId, partition, bucket), offset);
        }

        return new SourceEnumeratorState(
                assignedBuckets, assignedPartitions, remainLakeSnapshotSplits, tableBucketsOffset);
    }
}
