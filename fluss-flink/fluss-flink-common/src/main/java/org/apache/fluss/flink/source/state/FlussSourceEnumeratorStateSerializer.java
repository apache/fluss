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

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        if (lakeSource != null) {
            serializeRemainingHybridLakeFlussSplits(out, state);
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

        List<SourceSplitBase> remainingHybridLakeFlussSplits = null;
        if (lakeSource != null) {
            // todo: add a ut for serialize remaining hybrid lake fluss splits
            remainingHybridLakeFlussSplits = deserializeRemainingHybridLakeFlussSplits(in);
        }

        return new SourceEnumeratorState(
                assignedBuckets, assignedPartitions, remainingHybridLakeFlussSplits);
    }

    private void serializeRemainingHybridLakeFlussSplits(
            final DataOutputSerializer out, SourceEnumeratorState state) throws IOException {
        List<SourceSplitBase> remainingHybridLakeFlussSplits =
                state.getRemainingHybridLakeFlussSplits();
        if (remainingHybridLakeFlussSplits != null) {
            // write that hybrid lake fluss splits is not null
            out.writeBoolean(true);
            out.writeInt(remainingHybridLakeFlussSplits.size());
            SourceSplitSerializer sourceSplitSerializer = new SourceSplitSerializer(lakeSource);
            out.writeInt(sourceSplitSerializer.getVersion());
            for (SourceSplitBase split : remainingHybridLakeFlussSplits) {
                byte[] serializeBytes = sourceSplitSerializer.serialize(split);
                out.writeInt(serializeBytes.length);
                out.write(serializeBytes);
            }
        } else {
            // write that hybrid lake fluss splits is null
            out.writeBoolean(false);
        }
    }

    @Nullable
    private List<SourceSplitBase> deserializeRemainingHybridLakeFlussSplits(
            final DataInputDeserializer in) throws IOException {
        if (in.readBoolean()) {
            int numSplits = in.readInt();
            List<SourceSplitBase> splits = new ArrayList<>(numSplits);
            SourceSplitSerializer sourceSplitSerializer = new SourceSplitSerializer(lakeSource);
            int version = in.readInt();
            for (int i = 0; i < numSplits; i++) {
                int splitSizeInBytes = in.readInt();
                byte[] splitBytes = new byte[splitSizeInBytes];
                in.readFully(splitBytes);
                splits.add(sourceSplitSerializer.deserialize(version, splitBytes));
            }
            return splits;
        } else {
            return null;
        }
    }
}
