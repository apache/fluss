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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.types.Tuple2;

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

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Serializer for {@link SourceEnumeratorState}.
 *
 * <p>This serializer manages the versioned persistence of the enumerator's state, including
 * assigned buckets, partitions, and remaining hybrid lake/Fluss splits.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 0:</b> Initial version. Remaining hybrid lake splits are only (de)serialized if
 *       the {@code lakeSource} is non-null.
 *   <li><b>Version 1 (Current):</b> Decouples split serialization from the {@code lakeSource}
 *       presence. It always attempts to (de)serialize the splits, using an internal boolean flag to
 *       indicate presence. This ensures state consistency regardless of the current runtime
 *       configuration.
 * </ul>
 *
 * <p><b>Compatibility Note:</b> This serializer is designed for backward compatibility. It can
 * deserialize states from Version 0, but always produces Version 1 during serialization.
 */
public class FlussSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<SourceEnumeratorState> {

    @Nullable private final LakeSource<LakeSplit> lakeSource;

    private static final int VERSION_0 = 0;
    private static final int VERSION_1 = 1;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = VERSION_1;

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

        // serialize assign bucket and partitions
        serializeAssignBucketAndPartitions(
                out, state.getAssignedBuckets(), state.getAssignedPartitions());

        // serialize remain hybrid lake splits
        serializeRemainingHybridLakeFlussSplits(out, state);

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    private void serializeAssignBucketAndPartitions(
            DataOutputSerializer out,
            Set<TableBucket> assignedBuckets,
            Map<Long, String> assignedPartitions)
            throws IOException {
        // write assigned buckets
        out.writeInt(assignedBuckets.size());
        for (TableBucket tableBucket : assignedBuckets) {
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
        out.writeInt(assignedPartitions.size());
        for (Map.Entry<Long, String> entry : assignedPartitions.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeUTF(entry.getValue());
        }
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

    @VisibleForTesting
    protected byte[] serializeV0(SourceEnumeratorState state) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        serializeAssignBucketAndPartitions(
                out, state.getAssignedBuckets(), state.getAssignedPartitions());
        if (lakeSource != null) {
            serializeRemainingHybridLakeFlussSplits(out, state);
        }
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public SourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_1:
                return deserializeV1(serialized);
            case VERSION_0:
                return deserializeV0(serialized);
            default:
                throw new IOException(
                        String.format(
                                "The bytes are serialized with version %d, "
                                        + "while this deserializer only supports version up to %d",
                                version, CURRENT_VERSION));
        }
    }

    private SourceEnumeratorState deserializeV0(byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        Tuple2<Set<TableBucket>, Map<Long, String>> assignBucketAndPartitions =
                deserializeAssignBucketAndPartitions(in);
        List<SourceSplitBase> remainingHybridLakeFlussSplits = null;
        // in version 0, deserialize remaining hybrid lake Fluss splits only when lakeSource is
        // not null.
        if (lakeSource != null) {
            remainingHybridLakeFlussSplits = deserializeRemainingHybridLakeFlussSplits(in);
        }
        return new SourceEnumeratorState(
                assignBucketAndPartitions.f0,
                assignBucketAndPartitions.f1,
                remainingHybridLakeFlussSplits);
    }

    private SourceEnumeratorState deserializeV1(byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        Tuple2<Set<TableBucket>, Map<Long, String>> assignBucketAndPartitions =
                deserializeAssignBucketAndPartitions(in);
        List<SourceSplitBase> remainingHybridLakeFlussSplits =
                deserializeRemainingHybridLakeFlussSplits(in);
        // in version 1,  always attempt to deserialize remaining hybrid lake/Fluss
        // splits. The serialized state encodes their presence via a boolean flag, so
        // this logic no longer depends on the lakeSource flag. This unconditional
        // deserialization is the intended behavior change compared to VERSION_0.
        return new SourceEnumeratorState(
                assignBucketAndPartitions.f0,
                assignBucketAndPartitions.f1,
                remainingHybridLakeFlussSplits);
    }

    private Tuple2<Set<TableBucket>, Map<Long, String>> deserializeAssignBucketAndPartitions(
            DataInputDeserializer in) throws IOException {
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
        return Tuple2.of(assignedBuckets, assignedPartitions);
    }

    @Nullable
    private List<SourceSplitBase> deserializeRemainingHybridLakeFlussSplits(
            final DataInputDeserializer in) throws IOException {
        if (in.readBoolean()) {
            int numSplits = in.readInt();
            List<SourceSplitBase> splits = new ArrayList<>(numSplits);
            SourceSplitSerializer sourceSplitSerializer =
                    new SourceSplitSerializer(
                            checkNotNull(
                                    lakeSource,
                                    "lake source must not be null when there are hybrid lake splits."));
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
