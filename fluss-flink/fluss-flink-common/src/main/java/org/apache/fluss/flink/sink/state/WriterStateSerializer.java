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

package org.apache.fluss.flink.sink.state;

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for {@link WriterState}.
 *
 * <p>This serializer is used by Flink's state backend to serialize/deserialize WriterState objects
 * for checkpoint and recovery.
 *
 * <p>Version history:
 *
 * <ul>
 *   <li>Version 1: Initial version with {@code Map<Integer, Long>} (deprecated, does not support
 *       partitioned tables)
 *   <li>Version 2: Current version with {@code Map<TableBucket, Long>} to support partitioned
 *       tables
 * </ul>
 */
public class WriterStateSerializer extends TypeSerializer<WriterState> {

    private static final long serialVersionUID = 1L;

    private static final int VERSION_1 = 1; // Legacy version: Map<Integer, Long>

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<WriterState> duplicate() {
        return this;
    }

    @Override
    public WriterState createInstance() {
        return WriterState.empty(0);
    }

    @Override
    public WriterState copy(WriterState from) {
        // WriterState is immutable, but to be safe we create a new instance
        return new WriterState(from.getCheckpointId(), from.getBucketOffsets());
    }

    @Override
    public WriterState copy(WriterState from, WriterState reuse) {
        // Ignore reuse, just copy
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // Variable length
    }

    @Override
    public void serialize(WriterState state, DataOutputView target) throws IOException {
        // Write version for backward compatibility
        target.writeInt(VERSION_1);

        // Write checkpoint ID
        target.writeLong(state.getCheckpointId());

        // Write bucket offsets map
        Map<TableBucket, Long> bucketOffsets = state.getBucketOffsets();
        target.writeInt(bucketOffsets.size());
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            // Write TableBucket fields
            target.writeLong(tableBucket.getTableId());
            // Write partitionId (nullable)
            Long partitionId = tableBucket.getPartitionId();
            target.writeBoolean(partitionId != null);
            if (partitionId != null) {
                target.writeLong(partitionId);
            }
            target.writeInt(tableBucket.getBucket());
            // Write offset
            target.writeLong(entry.getValue());
        }
    }

    @Override
    public WriterState deserialize(DataInputView source) throws IOException {
        // Read version
        int version = source.readInt();
        if (version == VERSION_1) {
            return deserializeV1(source);
        } else {
            throw new IOException("Unsupported version: " + version);
        }
    }

    @Override
    public WriterState deserialize(WriterState reuse, DataInputView source) throws IOException {
        // Ignore reuse, just deserialize
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // Read and write the serialized data
        WriterState state = deserialize(source);
        serialize(state, target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof WriterStateSerializer;
    }

    @Override
    public int hashCode() {
        return WriterStateSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<WriterState> snapshotConfiguration() {
        return new WriterStateSerializerSnapshot();
    }

    /**
     * Deserialize version 1 format: {@code Map<Integer, Long>}.
     *
     * <p>This is kept for backward compatibility. The deserialized state will use a default tableId
     * of 0 and no partitionId.
     */
    private WriterState deserializeV1(DataInputView source) throws IOException {
        // Read checkpoint ID
        long checkpointId = source.readLong();

        // Read bucket offsets map
        int size = source.readInt();
        Map<TableBucket, Long> bucketOffsets = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            // Read TableBucket fields
            long tableId = source.readLong();
            boolean hasPartitionId = source.readBoolean();
            @Nullable Long partitionId = hasPartitionId ? source.readLong() : null;
            int bucketId = source.readInt();
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            // Read offset
            long offset = source.readLong();
            bucketOffsets.put(tableBucket, offset);
        }

        return new WriterState(checkpointId, bucketOffsets);
    }

    /** Serializer snapshot for {@link WriterStateSerializer}. */
    public static class WriterStateSerializerSnapshot
            implements TypeSerializerSnapshot<WriterState> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            // No configuration to write
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            // No configuration to read
        }

        @Override
        public TypeSerializer<WriterState> restoreSerializer() {
            return new WriterStateSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<WriterState> resolveSchemaCompatibility(
                TypeSerializer<WriterState> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }
}
