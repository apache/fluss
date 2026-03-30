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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link TypeSerializer} for {@link WriterState}.
 *
 * <p>This serializer extends {@link TypeSerializer} for use with Flink's {@code
 * ListStateDescriptor} in Union List State.
 *
 * <p>Serialization format:
 *
 * <ul>
 *   <li>int: number of bucket offsets
 *   <li>For each bucket offset:
 *       <ul>
 *         <li>long: table ID
 *         <li>boolean: has partition ID
 *         <li>long: partition ID (if has partition ID is true)
 *         <li>int: bucket ID
 *         <li>long: offset
 *       </ul>
 * </ul>
 *
 * <p>This serializer uses a stable binary format that supports both partitioned and non-partitioned
 * tables via {@link TableBucket}.
 */
public class WriterStateSerializer extends TypeSerializer<WriterState> {

    private static final long serialVersionUID = 1L;

    /** The current version of the serialization format. */
    private static final int CURRENT_VERSION = 1;
    // -------------------------------------------------------------------------
    //  TypeSerializer methods
    // -------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        // WriterState is immutable - its bucketOffsets map is unmodifiable
        return true;
    }

    @Override
    public TypeSerializer<WriterState> duplicate() {
        // This serializer is stateless, so it can be shared
        return this;
    }

    @Override
    public WriterState createInstance() {
        return WriterState.empty();
    }

    @Override
    public WriterState copy(WriterState from) {
        // WriterState is immutable, so we can return the same instance
        return from;
    }

    @Override
    public WriterState copy(WriterState from, WriterState reuse) {
        // WriterState is immutable, so we can return the same instance
        return from;
    }

    @Override
    public int getLength() {
        // Variable length due to dynamic number of bucket offsets
        return -1;
    }

    @Override
    public void serialize(WriterState record, DataOutputView target) throws IOException {
        target.writeInt(CURRENT_VERSION);
        Map<TableBucket, Long> bucketOffsets = record.getBucketOffsets();
        target.writeInt(bucketOffsets.size());
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            target.writeLong(bucket.getTableId());
            target.writeBoolean(bucket.getPartitionId() != null);
            if (bucket.getPartitionId() != null) {
                target.writeLong(bucket.getPartitionId());
            }
            target.writeInt(bucket.getBucket());
            target.writeLong(entry.getValue());
        }
    }

    @Override
    public WriterState deserialize(DataInputView source) throws IOException {
        int version = source.readInt();
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported version: " + version + ". Expected version: " + CURRENT_VERSION);
        }
        int size = source.readInt();
        Map<TableBucket, Long> bucketOffsets = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            long tableId = source.readLong();
            boolean hasPartitionId = source.readBoolean();
            Long partitionId = hasPartitionId ? source.readLong() : null;
            int bucketId = source.readInt();
            long offset = source.readLong();
            bucketOffsets.put(new TableBucket(tableId, partitionId, bucketId), offset);
        }
        return new WriterState(bucketOffsets);
    }

    @Override
    public WriterState deserialize(WriterState reuse, DataInputView source) throws IOException {
        // WriterState is immutable, so we cannot reuse instances
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int version = source.readInt();
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported version: " + version + ". Expected version: " + CURRENT_VERSION);
        }
        target.writeInt(version);
        // Copy bucket offsets size
        int size = source.readInt();
        target.writeInt(size);
        // Copy each bucket offset entry
        for (int i = 0; i < size; i++) {
            // Copy table ID
            target.writeLong(source.readLong());
            // Copy has partition ID flag and partition ID if present
            boolean hasPartitionId = source.readBoolean();
            target.writeBoolean(hasPartitionId);
            if (hasPartitionId) {
                target.writeLong(source.readLong());
            }
            // Copy bucket ID
            target.writeInt(source.readInt());
            // Copy offset
            target.writeLong(source.readLong());
        }
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

    /** Serializer configuration snapshot for compatibility and format evolution. */
    public static final class WriterStateSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<WriterState> {

        public WriterStateSerializerSnapshot() {
            super(WriterStateSerializer::new);
        }
    }
}
