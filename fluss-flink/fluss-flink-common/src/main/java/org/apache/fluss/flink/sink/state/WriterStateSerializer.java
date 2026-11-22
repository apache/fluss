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
 * <p>V1 serialization format:
 *
 * <ul>
 *   <li>int: version (1)
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
 * <p>V2 serialization format:
 *
 * <ul>
 *   <li>int: version (2)
 *   <li>long: table ID
 *   <li>int: number of bucket offsets
 *   <li>For each bucket offset:
 *       <ul>
 *         <li>boolean: has partition ID
 *         <li>long: partition ID (if has partition ID is true)
 *         <li>int: bucket ID
 *         <li>long: offset
 *       </ul>
 * </ul>
 */
public class WriterStateSerializer extends TypeSerializer<WriterState> {

    private static final long serialVersionUID = 1L;

    private static final int V1_VERSION = 1;
    private static final int V2_VERSION = 2;

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
        if (record.getStateFormat() == WriterState.StateFormat.V1_LEGACY) {
            serializeV1(record, target);
        } else if (record.getStateFormat() == WriterState.StateFormat.V2_COMPLETE) {
            serializeV2(record, target);
        } else {
            throw new IOException("Unsupported writer state format: " + record.getStateFormat());
        }
    }

    private void serializeV1(WriterState record, DataOutputView target) throws IOException {
        target.writeInt(V1_VERSION);
        Map<TableBucket, Long> bucketOffsets = record.getBucketOffsets();
        target.writeInt(bucketOffsets.size());
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            target.writeLong(bucket.getTableId());
            writeBucketOffset(target, bucket, entry.getValue());
        }
    }

    private void serializeV2(WriterState record, DataOutputView target) throws IOException {
        target.writeInt(V2_VERSION);
        target.writeLong(record.getTableId());
        Map<TableBucket, Long> bucketOffsets = record.getBucketOffsets();
        target.writeInt(bucketOffsets.size());
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            writeBucketOffset(target, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public WriterState deserialize(DataInputView source) throws IOException {
        int version = source.readInt();
        if (version == V1_VERSION) {
            return deserializeV1(source);
        } else if (version == V2_VERSION) {
            return deserializeV2(source);
        } else {
            throw new IOException("Unsupported writer state version: " + version);
        }
    }

    @Override
    public WriterState deserialize(WriterState reuse, DataInputView source) throws IOException {
        // WriterState is immutable, so we cannot reuse instances
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int version = source.readInt();
        WriterState state;
        if (version == V1_VERSION) {
            state = deserializeV1(source);
        } else if (version == V2_VERSION) {
            state = deserializeV2(source);
        } else {
            throw new IOException("Unsupported writer state version: " + version);
        }
        serialize(state, target);
    }

    private WriterState deserializeV1(DataInputView source) throws IOException {
        int size = source.readInt();
        Map<TableBucket, Long> bucketOffsets = readBucketOffsets(source, size, null);
        return new WriterState(bucketOffsets);
    }

    private WriterState deserializeV2(DataInputView source) throws IOException {
        long tableId = source.readLong();
        validateTableId(tableId);
        int size = source.readInt();
        Map<TableBucket, Long> bucketOffsets = readBucketOffsets(source, size, tableId);
        return WriterState.complete(tableId, bucketOffsets);
    }

    private static Map<TableBucket, Long> readBucketOffsets(
            DataInputView source, int size, Long tableIdFromStateHeader) throws IOException {
        Map<TableBucket, Long> bucketOffsets = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            long tableId =
                    tableIdFromStateHeader == null ? source.readLong() : tableIdFromStateHeader;
            boolean hasPartitionId = source.readBoolean();
            Long partitionId = hasPartitionId ? source.readLong() : null;
            int bucketId = source.readInt();
            long offset = source.readLong();
            TableBucket bucket = new TableBucket(tableId, partitionId, bucketId);
            bucketOffsets.put(bucket, offset);
        }
        return bucketOffsets;
    }

    private static void writeBucketOffset(DataOutputView target, TableBucket bucket, long offset)
            throws IOException {
        target.writeBoolean(bucket.getPartitionId() != null);
        if (bucket.getPartitionId() != null) {
            target.writeLong(bucket.getPartitionId());
        }
        target.writeInt(bucket.getBucket());
        target.writeLong(offset);
    }

    private static void validateTableId(long tableId) throws IOException {
        if (tableId < 0) {
            throw new IOException("Invalid complete writer state table ID: " + tableId);
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
