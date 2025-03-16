/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink.committable;

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Log start/end offset committable for a Fluss table. Used to commit to Fluss to notify data has
 * been tiered to Paimon.
 */
public class FlussLogOffsetCommittable {

    private final long tableId;

    // partition id -> partition name
    private final Map<Long, String> partitionNameById;
    // table bucket -> offset
    private final Map<TableBucket, Long> bucketLogEndOffsets;

    public FlussLogOffsetCommittable(
            long tableId,
            Map<Long, String> partitionNameById,
            Map<TableBucket, Long> bucketLogEndOffsets) {
        this.tableId = tableId;
        this.partitionNameById = partitionNameById;
        this.bucketLogEndOffsets = bucketLogEndOffsets;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<TableBucket, Long> getBucketEndOffsets() {
        return bucketLogEndOffsets;
    }

    public Map<Long, String> getPartitionNameById() {
        return partitionNameById;
    }

    public byte[] toBytes() throws IOException {
        byte[] partitionNameByIdBytes = toBytes(partitionNameById);
        byte[] logEndOffsetBytes = BucketOffsetsSerdeUtils.serialize(bucketLogEndOffsets);
        return ByteBuffer.allocate(
                        8 + 4 + partitionNameByIdBytes.length + 4 + logEndOffsetBytes.length)
                .putLong(tableId)
                .putInt(partitionNameByIdBytes.length)
                .put(partitionNameByIdBytes)
                .putInt(logEndOffsetBytes.length)
                .put(logEndOffsetBytes)
                .array();
    }

    private byte[] toBytes(Map<Long, String> partitionNameById) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out)) {
            view.writeInt(partitionNameById.size());
            for (Map.Entry<Long, String> partitionIdAndName : partitionNameById.entrySet()) {
                view.writeLong(partitionIdAndName.getKey());
                view.writeUTF(partitionIdAndName.getValue());
            }
            return out.toByteArray();
        }
    }

    public static FlussLogOffsetCommittable fromBytes(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long tableId = buffer.getLong();

        int partitionNameByIdBytesLen = buffer.getInt();
        byte[] partitionNameByIdBytes = new byte[partitionNameByIdBytesLen];
        buffer.get(partitionNameByIdBytes);
        Map<Long, String> partitionNameById = toPartitionNameById(partitionNameByIdBytes);

        int logEndOffsetLen = buffer.getInt();
        byte[] logEndOffsetBytes = new byte[logEndOffsetLen];
        buffer.get(logEndOffsetBytes);
        Map<TableBucket, Long> bucketLogEndOffsets =
                BucketOffsetsSerdeUtils.deserialize(tableId, logEndOffsetBytes);

        return new FlussLogOffsetCommittable(tableId, partitionNameById, bucketLogEndOffsets);
    }

    private static Map<Long, String> toPartitionNameById(byte[] bytes) throws IOException {
        try (ByteArrayInputStream input = new ByteArrayInputStream(bytes);
                DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(input)) {
            int partitionNameByIdSize = inputView.readInt();
            Map<Long, String> partitionNameById = new HashMap<>();
            for (int i = 0; i < partitionNameByIdSize; i++) {
                partitionNameById.put(inputView.readLong(), inputView.readUTF());
            }
            return partitionNameById;
        }
    }
}
