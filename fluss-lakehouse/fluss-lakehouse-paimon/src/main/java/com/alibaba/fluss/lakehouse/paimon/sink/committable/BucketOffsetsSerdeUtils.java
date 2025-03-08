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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Utils for serialize/deserialize the mapping from table bucket to a log offset of a table. */
class BucketOffsetsSerdeUtils {

    static byte[] serialize(Map<TableBucket, Long> logOffsets) {
        if (logOffsets.isEmpty()) {
            return ByteBuffer.allocate(4).putInt(0).array();
        }

        TableBucket firstTableBucket = logOffsets.keySet().iterator().next();
        boolean isPartitioned = firstTableBucket.getPartitionId() != null;
        ByteBuffer buffer =
                ByteBuffer.allocate(
                        // 4 is for the size of logOffset
                        // 1 is table is partitioned or not
                        // 12 is size of bucket id and log offset,
                        // 8 is size of partition id if partitioned
                        4 + 1 + (12 + (isPartitioned ? 8 : 0)) * logOffsets.size());

        buffer.putInt(logOffsets.size());
        buffer.put((byte) (isPartitioned ? 1 : 0));
        for (Map.Entry<TableBucket, Long> offsetEntry : logOffsets.entrySet()) {
            if (isPartitioned) {
                Long partitionId = offsetEntry.getKey().getPartitionId();
                checkNotNull(partitionId, "partitionId must be not null");
                buffer.putLong(offsetEntry.getKey().getPartitionId());
            }
            buffer.putInt(offsetEntry.getKey().getBucket());
            buffer.putLong(offsetEntry.getValue());
        }
        return buffer.array();
    }

    static Map<TableBucket, Long> deserialize(long tableId, byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int size = buffer.getInt();
        if (size == 0) {
            return Collections.emptyMap();
        }

        Map<TableBucket, Long> bucketOffsets = new HashMap<>(size);
        boolean isPartitioned = buffer.get() == 1;

        for (int i = 0; i < size; i++) {
            Long partitionId = null;
            if (isPartitioned) {
                partitionId = buffer.getLong();
            }
            int bucket = buffer.getInt();
            long offset = buffer.getLong();
            bucketOffsets.put(new TableBucket(tableId, partitionId, bucket), offset);
        }
        return bucketOffsets;
    }
}
