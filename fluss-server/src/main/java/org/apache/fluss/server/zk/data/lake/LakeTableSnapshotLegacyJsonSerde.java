/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Json serializer and deserializer for {@link LakeTableSnapshot}.
 *
 * <p><b>Note:</b> This class is primarily used for backward compatibility to deserialize legacy
 * version 1 lake snapshot data stored in ZooKeeper. The current storage format (version 2) stores
 * only file paths in ZooKeeper, with actual snapshot data stored in remote files. This serde is
 * used by {@link LakeTableJsonSerde} to handle version 1 format deserialization.
 *
 * <p>The version 1 format stores the full {@link LakeTableSnapshot} data directly in the ZooKeeper
 * node, which includes:
 *
 * <ul>
 *   <li>version: 1
 *   <li>snapshot_id: the snapshot ID
 *   <li>table_id: the table ID (derived from the first bucket)
 *   <li>buckets: array of bucket objects, each containing bucket_id, optional partition_id, and
 *       log_end_offset
 * </ul>
 *
 * @see LakeTableJsonSerde for the current format (version 2) that uses this serde for legacy
 *     compatibility
 */
public class LakeTableSnapshotLegacyJsonSerde
        implements JsonSerializer<LakeTableSnapshot>, JsonDeserializer<LakeTableSnapshot> {

    public static final LakeTableSnapshotLegacyJsonSerde INSTANCE =
            new LakeTableSnapshotLegacyJsonSerde();

    private static final String VERSION_KEY = "version";

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKETS = "buckets";
    private static final String BUCKET_ID = "bucket_id";
    private static final String LOG_END_OFFSET = "log_end_offset";

    private static final int VERSION_1 = 1;
    private static final int CURRENT_VERSION = VERSION_1;

    @Override
    public void serialize(LakeTableSnapshot lakeTableSnapshot, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION_1);
        generator.writeNumberField(SNAPSHOT_ID, lakeTableSnapshot.getSnapshotId());

        Map<TableBucket, Long> bucketLogEndOffset = lakeTableSnapshot.getBucketLogEndOffset();
        // Get table id from the first table bucket, all buckets should have the same table id
        if (!bucketLogEndOffset.isEmpty()) {
            TableBucket firstBucket = bucketLogEndOffset.keySet().iterator().next();
            long tableId = firstBucket.getTableId();
            generator.writeNumberField(TABLE_ID, tableId);
        }

        generator.writeArrayFieldStart(BUCKETS);
        for (Map.Entry<TableBucket, Long> tableBucketOffsetEntry : bucketLogEndOffset.entrySet()) {
            generator.writeStartObject();
            TableBucket tableBucket = tableBucketOffsetEntry.getKey();
            if (tableBucket.getPartitionId() != null) {
                generator.writeNumberField(PARTITION_ID, tableBucket.getPartitionId());
            }
            generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());
            generator.writeNumberField(LOG_END_OFFSET, tableBucketOffsetEntry.getValue());
            generator.writeEndObject();
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }

    @Override
    public LakeTableSnapshot deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported version: " + node.get(VERSION_KEY).asInt());
        }
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        long tableId = node.get(TABLE_ID).asLong();
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        for (JsonNode bucket : node.get(BUCKETS)) {
            TableBucket tableBucket;
            Long partitionId =
                    bucket.get(PARTITION_ID) != null ? bucket.get(PARTITION_ID).asLong() : null;
            tableBucket = new TableBucket(tableId, partitionId, bucket.get(BUCKET_ID).asInt());
            if (bucket.get(LOG_END_OFFSET) != null) {
                bucketLogEndOffset.put(tableBucket, bucket.get(LOG_END_OFFSET).asLong());
            } else {
                bucketLogEndOffset.put(tableBucket, null);
            }
        }
        return new LakeTableSnapshot(snapshotId, bucketLogEndOffset);
    }
}
