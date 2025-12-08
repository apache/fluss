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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Json serializer and deserializer for {@link LakeTableSnapshot}.
 *
 * <p>This serde supports two storage format versions:
 *
 * <ul>
 *   <li>Version 1 (legacy): Each bucket object contains full information including repeated
 *       partition names and partition_id in each bucket entry.
 *   <li>Version 2 (current): Compact format that optimizes layout to avoid duplication:
 *       <ul>
 *         <li>Extracts partition names to a top-level "partition_names" map
 *         <li>Groups buckets by partition_id in "buckets" to avoid repeating partition_id in each
 *             bucket
 *         <li>Non-partition table uses array format: "buckets": [...]
 *         <li>Partition table uses object format: "buckets": {"1": [...], "2": [...]}, "1", "2" is
 *             for the partition id
 *       </ul>
 *       Field names remain the same as Version 1, only the layout is optimized.
 * </ul>
 */
public class LakeTableSnapshotJsonSerde
        implements JsonSerializer<LakeTableSnapshot>, JsonDeserializer<LakeTableSnapshot> {

    public static final LakeTableSnapshotJsonSerde INSTANCE = new LakeTableSnapshotJsonSerde();

    private static final String VERSION_KEY = "version";

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKETS = "buckets";
    private static final String BUCKET_ID = "bucket_id";
    private static final String LOG_END_OFFSET = "log_end_offset";

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int CURRENT_VERSION = VERSION_2;

    @Override
    public void serialize(LakeTableSnapshot lakeTableSnapshot, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, CURRENT_VERSION);
        generator.writeNumberField(SNAPSHOT_ID, lakeTableSnapshot.getSnapshotId());
        generator.writeNumberField(TABLE_ID, lakeTableSnapshot.getTableId());

        // Group buckets by partition_id to avoid repeating partition_id in each bucket
        Map<Long, List<TableBucket>> partitionBuckets = new HashMap<>();
        List<TableBucket> nonPartitionBuckets = new ArrayList<>();

        for (TableBucket tableBucket : lakeTableSnapshot.getBucketLogEndOffset().keySet()) {
            if (tableBucket.getPartitionId() != null) {
                partitionBuckets
                        .computeIfAbsent(tableBucket.getPartitionId(), k -> new ArrayList<>())
                        .add(tableBucket);
            } else {
                nonPartitionBuckets.add(tableBucket);
            }
        }

        // Serialize buckets: use array for non-partition buckets, object for partition buckets
        if (!nonPartitionBuckets.isEmpty() || !partitionBuckets.isEmpty()) {
            if (!partitionBuckets.isEmpty()) {
                generator.writeObjectFieldStart(BUCKETS);
                for (Map.Entry<Long, java.util.List<TableBucket>> entry :
                        partitionBuckets.entrySet()) {
                    // Partition table:  grouped by partition_id, first write partition_id
                    generator.writeArrayFieldStart(String.valueOf(entry.getKey()));
                    for (TableBucket tableBucket : entry.getValue()) {
                        // write bucket
                        writeBucketObject(generator, lakeTableSnapshot, tableBucket);
                    }
                    generator.writeEndArray();
                }
                generator.writeEndObject();
            } else {
                // Non-partition table: use array format directly
                generator.writeArrayFieldStart(BUCKETS);
                for (TableBucket tableBucket : nonPartitionBuckets) {
                    writeBucketObject(generator, lakeTableSnapshot, tableBucket);
                }
                generator.writeEndArray();
            }
        }

        generator.writeEndObject();
    }

    /** Helper method to write a bucket object. */
    private void writeBucketObject(
            JsonGenerator generator, LakeTableSnapshot lakeTableSnapshot, TableBucket tableBucket)
            throws IOException {
        generator.writeStartObject();

        generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());

        if (lakeTableSnapshot.getLogEndOffset(tableBucket).isPresent()) {
            generator.writeNumberField(
                    LOG_END_OFFSET, lakeTableSnapshot.getLogEndOffset(tableBucket).get());
        }

        generator.writeEndObject();
    }

    @Override
    public LakeTableSnapshot deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        if (version == VERSION_1) {
            return deserializeVersion1(node);
        } else if (version == VERSION_2) {
            return deserializeVersion2(node);
        } else {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
    }

    /** Deserialize Version 1 format (legacy). */
    private LakeTableSnapshot deserializeVersion1(JsonNode node) {
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        long tableId = node.get(TABLE_ID).asLong();
        Iterator<JsonNode> buckets = node.get(BUCKETS).elements();
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        while (buckets.hasNext()) {
            JsonNode bucket = buckets.next();
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
        return new LakeTableSnapshot(snapshotId, tableId, bucketLogEndOffset);
    }

    /** Deserialize Version 2 format (compact layout). */
    private LakeTableSnapshot deserializeVersion2(JsonNode node) {
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        long tableId = node.get(TABLE_ID).asLong();

        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();

        // Deserialize buckets: array format for non-partition table, object format for partition
        // table
        JsonNode bucketsNode = node.get(BUCKETS);
        if (bucketsNode != null) {
            if (bucketsNode.isArray()) {
                // Non-partition table: array format
                Iterator<JsonNode> buckets = bucketsNode.elements();
                while (buckets.hasNext()) {
                    JsonNode bucket = buckets.next();
                    TableBucket tableBucket =
                            new TableBucket(tableId, bucket.get(BUCKET_ID).asInt());
                    readBucketFields(bucket, tableBucket, bucketLogEndOffset);
                }
            } else {
                // Partition table: object format grouped by partition_id
                Iterator<Map.Entry<String, JsonNode>> partitions = bucketsNode.fields();
                while (partitions.hasNext()) {
                    Map.Entry<String, JsonNode> entry = partitions.next();
                    String partitionKey = entry.getKey();
                    Long actualPartitionId = Long.parseLong(partitionKey);
                    Iterator<JsonNode> buckets = entry.getValue().elements();
                    while (buckets.hasNext()) {
                        JsonNode bucket = buckets.next();
                        TableBucket tableBucket =
                                new TableBucket(
                                        tableId, actualPartitionId, bucket.get(BUCKET_ID).asInt());
                        readBucketFields(bucket, tableBucket, bucketLogEndOffset);
                    }
                }
            }
        }
        return new LakeTableSnapshot(snapshotId, tableId, bucketLogEndOffset);
    }

    /** Helper method to read bucket fields from JSON. */
    private void readBucketFields(
            JsonNode bucket, TableBucket tableBucket, Map<TableBucket, Long> bucketLogEndOffset) {
        if (bucket.has(LOG_END_OFFSET) && bucket.get(LOG_END_OFFSET) != null) {
            bucketLogEndOffset.put(tableBucket, bucket.get(LOG_END_OFFSET).asLong());
        } else {
            bucketLogEndOffset.put(tableBucket, null);
        }
    }

    /** Serialize the {@link LakeTableSnapshot} to json bytes using current version. */
    public static byte[] toJson(LakeTableSnapshot lakeTableSnapshot) {
        return JsonSerdeUtils.writeValueAsBytes(lakeTableSnapshot, INSTANCE);
    }

    /** Serialize the {@link LakeTableSnapshot} to json bytes using Version 1 format. */
    @VisibleForTesting
    public static byte[] toJsonVersion1(LakeTableSnapshot lakeTableSnapshot) {
        return JsonSerdeUtils.writeValueAsBytes(lakeTableSnapshot, new Version1Serializer());
    }

    /** Deserialize the json bytes to {@link LakeTableSnapshot}. */
    public static LakeTableSnapshot fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    /** Version 1 serializer for backward compatibility testing. */
    private static class Version1Serializer implements JsonSerializer<LakeTableSnapshot> {
        @Override
        public void serialize(LakeTableSnapshot lakeTableSnapshot, JsonGenerator generator)
                throws IOException {
            generator.writeStartObject();
            generator.writeNumberField(VERSION_KEY, VERSION_1);
            generator.writeNumberField(SNAPSHOT_ID, lakeTableSnapshot.getSnapshotId());
            generator.writeNumberField(TABLE_ID, lakeTableSnapshot.getTableId());

            generator.writeArrayFieldStart(BUCKETS);
            for (TableBucket tableBucket : lakeTableSnapshot.getBucketLogEndOffset().keySet()) {
                generator.writeStartObject();
                generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());
                if (lakeTableSnapshot.getLogEndOffset(tableBucket).isPresent()) {
                    generator.writeNumberField(
                            LOG_END_OFFSET, lakeTableSnapshot.getLogEndOffset(tableBucket).get());
                }

                generator.writeEndObject();
            }
            generator.writeEndArray();

            generator.writeEndObject();
        }
    }
}
