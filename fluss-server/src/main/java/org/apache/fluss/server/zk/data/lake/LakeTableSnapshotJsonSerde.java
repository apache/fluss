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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Json serializer and deserializer for {@link LakeTableSnapshot}.
 *
 * <p>This serde supports two storage format versions:
 *
 * <ul>
 *   <li>Version 1 (legacy): Each bucket object contains full information including repeated
 *       partition names and partition_id in each bucket entry.
 *   <li>Version 2 (current): Compact format that uses different property keys for partitioned and
 *       non-partitioned tables to simplify deserialization:
 *       <ul>
 *         <li>Non-partition table uses "bucket_offsets": [100, 200, 300], where array index
 *             represents bucket id (0, 1, 2) and value represents log_end_offset. For buckets
 *             without end offset, -1 is written. Missing bucket ids in the sequence are also filled
 *             with -1.
 *         <li>Partition table uses "partition_bucket_offsets": {"1": [100, 200], "2": [300, 400]},
 *             where key is partition id, array index represents bucket id (0, 1) and value
 *             represents log_end_offset. For buckets without end offset, -1 is written. Missing
 *             bucket ids in the sequence are also filled with -1.
 *       </ul>
 *       During deserialization, values of -1 are ignored and not added to the bucket log end offset
 *       map.
 * </ul>
 */
public class LakeTableSnapshotJsonSerde
        implements JsonSerializer<LakeTableSnapshot>, JsonDeserializer<LakeTableSnapshot> {

    public static final LakeTableSnapshotJsonSerde INSTANCE = new LakeTableSnapshotJsonSerde();

    private static final long UNKNOWN_LOG_OFFSET = -1;

    private static final String VERSION_KEY = "version";

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKETS = "buckets";
    private static final String BUCKET_OFFSETS = "bucket_offsets";
    private static final String PARTITION_BUCKET_OFFSETS = "partition_bucket_offsets";
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

        Map<TableBucket, Long> bucketLogEndOffset = lakeTableSnapshot.getBucketLogEndOffset();

        if (!bucketLogEndOffset.isEmpty()) {
            // Get table_id from the first bucket (all buckets should have the same table_id)
            long tableId = bucketLogEndOffset.keySet().iterator().next().getTableId();
            generator.writeNumberField(TABLE_ID, tableId);

            // Group buckets by partition_id
            Map<Long, List<TableBucket>> partitionBuckets = new TreeMap<>();
            List<TableBucket> nonPartitionBuckets = new ArrayList<>();

            for (TableBucket tableBucket : bucketLogEndOffset.keySet()) {
                if (tableBucket.getPartitionId() != null) {
                    partitionBuckets
                            .computeIfAbsent(tableBucket.getPartitionId(), k -> new ArrayList<>())
                            .add(tableBucket);
                } else {
                    nonPartitionBuckets.add(tableBucket);
                }
            }
            if (!partitionBuckets.isEmpty()) {
                checkState(
                        nonPartitionBuckets.isEmpty(),
                        "nonPartitionBuckets must be empty when partitionBuckets is not empty");
                // Partition table: object format grouped by partition_id
                generator.writeObjectFieldStart(PARTITION_BUCKET_OFFSETS);
                for (Map.Entry<Long, List<TableBucket>> entry : partitionBuckets.entrySet()) {
                    Long partitionId = entry.getKey();
                    List<TableBucket> buckets = entry.getValue();
                    // Write array of log_end_offset values, array index represents bucket id
                    generator.writeArrayFieldStart(String.valueOf(partitionId));
                    serializeBucketLogEndOffset(bucketLogEndOffset, buckets, generator);
                    generator.writeEndArray();
                }
                generator.writeEndObject();
            } else {
                checkState(
                        !nonPartitionBuckets.isEmpty(),
                        "nonPartitionBuckets must be not empty when partitionBuckets is empty");
                // Non-partition table: array format, array index represents bucket id
                generator.writeArrayFieldStart(BUCKET_OFFSETS);
                serializeBucketLogEndOffset(bucketLogEndOffset, nonPartitionBuckets, generator);
                generator.writeEndArray();
            }
        }
        generator.writeEndObject();
    }

    private void serializeBucketLogEndOffset(
            Map<TableBucket, Long> bucketLogEndOffset,
            List<TableBucket> buckets,
            JsonGenerator generator)
            throws IOException {
        // sort by bucket id
        buckets.sort(Comparator.comparingInt(TableBucket::getBucket));
        int currentBucketId = 0;
        for (TableBucket tableBucket : buckets) {
            int bucketId = tableBucket.getBucket();
            // Fill null values for missing bucket ids
            while (currentBucketId < bucketId) {
                generator.writeNumber(UNKNOWN_LOG_OFFSET);
                currentBucketId++;
            }
            long logEndOffset = checkNotNull(bucketLogEndOffset.get(tableBucket));
            generator.writeNumber(logEndOffset);
            currentBucketId++;
        }
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
        return new LakeTableSnapshot(snapshotId, bucketLogEndOffset);
    }

    /**
     * Deserialize Version 2 format (uses different property keys for partitioned and
     * non-partitioned tables).
     */
    private LakeTableSnapshot deserializeVersion2(JsonNode node) {
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();

        // Check for bucket_offsets (non-partition table) or partition_bucket_offsets (partition
        // table)
        JsonNode bucketOffsetsNode = node.get(BUCKET_OFFSETS);
        JsonNode partitionBucketOffsetsNode = node.get(PARTITION_BUCKET_OFFSETS);
        if (bucketOffsetsNode != null || partitionBucketOffsetsNode != null) {
            if (bucketOffsetsNode != null && partitionBucketOffsetsNode != null) {
                throw new IllegalArgumentException(
                        "Both bucket_offsets and partition_bucket_offsets cannot be present at the same time");
            }
            JsonNode tableIdNode = node.get(TABLE_ID);
            // Non-partition table: array format, array index represents bucket id
            if (tableIdNode == null) {
                throw new IllegalArgumentException(
                        "table_id is required when bucket_offsets or partition_bucket_offsets is present in version 2 format");
            }
            long tableId = tableIdNode.asLong();

            if (bucketOffsetsNode != null) {

                Iterator<JsonNode> elements = bucketOffsetsNode.elements();
                int bucketId = 0;
                while (elements.hasNext()) {
                    JsonNode logEndOffsetNode = elements.next();
                    TableBucket tableBucket = new TableBucket(tableId, bucketId);
                    long logEndOffset = logEndOffsetNode.asLong();
                    if (logEndOffset != UNKNOWN_LOG_OFFSET) {
                        bucketLogEndOffset.put(tableBucket, logEndOffset);
                    }
                    bucketId++;
                }
            } else {
                Iterator<Map.Entry<String, JsonNode>> partitions =
                        partitionBucketOffsetsNode.fields();
                while (partitions.hasNext()) {
                    Map.Entry<String, JsonNode> entry = partitions.next();
                    String partitionKey = entry.getKey();
                    Long partitionId = Long.parseLong(partitionKey);
                    JsonNode logEndOffsetsArray = entry.getValue();
                    // Array index represents bucket id, value represents log_end_offset
                    Iterator<JsonNode> elements = logEndOffsetsArray.elements();
                    int bucketId = 0;
                    while (elements.hasNext()) {
                        JsonNode logEndOffsetNode = elements.next();
                        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                        long logEndOffset = logEndOffsetNode.asLong();
                        if (logEndOffset != UNKNOWN_LOG_OFFSET) {
                            bucketLogEndOffset.put(tableBucket, logEndOffset);
                        }
                        bucketId++;
                    }
                }
            }
        }
        return new LakeTableSnapshot(snapshotId, bucketLogEndOffset);
    }

    /** Serialize the {@link LakeTableSnapshot} to json bytes using current version. */
    public static byte[] toJson(LakeTableSnapshot lakeTableSnapshot) {
        return JsonSerdeUtils.writeValueAsBytes(lakeTableSnapshot, INSTANCE);
    }

    /** Serialize the {@link LakeTableSnapshot} to json bytes using Version 1 format. */
    @VisibleForTesting
    public static byte[] toJsonVersion1(LakeTableSnapshot lakeTableSnapshot, long tableId) {
        return JsonSerdeUtils.writeValueAsBytes(lakeTableSnapshot, new Version1Serializer(tableId));
    }

    /** Deserialize the json bytes to {@link LakeTableSnapshot}. */
    public static LakeTableSnapshot fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    /** Version 1 serializer for backward compatibility testing. */
    private static class Version1Serializer implements JsonSerializer<LakeTableSnapshot> {

        private final long tableId;

        private Version1Serializer(long tableId) {
            this.tableId = tableId;
        }

        @Override
        public void serialize(LakeTableSnapshot lakeTableSnapshot, JsonGenerator generator)
                throws IOException {
            generator.writeStartObject();
            generator.writeNumberField(VERSION_KEY, VERSION_1);
            generator.writeNumberField(SNAPSHOT_ID, lakeTableSnapshot.getSnapshotId());
            generator.writeNumberField(TABLE_ID, tableId);

            generator.writeArrayFieldStart(BUCKETS);
            for (TableBucket tableBucket : lakeTableSnapshot.getBucketLogEndOffset().keySet()) {
                generator.writeStartObject();
                generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());
                if (tableBucket.getPartitionId() != null) {
                    generator.writeNumberField(PARTITION_ID, tableBucket.getPartitionId());
                }
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
