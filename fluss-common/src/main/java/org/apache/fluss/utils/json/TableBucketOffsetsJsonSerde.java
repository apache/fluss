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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Json serializer and deserializer for {@link TableBucketOffsets}.
 *
 * <p>This serde supports the following JSON format:
 *
 * <ul>
 *   <li>Non-partition table uses "bucket_offsets": [1234, 5678, 1992], where array index represents
 *       bucket id (0, 1, 2) and value represents the offset. Missing bucket ids in the sequence are
 *       filled with -1.
 *   <li>Partition table uses "partition_offsets": [{"partition_id": 3001, "bucket_offsets": [1234,
 *       5678, 1992]}, ...], where each element contains a partition_id and a bucket_offsets array.
 *       The array index in bucket_offsets represents bucket id and value represents the offset.
 *       Missing bucket ids in the sequence are filled with -1.
 * </ul>
 *
 * <p>During deserialization, values of -1 are ignored and not added to the offsets map.
 *
 * <p>The serialized format includes:
 *
 * <ul>
 *   <li>"version": the format version
 *   <li>"table_id": the table ID that all buckets belong to
 *   <li>"bucket_offsets": array of offsets for non-partitioned table buckets (optional)
 *   <li>"partition_offsets": array of partition offset objects for partitioned table buckets
 *       (optional)
 * </ul>
 */
public class TableBucketOffsetsJsonSerde
        implements JsonSerializer<TableBucketOffsets>, JsonDeserializer<TableBucketOffsets> {

    public static final TableBucketOffsetsJsonSerde INSTANCE = new TableBucketOffsetsJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String TABLE_ID_KEY = "table_id";
    private static final String BUCKET_OFFSETS_KEY = "bucket_offsets";
    private static final String PARTITION_OFFSETS_KEY = "partition_offsets";
    private static final String PARTITION_ID_KEY = "partition_id";

    private static final int VERSION = 1;
    private static final long UNKNOWN_OFFSET = -1;

    /**
     * Serializes a {@link TableBucketOffsets} object to JSON format.
     *
     * <p>This method writes the table bucket offsets in the JSON format. It groups buckets by
     * partition_id and writes non-partitioned buckets to "bucket_offsets" array and partitioned
     * buckets to "partition_offsets" array. The array index represents the bucket id.
     *
     * <p>This method validates that all buckets in the offsets map have the same table_id as the
     * {@link TableBucketOffsets#getTableId()}.
     *
     * @param tableBucketOffsets the {@link TableBucketOffsets} object to serialize
     * @param generator the JSON generator to write to
     * @throws IOException if an I/O error occurs during serialization
     * @throws IllegalStateException if buckets have inconsistent table IDs
     */
    @Override
    public void serialize(TableBucketOffsets tableBucketOffsets, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        long expectedTableId = tableBucketOffsets.getTableId();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(TABLE_ID_KEY, expectedTableId);

        Map<TableBucket, Long> offsets = tableBucketOffsets.getOffsets();
        if (!offsets.isEmpty()) {
            // Group buckets by partition_id and validate table_id consistency
            Map<Long, List<TableBucket>> partitionBuckets = new TreeMap<>();
            List<TableBucket> nonPartitionBuckets = new ArrayList<>();

            for (TableBucket tableBucket : offsets.keySet()) {
                // Check that all buckets have the same table_id
                checkState(
                        tableBucket.getTableId() == expectedTableId,
                        "All buckets must have the same table_id. Expected: %d, but found: %d in bucket: %s",
                        expectedTableId,
                        tableBucket.getTableId(),
                        tableBucket);

                if (tableBucket.getPartitionId() != null) {
                    partitionBuckets
                            .computeIfAbsent(tableBucket.getPartitionId(), k -> new ArrayList<>())
                            .add(tableBucket);
                } else {
                    nonPartitionBuckets.add(tableBucket);
                }
            }

            // Serialize non-partitioned table bucket offsets
            if (!nonPartitionBuckets.isEmpty()) {
                checkState(
                        partitionBuckets.isEmpty(),
                        "partitionBuckets must be empty when nonPartitionBuckets is not empty");
                generator.writeArrayFieldStart(BUCKET_OFFSETS_KEY);
                serializeBucketLogEndOffset(offsets, nonPartitionBuckets, generator);
                generator.writeEndArray();
            } else {
                // nonPartitionBuckets is empty, partitionBuckets is must not empty
                checkState(
                        !partitionBuckets.isEmpty(),
                        "partitionBuckets must be not empty when nonPartitionBuckets is empty");
                generator.writeArrayFieldStart(PARTITION_OFFSETS_KEY);
                for (Map.Entry<Long, List<TableBucket>> entry : partitionBuckets.entrySet()) {
                    Long partitionId = entry.getKey();
                    List<TableBucket> buckets = entry.getValue();
                    generator.writeStartObject();
                    generator.writeNumberField(PARTITION_ID_KEY, partitionId);
                    generator.writeArrayFieldStart(BUCKET_OFFSETS_KEY);
                    serializeBucketLogEndOffset(offsets, buckets, generator);
                    generator.writeEndArray();
                    generator.writeEndObject();
                }
                generator.writeEndArray();
            }
        }

        generator.writeEndObject();
    }

    /**
     * Deserializes a JSON node to a {@link TableBucketOffsets} object.
     *
     * <p>This method reads the JSON format and reconstructs the table bucket offsets map. The array
     * index in "bucket_offsets" represents the bucket id, and the value represents the offset.
     *
     * @param node the JSON node to deserialize
     * @return the deserialized {@link TableBucketOffsets} object
     * @throws IllegalArgumentException if the version is not supported
     */
    @Override
    public TableBucketOffsets deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }

        long tableId = node.get(TABLE_ID_KEY).asLong();
        Map<TableBucket, Long> offsets = new HashMap<>();

        // Deserialize non-partitioned table bucket offsets
        JsonNode bucketOffsetsNode = node.get(BUCKET_OFFSETS_KEY);
        JsonNode partitionBucketOffsetsNode = node.get(PARTITION_OFFSETS_KEY);
        if (bucketOffsetsNode != null || partitionBucketOffsetsNode != null) {
            if (bucketOffsetsNode != null && partitionBucketOffsetsNode != null) {
                throw new IllegalArgumentException(
                        "Both bucket_offsets and partition_bucket_offsets cannot be present at the same time");
            }

            if (bucketOffsetsNode != null) {
                int bucketId = 0;
                for (JsonNode bucketOffsetNode : bucketOffsetsNode) {
                    long offset = bucketOffsetNode.asLong();
                    // Ignore unknown offsets (filled for missing bucket ids)
                    if (offset != UNKNOWN_OFFSET) {
                        TableBucket tableBucket = new TableBucket(tableId, bucketId);
                        offsets.put(tableBucket, offset);
                    }
                    bucketId++;
                }
            } else {
                for (JsonNode partitionOffsetNode : partitionBucketOffsetsNode) {
                    long partitionId = partitionOffsetNode.get(PARTITION_ID_KEY).asLong();
                    JsonNode bucketOffsetsArray = partitionOffsetNode.get(BUCKET_OFFSETS_KEY);
                    int bucketId = 0;
                    for (JsonNode bucketOffsetNode : bucketOffsetsArray) {
                        long offset = bucketOffsetNode.asLong();
                        // Ignore unknown offsets (filled for missing bucket ids)
                        if (offset != UNKNOWN_OFFSET) {
                            TableBucket tableBucket =
                                    new TableBucket(tableId, partitionId, bucketId);
                            offsets.put(tableBucket, offset);
                        }
                        bucketId++;
                    }
                }
            }
        }

        return new TableBucketOffsets(tableId, offsets);
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
                generator.writeNumber(UNKNOWN_OFFSET);
                currentBucketId++;
            }
            long logEndOffset = checkNotNull(bucketLogEndOffset.get(tableBucket));
            generator.writeNumber(logEndOffset);
            currentBucketId++;
        }
    }
}
