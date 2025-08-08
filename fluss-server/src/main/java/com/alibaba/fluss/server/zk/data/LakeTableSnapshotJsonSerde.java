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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Json serializer and deserializer for {@link LakeTableSnapshot}. */
public class LakeTableSnapshotJsonSerde
        implements JsonSerializer<LakeTableSnapshot>, JsonDeserializer<LakeTableSnapshot> {

    public static final LakeTableSnapshotJsonSerde INSTANCE = new LakeTableSnapshotJsonSerde();

    private static final String VERSION_KEY = "version";

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKETS = "buckets";
    private static final String BUCKET_ID = "bucket_id";
    private static final String LOG_START_OFFSET = "log_start_offset";
    private static final String LOG_END_OFFSET = "log_end_offset";
    private static final String PARTITION_NAME = "partition_name";

    private static final int VERSION = 1;

    @Override
    public void serialize(LakeTableSnapshot lakeTableSnapshot, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(SNAPSHOT_ID, lakeTableSnapshot.getSnapshotId());
        generator.writeNumberField(TABLE_ID, lakeTableSnapshot.getTableId());

        generator.writeArrayFieldStart(BUCKETS);
        for (TableBucket tableBucket : lakeTableSnapshot.getBucketLogEndOffset().keySet()) {
            generator.writeStartObject();

            if (tableBucket.getPartitionId() != null) {
                generator.writeNumberField(PARTITION_ID, tableBucket.getPartitionId());
                // have partition name
                String partitionName =
                        lakeTableSnapshot
                                .getPartitionNameIdByPartitionId()
                                .get(tableBucket.getPartitionId());
                if (partitionName != null) {
                    generator.writeStringField(PARTITION_NAME, partitionName);
                }
            }
            generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());

            if (lakeTableSnapshot.getLogStartOffset(tableBucket).isPresent()) {
                generator.writeNumberField(
                        LOG_START_OFFSET, lakeTableSnapshot.getLogStartOffset(tableBucket).get());
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

    @Override
    public LakeTableSnapshot deserialize(JsonNode node) {
        if (node.get(VERSION_KEY).asInt() != VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported version: " + node.get(VERSION_KEY).asInt());
        }
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        long tableId = node.get(TABLE_ID).asLong();
        Iterator<JsonNode> buckets = node.get(BUCKETS).elements();
        Map<TableBucket, Long> bucketLogStartOffset = new HashMap<>();
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        Map<Long, String> partitionNameIdByPartitionId = new HashMap<>();
        while (buckets.hasNext()) {
            JsonNode bucket = buckets.next();
            TableBucket tableBucket;
            Long partitionId =
                    bucket.get(PARTITION_ID) != null ? bucket.get(PARTITION_ID).asLong() : null;
            tableBucket = new TableBucket(tableId, partitionId, bucket.get(BUCKET_ID).asInt());

            if (bucket.get(LOG_START_OFFSET) != null) {
                bucketLogStartOffset.put(tableBucket, bucket.get(LOG_START_OFFSET).asLong());
            } else {
                bucketLogStartOffset.put(tableBucket, null);
            }

            if (bucket.get(LOG_END_OFFSET) != null) {
                bucketLogEndOffset.put(tableBucket, bucket.get(LOG_END_OFFSET).asLong());
            } else {
                bucketLogEndOffset.put(tableBucket, null);
            }

            if (partitionId != null && bucket.get(PARTITION_NAME) != null) {
                partitionNameIdByPartitionId.put(
                        tableBucket.getPartitionId(), bucket.get(PARTITION_NAME).asText());
            }
        }
        return new LakeTableSnapshot(
                snapshotId,
                tableId,
                bucketLogStartOffset,
                bucketLogEndOffset,
                partitionNameIdByPartitionId);
    }
}
