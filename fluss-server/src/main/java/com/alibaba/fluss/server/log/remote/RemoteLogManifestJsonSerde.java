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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;
import com.alibaba.fluss.utils.json.JsonSerializer;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/** The json serde for {@link RemoteLogManifest}. */
public class RemoteLogManifestJsonSerde
        implements JsonSerializer<RemoteLogManifest>, JsonDeserializer<RemoteLogManifest> {
    public static final RemoteLogManifestJsonSerde INSTANCE = new RemoteLogManifestJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String DATABASE_NAME_FIELD = "database";
    private static final String TABLE_NAME_FIELD = "table";
    private static final String PARTITION_NAME_FIELD = "partition_name";
    private static final String TABLE_ID_FIELD = "table_id";
    private static final String PARTITION_ID_FIELD = "partition_id";
    private static final String BUCKET_ID_FIELD = "bucket_id";
    private static final String MANIFEST_ENTRIES_FILED = "remote_log_segments";
    private static final String REMOTE_LOG_SEGMENT_ID_FIELD = "segment_id";
    private static final String START_OFFSET_FIELD = "start_offset";
    private static final String END_OFFSET_FIELD = "end_offset";
    private static final String MAX_TIMESTAMP_FIELD = "max_timestamp";
    private static final String SEGMENT_SIZE_IN_BYTES_FIELD = "size_in_bytes";
    private static final int SNAPSHOT_VERSION = 1;

    @Override
    public void serialize(RemoteLogManifest manifest, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, SNAPSHOT_VERSION);

        // serialize metadata
        PhysicalTablePath physicalTablePath = manifest.getPhysicalTablePath();
        generator.writeStringField(DATABASE_NAME_FIELD, physicalTablePath.getDatabaseName());
        generator.writeStringField(TABLE_NAME_FIELD, physicalTablePath.getTableName());
        String partitionName = physicalTablePath.getPartitionName();
        if (partitionName != null) {
            generator.writeStringField(PARTITION_NAME_FIELD, partitionName);
        }
        TableBucket tb = manifest.getTableBucket();
        generator.writeNumberField(TABLE_ID_FIELD, tb.getTableId());
        if (tb.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID_FIELD, tb.getPartitionId());
        }
        generator.writeNumberField(BUCKET_ID_FIELD, tb.getBucket());

        // serialize writer id entries.
        generator.writeArrayFieldStart(MANIFEST_ENTRIES_FILED);
        for (RemoteLogSegment remoteLogSegment : manifest.getRemoteLogSegmentList()) {
            generator.writeStartObject();
            generator.writeStringField(
                    REMOTE_LOG_SEGMENT_ID_FIELD, remoteLogSegment.remoteLogSegmentId().toString());
            generator.writeNumberField(START_OFFSET_FIELD, remoteLogSegment.remoteLogStartOffset());
            generator.writeNumberField(END_OFFSET_FIELD, remoteLogSegment.remoteLogEndOffset());
            generator.writeNumberField(MAX_TIMESTAMP_FIELD, remoteLogSegment.maxTimestamp());
            generator.writeNumberField(
                    SEGMENT_SIZE_IN_BYTES_FIELD, remoteLogSegment.segmentSizeInBytes());
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public RemoteLogManifest deserialize(JsonNode node) {
        String databaseName = node.get(DATABASE_NAME_FIELD).asText();
        String tableName = node.get(TABLE_NAME_FIELD).asText();
        JsonNode partitionNameNode = node.get(PARTITION_NAME_FIELD);
        PhysicalTablePath physicalTablePath;
        if (partitionNameNode == null) {
            physicalTablePath = PhysicalTablePath.of(databaseName, tableName, null);
        } else {
            physicalTablePath =
                    PhysicalTablePath.of(databaseName, tableName, partitionNameNode.asText());
        }

        long tableId = node.get(TABLE_ID_FIELD).asLong();
        JsonNode partitionIdNode = node.get(PARTITION_ID_FIELD);
        Long partitionId = partitionIdNode == null ? null : partitionIdNode.asLong();
        int bucketId = node.get(BUCKET_ID_FIELD).asInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        Iterator<JsonNode> entriesJson = node.get(MANIFEST_ENTRIES_FILED).elements();
        List<RemoteLogSegment> snapshotEntries = new ArrayList<>();
        while (entriesJson.hasNext()) {
            JsonNode entryJson = entriesJson.next();
            String remoteLogSegmentId = entryJson.get(REMOTE_LOG_SEGMENT_ID_FIELD).asText();
            long startOffset = entryJson.get(START_OFFSET_FIELD).asLong();
            long endOffset = entryJson.get(END_OFFSET_FIELD).asLong();
            long maxTimestamp = entryJson.get(MAX_TIMESTAMP_FIELD).asLong();
            int segmentSizeInBytes = entryJson.get(SEGMENT_SIZE_IN_BYTES_FIELD).asInt();
            snapshotEntries.add(
                    RemoteLogSegment.Builder.builder()
                            .physicalTablePath(physicalTablePath)
                            .tableBucket(tableBucket)
                            .remoteLogSegmentId(UUID.fromString(remoteLogSegmentId))
                            .remoteLogStartOffset(startOffset)
                            .remoteLogEndOffset(endOffset)
                            .maxTimestamp(maxTimestamp)
                            .segmentSizeInBytes(segmentSizeInBytes)
                            .build());
        }

        return new RemoteLogManifest(physicalTablePath, tableBucket, snapshotEntries);
    }

    public static RemoteLogManifest fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    public static byte[] toJson(RemoteLogManifest t) {
        return JsonSerdeUtils.writeValueAsBytes(t, INSTANCE);
    }
}
