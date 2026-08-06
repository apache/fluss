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

package org.apache.fluss.remote;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
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
    private static final String MANIFEST_ENTRIES_FIELD = "remote_log_segments";
    private static final String GENERATION_FIELD = "generation";
    private static final String REMOTE_LOG_START_OFFSET_FIELD = "remote_log_start_offset";
    private static final String UNREFERENCED_SEGMENTS_FIELD = "unreferenced_segments";
    private static final String SEGMENT_FIELD = "segment";
    private static final String UNREFERENCED_AT_MS_FIELD = "unreferenced_at_ms";
    private static final String REASON_FIELD = "reason";
    private static final String REPLACEMENT_SEGMENT_ID_FIELD = "replacement_segment_id";
    private static final String REMOTE_LOG_SEGMENT_ID_FIELD = "segment_id";
    private static final String START_OFFSET_FIELD = "start_offset";
    private static final String END_OFFSET_FIELD = "end_offset";
    private static final String MAX_TIMESTAMP_FIELD = "max_timestamp";
    private static final String SEGMENT_SIZE_IN_BYTES_FIELD = "size_in_bytes";

    @Override
    public void serialize(RemoteLogManifest manifest, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, manifest.getVersion());
        if (manifest.getVersion() == RemoteLogManifest.VERSION_2) {
            generator.writeNumberField(GENERATION_FIELD, manifest.getGeneration());
        }

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

        if (manifest.getVersion() == RemoteLogManifest.VERSION_2
                && manifest.getPersistedRemoteLogStartOffset() != null) {
            generator.writeNumberField(
                    REMOTE_LOG_START_OFFSET_FIELD, manifest.getPersistedRemoteLogStartOffset());
        }

        generator.writeArrayFieldStart(MANIFEST_ENTRIES_FIELD);
        for (RemoteLogSegment remoteLogSegment : manifest.getRemoteLogSegmentList()) {
            serializeSegment(remoteLogSegment, generator);
        }
        generator.writeEndArray();

        if (manifest.getVersion() == RemoteLogManifest.VERSION_2) {
            generator.writeArrayFieldStart(UNREFERENCED_SEGMENTS_FIELD);
            for (UnreferencedRemoteLogSegment unreferencedSegment :
                    manifest.getUnreferencedRemoteLogSegments()) {
                generator.writeStartObject();
                generator.writeFieldName(SEGMENT_FIELD);
                serializeSegment(unreferencedSegment.remoteLogSegment(), generator);
                generator.writeNumberField(
                        UNREFERENCED_AT_MS_FIELD, unreferencedSegment.unreferencedAtMs());
                generator.writeStringField(REASON_FIELD, unreferencedSegment.reason().name());
                if (unreferencedSegment.replacementSegmentId() != null) {
                    generator.writeStringField(
                            REPLACEMENT_SEGMENT_ID_FIELD,
                            unreferencedSegment.replacementSegmentId().toString());
                }
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
    }

    @Override
    public RemoteLogManifest deserialize(JsonNode node) {
        int version = required(node, VERSION_KEY).asInt();
        if (version != RemoteLogManifest.VERSION_1 && version != RemoteLogManifest.VERSION_2) {
            throw new IllegalArgumentException(
                    "Unsupported remote log manifest version: " + version);
        }
        if (version == RemoteLogManifest.VERSION_1) {
            rejectField(node, GENERATION_FIELD, version);
            rejectField(node, REMOTE_LOG_START_OFFSET_FIELD, version);
            rejectField(node, UNREFERENCED_SEGMENTS_FIELD, version);
        }

        String databaseName = required(node, DATABASE_NAME_FIELD).asText();
        String tableName = required(node, TABLE_NAME_FIELD).asText();
        JsonNode partitionNameNode = node.get(PARTITION_NAME_FIELD);
        PhysicalTablePath physicalTablePath;
        if (partitionNameNode == null) {
            physicalTablePath = PhysicalTablePath.of(databaseName, tableName, null);
        } else {
            physicalTablePath =
                    PhysicalTablePath.of(databaseName, tableName, partitionNameNode.asText());
        }

        long tableId = required(node, TABLE_ID_FIELD).asLong();
        JsonNode partitionIdNode = node.get(PARTITION_ID_FIELD);
        Long partitionId = partitionIdNode == null ? null : partitionIdNode.asLong();
        int bucketId = required(node, BUCKET_ID_FIELD).asInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        JsonNode entriesNode = required(node, MANIFEST_ENTRIES_FIELD);
        if (!entriesNode.isArray()) {
            throw new IllegalArgumentException(MANIFEST_ENTRIES_FIELD + " must be an array");
        }
        List<RemoteLogSegment> activeSegments = new ArrayList<>();
        for (JsonNode entryJson : entriesNode) {
            activeSegments.add(parseSegment(entryJson, physicalTablePath, tableBucket));
        }

        if (version == RemoteLogManifest.VERSION_1) {
            return new RemoteLogManifest(physicalTablePath, tableBucket, activeSegments);
        }

        long generation = required(node, GENERATION_FIELD).asLong();
        JsonNode remoteStartNode = node.get(REMOTE_LOG_START_OFFSET_FIELD);
        Long remoteStartOffset = remoteStartNode == null ? null : remoteStartNode.asLong();
        if (!activeSegments.isEmpty() && remoteStartOffset == null) {
            throw new IllegalArgumentException(
                    "Non-empty V2 manifest is missing " + REMOTE_LOG_START_OFFSET_FIELD);
        }
        if (activeSegments.isEmpty() && remoteStartOffset != null) {
            throw new IllegalArgumentException(
                    "Empty V2 manifest must not contain " + REMOTE_LOG_START_OFFSET_FIELD);
        }

        JsonNode unreferencedNode = required(node, UNREFERENCED_SEGMENTS_FIELD);
        if (!unreferencedNode.isArray()) {
            throw new IllegalArgumentException(UNREFERENCED_SEGMENTS_FIELD + " must be an array");
        }
        List<UnreferencedRemoteLogSegment> unreferencedSegments = new ArrayList<>();
        for (JsonNode entryJson : unreferencedNode) {
            RemoteLogSegment segment =
                    parseSegment(
                            required(entryJson, SEGMENT_FIELD), physicalTablePath, tableBucket);
            long unreferencedAtMs = required(entryJson, UNREFERENCED_AT_MS_FIELD).asLong();
            UnreferencedRemoteLogSegment.Reason reason;
            try {
                reason =
                        UnreferencedRemoteLogSegment.Reason.valueOf(
                                required(entryJson, REASON_FIELD).asText());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported unreferenced segment reason", e);
            }
            JsonNode replacementIdNode = entryJson.get(REPLACEMENT_SEGMENT_ID_FIELD);
            UUID replacementSegmentId =
                    replacementIdNode == null ? null : UUID.fromString(replacementIdNode.asText());
            unreferencedSegments.add(
                    new UnreferencedRemoteLogSegment(
                            segment, unreferencedAtMs, reason, replacementSegmentId));
        }
        return RemoteLogManifest.createV2(
                generation,
                physicalTablePath,
                tableBucket,
                activeSegments,
                remoteStartOffset,
                unreferencedSegments);
    }

    private static void serializeSegment(RemoteLogSegment segment, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeStringField(
                REMOTE_LOG_SEGMENT_ID_FIELD, segment.remoteLogSegmentId().toString());
        generator.writeNumberField(START_OFFSET_FIELD, segment.remoteLogStartOffset());
        generator.writeNumberField(END_OFFSET_FIELD, segment.remoteLogEndOffset());
        generator.writeNumberField(MAX_TIMESTAMP_FIELD, segment.maxTimestamp());
        generator.writeNumberField(SEGMENT_SIZE_IN_BYTES_FIELD, segment.segmentSizeInBytes());
        generator.writeEndObject();
    }

    private static RemoteLogSegment parseSegment(
            JsonNode node, PhysicalTablePath physicalTablePath, TableBucket tableBucket) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(physicalTablePath)
                .tableBucket(tableBucket)
                .remoteLogSegmentId(
                        UUID.fromString(required(node, REMOTE_LOG_SEGMENT_ID_FIELD).asText()))
                .remoteLogStartOffset(required(node, START_OFFSET_FIELD).asLong())
                .remoteLogEndOffset(required(node, END_OFFSET_FIELD).asLong())
                .maxTimestamp(required(node, MAX_TIMESTAMP_FIELD).asLong())
                .segmentSizeInBytes(required(node, SEGMENT_SIZE_IN_BYTES_FIELD).asInt())
                .build();
    }

    private static JsonNode required(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return field;
    }

    private static void rejectField(JsonNode node, String fieldName, int version) {
        if (node.get(fieldName) != null) {
            throw new IllegalArgumentException(
                    "Field " + fieldName + " is not valid for manifest version " + version);
        }
    }

    public static RemoteLogManifest fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    public static byte[] toJson(RemoteLogManifest t) {
        return JsonSerdeUtils.writeValueAsBytes(t, INSTANCE);
    }
}
