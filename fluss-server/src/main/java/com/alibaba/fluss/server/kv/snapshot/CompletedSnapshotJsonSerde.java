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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Json serializer and deserializer for {@link CompletedSnapshot}. */
public class CompletedSnapshotJsonSerde
        implements JsonSerializer<CompletedSnapshot>, JsonDeserializer<CompletedSnapshot> {

    public static final CompletedSnapshotJsonSerde INSTANCE = new CompletedSnapshotJsonSerde();

    private static final int VERSION = 1;
    private static final String VERSION_KEY = "version";
    // for table bucket the snapshot belongs to
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKET_ID = "bucket_id";

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String SNAPSHOT_LOCATION = "snapshot_location";

    // for kv snapshot's files
    private static final String KV_SNAPSHOT_HANDLE = "kv_snapshot_handle";
    private static final String KV_SHARED_FILES_HANDLE = "shared_file_handles";
    private static final String KV_PRIVATE_FILES_HANDLE = "private_file_handles";
    private static final String KV_FILE_HANDLE = "kv_file_handle";
    private static final String KV_FILE_PATH = "path";
    private static final String KV_FILE_SIZE = "size";
    private static final String KV_FILE_LOCAL_PATH = "local_path";
    private static final String SNAPSHOT_INCREMENTAL_SIZE = "snapshot_incremental_size";

    // -- for the next log offset when the snapshot is triggered;
    private static final String LOG_OFFSET = "log_offset";

    @Override
    public void serialize(CompletedSnapshot completedSnapshot, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);

        // serialize table bucket
        TableBucket tableBucket = completedSnapshot.getTableBucket();
        generator.writeNumberField(TABLE_ID, tableBucket.getTableId());
        if (tableBucket.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID, tableBucket.getPartitionId());
        }
        generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());

        // serialize snapshot id
        generator.writeNumberField(SNAPSHOT_ID, completedSnapshot.getSnapshotID());

        // serialize snapshot location
        generator.writeStringField(
                SNAPSHOT_LOCATION, completedSnapshot.getSnapshotLocation().toString());

        // serialize kv snapshot handle
        generator.writeObjectFieldStart(KV_SNAPSHOT_HANDLE);
        KvSnapshotHandle kvSnapshotHandle = completedSnapshot.getKvSnapshotHandle();

        // serialize shared file handles
        generator.writeArrayFieldStart(KV_SHARED_FILES_HANDLE);
        serializeKvFileHandles(generator, kvSnapshotHandle.getSharedKvFileHandles());
        generator.writeEndArray();

        // serialize private file handles
        generator.writeArrayFieldStart(KV_PRIVATE_FILES_HANDLE);
        serializeKvFileHandles(generator, kvSnapshotHandle.getPrivateFileHandles());
        generator.writeEndArray();

        // serialize persisted size of this snapshot
        generator.writeNumberField(
                SNAPSHOT_INCREMENTAL_SIZE, kvSnapshotHandle.getIncrementalSize());
        generator.writeEndObject();

        // serialize log offset
        generator.writeNumberField(LOG_OFFSET, completedSnapshot.getLogOffset());

        generator.writeEndObject();
    }

    private void serializeKvFileHandles(
            JsonGenerator generator, List<KvFileHandleAndLocalPath> kvFileHandleAndLocalPaths)
            throws IOException {
        for (KvFileHandleAndLocalPath fileHandleAndLocalPath : kvFileHandleAndLocalPaths) {
            generator.writeStartObject();

            // serialize kv file handle
            KvFileHandle kvFileHandle = fileHandleAndLocalPath.getKvFileHandle();
            generator.writeObjectFieldStart(KV_FILE_HANDLE);
            generator.writeStringField(KV_FILE_PATH, kvFileHandle.getFilePath());
            generator.writeNumberField(KV_FILE_SIZE, kvFileHandle.getSize());
            generator.writeEndObject();

            // serialize kv file local path
            generator.writeStringField(KV_FILE_LOCAL_PATH, fileHandleAndLocalPath.getLocalPath());

            generator.writeEndObject();
        }
    }

    @Override
    public CompletedSnapshot deserialize(JsonNode node) {
        JsonNode partitionIdNode = node.get(PARTITION_ID);
        Long partitionId = partitionIdNode == null ? null : partitionIdNode.asLong();
        // deserialize table bucket
        TableBucket tableBucket =
                new TableBucket(
                        node.get(TABLE_ID).asLong(), partitionId, node.get(BUCKET_ID).asInt());

        // deserialize snapshot id
        long snapshotId = node.get(SNAPSHOT_ID).asLong();

        // deserialize snapshot location
        String snapshotLocation = node.get(SNAPSHOT_LOCATION).asText();

        // deserialize kv snapshot file handle
        JsonNode kvSnapshotFileHandleNode = node.get(KV_SNAPSHOT_HANDLE);

        // deserialize shared file handles
        List<KvFileHandleAndLocalPath> sharedFileHandles =
                deserializeKvFileHandles(kvSnapshotFileHandleNode, KV_SHARED_FILES_HANDLE);

        // deserialize private file handles
        List<KvFileHandleAndLocalPath> privateFileHandles =
                deserializeKvFileHandles(kvSnapshotFileHandleNode, KV_PRIVATE_FILES_HANDLE);

        // deserialize snapshot incremental size
        long incrementalSize = kvSnapshotFileHandleNode.get(SNAPSHOT_INCREMENTAL_SIZE).asLong();

        // deserialize log offset
        long logOffset = node.get(LOG_OFFSET).asLong();

        // construct CompletedSnapshot
        KvSnapshotHandle kvSnapshotHandle =
                new KvSnapshotHandle(sharedFileHandles, privateFileHandles, incrementalSize);
        return new CompletedSnapshot(
                tableBucket, snapshotId, new FsPath(snapshotLocation), kvSnapshotHandle, logOffset);
    }

    private List<KvFileHandleAndLocalPath> deserializeKvFileHandles(
            JsonNode node, String kvHandleType) {
        List<KvFileHandleAndLocalPath> kvFileHandleAndLocalPaths = new ArrayList<>();
        for (JsonNode kvFileHandleAndLocalPathNode : node.get(kvHandleType)) {
            // deserialize kv file handle
            JsonNode kvFileHandleNode = kvFileHandleAndLocalPathNode.get(KV_FILE_HANDLE);
            String filePath = kvFileHandleNode.get(KV_FILE_PATH).asText();
            long fileSize = kvFileHandleNode.get(KV_FILE_SIZE).asLong();
            KvFileHandle kvFileHandle = new KvFileHandle(filePath, fileSize);

            // deserialize kv file local path
            String localPath = kvFileHandleAndLocalPathNode.get(KV_FILE_LOCAL_PATH).asText();
            KvFileHandleAndLocalPath kvFileHandleAndLocalPath =
                    KvFileHandleAndLocalPath.of(kvFileHandle, localPath);
            kvFileHandleAndLocalPaths.add(kvFileHandleAndLocalPath);
        }
        return kvFileHandleAndLocalPaths;
    }

    /** Serialize the {@link CompletedSnapshot} to json bytes. */
    public static byte[] toJson(CompletedSnapshot completedSnapshot) {
        return JsonSerdeUtils.writeValueAsBytes(completedSnapshot, INSTANCE);
    }

    /** Deserialize the json bytes to {@link CompletedSnapshot}. */
    public static CompletedSnapshot fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }
}
