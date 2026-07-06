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

package org.apache.fluss.utils.json;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.kv.snapshot.KvFileHandle;
import org.apache.fluss.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Json serializer and deserializer for {@link CompletedSnapshot}.
 *
 * <p>This class is the single source of truth for KV snapshot {@code _METADATA} JSON format
 * constants. It provides full serialization/deserialization as well as lightweight extraction
 * utilities (e.g., {@link #parseSharedSstLocalPaths(byte[])}) for consumers that do not need a full
 * domain object.
 */
public class CompletedSnapshotJsonSerde
        implements JsonSerializer<CompletedSnapshot>, JsonDeserializer<CompletedSnapshot> {

    // ---------------------------------------------------------------------------------
    // JSON field name constants — _METADATA schema version 1
    // ---------------------------------------------------------------------------------

    /** Current schema version. */
    public static final int VERSION = 1;

    public static final String VERSION_KEY = "version";

    // Table bucket identification
    public static final String TABLE_ID = "table_id";
    public static final String PARTITION_ID = "partition_id";
    public static final String BUCKET_ID = "bucket_id";

    // Snapshot identity
    public static final String SNAPSHOT_ID = "snapshot_id";
    public static final String SNAPSHOT_LOCATION = "snapshot_location";

    // KV snapshot handle structure
    public static final String KV_SNAPSHOT_HANDLE = "kv_snapshot_handle";
    public static final String KV_SHARED_FILES_HANDLE = "shared_file_handles";
    public static final String KV_PRIVATE_FILES_HANDLE = "private_file_handles";

    // Individual file entry fields
    public static final String KV_FILE_HANDLE = "kv_file_handle";
    public static final String KV_FILE_PATH = "path";
    public static final String KV_FILE_SIZE = "size";
    public static final String KV_FILE_LOCAL_PATH = "local_path";

    // Snapshot size
    public static final String SNAPSHOT_INCREMENTAL_SIZE = "snapshot_incremental_size";

    // KV tablet state
    public static final String LOG_OFFSET = "log_offset";
    public static final String ROW_COUNT = "row_count";

    // Auto-increment ID ranges
    public static final String AUTO_INC_ID_RANGE = "auto_inc_id_range";
    public static final String AUTO_INC_COLUMN_ID = "column_id";
    public static final String AUTO_INC_ID_START = "start";
    public static final String AUTO_INC_ID_END = "end";

    public static final CompletedSnapshotJsonSerde INSTANCE = new CompletedSnapshotJsonSerde();

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

        // ROW_COUNT and AUTO_INC_ID_RANGE are added in v0.9, but they are nullable and optional, so
        // we don't bump JSON version here to guarantee the RPC protocol compatibility between
        // TabletServer and CoordinatorServer. See CoordinatorGateway#commitKvSnapshot RPC.

        // serialize row count if exists
        if (completedSnapshot.getRowCount() != null) {
            generator.writeNumberField(ROW_COUNT, completedSnapshot.getRowCount());
        }

        // serialize auto-increment id range for each auto-increment column
        if (completedSnapshot.getAutoIncIDRanges() != null
                && !completedSnapshot.getAutoIncIDRanges().isEmpty()) {
            generator.writeArrayFieldStart(AUTO_INC_ID_RANGE);
            for (AutoIncIDRange autoIncIDRange : completedSnapshot.getAutoIncIDRanges()) {
                generator.writeStartObject();
                generator.writeNumberField(AUTO_INC_COLUMN_ID, autoIncIDRange.getColumnId());
                generator.writeNumberField(AUTO_INC_ID_START, autoIncIDRange.getStart());
                generator.writeNumberField(AUTO_INC_ID_END, autoIncIDRange.getEnd());
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }

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

        Long rowCount = null;
        if (node.has(ROW_COUNT)) {
            rowCount = node.get(ROW_COUNT).asLong();
        }

        List<AutoIncIDRange> autoIncIDRanges = null;
        if (node.has(AUTO_INC_ID_RANGE)) {
            autoIncIDRanges = new ArrayList<>();
            for (JsonNode autoIncIDRangeNode : node.get(AUTO_INC_ID_RANGE)) {
                int columnId = autoIncIDRangeNode.get(AUTO_INC_COLUMN_ID).asInt();
                long start = autoIncIDRangeNode.get(AUTO_INC_ID_START).asLong();
                long end = autoIncIDRangeNode.get(AUTO_INC_ID_END).asLong();
                autoIncIDRanges.add(new AutoIncIDRange(columnId, start, end));
            }
        }

        return new CompletedSnapshot(
                tableBucket,
                snapshotId,
                new FsPath(snapshotLocation),
                kvSnapshotHandle,
                logOffset,
                rowCount,
                autoIncIDRanges);
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

    // ---------------------------------------------------------------------------------
    // Static convenience methods
    // ---------------------------------------------------------------------------------

    /** Serialize the {@link CompletedSnapshot} to json bytes. */
    public static byte[] toJson(CompletedSnapshot completedSnapshot) {
        return JsonSerdeUtils.writeValueAsBytes(completedSnapshot, INSTANCE);
    }

    /** Deserialize the json bytes to {@link CompletedSnapshot}. */
    public static CompletedSnapshot fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    /**
     * Parses a {@code _METADATA} JSON payload and returns the set of shared SST file names (local
     * path basenames) referenced by the snapshot.
     *
     * <p>This method navigates the JSON tree: {@code kv_snapshot_handle → shared_file_handles[*] →
     * local_path} and collects non-empty values.
     *
     * @param metadataJsonBytes raw bytes of the {@code _METADATA} file
     * @return set of shared SST file names (e.g. {@code "abc-def-0.sst"})
     * @throws IOException if the bytes cannot be parsed as valid JSON or the expected structure is
     *     missing
     */
    public static Set<String> parseSharedSstLocalPaths(byte[] metadataJsonBytes)
            throws IOException {
        JsonNode root = JsonSerdeUtils.OBJECT_MAPPER_INSTANCE.readTree(metadataJsonBytes);
        JsonNode kvSnapshotHandle = root.get(KV_SNAPSHOT_HANDLE);
        if (kvSnapshotHandle == null) {
            throw new IOException("Missing '" + KV_SNAPSHOT_HANDLE + "' in _METADATA JSON payload");
        }
        JsonNode sharedFilesNode = kvSnapshotHandle.get(KV_SHARED_FILES_HANDLE);
        if (sharedFilesNode == null || !sharedFilesNode.isArray()) {
            throw new IOException(
                    "Missing or non-array '"
                            + KV_SHARED_FILES_HANDLE
                            + "' in _METADATA JSON payload");
        }

        Set<String> fileNames = new HashSet<>();
        for (JsonNode entry : sharedFilesNode) {
            JsonNode localPathNode = entry.get(KV_FILE_LOCAL_PATH);
            if (localPathNode == null || !localPathNode.isTextual()) {
                throw new IOException(
                        "Missing or non-textual '"
                                + KV_FILE_LOCAL_PATH
                                + "' in "
                                + KV_SHARED_FILES_HANDLE
                                + " entry");
            }
            String localPath = localPathNode.asText();
            if (localPath.isEmpty()) {
                throw new IOException(
                        "Empty '"
                                + KV_FILE_LOCAL_PATH
                                + "' in "
                                + KV_SHARED_FILES_HANDLE
                                + " entry");
            }
            fileNames.add(localPath);
        }
        return fileNames;
    }
}
