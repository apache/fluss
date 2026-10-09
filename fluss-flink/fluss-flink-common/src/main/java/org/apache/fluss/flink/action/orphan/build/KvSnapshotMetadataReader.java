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

package org.apache.fluss.flink.action.orphan.build;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Reads shared-file references from KV snapshot metadata without loading snapshot runtime state.
 */
final class KvSnapshotMetadataReader {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String KV_SNAPSHOT_HANDLE = "kv_snapshot_handle";
    private static final String KV_SHARED_FILES_HANDLE = "shared_file_handles";
    private static final String KV_FILE_HANDLE = "kv_file_handle";
    private static final String KV_FILE_PATH = "path";

    private KvSnapshotMetadataReader() {}

    /**
     * Parses a {@code _METADATA} JSON payload and returns the remote paths of shared SST objects
     * referenced by the snapshot.
     *
     * <p>Remote storage assigns an opaque name to each uploaded object. It is therefore unsafe for
     * cleanup to compare a scanned remote object with {@code local_path}, which is only the RocksDB
     * filename on the tablet server. This method navigates {@code kv_snapshot_handle →
     * shared_file_handles[*] → kv_file_handle.path} and fails closed when any entry is malformed.
     */
    public static Set<String> parseSharedSstRemotePaths(byte[] metadataJsonBytes)
            throws IOException {
        JsonNode sharedFilesNode = parseSharedFileHandles(metadataJsonBytes);
        Set<String> remotePaths = new HashSet<>();
        for (JsonNode entry : sharedFilesNode) {
            JsonNode fileHandleNode = entry.get(KV_FILE_HANDLE);
            if (fileHandleNode == null || !fileHandleNode.isObject()) {
                throw new IOException(
                        "Missing or non-object '"
                                + KV_FILE_HANDLE
                                + "' in "
                                + KV_SHARED_FILES_HANDLE
                                + " entry");
            }
            JsonNode remotePathNode = fileHandleNode.get(KV_FILE_PATH);
            if (remotePathNode == null || !remotePathNode.isTextual()) {
                throw new IOException(
                        "Missing or non-textual '"
                                + KV_FILE_HANDLE
                                + "."
                                + KV_FILE_PATH
                                + "' in "
                                + KV_SHARED_FILES_HANDLE
                                + " entry");
            }
            String remotePath = remotePathNode.asText();
            if (remotePath.isEmpty()) {
                throw new IOException(
                        "Empty '"
                                + KV_FILE_HANDLE
                                + "."
                                + KV_FILE_PATH
                                + "' in "
                                + KV_SHARED_FILES_HANDLE
                                + " entry");
            }
            remotePaths.add(remotePath);
        }
        return remotePaths;
    }

    private static JsonNode parseSharedFileHandles(byte[] metadataJsonBytes) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(metadataJsonBytes);
        JsonNode kvSnapshotHandle = root == null ? null : root.get(KV_SNAPSHOT_HANDLE);
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
        return sharedFilesNode;
    }
}
