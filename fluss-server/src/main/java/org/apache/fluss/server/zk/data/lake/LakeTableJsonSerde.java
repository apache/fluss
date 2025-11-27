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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Json serializer and deserializer for {@link LakeTable}.
 *
 * <p>This serde supports two storage format versions:
 *
 * <ul>
 *   <li>Version 1 (legacy): ZK node contains full {@link LakeTableSnapshot} data. During
 *       deserialization, it uses {@link LakeTableSnapshotJsonSerde} to deserialize and wraps the
 *       result in a {@link LakeTable}.
 *   <li>Version 2 (current): ZK node contains only the metadata file path. The actual snapshot data
 *       is stored in a remote file.
 * </ul>
 */
public class LakeTableJsonSerde implements JsonSerializer<LakeTable>, JsonDeserializer<LakeTable> {

    public static final LakeTableJsonSerde INSTANCE = new LakeTableJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String METADATA_PATH_KEY = "metadata_path";
    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int CURRENT_VERSION = VERSION_2;

    @Override
    public void serialize(LakeTable lakeTable, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, CURRENT_VERSION);

        FsPath lakeTableSnapshotFileHandle = lakeTable.getLakeTableSnapshotFileHandle();
        checkNotNull(lakeTableSnapshotFileHandle);
        generator.writeStringField(METADATA_PATH_KEY, lakeTableSnapshotFileHandle.toString());
        generator.writeEndObject();
    }

    @Override
    public LakeTable deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        if (version == VERSION_1) {
            // Version 1: ZK node contains full snapshot data, use LakeTableSnapshotJsonSerde
            LakeTableSnapshot snapshot = LakeTableSnapshotJsonSerde.INSTANCE.deserialize(node);
            return new LakeTable(snapshot);
        } else if (version == VERSION_2) {
            // Version 2: ZK node contains only metadata file path
            if (!node.has(METADATA_PATH_KEY) || node.get(METADATA_PATH_KEY).isNull()) {
                throw new IllegalArgumentException(
                        "Version 2 ZK node must have non-null 'metadata_path' field");
            }
            FsPath metadataPath = new FsPath(node.get(METADATA_PATH_KEY).asText());
            return new LakeTable(metadataPath);
        } else {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
    }
}
