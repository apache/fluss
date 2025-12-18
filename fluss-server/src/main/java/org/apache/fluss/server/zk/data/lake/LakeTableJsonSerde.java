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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
 *   <li>Version 2 (current): ZK node contains only the lake table snapshot file paths. The actual
 *       snapshot data is stored in a remote file pointed by the lake table snapshot file path.
 * </ul>
 */
public class LakeTableJsonSerde implements JsonSerializer<LakeTable>, JsonDeserializer<LakeTable> {

    public static final LakeTableJsonSerde INSTANCE = new LakeTableJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String LAKE_SNAPSHOTS = "lake_snapshots";

    private static final String SNAPSHOT_ID_KEY = "snapshot_id";
    private static final String TIERED_OFFSETS_KEY = "tiered_offsets";
    private static final String READABLE_OFFSETS_KEY = "readable_offsets";

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int CURRENT_VERSION = VERSION_2;

    @Override
    public void serialize(LakeTable lakeTable, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, CURRENT_VERSION);

        generator.writeArrayFieldStart(LAKE_SNAPSHOTS);
        for (LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata :
                checkNotNull(lakeTable.getLakeSnapshotMetadatas())) {
            generator.writeStartObject();

            generator.writeNumberField(SNAPSHOT_ID_KEY, lakeSnapshotMetadata.getSnapshotId());
            generator.writeStringField(
                    TIERED_OFFSETS_KEY, lakeSnapshotMetadata.getTieredOffsetsFilePath().toString());
            if (lakeSnapshotMetadata.getReadableOffsetsFilePath() != null) {
                generator.writeStringField(
                        READABLE_OFFSETS_KEY,
                        lakeSnapshotMetadata.getReadableOffsetsFilePath().toString());
            }
            generator.writeEndObject();
        }

        generator.writeEndArray();

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
            // Version 2: ZK node contains lake snapshot file paths
            JsonNode lakeSnapshotsNode = node.get(LAKE_SNAPSHOTS);
            if (lakeSnapshotsNode == null || !lakeSnapshotsNode.isArray()) {
                throw new IllegalArgumentException(
                        "Invalid lake_snapshots field in version 2 format");
            }

            List<LakeTable.LakeSnapshotMetadata> lakeSnapshotMetadatas = new ArrayList<>();
            Iterator<JsonNode> elements = lakeSnapshotsNode.elements();
            while (elements.hasNext()) {
                JsonNode snapshotNode = elements.next();
                long snapshotId = snapshotNode.get(SNAPSHOT_ID_KEY).asLong();
                String tieredOffsetsPath = snapshotNode.get(TIERED_OFFSETS_KEY).asText();
                JsonNode readableOffsetsNode = snapshotNode.get(READABLE_OFFSETS_KEY);
                FsPath readableOffsetsPath =
                        readableOffsetsNode != null
                                ? new FsPath(readableOffsetsNode.asText())
                                : null;

                LakeTable.LakeSnapshotMetadata metadata =
                        new LakeTable.LakeSnapshotMetadata(
                                snapshotId, new FsPath(tieredOffsetsPath), readableOffsetsPath);
                lakeSnapshotMetadatas.add(metadata);
            }
            return new LakeTable(lakeSnapshotMetadatas);
        } else {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
    }
}
