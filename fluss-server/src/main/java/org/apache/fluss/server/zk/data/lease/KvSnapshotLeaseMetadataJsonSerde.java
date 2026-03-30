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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Json serializer and deserializer for {@link KvSnapshotLeaseMetadata}. */
public class KvSnapshotLeaseMetadataJsonSerde
        implements JsonSerializer<KvSnapshotLeaseMetadata>,
                JsonDeserializer<KvSnapshotLeaseMetadata> {

    public static final KvSnapshotLeaseMetadataJsonSerde INSTANCE =
            new KvSnapshotLeaseMetadataJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String EXPIRATION_TIME = "expiration_time";
    private static final String TABLES = "tables";
    private static final String TABLE_ID = "table_id";
    private static final String KV_SNAPSHOT_PATH = "lease_metadata_path";

    private static final int VERSION = 1;

    @Override
    public void serialize(KvSnapshotLeaseMetadata kvSnapshotLeaseMetadata, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(EXPIRATION_TIME, kvSnapshotLeaseMetadata.getExpirationTime());

        generator.writeFieldName(TABLES);
        generator.writeStartArray();
        for (Map.Entry<Long, FsPath> entry :
                kvSnapshotLeaseMetadata.getTableIdToRemoteMetadataFilePath().entrySet()) {
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID, entry.getKey());
            generator.writeStringField(KV_SNAPSHOT_PATH, entry.getValue().toString());
            generator.writeEndObject();
        }
        // end tables
        generator.writeEndArray();

        // end root
        generator.writeEndObject();
    }

    @Override
    public KvSnapshotLeaseMetadata deserialize(JsonNode node) {
        long expirationTime = node.get(EXPIRATION_TIME).asLong();

        Map<Long, FsPath> tableIdToRemoteMetadataFilePath = new HashMap<>();
        JsonNode tablesNode = node.get(TABLES);

        for (JsonNode tableNode : tablesNode) {
            long tableId = tableNode.get(TABLE_ID).asLong();
            String kvSnapshotPath = tableNode.get(KV_SNAPSHOT_PATH).asText();
            tableIdToRemoteMetadataFilePath.put(tableId, new FsPath(kvSnapshotPath));
        }

        return new KvSnapshotLeaseMetadata(expirationTime, tableIdToRemoteMetadataFilePath);
    }
}
