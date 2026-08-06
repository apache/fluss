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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static org.apache.fluss.server.zk.data.RemoteLogManifestHandle.fromRemoteLogManifestPath;

/** Json serializer and deserializer for {@link RemoteLogManifestHandle}. */
@Internal
public class RemoteLogManifestHandleJsonSerde
        implements JsonSerializer<RemoteLogManifestHandle>,
                JsonDeserializer<RemoteLogManifestHandle> {

    public static final RemoteLogManifestHandleJsonSerde INSTANCE =
            new RemoteLogManifestHandleJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String REMOTE_LOG_MANIFEST_PATH = "remote_log_manifest_path";
    private static final String MANIFEST_GENERATION = "manifest_generation";
    private static final String REMOTE_LOG_START_OFFSET = "remote_log_start_offset";
    private static final String REMOTE_LOG_END_OFFSET = "remote_log_end_offset";

    @Override
    public void serialize(RemoteLogManifestHandle remoteLogManifestHandle, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, remoteLogManifestHandle.getVersion());
        generator.writeStringField(
                REMOTE_LOG_MANIFEST_PATH,
                remoteLogManifestHandle.getRemoteLogManifestPath().toString());
        generator.writeNumberField(
                REMOTE_LOG_END_OFFSET, remoteLogManifestHandle.getRemoteLogEndOffset());
        if (remoteLogManifestHandle.getVersion() == RemoteLogManifestHandle.VERSION_2) {
            generator.writeNumberField(
                    MANIFEST_GENERATION,
                    remoteLogManifestHandle.getManifestGeneration().getAsLong());
            generator.writeNumberField(
                    REMOTE_LOG_START_OFFSET,
                    remoteLogManifestHandle.getRemoteLogStartOffset().getAsLong());
        }

        generator.writeEndObject();
    }

    @Override
    public RemoteLogManifestHandle deserialize(JsonNode node) {
        int version = required(node, VERSION_KEY).asInt();
        String remoteLogManifestPath = required(node, REMOTE_LOG_MANIFEST_PATH).asText();
        long remoteLogEndOffset = required(node, REMOTE_LOG_END_OFFSET).asLong();
        if (version == RemoteLogManifestHandle.VERSION_1) {
            rejectField(node, MANIFEST_GENERATION, version);
            rejectField(node, REMOTE_LOG_START_OFFSET, version);
            return new RemoteLogManifestHandle(
                    fromRemoteLogManifestPath(remoteLogManifestPath), remoteLogEndOffset);
        }
        if (version == RemoteLogManifestHandle.VERSION_2) {
            return RemoteLogManifestHandle.v2(
                    fromRemoteLogManifestPath(remoteLogManifestPath),
                    required(node, MANIFEST_GENERATION).asLong(),
                    required(node, REMOTE_LOG_START_OFFSET).asLong(),
                    remoteLogEndOffset);
        }
        throw new IllegalArgumentException(
                "Unsupported remote log manifest handle version: " + version);
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException(
                    "Missing required remote log manifest handle field: " + field);
        }
        return value;
    }

    private static void rejectField(JsonNode node, String field, int version) {
        if (node.has(field)) {
            throw new IllegalArgumentException(
                    "Remote log manifest handle field "
                            + field
                            + " is not valid for version "
                            + version);
        }
    }
}
