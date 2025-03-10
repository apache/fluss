/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static com.alibaba.fluss.server.zk.data.RemoteLogManifestHandle.fromRemoteLogManifestPath;

/** Json serializer and deserializer for {@link RemoteLogManifestHandle}. */
@Internal
public class RemoteLogManifestHandleJsonSerde
        implements JsonSerializer<RemoteLogManifestHandle>,
                JsonDeserializer<RemoteLogManifestHandle> {

    public static final RemoteLogManifestHandleJsonSerde INSTANCE =
            new RemoteLogManifestHandleJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String REMOTE_LOG_MANIFEST_PATH = "remote_log_manifest_path";
    private static final String REMOTE_LOG_END_OFFSET = "remote_log_end_offset";
    private static final int VERSION = 1;

    @Override
    public void serialize(RemoteLogManifestHandle remoteLogManifestHandle, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(
                REMOTE_LOG_MANIFEST_PATH,
                remoteLogManifestHandle.getRemoteLogManifestPath().toString());
        generator.writeNumberField(
                REMOTE_LOG_END_OFFSET, remoteLogManifestHandle.getRemoteLogEndOffset());

        generator.writeEndObject();
    }

    @Override
    public RemoteLogManifestHandle deserialize(JsonNode node) {
        String remoteLogManifestPath = node.get(REMOTE_LOG_MANIFEST_PATH).asText();
        long remoteLogEndOffset = node.get(REMOTE_LOG_END_OFFSET).asLong();
        return new RemoteLogManifestHandle(
                fromRemoteLogManifestPath(remoteLogManifestPath), remoteLogEndOffset);
    }
}
