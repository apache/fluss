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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** JSON serde for inline and remote rebalance task znode formats. */
public final class RebalanceTaskZNodeJsonSerde
        implements JsonSerializer<RebalanceTaskZNodeData>,
                JsonDeserializer<RebalanceTaskZNodeData> {

    public static final RebalanceTaskZNodeJsonSerde INSTANCE = new RebalanceTaskZNodeJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String REBALANCE_ID = "rebalance_id";
    private static final String REBALANCE_TASK_PATH = "rebalance_task_path";

    private static final int INLINE_VERSION_1 = 1;
    private static final int INLINE_VERSION_2 = 2;
    private static final int REMOTE_VERSION = 3;

    @Override
    public void serialize(RebalanceTaskZNodeData data, JsonGenerator generator) throws IOException {
        if (data.isInline()) {
            RebalanceTaskJsonSerde.INSTANCE.serialize(data.getInlineTask(), generator);
            return;
        }

        RebalanceTaskHandle handle = data.getRemoteTaskHandle();
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, REMOTE_VERSION);
        generator.writeStringField(REBALANCE_ID, handle.getRebalanceId());
        generator.writeStringField(REBALANCE_TASK_PATH, handle.getTaskPath().toString());
        generator.writeEndObject();
    }

    @Override
    public RebalanceTaskZNodeData deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        if (version == INLINE_VERSION_1 || version == INLINE_VERSION_2) {
            return RebalanceTaskZNodeData.inline(RebalanceTaskJsonSerde.INSTANCE.deserialize(node));
        }
        if (version == REMOTE_VERSION) {
            JsonNode rebalanceId = node.get(REBALANCE_ID);
            JsonNode taskPath = node.get(REBALANCE_TASK_PATH);
            if (rebalanceId == null || taskPath == null) {
                throw new IllegalArgumentException("Invalid remote rebalance task handle.");
            }
            return RebalanceTaskZNodeData.remote(
                    new RebalanceTaskHandle(rebalanceId.asText(), new FsPath(taskPath.asText())));
        }
        throw new IllegalArgumentException("Unsupported rebalance task version: " + version);
    }
}
