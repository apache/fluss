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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Json serde for {@link MarkDoneState}: {@code {"initialized": true, "pending": {"<partition>":
 * <lastUpdateTimeMs>}}}. Evolves via field-level compatibility (unknown fields ignored, missing
 * fields defaulted), no version gating.
 */
public class MarkDoneStateJsonSerde
        implements JsonSerializer<MarkDoneState>, JsonDeserializer<MarkDoneState> {

    public static final MarkDoneStateJsonSerde INSTANCE = new MarkDoneStateJsonSerde();

    private static final String INITIALIZED_FIELD = "initialized";
    private static final String PENDING_FIELD = "pending";

    @Override
    public void serialize(MarkDoneState state, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeBooleanField(INITIALIZED_FIELD, state.isInitialized());
        generator.writeObjectFieldStart(PENDING_FIELD);
        for (Map.Entry<String, Long> entry : state.getPendingPartitions().entrySet()) {
            generator.writeNumberField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();
        generator.writeEndObject();
    }

    @Override
    public MarkDoneState deserialize(JsonNode node) {
        boolean initialized =
                node.has(INITIALIZED_FIELD) && node.get(INITIALIZED_FIELD).asBoolean();
        Map<String, Long> pendingPartitions = new HashMap<>();
        JsonNode pendingNode = node.get(PENDING_FIELD);
        if (pendingNode != null) {
            Iterator<Map.Entry<String, JsonNode>> fields = pendingNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                pendingPartitions.put(field.getKey(), field.getValue().asLong());
            }
        }
        return new MarkDoneState(initialized, pendingPartitions);
    }

    /** Serializes the given state to a JSON string. */
    public static String toJson(MarkDoneState state) {
        return new String(
                JsonSerdeUtils.writeValueAsBytes(state, INSTANCE), StandardCharsets.UTF_8);
    }

    /** Deserializes the state from a JSON string. */
    public static MarkDoneState fromJson(String json) {
        return JsonSerdeUtils.readValue(json.getBytes(StandardCharsets.UTF_8), INSTANCE);
    }
}
