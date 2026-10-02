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

package org.apache.fluss.lake.paimon.tiering.markdone;

import org.apache.fluss.exception.UnsupportedVersionException;
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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * JSON serde for {@link PartitionMarkDoneState}. Incompatible state changes must increment the
 * version so older readers reject the format without rewriting it.
 */
public class PartitionMarkDoneStateJsonSerde
        implements JsonSerializer<PartitionMarkDoneState>,
                JsonDeserializer<PartitionMarkDoneState> {

    public static final PartitionMarkDoneStateJsonSerde INSTANCE =
            new PartitionMarkDoneStateJsonSerde();

    private static final int VERSION = 1;
    private static final String VERSION_FIELD = "version";
    private static final String INITIALIZED_FIELD = "initialized";
    private static final String TRACKED_PARTITION_LAST_UPDATE_TIMES_FIELD =
            "trackedPartitionLastUpdateTimes";

    @Override
    public void serialize(PartitionMarkDoneState state, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_FIELD, VERSION);
        generator.writeBooleanField(INITIALIZED_FIELD, state.isInitialized());
        generator.writeObjectFieldStart(TRACKED_PARTITION_LAST_UPDATE_TIMES_FIELD);
        for (Map.Entry<String, Long> entry :
                state.getTrackedPartitionLastUpdateTimes().entrySet()) {
            generator.writeNumberField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();
        generator.writeEndObject();
    }

    @Override
    public PartitionMarkDoneState deserialize(JsonNode node) {
        JsonNode versionNode = node.get(VERSION_FIELD);
        checkArgument(
                versionNode != null && versionNode.isInt(),
                "Field %s must be an integer.",
                VERSION_FIELD);
        if (versionNode.intValue() != VERSION) {
            throw new UnsupportedVersionException(
                    "Unsupported mark-done state version: "
                            + versionNode
                            + "; supported version: "
                            + VERSION);
        }
        JsonNode initializedNode = node.get(INITIALIZED_FIELD);
        checkArgument(
                initializedNode != null && initializedNode.isBoolean(),
                "Field %s must be a boolean.",
                INITIALIZED_FIELD);
        JsonNode trackedPartitionsNode = node.get(TRACKED_PARTITION_LAST_UPDATE_TIMES_FIELD);
        checkArgument(
                trackedPartitionsNode != null && trackedPartitionsNode.isObject(),
                "Field %s must be an object.",
                TRACKED_PARTITION_LAST_UPDATE_TIMES_FIELD);
        Map<String, Long> trackedPartitionLastUpdateTimes = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = trackedPartitionsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            checkArgument(
                    field.getValue().canConvertToLong(),
                    "Last update time of partition %s must be a long.",
                    field.getKey());
            trackedPartitionLastUpdateTimes.put(field.getKey(), field.getValue().asLong());
        }
        return new PartitionMarkDoneState(
                initializedNode.asBoolean(), trackedPartitionLastUpdateTimes);
    }

    /** Serializes the given state to a JSON string. */
    public static String toJson(PartitionMarkDoneState state) {
        return new String(
                JsonSerdeUtils.writeValueAsBytes(state, INSTANCE), StandardCharsets.UTF_8);
    }

    /** Deserializes the state from a JSON string. */
    public static PartitionMarkDoneState fromJson(String json) {
        return JsonSerdeUtils.readValue(json.getBytes(StandardCharsets.UTF_8), INSTANCE);
    }
}
