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

import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Json serializer and deserializer for {@link LakeTieringTableState}, e.g. {@code
 * {"version":1,"partition_done_initialized":true,"partition_update_times":{"5":1704153550000}}}.
 *
 * <p>Validation is strict for known fields, tolerant for unknown ones. {@code version} is mandatory
 * and positive; a corrupt known field fails fast. A higher-than-supported {@code version} is
 * neither read nor written; the caller keeps the raw bytes and passes them through unchanged so a
 * newer build's state is never dropped.
 */
public class LakeTieringTableStateJsonSerde
        implements JsonSerializer<LakeTieringTableState>, JsonDeserializer<LakeTieringTableState> {

    public static final LakeTieringTableStateJsonSerde INSTANCE =
            new LakeTieringTableStateJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String PARTITION_DONE_INITIALIZED_KEY = "partition_done_initialized";
    private static final String PARTITION_UPDATE_TIMES_KEY = "partition_update_times";

    @Override
    public void serialize(LakeTieringTableState state, JsonGenerator generator) throws IOException {
        // Defense: this build never constructs a higher-version state to write (it passes the raw
        // bytes through instead), so this guards against misuse of the version-taking constructor.
        if (state.getVersion() > LakeTieringTableState.CURRENT_VERSION) {
            throw new IllegalStateException(
                    "Refusing to serialize unsupported tiering state version "
                            + state.getVersion()
                            + " > "
                            + LakeTieringTableState.CURRENT_VERSION
                            + ".");
        }
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, state.getVersion());
        generator.writeBooleanField(
                PARTITION_DONE_INITIALIZED_KEY, state.isPartitionDoneInitialized());
        generator.writeObjectFieldStart(PARTITION_UPDATE_TIMES_KEY);
        for (Map.Entry<Long, Long> entry : state.getPartitionUpdateTimes().entrySet()) {
            generator.writeNumberField(String.valueOf(entry.getKey()), entry.getValue());
        }
        generator.writeEndObject();
        generator.writeEndObject();
    }

    @Override
    public LakeTieringTableState deserialize(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException(
                    "Corrupt tiering state: expected a JSON object but got " + node);
        }

        // version is mandatory and a positive integer; it gates the rest of the parsing.
        JsonNode versionNode = node.get(VERSION_KEY);
        if (versionNode == null || !versionNode.isInt() || versionNode.asInt() <= 0) {
            throw new IllegalArgumentException(
                    "Corrupt tiering state: '"
                            + VERSION_KEY
                            + "' must be a positive integer but got "
                            + versionNode);
        }
        int version = versionNode.asInt();

        // A newer version cannot be interpreted here; the caller must pass the raw bytes through.
        if (version > LakeTieringTableState.CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported tiering state version "
                            + version
                            + " > "
                            + LakeTieringTableState.CURRENT_VERSION
                            + "; pass the raw bytes through unchanged.");
        }

        // partition_done_initialized: optional, must be a boolean when present.
        JsonNode initializedNode = node.get(PARTITION_DONE_INITIALIZED_KEY);
        if (initializedNode != null && !initializedNode.isBoolean()) {
            throw new IllegalArgumentException(
                    "Corrupt tiering state: '"
                            + PARTITION_DONE_INITIALIZED_KEY
                            + "' must be a boolean but got "
                            + initializedNode);
        }
        boolean partitionDoneInitialized = initializedNode != null && initializedNode.asBoolean();

        // partition_update_times: optional object; positive-id keys, non-negative values.
        Map<Long, Long> partitionUpdateTimes = new HashMap<>();
        JsonNode updateTimesNode = node.get(PARTITION_UPDATE_TIMES_KEY);
        if (updateTimesNode != null && !updateTimesNode.isNull()) {
            if (!updateTimesNode.isObject()) {
                throw new IllegalArgumentException(
                        "Corrupt tiering state: '"
                                + PARTITION_UPDATE_TIMES_KEY
                                + "' must be a JSON object but got "
                                + updateTimesNode);
            }
            Iterator<Map.Entry<String, JsonNode>> fields = updateTimesNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                long partitionId;
                try {
                    partitionId = Long.parseLong(field.getKey());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Corrupt tiering state: invalid partitionId key '"
                                    + field.getKey()
                                    + "' in "
                                    + PARTITION_UPDATE_TIMES_KEY);
                }
                if (partitionId <= 0) {
                    throw new IllegalArgumentException(
                            "Corrupt tiering state: partitionId must be positive but got "
                                    + partitionId);
                }
                JsonNode valueNode = field.getValue();
                if (!valueNode.canConvertToLong() || valueNode.asLong() < 0) {
                    throw new IllegalArgumentException(
                            "Corrupt tiering state: update time for partition "
                                    + partitionId
                                    + " must be a non-negative integer but got "
                                    + valueNode);
                }
                partitionUpdateTimes.put(partitionId, valueNode.asLong());
            }
        }

        return new LakeTieringTableState(version, partitionDoneInitialized, partitionUpdateTimes);
    }
}
