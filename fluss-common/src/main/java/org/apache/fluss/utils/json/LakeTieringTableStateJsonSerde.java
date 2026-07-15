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
 * <p>Field-level compatibility: unknown fields are ignored and missing fields fall back to
 * defaults, and {@code version} is NOT strictly gated on read, so a higher-version payload still
 * degrades to the fields this build understands. Writers must honor the version rule documented on
 * {@link LakeTieringTableState} (do not overwrite a state whose version is higher than {@link
 * LakeTieringTableState#CURRENT_VERSION}).
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
        // no strict version gating on read: read best-effort and ignore unknown fields.
        int version =
                node.has(VERSION_KEY)
                        ? node.get(VERSION_KEY).asInt()
                        : LakeTieringTableState.VERSION_1;

        // VERSION_1 fields, read for any version so a higher (unsupported) version degrades to the
        // fields this build knows. When evolving, bump CURRENT_VERSION and read the new fields here
        // guarded by `if (version >= VERSION_2)`.
        boolean partitionDoneInitialized =
                node.has(PARTITION_DONE_INITIALIZED_KEY)
                        && node.get(PARTITION_DONE_INITIALIZED_KEY).asBoolean();

        Map<Long, Long> partitionUpdateTimes = new HashMap<>();
        JsonNode updateTimesNode = node.get(PARTITION_UPDATE_TIMES_KEY);
        if (updateTimesNode != null && updateTimesNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = updateTimesNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                partitionUpdateTimes.put(Long.valueOf(field.getKey()), field.getValue().asLong());
            }
        }

        return new LakeTieringTableState(version, partitionDoneInitialized, partitionUpdateTimes);
    }
}
