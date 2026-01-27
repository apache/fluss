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

package org.apache.fluss.server.zk.data.producer;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * JSON serializer and deserializer for {@link ProducerOffsets}.
 *
 * <p>The serialized format is:
 *
 * <pre>{@code
 * {
 *   "version": 1,
 *   "expiration_time": 1735538268000,
 *   "tables": [
 *     {
 *       "table_id": 100,
 *       "offsets_path": "oss://bucket/path/uuid.offsets"
 *     }
 *   ]
 * }
 * }</pre>
 */
public class ProducerOffsetsJsonSerde
        implements JsonSerializer<ProducerOffsets>, JsonDeserializer<ProducerOffsets> {

    public static final ProducerOffsetsJsonSerde INSTANCE = new ProducerOffsetsJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String EXPIRATION_TIME_KEY = "expiration_time";
    private static final String TABLES_KEY = "tables";
    private static final String TABLE_ID_KEY = "table_id";
    private static final String OFFSETS_PATH_KEY = "offsets_path";

    private static final int CURRENT_VERSION = 1;

    @Override
    public void serialize(ProducerOffsets producerOffsets, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        generator.writeNumberField(VERSION_KEY, CURRENT_VERSION);
        generator.writeNumberField(EXPIRATION_TIME_KEY, producerOffsets.getExpirationTime());

        generator.writeArrayFieldStart(TABLES_KEY);
        for (ProducerOffsets.TableOffsetMetadata tableOffset : producerOffsets.getTableOffsets()) {
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID_KEY, tableOffset.getTableId());
            generator.writeStringField(OFFSETS_PATH_KEY, tableOffset.getOffsetsPath().toString());
            generator.writeEndObject();
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }

    @Override
    public ProducerOffsets deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported ProducerOffsets version: "
                            + version
                            + ", expected: "
                            + CURRENT_VERSION);
        }

        long expirationTime = node.get(EXPIRATION_TIME_KEY).asLong();

        List<ProducerOffsets.TableOffsetMetadata> tableOffsets = new ArrayList<>();
        JsonNode tablesNode = node.get(TABLES_KEY);
        if (tablesNode != null && tablesNode.isArray()) {
            Iterator<JsonNode> elements = tablesNode.elements();
            while (elements.hasNext()) {
                JsonNode tableNode = elements.next();
                long tableId = tableNode.get(TABLE_ID_KEY).asLong();
                String offsetsPath = tableNode.get(OFFSETS_PATH_KEY).asText();
                tableOffsets.add(
                        new ProducerOffsets.TableOffsetMetadata(tableId, new FsPath(offsetsPath)));
            }
        }

        return new ProducerOffsets(expirationTime, tableOffsets);
    }
}
