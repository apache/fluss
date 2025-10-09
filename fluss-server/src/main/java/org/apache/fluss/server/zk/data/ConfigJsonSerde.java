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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Json serializer and deserializer for config properties. */
public class ConfigJsonSerde
        implements JsonSerializer<Map<String, String>>, JsonDeserializer<Map<String, String>> {

    public static final ConfigJsonSerde INSTANCE = new ConfigJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String CONFIGS = "configs";
    private static final int VERSION = 1;

    @Override
    public void serialize(Map<String, String> properties, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeObjectFieldStart(CONFIGS);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getValue() != null) {
                generator.writeStringField(property.getKey(), property.getValue());
            } else {
                generator.writeNullField(property.getKey());
            }
        }
        generator.writeEndObject();

        generator.writeEndObject();
    }

    @Override
    public Map<String, String> deserialize(JsonNode node) {
        Map<String, String> properties = new HashMap<>();
        JsonNode bucketsNode = node.get(CONFIGS);
        Iterator<Map.Entry<String, JsonNode>> fields = bucketsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            properties.put(
                    field.getKey(), field.getValue().isNull() ? null : field.getValue().asText());
        }

        return properties;
    }
}
