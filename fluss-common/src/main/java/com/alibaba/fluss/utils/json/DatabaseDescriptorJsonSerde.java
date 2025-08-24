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

package com.alibaba.fluss.utils.json;

import com.alibaba.fluss.metadata.DatabaseDescriptor;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Json serializer and deserializer for {@link DatabaseDescriptor}. */
public class DatabaseDescriptorJsonSerde
        implements JsonSerializer<DatabaseDescriptor>, JsonDeserializer<DatabaseDescriptor> {

    public static final DatabaseDescriptorJsonSerde INSTANCE = new DatabaseDescriptorJsonSerde();

    static final String CUSTOM_PROPERTIES_NAME = "custom_properties";
    static final String COMMENT_NAME = "comment";

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    @Override
    public void serialize(DatabaseDescriptor databaseDescriptor, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);

        // serialize comment.
        if (databaseDescriptor.getComment().isPresent()) {
            generator.writeStringField(COMMENT_NAME, databaseDescriptor.getComment().get());
        }

        // serialize properties.
        generator.writeObjectFieldStart(CUSTOM_PROPERTIES_NAME);
        for (Map.Entry<String, String> entry :
                databaseDescriptor.getCustomProperties().entrySet()) {
            generator.writeObjectField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();

        generator.writeEndObject();
    }

    @Override
    public DatabaseDescriptor deserialize(JsonNode node) {
        DatabaseDescriptor.Builder builder = DatabaseDescriptor.builder();

        JsonNode commentNode = node.get(COMMENT_NAME);
        if (commentNode != null) {
            builder.comment(commentNode.asText());
        }

        builder.customProperties(deserializeProperties(node.get(CUSTOM_PROPERTIES_NAME)));

        return builder.build();
    }

    private Map<String, String> deserializeProperties(JsonNode node) {
        HashMap<String, String> properties = new HashMap<>();
        Iterator<String> optionsKeys = node.fieldNames();
        while (optionsKeys.hasNext()) {
            String key = optionsKeys.next();
            properties.put(key, node.get(key).asText());
        }
        return properties;
    }
}
