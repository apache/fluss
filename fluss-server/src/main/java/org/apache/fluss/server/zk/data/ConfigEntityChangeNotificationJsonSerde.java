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

/** Json serializer and deserializer for config entity change notification. */
public class ConfigEntityChangeNotificationJsonSerde
        implements JsonSerializer<String>, JsonDeserializer<String> {

    public static final ConfigEntityChangeNotificationJsonSerde INSTANCE =
            new ConfigEntityChangeNotificationJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String ENTITY_PATH = "entity_path";
    private static final int VERSION = 1;

    @Override
    public void serialize(String entityPath, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(ENTITY_PATH, entityPath);
        generator.writeEndObject();
    }

    @Override
    public String deserialize(JsonNode node) {
        return node.get(ENTITY_PATH).asText();
    }
}
