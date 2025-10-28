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

import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Json serializer and deserializer for {@link ServerTags}. */
public class ServerTagsJsonSerde
        implements JsonSerializer<ServerTags>, JsonDeserializer<ServerTags> {

    public static final ServerTagsJsonSerde INSTANCE = new ServerTagsJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String SERVER_TAGS = "server_tags";
    private static final int VERSION = 1;

    @Override
    public void serialize(ServerTags serverTags, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeObjectFieldStart(SERVER_TAGS);
        for (Map.Entry<Integer, ServerTag> entry : serverTags.getServerTags().entrySet()) {
            generator.writeNumberField(String.valueOf(entry.getKey()), entry.getValue().value);
        }
        generator.writeEndObject();

        generator.writeEndObject();
    }

    @Override
    public ServerTags deserialize(JsonNode node) {
        JsonNode serverTagsNode = node.get(SERVER_TAGS);
        Map<Integer, ServerTag> serverTags = new HashMap<>();
        Iterator<String> fieldNames = serverTagsNode.fieldNames();
        while (fieldNames.hasNext()) {
            String serverId = fieldNames.next();
            serverTags.put(
                    Integer.valueOf(serverId),
                    ServerTag.valueOf(serverTagsNode.get(serverId).asInt()));
        }
        return new ServerTags(serverTags);
    }
}
