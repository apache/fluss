/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link TabletServerRegistration}. */
@Internal
public class TabletServerRegistrationJsonSerde
        implements JsonSerializer<TabletServerRegistration>,
                JsonDeserializer<TabletServerRegistration> {

    public static final TabletServerRegistrationJsonSerde INSTANCE =
            new TabletServerRegistrationJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String REGISTER_TIMESTAMP = "register_timestamp";

    @Override
    public void serialize(
            TabletServerRegistration tabletServerRegistration, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(HOST, tabletServerRegistration.getHost());
        generator.writeNumberField(PORT, tabletServerRegistration.getPort());
        generator.writeNumberField(
                REGISTER_TIMESTAMP, tabletServerRegistration.getRegisterTimestamp());
        generator.writeEndObject();
    }

    @Override
    public TabletServerRegistration deserialize(JsonNode node) {
        String host = node.get(HOST).asText();
        int port = node.get(PORT).asInt();
        long registerTimestamp = node.get(REGISTER_TIMESTAMP).asLong();
        return new TabletServerRegistration(host, port, registerTimestamp);
    }
}
