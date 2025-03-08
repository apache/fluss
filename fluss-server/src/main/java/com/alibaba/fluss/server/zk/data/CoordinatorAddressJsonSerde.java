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

/** Json serializer and deserializer for {@link CoordinatorAddress}. */
@Internal
public class CoordinatorAddressJsonSerde
        implements JsonSerializer<CoordinatorAddress>, JsonDeserializer<CoordinatorAddress> {

    public static final CoordinatorAddressJsonSerde INSTANCE = new CoordinatorAddressJsonSerde();
    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    private static final String ID = "id";
    private static final String HOST = "host";
    private static final String PORT = "port";

    private static void writeVersion(JsonGenerator generator) throws IOException {
        generator.writeNumberField(VERSION_KEY, VERSION);
    }

    @Override
    public void serialize(CoordinatorAddress coordinatorAddress, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        writeVersion(generator);
        generator.writeStringField(ID, coordinatorAddress.getId());
        generator.writeStringField(HOST, coordinatorAddress.getHost());
        generator.writeNumberField(PORT, coordinatorAddress.getPort());
        generator.writeEndObject();
    }

    @Override
    public CoordinatorAddress deserialize(JsonNode node) {
        String id = node.get(ID).asText();
        String host = node.get(HOST).asText();
        int port = node.get(PORT).asInt();
        return new CoordinatorAddress(id, host, port);
    }
}
