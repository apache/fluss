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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Json serializer and deserializer for {@link CoordinatorAddress}. */
@Internal
public class CoordinatorAddressJsonSerde
        implements JsonSerializer<CoordinatorAddress>, JsonDeserializer<CoordinatorAddress> {

    public static final CoordinatorAddressJsonSerde INSTANCE = new CoordinatorAddressJsonSerde();
    private static final String VERSION_KEY = "version";
    private static final int VERSION = 2;

    private static final String ID = "id";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String LISTENERS = "listeners";

    private static void writeVersion(JsonGenerator generator) throws IOException {
        generator.writeNumberField(VERSION_KEY, VERSION);
    }

    @Override
    public void serialize(CoordinatorAddress coordinatorAddress, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        writeVersion(generator);
        generator.writeStringField(ID, coordinatorAddress.getId());
        generator.writeStringField(
                LISTENERS, Endpoint.toListenersString(coordinatorAddress.getEndpoints()));
        generator.writeEndObject();
    }

    @Override
    public CoordinatorAddress deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        String id = node.get(ID).asText();
        List<Endpoint> endpoints;
        if (version == 1) {
            String host = node.get(HOST).asText();
            int port = node.get(PORT).asInt();
            endpoints = Collections.singletonList(new Endpoint(host, port, "CLIENT"));
        } else {
            endpoints = Endpoint.fromListenersString(node.get(LISTENERS).asText());
        }
        return new CoordinatorAddress(id, endpoints);
    }
}
