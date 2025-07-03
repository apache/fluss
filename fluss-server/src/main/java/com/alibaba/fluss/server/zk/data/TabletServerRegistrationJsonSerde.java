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
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;

/** Json serializer and deserializer for {@link TabletServerRegistration}. */
@Internal
public class TabletServerRegistrationJsonSerde
        implements JsonSerializer<TabletServerRegistration>,
                JsonDeserializer<TabletServerRegistration> {

    public static final TabletServerRegistrationJsonSerde INSTANCE =
            new TabletServerRegistrationJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 3;

    @Deprecated private static final String HOST = "host";
    @Deprecated private static final String PORT = "port";
    private static final String REGISTER_TIMESTAMP = "register_timestamp";
    private static final String LISTENERS = "listeners";
    private static final String RACK = "rack";

    @Override
    public void serialize(
            TabletServerRegistration tabletServerRegistration, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(
                LISTENERS, Endpoint.toListenersString(tabletServerRegistration.getEndpoints()));
        generator.writeNumberField(
                REGISTER_TIMESTAMP, tabletServerRegistration.getRegisterTimestamp());
        if (tabletServerRegistration.getRack() != null) {
            generator.writeStringField(RACK, tabletServerRegistration.getRack());
        }
        generator.writeEndObject();
    }

    @Override
    public TabletServerRegistration deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        List<Endpoint> endpoints;
        String rack = null;
        if (version == 1) {
            String host = node.get(HOST).asText();
            int port = node.get(PORT).asInt();
            endpoints = Collections.singletonList(new Endpoint(host, port, DEFAULT_LISTENER_NAME));
        } else if (version == 2) {
            endpoints = Endpoint.fromListenersString(node.get(LISTENERS).asText());
        } else {
            endpoints = Endpoint.fromListenersString(node.get(LISTENERS).asText());
            if (node.has(RACK)) {
                rack = node.get(RACK).asText();
            }
        }

        long registerTimestamp = node.get(REGISTER_TIMESTAMP).asLong();
        return new TabletServerRegistration(rack, endpoints, registerTimestamp);
    }
}
