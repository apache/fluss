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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Json serializer and deserializer for {@link CoordinatorAddress}. */
@Internal
public class CoordinatorAddressJsonSerde
        implements JsonSerializer<CoordinatorAddress>, JsonDeserializer<CoordinatorAddress> {

    public static final CoordinatorAddressJsonSerde INSTANCE = new CoordinatorAddressJsonSerde();
    private static final String VERSION_KEY = "version";
    private static final int VERSION = 3;

    private static final String ID = "id";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String LISTENERS = "listeners";
    private static final String SUPPORTED_FEATURES = "supported_features";

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
        generator.writeArrayFieldStart(SUPPORTED_FEATURES);
        for (String feature : new TreeSet<>(coordinatorAddress.getSupportedFeatures())) {
            generator.writeString(feature);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public CoordinatorAddress deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        String id = node.get(ID).asText();
        List<Endpoint> endpoints;
        Set<String> supportedFeatures = new HashSet<>();
        if (version == 1) {
            String host = node.get(HOST).asText();
            int port = node.get(PORT).asInt();
            endpoints = Collections.singletonList(new Endpoint(host, port, "CLIENT"));
        } else {
            endpoints = Endpoint.fromListenersString(node.get(LISTENERS).asText());
            JsonNode features = node.get(SUPPORTED_FEATURES);
            if (features != null) {
                Iterator<JsonNode> elements = features.elements();
                while (elements.hasNext()) {
                    supportedFeatures.add(elements.next().asText());
                }
            }
        }
        return new CoordinatorAddress(id, endpoints, supportedFeatures);
    }
}
