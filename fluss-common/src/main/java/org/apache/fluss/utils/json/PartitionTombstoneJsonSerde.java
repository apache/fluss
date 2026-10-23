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

package org.apache.fluss.utils.json;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/** Json serializer and deserializer for {@link PartitionTombstone}. */
@Internal
public class PartitionTombstoneJsonSerde
        implements JsonSerializer<PartitionTombstone>, JsonDeserializer<PartitionTombstone> {

    public static final PartitionTombstoneJsonSerde INSTANCE = new PartitionTombstoneJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String FLOOR_KEY = "floor";
    private static final String EXPLICIT_SET_KEY = "explicit_set";
    private static final String TOMBSTONE_VERSION_KEY = "tombstone_version";

    private static final int VERSION = 1;

    @Override
    public void serialize(PartitionTombstone tombstone, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize envelope version.
        generator.writeNumberField(VERSION_KEY, VERSION);

        // serialize floor.
        generator.writeNumberField(FLOOR_KEY, tombstone.getFloor());

        // serialize explicit_set.
        generator.writeArrayFieldStart(EXPLICIT_SET_KEY);
        for (Long pid : tombstone.getExplicitSet()) {
            generator.writeNumber(pid);
        }
        generator.writeEndArray();

        // serialize model version (separate from envelope version).
        generator.writeNumberField(TOMBSTONE_VERSION_KEY, tombstone.getVersion());

        generator.writeEndObject();
    }

    @Override
    public PartitionTombstone deserialize(JsonNode node) {
        long floor = node.get(FLOOR_KEY).asLong();
        long tombstoneVersion = node.get(TOMBSTONE_VERSION_KEY).asLong();
        Set<Long> explicitSet = new HashSet<>();
        JsonNode explicitSetNode = node.get(EXPLICIT_SET_KEY);
        if (explicitSetNode != null && explicitSetNode.isArray()) {
            Iterator<JsonNode> iter = explicitSetNode.elements();
            while (iter.hasNext()) {
                explicitSet.add(iter.next().asLong());
            }
        }
        return new PartitionTombstone(floor, explicitSet, tombstoneVersion);
    }
}
