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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Json serializer and deserializer for {@link TableAssignment}. */
@Internal
public class TableAssignmentJsonSerde
        implements JsonSerializer<TableAssignment>, JsonDeserializer<TableAssignment> {

    public static final TableAssignmentJsonSerde INSTANCE = new TableAssignmentJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String BUCKETS = "buckets";
    private static final int VERSION = 1;

    @Override
    public void serialize(TableAssignment tableAssignment, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        serializeBucketAssignments(generator, tableAssignment.getBucketAssignments());

        generator.writeEndObject();
    }

    @Override
    public TableAssignment deserialize(JsonNode node) {
        JsonNode bucketsNode = node.get(BUCKETS);
        Map<Integer, BucketAssignment> assignments = deserializeBucketAssignments(bucketsNode);
        return new TableAssignment(assignments);
    }

    public static void serializeBucketAssignments(
            JsonGenerator generator, Map<Integer, BucketAssignment> bucketAssignments)
            throws IOException {
        generator.writeObjectFieldStart(BUCKETS);
        for (Map.Entry<Integer, BucketAssignment> entry : bucketAssignments.entrySet()) {
            // write assignment for one bucket
            generator.writeArrayFieldStart(String.valueOf(entry.getKey()));
            for (Integer replica : entry.getValue().getReplicas()) {
                generator.writeNumber(replica);
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
    }

    public static Map<Integer, BucketAssignment> deserializeBucketAssignments(
            JsonNode bucketsNode) {
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        Iterator<String> fieldNames = bucketsNode.fieldNames();
        while (fieldNames.hasNext()) {
            String bucketId = fieldNames.next();
            Iterator<JsonNode> replicaNodes = bucketsNode.get(bucketId).elements();
            List<Integer> replicas = new ArrayList<>();
            while (replicaNodes.hasNext()) {
                replicas.add(replicaNodes.next().asInt());
            }
            bucketAssignments.put(Integer.valueOf(bucketId), new BucketAssignment(replicas));
        }
        return bucketAssignments;
    }
}
