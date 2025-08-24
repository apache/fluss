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
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Map;

/** Json serializer and deserializer for {@link PartitionAssignment}. */
@Internal
public class PartitionAssignmentJsonSerde
        implements JsonSerializer<PartitionAssignment>, JsonDeserializer<PartitionAssignment> {

    public static final PartitionAssignmentJsonSerde INSTANCE = new PartitionAssignmentJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String TABLE_ID = "table_id";
    private static final String BUCKETS = "buckets";
    private static final int VERSION = 1;

    @Override
    public void serialize(PartitionAssignment partitionAssignment, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);

        generator.writeNumberField(TABLE_ID, partitionAssignment.getTableId());
        TableAssignmentJsonSerde.serializeBucketAssignments(
                generator, partitionAssignment.getBucketAssignments());

        generator.writeEndObject();
    }

    @Override
    public PartitionAssignment deserialize(JsonNode node) {
        JsonNode bucketsNode = node.get(BUCKETS);
        Map<Integer, BucketAssignment> assignments =
                TableAssignmentJsonSerde.deserializeBucketAssignments(bucketsNode);
        long tableId = node.get(TABLE_ID).asLong();
        return new PartitionAssignment(tableId, assignments);
    }
}
