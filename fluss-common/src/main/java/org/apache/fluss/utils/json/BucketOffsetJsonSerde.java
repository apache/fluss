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

import org.apache.fluss.lake.committer.BucketOffset;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** Json serializer and deserializer for {@link BucketOffset}. */
public class BucketOffsetJsonSerde
        implements JsonSerializer<BucketOffset>, JsonDeserializer<BucketOffset> {

    public static final BucketOffsetJsonSerde INSTANCE = new BucketOffsetJsonSerde();
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKET_ID = "bucket_id";
    private static final String PARTITION_NAME = "partition_name";
    private static final String LOG_OFFSET = "log_offset";

    @Override
    public BucketOffset deserialize(JsonNode node) {
        JsonNode partitionIdNode = node.get(PARTITION_ID);
        Long partitionId = partitionIdNode == null ? null : partitionIdNode.asLong();
        int bucketId = node.get(BUCKET_ID).asInt();

        // deserialize partition name
        JsonNode partitionNameNode = node.get(PARTITION_NAME);
        String partitionName = partitionNameNode == null ? null : partitionNameNode.asText();

        // deserialize log offset
        long logOffset = node.get(LOG_OFFSET).asLong();

        return new BucketOffset(logOffset, bucketId, partitionId, partitionName);
    }

    @Override
    public void serialize(BucketOffset bucketOffset, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // write partition id
        if (bucketOffset.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID, bucketOffset.getPartitionId());
        }
        generator.writeNumberField(BUCKET_ID, bucketOffset.getBucket());

        // serialize partition name
        if (bucketOffset.getPartitionQualifiedName() != null) {
            generator.writeStringField(PARTITION_NAME, bucketOffset.getPartitionQualifiedName());
        }

        // serialize bucket offset
        generator.writeNumberField(LOG_OFFSET, bucketOffset.getLogOffset());

        generator.writeEndObject();
    }
}
