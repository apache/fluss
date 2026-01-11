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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Json serializer and deserializer for {@link RebalancePlanForBucket}. */
public class RebalancePlanForBucketJsonSerde
        implements JsonSerializer<RebalancePlanForBucket>,
                JsonDeserializer<RebalancePlanForBucket> {

    public static final RebalancePlanForBucketJsonSerde INSTANCE =
            new RebalancePlanForBucketJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    private static final String TABLE_ID = "table_id";
    private static final String BUCKET_ID = "bucket_id";
    private static final String PARTITION_ID = "partition_id";

    private static final String ORIGINAL_LEADER = "original_leader";
    private static final String NEW_LEADER = "new_leader";
    private static final String ORIGIN_REPLICAS = "origin_replicas";
    private static final String NEW_REPLICAS = "new_replicas";

    @Override
    public RebalancePlanForBucket deserialize(JsonNode node) {
        long tableId = node.get(TABLE_ID).asLong();
        Long partitionId = null;
        if (node.has(PARTITION_ID)) {
            partitionId = node.get(PARTITION_ID).asLong();
        }
        int bucketId = node.get(BUCKET_ID).asInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        int originLeader = node.get(ORIGINAL_LEADER).asInt();
        int newLeader = node.get(NEW_LEADER).asInt();

        List<Integer> originReplicas = new ArrayList<>();
        Iterator<JsonNode> elements = node.get(ORIGIN_REPLICAS).elements();
        while (elements.hasNext()) {
            originReplicas.add(elements.next().asInt());
        }

        List<Integer> newReplicas = new ArrayList<>();
        elements = node.get(NEW_REPLICAS).elements();
        while (elements.hasNext()) {
            newReplicas.add(elements.next().asInt());
        }
        return new RebalancePlanForBucket(
                tableBucket, originLeader, newLeader, originReplicas, newReplicas);
    }

    @Override
    public void serialize(RebalancePlanForBucket rebalancePlanForBucket, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);

        TableBucket tableBucket = rebalancePlanForBucket.getTableBucket();
        generator.writeNumberField(TABLE_ID, tableBucket.getTableId());
        generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());
        Long partitionId = tableBucket.getPartitionId();
        if (null != partitionId) {
            generator.writeNumberField(PARTITION_ID, partitionId);
        }

        generator.writeNumberField(ORIGINAL_LEADER, rebalancePlanForBucket.getOriginalLeader());
        generator.writeNumberField(NEW_LEADER, rebalancePlanForBucket.getNewLeader());

        generator.writeArrayFieldStart(ORIGIN_REPLICAS);
        for (Integer replica : rebalancePlanForBucket.getOriginReplicas()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();

        generator.writeArrayFieldStart(NEW_REPLICAS);
        for (Integer replica : rebalancePlanForBucket.getNewReplicas()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }
}
