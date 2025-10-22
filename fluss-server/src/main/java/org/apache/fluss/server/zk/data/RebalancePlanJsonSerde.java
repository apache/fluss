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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Json serializer and deserializer for {@link RebalancePlan}. */
public class RebalancePlanJsonSerde
        implements JsonSerializer<RebalancePlan>, JsonDeserializer<RebalancePlan> {

    public static final RebalancePlanJsonSerde INSTANCE = new RebalancePlanJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String REBALANCE_PLAN = "rebalance_plan";

    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";

    private static final String BUCKETS = "buckets";
    private static final String BUCKET_ID = "bucket_id";
    private static final String ORIGINAL_LEADER = "original_leader";
    private static final String NEW_LEADER = "new_leader";
    private static final String ORIGIN_REPLICAS = "origin_replicas";
    private static final String NEW_REPLICAS = "new_replicas";

    private static final int VERSION = 1;

    @Override
    public void serialize(RebalancePlan rebalancePlan, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);

        generator.writeArrayFieldStart(REBALANCE_PLAN);
        // first to write none-partitioned tables.
        for (Map.Entry<Long, List<RebalancePlanForBucket>> entry :
                rebalancePlan.getPlanForBuckets().entrySet()) {
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID, entry.getKey());
            generator.writeArrayFieldStart(BUCKETS);
            for (RebalancePlanForBucket bucketPlan : entry.getValue()) {
                serializeRebalancePlanForBucket(generator, bucketPlan);
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }

        // then to write partitioned tables.
        for (Map.Entry<TablePartition, List<RebalancePlanForBucket>> entry :
                rebalancePlan.getPlanForBucketsOfPartitionedTable().entrySet()) {
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID, entry.getKey().getTableId());
            generator.writeNumberField(PARTITION_ID, entry.getKey().getPartitionId());
            generator.writeArrayFieldStart(BUCKETS);
            for (RebalancePlanForBucket bucketPlan : entry.getValue()) {
                serializeRebalancePlanForBucket(generator, bucketPlan);
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }

        generator.writeEndArray();

        generator.writeEndObject();
    }

    @Override
    public RebalancePlan deserialize(JsonNode node) {
        JsonNode rebalancePlanNode = node.get(REBALANCE_PLAN);
        Map<TableBucket, RebalancePlanForBucket> planForBuckets = new HashMap<>();

        for (JsonNode tablePartitionPlanNode : rebalancePlanNode) {
            long tableId = tablePartitionPlanNode.get(TABLE_ID).asLong();

            Long partitionId = null;
            if (tablePartitionPlanNode.has(PARTITION_ID)) {
                partitionId = tablePartitionPlanNode.get(PARTITION_ID).asLong();
            }

            JsonNode bucketPlanNodes = tablePartitionPlanNode.get(BUCKETS);
            for (JsonNode bucketPlanNode : bucketPlanNodes) {
                int bucketId = bucketPlanNode.get(BUCKET_ID).asInt();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

                int originLeader = bucketPlanNode.get(ORIGINAL_LEADER).asInt();

                int newLeader = bucketPlanNode.get(NEW_LEADER).asInt();

                List<Integer> originReplicas = new ArrayList<>();
                Iterator<JsonNode> elements = bucketPlanNode.get(ORIGIN_REPLICAS).elements();
                while (elements.hasNext()) {
                    originReplicas.add(elements.next().asInt());
                }

                List<Integer> newReplicas = new ArrayList<>();
                elements = bucketPlanNode.get(NEW_REPLICAS).elements();
                while (elements.hasNext()) {
                    newReplicas.add(elements.next().asInt());
                }

                planForBuckets.put(
                        tableBucket,
                        new RebalancePlanForBucket(
                                tableBucket, originLeader, newLeader, originReplicas, newReplicas));
            }
        }

        return new RebalancePlan(planForBuckets);
    }

    private void serializeRebalancePlanForBucket(
            JsonGenerator generator, RebalancePlanForBucket bucketPlan) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(BUCKET_ID, bucketPlan.getBucketId());
        generator.writeNumberField(ORIGINAL_LEADER, bucketPlan.getOriginalLeader());
        generator.writeNumberField(NEW_LEADER, bucketPlan.getNewLeader());
        generator.writeArrayFieldStart(ORIGIN_REPLICAS);
        for (Integer replica : bucketPlan.getOriginReplicas()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();
        generator.writeArrayFieldStart(NEW_REPLICAS);
        for (Integer replica : bucketPlan.getNewReplicas()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }
}
