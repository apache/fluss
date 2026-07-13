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
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Json serializer and deserializer for {@link RebalanceRound}. */
public class RebalanceRoundJsonSerde
        implements JsonSerializer<RebalanceRound>, JsonDeserializer<RebalanceRound> {

    public static final RebalanceRoundJsonSerde INSTANCE = new RebalanceRoundJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String ROUND_INDEX = "round_index";
    private static final String REBALANCE_PLAN = "rebalance_plan";

    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";

    private static final String BUCKETS = "buckets";
    private static final String BUCKET_ID = "bucket_id";
    private static final String ORIGINAL_LEADER = "original_leader";
    private static final String NEW_LEADER = "new_leader";
    private static final String ORIGIN_REPLICAS = "origin_replicas";
    private static final String NEW_REPLICAS = "new_replicas";
    private static final String REBALANCE_STATUS = "rebalance_status";

    private static final int VERSION = 1;

    @Override
    public void serialize(RebalanceRound rebalanceRound, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(ROUND_INDEX, rebalanceRound.getRoundIndex());

        Map<Long, List<RebalanceResultForBucket>> resultsForBuckets = new LinkedHashMap<>();
        Map<TablePartition, List<RebalanceResultForBucket>> resultsForBucketsOfPartitionedTable =
                new LinkedHashMap<>();
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                rebalanceRound.getProgressForBucketMap().entrySet()) {
            TableBucket tableBucket = entry.getKey();
            if (tableBucket.getPartitionId() == null) {
                resultsForBuckets
                        .computeIfAbsent(tableBucket.getTableId(), k -> new ArrayList<>())
                        .add(entry.getValue());
            } else {
                resultsForBucketsOfPartitionedTable
                        .computeIfAbsent(
                                new TablePartition(
                                        tableBucket.getTableId(), tableBucket.getPartitionId()),
                                k -> new ArrayList<>())
                        .add(entry.getValue());
            }
        }

        generator.writeArrayFieldStart(REBALANCE_PLAN);
        for (Map.Entry<Long, List<RebalanceResultForBucket>> entry : resultsForBuckets.entrySet()) {
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID, entry.getKey());
            generator.writeArrayFieldStart(BUCKETS);
            for (RebalanceResultForBucket resultForBucket : entry.getValue()) {
                serializeRebalanceResultForBucket(generator, resultForBucket);
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }
        for (Map.Entry<TablePartition, List<RebalanceResultForBucket>> entry :
                resultsForBucketsOfPartitionedTable.entrySet()) {
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID, entry.getKey().getTableId());
            generator.writeNumberField(PARTITION_ID, entry.getKey().getPartitionId());
            generator.writeArrayFieldStart(BUCKETS);
            for (RebalanceResultForBucket resultForBucket : entry.getValue()) {
                serializeRebalanceResultForBucket(generator, resultForBucket);
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public RebalanceRound deserialize(JsonNode node) {
        int roundIndex = node.get(ROUND_INDEX).asInt();
        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new LinkedHashMap<>();
        for (JsonNode tablePartitionPlanNode : node.get(REBALANCE_PLAN)) {
            long tableId = tablePartitionPlanNode.get(TABLE_ID).asLong();

            Long partitionId = null;
            if (tablePartitionPlanNode.has(PARTITION_ID)) {
                partitionId = tablePartitionPlanNode.get(PARTITION_ID).asLong();
            }

            for (JsonNode bucketPlanNode : tablePartitionPlanNode.get(BUCKETS)) {
                int bucketId = bucketPlanNode.get(BUCKET_ID).asInt();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                RebalancePlanForBucket planForBucket =
                        new RebalancePlanForBucket(
                                tableBucket,
                                bucketPlanNode.get(ORIGINAL_LEADER).asInt(),
                                bucketPlanNode.get(NEW_LEADER).asInt(),
                                toIntegerList(bucketPlanNode.get(ORIGIN_REPLICAS)),
                                toIntegerList(bucketPlanNode.get(NEW_REPLICAS)));
                RebalanceStatus rebalanceStatus =
                        RebalanceStatus.of(bucketPlanNode.get(REBALANCE_STATUS).asInt());
                progressForBucketMap.put(
                        tableBucket, RebalanceResultForBucket.of(planForBucket, rebalanceStatus));
            }
        }
        return new RebalanceRound(roundIndex, progressForBucketMap);
    }

    private void serializeRebalanceResultForBucket(
            JsonGenerator generator, RebalanceResultForBucket resultForBucket) throws IOException {
        RebalancePlanForBucket bucketPlan = resultForBucket.plan();
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
        generator.writeNumberField(REBALANCE_STATUS, resultForBucket.status().getCode());
        generator.writeEndObject();
    }

    private List<Integer> toIntegerList(JsonNode node) {
        List<Integer> values = new ArrayList<>();
        Iterator<JsonNode> elements = node.elements();
        while (elements.hasNext()) {
            values.add(elements.next().asInt());
        }
        return values;
    }
}
