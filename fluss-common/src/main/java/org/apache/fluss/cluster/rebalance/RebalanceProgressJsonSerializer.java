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

package org.apache.fluss.cluster.rebalance;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.Map;

/** Json serializer for {@link RebalanceProgress}. */
public class RebalanceProgressJsonSerializer implements JsonSerializer<RebalanceProgress> {

    public static final RebalanceProgressJsonSerializer INSTANCE =
            new RebalanceProgressJsonSerializer();

    private static final String REBALANCE_ID = "rebalance_id";
    private static final String REBALANCE_STATUS = "rebalance_status";
    private static final String PROGRESS = "progress";
    private static final String PROGRESS_FOR_BUCKETS = "progress_for_buckets";

    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKET_ID = "bucket_id";
    private static final String ORIGINAL_LEADER = "original_leader";
    private static final String NEW_LEADER = "new_leader";
    private static final String ORIGIN_REPLICAS = "origin_replicas";
    private static final String NEW_REPLICAS = "new_replicas";

    @Override
    public void serialize(RebalanceProgress rebalanceProgress, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        generator.writeStringField(REBALANCE_ID, rebalanceProgress.rebalanceId());
        generator.writeNumberField(REBALANCE_STATUS, rebalanceProgress.status().getCode());
        generator.writeStringField(PROGRESS, rebalanceProgress.formatAsPercentage());

        Map<TableBucket, RebalanceResultForBucket> resultForBucketMap =
                rebalanceProgress.progressForBucketMap();

        // RebalanceProgress.progressForBucketMap
        generator.writeArrayFieldStart(PROGRESS_FOR_BUCKETS);
        for (RebalanceResultForBucket rebalanceResultForBucket : resultForBucketMap.values()) {
            TableBucket tableBucket = rebalanceResultForBucket.tableBucket();
            RebalancePlanForBucket plan = rebalanceResultForBucket.plan();

            // RebalanceResultForBucket.plan
            generator.writeStartObject();
            generator.writeNumberField(TABLE_ID, tableBucket.getTableId());
            generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());
            Long partitionId = tableBucket.getPartitionId();
            if (null != partitionId) {
                generator.writeNumberField(PARTITION_ID, partitionId);
            }
            generator.writeNumberField(ORIGINAL_LEADER, plan.getOriginalLeader());
            generator.writeNumberField(NEW_LEADER, plan.getNewLeader());
            generator.writeArrayFieldStart(ORIGIN_REPLICAS);
            for (Integer replica : plan.getOriginReplicas()) {
                generator.writeNumber(replica);
            }
            generator.writeEndArray();
            generator.writeArrayFieldStart(NEW_REPLICAS);
            for (Integer replica : plan.getNewReplicas()) {
                generator.writeNumber(replica);
            }
            generator.writeEndArray();

            // RebalanceResultForBucket.rebalanceStatus
            generator.writeNumberField(
                    REBALANCE_STATUS, rebalanceResultForBucket.status().getCode());

            generator.writeEndObject();
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }
}
