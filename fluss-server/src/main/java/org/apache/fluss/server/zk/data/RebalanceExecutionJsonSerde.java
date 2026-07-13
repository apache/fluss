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

import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link RebalanceExecution}. */
public class RebalanceExecutionJsonSerde
        implements JsonSerializer<RebalanceExecution>, JsonDeserializer<RebalanceExecution> {

    public static final RebalanceExecutionJsonSerde INSTANCE = new RebalanceExecutionJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String REBALANCE_ID = "rebalance_id";
    private static final String REBALANCE_STATUS = "rebalance_status";
    private static final String MAX_BUCKETS_PER_ROUND = "max_buckets_per_round";
    private static final String CURRENT_ROUND = "current_round";
    private static final String TOTAL_ROUNDS = "total_rounds";

    private static final int VERSION = 1;

    @Override
    public void serialize(RebalanceExecution execution, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(REBALANCE_ID, execution.getRebalanceId());
        generator.writeNumberField(REBALANCE_STATUS, execution.getRebalanceStatus().getCode());
        generator.writeNumberField(MAX_BUCKETS_PER_ROUND, execution.getMaxBucketsPerRound());
        generator.writeNumberField(CURRENT_ROUND, execution.getCurrentRound());
        generator.writeNumberField(TOTAL_ROUNDS, execution.getTotalRounds());
        generator.writeEndObject();
    }

    @Override
    public RebalanceExecution deserialize(JsonNode node) {
        return new RebalanceExecution(
                node.get(REBALANCE_ID).asText(),
                RebalanceStatus.of(node.get(REBALANCE_STATUS).asInt()),
                node.get(MAX_BUCKETS_PER_ROUND).asInt(),
                node.get(CURRENT_ROUND).asInt(),
                node.get(TOTAL_ROUNDS).asInt());
    }
}
