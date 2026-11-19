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
import org.apache.fluss.cluster.rebalance.RebalanceInfo;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/**
 * Json serializer and deserializer for a {@link RebalanceInfo} stored as a rebalance history entry.
 * The entry has no per-bucket plan, so its size does not grow with the size of the rebalance.
 */
@Internal
public class RebalanceInfoJsonSerde
        implements JsonSerializer<RebalanceInfo>, JsonDeserializer<RebalanceInfo> {

    public static final RebalanceInfoJsonSerde INSTANCE = new RebalanceInfoJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    private static final String REBALANCE_ID = "rebalance_id";
    private static final String REBALANCE_STATUS = "rebalance_status";
    private static final String STARTED_AT_MS = "started_at_ms";
    private static final String COMPLETED_AT_MS = "completed_at_ms";
    private static final String FAILURE_REASON = "failure_reason";

    @Override
    public void serialize(RebalanceInfo rebalanceInfo, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(REBALANCE_ID, rebalanceInfo.rebalanceId());
        generator.writeNumberField(REBALANCE_STATUS, rebalanceInfo.status().getCode());
        generator.writeNumberField(STARTED_AT_MS, rebalanceInfo.startedAtMs());
        generator.writeNumberField(COMPLETED_AT_MS, rebalanceInfo.completedAtMs());
        if (rebalanceInfo.failureReason().isPresent()) {
            generator.writeStringField(FAILURE_REASON, rebalanceInfo.failureReason().get());
        }
        generator.writeEndObject();
    }

    /** Fails with a {@link NullPointerException} for a status code this version does not know. */
    @Override
    public RebalanceInfo deserialize(JsonNode node) {
        return new RebalanceInfo(
                node.get(REBALANCE_ID).asText(),
                RebalanceStatus.of(node.get(REBALANCE_STATUS).asInt()),
                node.get(STARTED_AT_MS).asLong(),
                node.get(COMPLETED_AT_MS).asLong(),
                node.has(FAILURE_REASON) ? node.get(FAILURE_REASON).asText() : null);
    }
}
