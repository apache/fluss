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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Json serializer and deserializer for {@link LeaderAndIsr}. */
@Internal
public class LeaderAndIsrJsonSerde
        implements JsonSerializer<LeaderAndIsr>, JsonDeserializer<LeaderAndIsr> {

    public static final LeaderAndIsrJsonSerde INSTANCE = new LeaderAndIsrJsonSerde();
    private static final String VERSION_KEY = "version";
    private static final int VERSION = 2;

    private static final String LEADER = "leader";
    private static final String LEADER_EPOCH = "leader_epoch";
    private static final String ISR = "isr";
    private static final String COORDINATOR_EPOCH = "coordinator_epoch";
    private static final String BUCKET_EPOCH = "bucket_epoch";
    private static final String HOT_STANDBY_REPLICAS = "hot_standby_replicas";
    private static final String ISSR = "issr";

    @Override
    public void serialize(LeaderAndIsr leaderAndIsr, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(LEADER, leaderAndIsr.leader());
        generator.writeNumberField(LEADER_EPOCH, leaderAndIsr.leaderEpoch());
        generator.writeArrayFieldStart(ISR);
        for (Integer replica : leaderAndIsr.isr()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();
        generator.writeNumberField(COORDINATOR_EPOCH, leaderAndIsr.coordinatorEpoch());
        generator.writeNumberField(BUCKET_EPOCH, leaderAndIsr.bucketEpoch());

        generator.writeArrayFieldStart(HOT_STANDBY_REPLICAS);
        for (Integer replica : leaderAndIsr.standbyList()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();

        generator.writeArrayFieldStart(ISSR);
        for (Integer replica : leaderAndIsr.issr()) {
            generator.writeNumber(replica);
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }

    @Override
    public LeaderAndIsr deserialize(JsonNode node) {
        int version = node.get(VERSION_KEY).asInt();
        int leader = node.get(LEADER).asInt();
        int leaderEpoch = node.get(LEADER_EPOCH).asInt();
        int coordinatorEpoch = node.get(COORDINATOR_EPOCH).asInt();
        int bucketEpoch = node.get(BUCKET_EPOCH).asInt();
        List<Integer> isr = new ArrayList<>();
        Iterator<JsonNode> isrNodes = node.get(ISR).elements();
        while (isrNodes.hasNext()) {
            isr.add(isrNodes.next().asInt());
        }

        List<Integer> hotStandbyList = new ArrayList<>();
        List<Integer> iss = new ArrayList<>();
        if (version > 1) {
            Iterator<JsonNode> hotStandbyListNodes = node.get(HOT_STANDBY_REPLICAS).elements();
            while (hotStandbyListNodes.hasNext()) {
                hotStandbyList.add(hotStandbyListNodes.next().asInt());
            }

            Iterator<JsonNode> issNodes = node.get(ISSR).elements();
            while (issNodes.hasNext()) {
                iss.add(issNodes.next().asInt());
            }
        }

        return new LeaderAndIsr.Builder()
                .leader(leader)
                .leaderEpoch(leaderEpoch)
                .isr(isr)
                .coordinatorEpoch(coordinatorEpoch)
                .bucketEpoch(bucketEpoch)
                .standbyReplicas(hotStandbyList)
                .issr(iss)
                .build();
    }
}
