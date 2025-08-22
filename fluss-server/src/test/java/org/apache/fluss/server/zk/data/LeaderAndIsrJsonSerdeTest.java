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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

/** Test for {@link org.apache.fluss.server.zk.data.LeaderAndIsrJsonSerde}. */
public class LeaderAndIsrJsonSerdeTest extends JsonSerdeTestBase<LeaderAndIsr> {

    LeaderAndIsrJsonSerdeTest() {
        super(LeaderAndIsrJsonSerde.INSTANCE);
    }

    @Override
    protected LeaderAndIsr[] createObjects() {

        LeaderAndIsr leaderAndIsr1 =
                new LeaderAndIsr.Builder()
                        .leader(1)
                        .leaderEpoch(10)
                        .isr(Arrays.asList(1, 2, 3))
                        .coordinatorEpoch(100)
                        .bucketEpoch(1000)
                        .build();
        LeaderAndIsr leaderAndIsr2 =
                new LeaderAndIsr.Builder()
                        .leader(2)
                        .leaderEpoch(20)
                        .coordinatorEpoch(200)
                        .bucketEpoch(2000)
                        .build();
        LeaderAndIsr leaderAndIsr3 =
                new LeaderAndIsr.Builder()
                        .leader(1)
                        .leaderEpoch(10)
                        .isr(Arrays.asList(1, 2, 3))
                        .coordinatorEpoch(10)
                        .bucketEpoch(10)
                        .standbyReplicas(Collections.singletonList(2))
                        .issr(Collections.singletonList(2))
                        .build();
        return new LeaderAndIsr[] {leaderAndIsr1, leaderAndIsr2, leaderAndIsr3};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":2,\"leader\":1,\"leader_epoch\":10,\"isr\":[1,2,3],\"coordinator_epoch\":100,\"bucket_epoch\":1000,"
                    + "\"hot_standby_replicas\":[],\"issr\":[]}",
            "{\"version\":2,\"leader\":2,\"leader_epoch\":20,\"isr\":[],\"coordinator_epoch\":200,\"bucket_epoch\":2000,"
                    + "\"hot_standby_replicas\":[],\"issr\":[]}",
            "{\"version\":2,\"leader\":1,\"leader_epoch\":10,\"isr\":[1,2,3],\"coordinator_epoch\":10,\"bucket_epoch\":10,"
                    + "\"hot_standby_replicas\":[2],\"issr\":[2]}",
        };
    }

    @Test
    void testCompatibility() throws IOException {
        // compatibility with version 1
        JsonNode jsonInVersion1 =
                new ObjectMapper()
                        .readTree(
                                "{\"version\":1,\"leader\":1,\"leader_epoch\":10,\"isr\":[1,2,3],\"coordinator_epoch\":100,\"bucket_epoch\":1000}"
                                        .getBytes(StandardCharsets.UTF_8));
        LeaderAndIsr leaderAndIsr = LeaderAndIsrJsonSerde.INSTANCE.deserialize(jsonInVersion1);
        LeaderAndIsr expectedLeaderAndIsr =
                new LeaderAndIsr.Builder()
                        .leader(1)
                        .leaderEpoch(10)
                        .isr(Arrays.asList(1, 2, 3))
                        .coordinatorEpoch(100)
                        .bucketEpoch(1000)
                        .build();
        assertEquals(leaderAndIsr, expectedLeaderAndIsr);
    }
}
