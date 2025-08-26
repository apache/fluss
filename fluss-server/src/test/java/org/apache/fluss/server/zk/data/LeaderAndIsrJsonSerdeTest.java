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

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.Collections;

/** Test for {@link org.apache.fluss.server.zk.data.LeaderAndIsrJsonSerde}. */
public class LeaderAndIsrJsonSerdeTest extends JsonSerdeTestBase<LeaderAndIsr> {

    LeaderAndIsrJsonSerdeTest() {
        super(LeaderAndIsrJsonSerde.INSTANCE);
    }

    @Override
    protected LeaderAndIsr[] createObjects() {
        LeaderAndIsr leaderAndIsr1 = new LeaderAndIsr(1, 10, Arrays.asList(1, 2, 3), 100, 1000);
        LeaderAndIsr leaderAndIsr2 = new LeaderAndIsr(2, 20, Collections.emptyList(), 200, 2000);
        return new LeaderAndIsr[] {leaderAndIsr1, leaderAndIsr2};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"leader\":1,\"leader_epoch\":10,\"isr\":[1,2,3],\"coordinator_epoch\":100,\"bucket_epoch\":1000}",
            "{\"version\":1,\"leader\":2,\"leader_epoch\":20,\"isr\":[],\"coordinator_epoch\":200,\"bucket_epoch\":2000}"
        };
    }
}
