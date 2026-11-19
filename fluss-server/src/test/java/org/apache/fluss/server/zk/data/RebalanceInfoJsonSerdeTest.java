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

import org.apache.fluss.cluster.rebalance.RebalanceInfo;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RebalanceInfoJsonSerde}. */
public class RebalanceInfoJsonSerdeTest extends JsonSerdeTestBase<RebalanceInfo> {

    RebalanceInfoJsonSerdeTest() {
        super(RebalanceInfoJsonSerde.INSTANCE);
    }

    @Override
    protected RebalanceInfo[] createObjects() {
        return new RebalanceInfo[] {
            new RebalanceInfo("rebalance-completed", COMPLETED, 1000L, 2000L, null),
            new RebalanceInfo("rebalance-failed", FAILED, 1000L, 2000L, "no progress")
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"rebalance_id\":\"rebalance-completed\",\"rebalance_status\":3,"
                    + "\"started_at_ms\":1000,\"completed_at_ms\":2000}",
            "{\"version\":1,\"rebalance_id\":\"rebalance-failed\",\"rebalance_status\":2,"
                    + "\"started_at_ms\":1000,\"completed_at_ms\":2000,\"failure_reason\":\"no progress\"}"
        };
    }

    @Test
    void testUnknownStatusCodeFailsToDeserialize() {
        String unknownStatusJson =
                expectedJsons()[0].replace("\"rebalance_status\":3", "\"rebalance_status\":99");
        assertThatThrownBy(
                        () ->
                                JsonSerdeUtils.readValue(
                                        unknownStatusJson.getBytes(StandardCharsets.UTF_8),
                                        RebalanceInfoJsonSerde.INSTANCE))
                .isInstanceOf(NullPointerException.class);
    }
}
