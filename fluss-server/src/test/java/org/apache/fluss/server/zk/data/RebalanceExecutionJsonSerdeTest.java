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

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;

/** Test for {@link RebalanceExecutionJsonSerde}. */
class RebalanceExecutionJsonSerdeTest extends JsonSerdeTestBase<RebalanceExecution> {

    RebalanceExecutionJsonSerdeTest() {
        super(RebalanceExecutionJsonSerde.INSTANCE);
    }

    @Override
    protected RebalanceExecution[] createObjects() {
        return new RebalanceExecution[] {
            new RebalanceExecution("rebalance-execution-1", REBALANCING, 100, 1, 3),
            new RebalanceExecution("rebalance-execution-2", CANCELED, 50, 0, 2)
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"rebalance_id\":\"rebalance-execution-1\",\"rebalance_status\":1,\"max_buckets_per_round\":100,\"current_round\":1,\"total_rounds\":3}",
            "{\"version\":1,\"rebalance_id\":\"rebalance-execution-2\",\"rebalance_status\":4,\"max_buckets_per_round\":50,\"current_round\":0,\"total_rounds\":2}"
        };
    }
}
