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
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;

/** Test for {@link RebalancePlanJsonSerde}. */
public class RebalancePlanJsonSerdeTest extends JsonSerdeTestBase<RebalancePlan> {

    RebalancePlanJsonSerdeTest() {
        super(RebalancePlanJsonSerde.INSTANCE);
    }

    @Override
    protected RebalancePlan[] createObjects() {
        Map<TableBucket, RebalancePlanForBucket> bucketPlan = new HashMap<>();
        bucketPlan.put(
                new TableBucket(0L, 0),
                new RebalancePlanForBucket(
                        new TableBucket(0L, 0),
                        0,
                        3,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(3, 4, 5)));
        bucketPlan.put(
                new TableBucket(0L, 1),
                new RebalancePlanForBucket(
                        new TableBucket(0L, 1),
                        1,
                        1,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(1, 2, 3)));

        bucketPlan.put(
                new TableBucket(1L, 0L, 0),
                new RebalancePlanForBucket(
                        new TableBucket(1L, 0L, 0),
                        0,
                        3,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(3, 4, 5)));
        bucketPlan.put(
                new TableBucket(1L, 0L, 1),
                new RebalancePlanForBucket(
                        new TableBucket(1L, 0L, 1),
                        1,
                        1,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(1, 2, 3)));

        bucketPlan.put(
                new TableBucket(1L, 1L, 0),
                new RebalancePlanForBucket(
                        new TableBucket(1L, 1L, 0),
                        0,
                        3,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(3, 4, 5)));
        return new RebalancePlan[] {new RebalancePlan(NOT_STARTED, bucketPlan)};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"rebalance_status\":1,\"rebalance_plan\":"
                    + "[{\"table_id\":0,\"buckets\":"
                    + "[{\"bucket_id\":1,\"original_leader\":1,\"new_leader\":1,\"origin_replicas\":[0,1,2],\"new_replicas\":[1,2,3]},"
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]}]},"
                    + "{\"table_id\":1,\"partition_id\":0,\"buckets\":["
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]},"
                    + "{\"bucket_id\":1,\"original_leader\":1,\"new_leader\":1,\"origin_replicas\":[0,1,2],\"new_replicas\":[1,2,3]}]},"
                    + "{\"table_id\":1,\"partition_id\":1,\"buckets\":["
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]}]}]}"
        };
    }
}
