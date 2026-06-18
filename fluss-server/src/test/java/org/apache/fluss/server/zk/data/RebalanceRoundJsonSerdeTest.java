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
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RebalanceRoundJsonSerde}. */
class RebalanceRoundJsonSerdeTest extends JsonSerdeTestBase<RebalanceRound> {

    RebalanceRoundJsonSerdeTest() {
        super(RebalanceRoundJsonSerde.INSTANCE);
    }

    @Override
    protected RebalanceRound[] createObjects() {
        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new LinkedHashMap<>();
        putProgress(progressForBucketMap, new TableBucket(0L, 0), 0, 3, COMPLETED);
        putProgress(progressForBucketMap, new TableBucket(0L, 1), 1, 1, REBALANCING);
        putProgress(progressForBucketMap, new TableBucket(1L, 10L, 0), 0, 3, NOT_STARTED);
        putProgress(progressForBucketMap, new TableBucket(1L, 10L, 1), 1, 1, COMPLETED);
        return new RebalanceRound[] {new RebalanceRound(2, progressForBucketMap)};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"round_index\":2,\"rebalance_plan\":"
                    + "[{\"table_id\":0,\"buckets\":["
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5],\"rebalance_status\":3},"
                    + "{\"bucket_id\":1,\"original_leader\":1,\"new_leader\":1,\"origin_replicas\":[0,1,2],\"new_replicas\":[1,2,3],\"rebalance_status\":1}]},"
                    + "{\"table_id\":1,\"partition_id\":10,\"buckets\":["
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5],\"rebalance_status\":0},"
                    + "{\"bucket_id\":1,\"original_leader\":1,\"new_leader\":1,\"origin_replicas\":[0,1,2],\"new_replicas\":[1,2,3],\"rebalance_status\":3}]}]}"
        };
    }

    @Test
    void testHashCodeIgnoresProgressMapIterationOrder() {
        Map<TableBucket, RebalanceResultForBucket> firstProgressForBucketMap =
                new LinkedHashMap<>();
        Map<TableBucket, RebalanceResultForBucket> secondProgressForBucketMap =
                new LinkedHashMap<>();
        TableBucket firstBucket = new TableBucket(0L, 0);
        TableBucket secondBucket = new TableBucket(1L, 10L, 1);

        putProgress(firstProgressForBucketMap, firstBucket, 0, 3, COMPLETED);
        putProgress(firstProgressForBucketMap, secondBucket, 1, 1, REBALANCING);
        putProgress(secondProgressForBucketMap, secondBucket, 1, 1, REBALANCING);
        putProgress(secondProgressForBucketMap, firstBucket, 0, 3, COMPLETED);

        RebalanceRound firstRound = new RebalanceRound(1, firstProgressForBucketMap);
        RebalanceRound secondRound = new RebalanceRound(1, secondProgressForBucketMap);

        assertThat(firstRound).isEqualTo(secondRound);
        assertThat(firstRound).hasSameHashCodeAs(secondRound);
    }

    private void putProgress(
            Map<TableBucket, RebalanceResultForBucket> progressForBucketMap,
            TableBucket tableBucket,
            int originalLeader,
            int newLeader,
            RebalanceStatus rebalanceStatus) {
        RebalancePlanForBucket planForBucket =
                new RebalancePlanForBucket(
                        tableBucket,
                        originalLeader,
                        newLeader,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(newLeader, newLeader + 1, newLeader + 2));
        progressForBucketMap.put(
                tableBucket, RebalanceResultForBucket.of(planForBucket, rebalanceStatus));
    }
}
