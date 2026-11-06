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
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RebalanceTaskJsonSerde}. */
public class RebalanceTaskJsonSerdeTest extends JsonSerdeTestBase<RebalanceTask> {

    RebalanceTaskJsonSerdeTest() {
        super(RebalanceTaskJsonSerde.INSTANCE);
    }

    @Override
    protected RebalanceTask[] createObjects() {
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
        TableBucket failedBucket = new TableBucket(2L, 0);
        Map<TableBucket, RebalancePlanForBucket> failedBucketPlan =
                Collections.singletonMap(
                        failedBucket,
                        new RebalancePlanForBucket(
                                failedBucket, 0, 1, Arrays.asList(0, 1), Arrays.asList(1, 0)));
        return new RebalanceTask[] {
            new RebalanceTask("rebalance-task-21jd", NOT_STARTED, bucketPlan, 1000L, 2000L),
            // A finished task whose bucket ended with a status other than the task status.
            new RebalanceTask(
                    "rebalance-task-bucket-status",
                    COMPLETED,
                    failedBucketPlan,
                    Collections.singletonMap(failedBucket, FAILED),
                    1000L,
                    2000L)
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":2,\"rebalance_id\":\"rebalance-task-21jd\",\"rebalance_status\":0,"
                    + "\"started_at_ms\":1000,\"completed_at_ms\":2000,\"rebalance_plan\":"
                    + "[{\"table_id\":0,\"buckets\":"
                    + "[{\"bucket_id\":1,\"original_leader\":1,\"new_leader\":1,\"origin_replicas\":[0,1,2],\"new_replicas\":[1,2,3]},"
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]}]},"
                    + "{\"table_id\":1,\"partition_id\":0,\"buckets\":["
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]},"
                    + "{\"bucket_id\":1,\"original_leader\":1,\"new_leader\":1,\"origin_replicas\":[0,1,2],\"new_replicas\":[1,2,3]}]},"
                    + "{\"table_id\":1,\"partition_id\":1,\"buckets\":["
                    + "{\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]}]}]}",
            "{\"version\":2,\"rebalance_id\":\"rebalance-task-bucket-status\",\"rebalance_status\":3,"
                    + "\"started_at_ms\":1000,\"completed_at_ms\":2000,\"rebalance_plan\":"
                    + "[{\"table_id\":2,\"buckets\":"
                    + "[{\"bucket_id\":0,\"bucket_status\":2,\"original_leader\":0,\"new_leader\":1,\"origin_replicas\":[0,1],\"new_replicas\":[1,0]}]}]}"
        };
    }

    @Test
    void testVersion1Compatibility() throws IOException {
        // A version-1 document has no started_at_ms/completed_at_ms fields; both must
        // deserialize as -1.
        String v1Json =
                "{\"version\":1,\"rebalance_id\":\"rebalance-task-21jd\",\"rebalance_status\":0,"
                        + "\"rebalance_plan\":[]}";
        RebalanceTask actual =
                JsonSerdeUtils.readValue(
                        v1Json.getBytes(StandardCharsets.UTF_8), RebalanceTaskJsonSerde.INSTANCE);

        assertThat(actual.getStartedAtMs()).isEqualTo(-1L);
        assertThat(actual.getCompletedAtMs()).isEqualTo(-1L);
    }

    @Test
    void testUnknownBucketStatusFallsBackToTaskStatus() throws IOException {
        String json =
                "{\"version\":2,\"rebalance_id\":\"r\",\"rebalance_status\":3,"
                        + "\"rebalance_plan\":[{\"table_id\":2,\"buckets\":[{\"bucket_id\":0,"
                        + "\"bucket_status\":99,\"original_leader\":0,\"new_leader\":1,"
                        + "\"origin_replicas\":[0,1],\"new_replicas\":[1,0]}]}]}";
        RebalanceTask actual =
                JsonSerdeUtils.readValue(
                        json.getBytes(StandardCharsets.UTF_8), RebalanceTaskJsonSerde.INSTANCE);

        assertThat(actual.getBucketStatus(new TableBucket(2L, 0))).isEqualTo(COMPLETED);
    }

    @Test
    void testBucketStatusEqualToTaskStatusIsDropped() {
        TableBucket tb = new TableBucket(2L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan =
                Collections.singletonMap(
                        tb,
                        new RebalancePlanForBucket(tb, 0, 1, Arrays.asList(0), Arrays.asList(1)));
        RebalanceTask withOwnStatus =
                new RebalanceTask(
                        "r",
                        COMPLETED,
                        plan,
                        Collections.singletonMap(tb, COMPLETED),
                        1000L,
                        2000L);
        RebalanceTask withoutOwnStatus = new RebalanceTask("r", COMPLETED, plan, 1000L, 2000L);

        assertThat(withOwnStatus).isEqualTo(withoutOwnStatus);
        assertThat(withOwnStatus.hashCode()).isEqualTo(withoutOwnStatus.hashCode());
        assertThat(withOwnStatus.getBucketStatus(tb)).isEqualTo(COMPLETED);
    }
}
