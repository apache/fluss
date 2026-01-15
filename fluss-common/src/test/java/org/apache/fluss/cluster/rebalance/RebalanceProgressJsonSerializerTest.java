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

package org.apache.fluss.cluster.rebalance;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RebalanceProgressJsonSerializer}. */
public class RebalanceProgressJsonSerializerTest {

    @Test
    public void testSerializer() {
        String serialize =
                new String(
                        JsonSerdeUtils.writeValueAsBytes(
                                createProgressObj(), RebalanceProgressJsonSerializer.INSTANCE),
                        StandardCharsets.UTF_8);
        assertThat(serialize).isEqualTo(createProgressJson());
    }

    private RebalanceProgress createProgressObj() {
        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new HashMap<>();
        progressForBucketMap.put(
                new TableBucket(0L, 0),
                RebalanceResultForBucket.of(
                        new RebalancePlanForBucket(
                                new TableBucket(0L, 0),
                                0,
                                3,
                                Arrays.asList(0, 1, 2),
                                Arrays.asList(3, 4, 5)),
                        RebalanceStatus.COMPLETED));
        progressForBucketMap.put(
                new TableBucket(1L, 0L, 0),
                RebalanceResultForBucket.of(
                        new RebalancePlanForBucket(
                                new TableBucket(1L, 0L, 0),
                                0,
                                3,
                                Arrays.asList(0, 1, 2),
                                Arrays.asList(3, 4, 5)),
                        RebalanceStatus.COMPLETED));
        return new RebalanceProgress(
                "rebalance-task-21jd", RebalanceStatus.COMPLETED, 1d, progressForBucketMap);
    }

    private String createProgressJson() {
        return "{\"rebalance_id\":\"rebalance-task-21jd\",\"rebalance_status\":3,\"progress\":\"100%\",\"progress_for_buckets\":"
                + "[{\"table_id\":1,\"bucket_id\":0,\"partition_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5],\"rebalance_status\":3},"
                + "{\"table_id\":0,\"bucket_id\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5],\"rebalance_status\":3}]}";
    }
}
