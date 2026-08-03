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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the response checks that let {@link CoordinatorEventProcessor} complete a rebalance. */
class RebalanceResponseCheckTest {

    @Test
    void testLeaderOnlyRebalanceCompletionCheckRequiresSuccessfulResponseFromNewLeader() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        RebalancePlanForBucket planForBucket =
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 0, 2));
        NotifyLeaderAndIsrResultForBucket successResult =
                new NotifyLeaderAndIsrResultForBucket(tableBucket);
        NotifyLeaderAndIsrResultForBucket failedResult =
                new NotifyLeaderAndIsrResultForBucket(
                        tableBucket, new ApiError(Errors.UNKNOWN_SERVER_ERROR, "failed"));

        assertThat(
                        CoordinatorEventProcessor
                                .isSuccessfulLeaderOnlyRebalanceResponseFromNewLeader(
                                        successResult, 1, planForBucket))
                .isTrue();
        assertThat(
                        CoordinatorEventProcessor
                                .isSuccessfulLeaderOnlyRebalanceResponseFromNewLeader(
                                        successResult, 0, planForBucket))
                .isFalse();
        assertThat(
                        CoordinatorEventProcessor
                                .isSuccessfulLeaderOnlyRebalanceResponseFromNewLeader(
                                        failedResult, 1, planForBucket))
                .isFalse();
    }
}
