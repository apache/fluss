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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT test for adjust isr for tablet server. */
public class AdjustIsrTest extends ReplicaTestBase {

    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(3));
        return conf;
    }

    @Test
    void testExpandIsr() throws Exception {
        // replica set is 1,2,3 , isr set is 1.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb, Arrays.asList(1, 2, 3), Collections.singletonList(1), false);

        Replica replica = replicaManager.getReplicaOrException(tb);
        assertThat(replica.getIsr()).containsExactlyInAnyOrder(1);

        // 1. append one batch to leader.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        // mock follower 2 to fetch data from leader. fetch offset is 10 (which indicate the
        // follower catch up the leader, it will be added into isr list).
        replicaManager.fetchLogRecords(
                new FetchParams(
                        2, (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes()),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 10L, Integer.MAX_VALUE)),
                result -> {});
        retry(
                Duration.ofSeconds(20),
                () -> {
                    Replica replica1 = replicaManager.getReplicaOrException(tb);
                    assertThat(replica1.getIsr()).containsExactlyInAnyOrder(1, 2);
                });

        // mock follower 3 to fetch data from leader. fetch offset is 10 (which indicate the
        // follower catch up the leader, it will be added into isr list).
        replicaManager.fetchLogRecords(
                new FetchParams(
                        3, (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes()),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 10L, Integer.MAX_VALUE)),
                result -> {});
        retry(
                Duration.ofSeconds(20),
                () -> {
                    Replica replica1 = replicaManager.getReplicaOrException(tb);
                    assertThat(replica1.getIsr()).containsExactlyInAnyOrder(1, 2, 3);
                });
    }

    @Test
    void testShrinkIsr() {
        // replica set is 1,2,3 , isr set is 1,2,3.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb, Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3), false);

        Replica replica = replicaManager.getReplicaOrException(tb);
        assertThat(replica.getIsr()).containsExactlyInAnyOrder(1, 2, 3);

        // retry until shrink follower 2 and 3 out of isr set. As the scheduler will shrink isr
        // Periodic, follower 2 and 3 don't fetch data from leader, they will be removed from isr
        // list by the periodic shrink isr scheduler.
        retry(
                Duration.ofSeconds(20),
                () -> assertThat(replica.getIsr()).containsExactlyInAnyOrder(1));
    }

    @Test
    void testSubmitShrinkIsrAsLeaderFenced() throws Exception {
        // replica set is 1,2,3 , isr set is 1,2,3.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb, Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3), false);

        Replica replica = replicaManager.getReplicaOrException(tb);
        assertThat(replica.getIsr()).containsExactlyInAnyOrder(1, 2, 3);

        // To mock we prepare an isr shrink in Replica#maybeShrinkIsr();
        IsrState.PendingShrinkIsrState pendingShrinkIsrState =
                replica.prepareIsrShrink(
                        new IsrState.CommittedIsrState(Arrays.asList(1, 2, 3)),
                        Arrays.asList(1, 2),
                        Collections.singletonList(3));

        // Set leader epoch of this bucket in coordinatorServer gateway to 1 to mock leader epoch is
        // fenced.
        testCoordinatorGateway.setCurrentLeaderEpoch(tb, 1);
        assertThatThrownBy(
                        () ->
                                replica.submitAdjustIsr(pendingShrinkIsrState)
                                        .get(1, TimeUnit.MINUTES))
                .rootCause()
                .isInstanceOf(FencedLeaderEpochException.class)
                .hasMessageContaining("request leader epoch is fenced.");
    }
}
