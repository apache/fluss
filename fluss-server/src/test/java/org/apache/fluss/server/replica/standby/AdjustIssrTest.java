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

package org.apache.fluss.server.replica.standby;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_KEY_TYPE;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatchWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** UT Test for adjust isrr(InsyncStandbyReplica). */
public class AdjustIssrTest extends ReplicaTestBase {
    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(3));
        return conf;
    }

    @Test
    void testExpandIssr() throws Exception {
        // replica set is 1,2,3 , isr set is 1. standbySet is 2.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 0);
        makeKvTableAsLeader(
                tb,
                Arrays.asList(1, 2, 3),
                Collections.singletonList(1),
                Collections.singletonList(2));

        Replica replica = replicaManager.getReplicaOrException(tb);
        assertThat(replica.getIsr()).containsExactlyInAnyOrder(1);
        assertThat(replica.getStandbyReplicas()).containsExactlyInAnyOrder(2);
        assertThat(replica.getIssr()).isEmpty();

        // 1. put records to leader.
        List<Tuple2<Object[], Object[]>> data1 =
                Arrays.asList(
                        Tuple2.of(new Object[] {1}, new Object[] {1, "a"}),
                        Tuple2.of(new Object[] {2}, new Object[] {2, "b"}),
                        Tuple2.of(new Object[] {3}, new Object[] {3, "c"}),
                        Tuple2.of(new Object[] {1}, new Object[] {1, "a1"}));
        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(
                        tb,
                        genKvRecordBatchWithWriterId(
                                data1, DATA1_KEY_TYPE, DATA1_ROW_TYPE, 100L, 0)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 5));
        assertThat(replicaManager.getReplicaOrException(tb).getLocalLogEndOffset()).isEqualTo(5L);

        // mock follower 3 (not standby replica) to fetch data from leader. fetch offset is 5
        // (which indicate the follower catch up the leader, it will be added into isr list).
        replicaManager.fetchLogRecords(
                new FetchParams(
                        3, (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes()),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 5L, Integer.MAX_VALUE)),
                result -> {});
        retry(
                Duration.ofSeconds(20),
                () -> {
                    Replica replica1 = replicaManager.getReplicaOrException(tb);
                    assertThat(replica1.getIsr()).containsExactlyInAnyOrder(1, 3);
                });

        // mock follower 2 (standby replica) to fetch data from leader. fetch offset is 5, and
        // kvAppliedOffset is 5. (which indicate the follower catch up the leader, it will be added
        // into isr list and issr list).
        replicaManager.fetchLogRecords(
                new FetchParams(
                        2, (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes()),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 5L, Integer.MAX_VALUE, null, 5L)),
                result -> {});
        retry(
                Duration.ofSeconds(20),
                () -> {
                    Replica replica1 = replicaManager.getReplicaOrException(tb);
                    assertThat(replica1.getIsr()).containsExactlyInAnyOrder(1, 2, 3);
                    assertThat(replica1.getIssr()).containsExactlyInAnyOrder(2);
                });
    }

    private void makeKvTableAsLeader(
            TableBucket tb, List<Integer> replicas, List<Integer> isr, List<Integer> standbyList) {
        makeLeaderAndFollower(
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                DATA1_PHYSICAL_TABLE_PATH_PK,
                                tb,
                                replicas,
                                new LeaderAndIsr.Builder()
                                        .leader(TABLET_SERVER_ID)
                                        .isr(isr)
                                        .standbyReplicas(standbyList)
                                        .build())));
    }
}
