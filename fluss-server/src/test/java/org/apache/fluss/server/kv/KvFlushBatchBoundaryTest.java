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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.StorageBackpressureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests asynchronous KV flush boundaries through leader HW publication and WAL recovery. */
class KvFlushBatchBoundaryTest extends ReplicaTestBase {
    @Override
    protected KvFlushScheduler createTestKvFlushScheduler(Configuration conf) {
        return new KvFlushScheduler() {
            @Override
            public void enqueue(KvTablet tablet) {}

            @Override
            public void retryLater(KvTablet tablet, long delayMs) {}

            @Override
            public void close() {}
        };
    }

    @Test
    void testBackpressurePublishesWholeBatchAndRetryCompletes() throws Exception {
        Replica replica = createLeader();
        try {
            appendBatches(replica);
            long target = replica.getLocalLogEndOffset();
            assertThat(replica.getLogHighWatermark()).isZero();
            KvTablet tablet = replica.getKvTablet();
            rejectSecondWrite(tablet);
            tablet.runScheduledFlush();
            assertCommittedPrefix(replica);

            tablet.setBeforeNativeWrite(null);
            tablet.requestFlushRetry();
            tablet.runScheduledFlush();
            assertThat(tablet.getFlushedLogOffset()).isEqualTo(target);
            assertThat(replica.getLogHighWatermark()).isEqualTo(target);
            assertThat(replica.checkEnoughReplicasReachOffset(target).f0).isTrue();
            assertThat(tablet.getRowCount()).isEqualTo(601);
            assertThat(tablet.getKvPreWriteBuffer().pendingFlushBytes()).isZero();
            assertValue(tablet, 0, "before");
            assertValue(tablet, 600, "tail");
            assertThat(
                            replica.getLogTablet()
                                    .read(0, Integer.MAX_VALUE, FetchIsolation.HIGH_WATERMARK, true)
                                    .getRecords()
                                    .batches())
                    .extracting(batch -> batch.nextLogOffset())
                    .containsExactly(600L, 601L);
        } finally {
            deleteReplica(replica);
        }
    }

    @Test
    void testRecoveredBatchBoundariesSurviveBackpressureAndTruncation() throws Exception {
        conf.set(ConfigOptions.KV_WRITE_BATCH_SIZE, new MemorySize(64));
        Replica replica = createLeader();
        try {
            appendBatches(replica);
            replica.makeFollower(state(TABLET_SERVER_ID + 1, 1));
            replica.makeLeader(state(TABLET_SERVER_ID, 2));
            KvTablet tablet = replica.getKvTablet();
            tablet.requestFlush(
                    replica.getLocalLogEndOffset(),
                    failure -> {
                        throw new AssertionError(failure);
                    });
            rejectSecondWrite(tablet);
            tablet.runScheduledFlush();
            assertCommittedPrefix(replica);
            long hw = replica.getLogHighWatermark();
            replica.makeFollower(state(TABLET_SERVER_ID + 1, 3));
            assertThat(replica.isLeader()).isFalse();
            replica.truncateTo(hw);
            assertThat(replica.getLocalLogEndOffset()).isEqualTo(hw);
            assertThat(
                            replica.getLogTablet()
                                    .read(0, Integer.MAX_VALUE, FetchIsolation.LOG_END, true)
                                    .getRecords()
                                    .batches())
                    .singleElement()
                    .satisfies(batch -> assertThat(batch.nextLogOffset()).isEqualTo(hw));
            replica.makeLeader(state(TABLET_SERVER_ID, 4));
            assertThat(replica.getKvTablet().getRowCount()).isEqualTo(600);
            assertValue(replica.getKvTablet(), 0, "before");
            replica.putRecordsToLeader(
                    genKvRecordBatch(new Object[] {600, "tail"}), null, MergeMode.DEFAULT, 0);
            replica.getKvTablet().runScheduledFlush();
            assertThat(replica.getLogHighWatermark()).isEqualTo(601);
            assertValue(replica.getKvTablet(), 600, "tail");
        } finally {
            deleteReplica(replica);
        }
    }

    private void deleteReplica(Replica replica) throws Exception {
        try {
            replica.delete();
        } finally {
            replicaManager
                    .getServerMetricGroup()
                    .removeTableBucketMetricGroup(
                            DATA1_PHYSICAL_TABLE_PATH_PK.getTablePath(),
                            new TableBucket(DATA1_TABLE_ID_PK, 1));
        }
    }

    private Replica createLeader() throws Exception {
        Replica replica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, new TableBucket(DATA1_TABLE_ID_PK, 1));
        replica.makeLeader(state(TABLET_SERVER_ID, 0));
        return replica;
    }

    private NotifyLeaderAndIsrData state(int leader, int epoch) {
        return new NotifyLeaderAndIsrData(
                DATA1_PHYSICAL_TABLE_PATH_PK,
                new TableBucket(DATA1_TABLE_ID_PK, 1),
                Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1),
                new LeaderAndIsr(
                        leader,
                        epoch,
                        Collections.singletonList(leader),
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        epoch));
    }

    private void appendBatches(Replica replica) throws Exception {
        Object[][] rows = new Object[600][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[] {i, "before"};
        }
        replica.putRecordsToLeader(genKvRecordBatch(rows), null, MergeMode.DEFAULT, 0);
        replica.putRecordsToLeader(
                genKvRecordBatch(new Object[] {600, "tail"}), null, MergeMode.DEFAULT, 0);
        assertThat(replica.getLocalLogEndOffset()).isEqualTo(601);
    }

    private void rejectSecondWrite(KvTablet tablet) {
        AtomicInteger attempts = new AtomicInteger();
        tablet.setBeforeNativeWrite(
                () -> {
                    if (attempts.incrementAndGet() == 2) {
                        throw new StorageBackpressureException("Reject second native write");
                    }
                });
    }

    private void assertCommittedPrefix(Replica replica) throws Exception {
        KvTablet tablet = replica.getKvTablet();
        assertThat(tablet.getFlushState()).isEqualTo(KvTablet.FlushState.STORAGE_BLOCKED);
        assertThat(tablet.getFlushedLogOffset()).isEqualTo(600);
        assertThat(replica.getLogHighWatermark()).isEqualTo(600);
        assertThat(replica.checkEnoughReplicasReachOffset(600).f0).isTrue();
        assertThat(replica.checkEnoughReplicasReachOffset(601).f0).isFalse();
        assertThat(tablet.getRowCount()).isEqualTo(600);
        assertValue(tablet, 0, "before");
        assertValue(tablet, 599, "before");
        assertThat(
                        replica.getLogTablet()
                                .read(0, Integer.MAX_VALUE, FetchIsolation.HIGH_WATERMARK, true)
                                .getRecords()
                                .batches())
                .singleElement()
                .satisfies(batch -> assertThat(batch.nextLogOffset()).isEqualTo(600));
    }

    private void assertValue(KvTablet tablet, int key, String value) throws Exception {
        Tuple2<byte[], byte[]> expected =
                getKeyValuePairs(genKvRecords(new Object[] {key, value})).get(0);
        assertThat(tablet.multiGet(Collections.singletonList(expected.f0)).get(0).toByteArray())
                .containsExactly(expected.f1);
    }
}
