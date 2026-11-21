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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.ANOTHER_DATA1;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.replica.ReplicaManager.HIGH_WATERMARK_CHECKPOINT_FILE_NAME;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the high watermark persistence. */
final class HighWatermarkPersistenceTest extends ReplicaTestBase {

    @Test
    void testHighWatermarkPersistenceSingleReplica() throws Exception {
        replicaManager.checkpointHighWatermarks();
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        long highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(0L);

        // become leader.
        makeLogTableAsLeader(tableBucket.getBucket());

        // append record to replica and update high watermark to log end offset.
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replicaManager.checkpointHighWatermarks();
        highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(10L);

        // set the high watermark for local replica.
        replica.getLogTablet().updateHighWatermark(10L);
        replicaManager.checkpointHighWatermarks();
        highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(10L);
    }

    @Test
    void testHighWatermarkPersistenceMultipleReplicas() throws Exception {
        replicaManager.checkpointHighWatermarks();
        TableBucket tableBucket0 = new TableBucket(DATA1_TABLE_ID, 0);
        long highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(0L);

        // become leader.
        makeLogTableAsLeader(tableBucket0.getBucket());

        Replica replica0 = replicaManager.getReplicaOrException(tableBucket0);
        replicaManager.checkpointHighWatermarks();
        highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(0L);

        // set the high watermark for local replica.
        replica0.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replica0.getLogTablet().updateHighWatermark(10L);
        replicaManager.checkpointHighWatermarks();
        highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(10L);
        assertThat(replica0.getLogTablet().getHighWatermark()).isEqualTo(10L);

        // Add another append-only replica and set its high watermark.
        TableBucket tableBucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tableBucket1.getBucket());

        replicaManager.checkpointHighWatermarks();
        long highWatermark1 = highWatermarkFor(tableBucket1);
        assertThat(highWatermark1).isEqualTo(0);

        // set the highWatermark for local replica
        Replica replica1 = replicaManager.getReplicaOrException(tableBucket1);
        replica1.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replica1.getLogTablet().updateHighWatermark(10L);
        assertThat(replica1.getLogTablet().getHighWatermark()).isEqualTo(10L);

        // change the highWatermark for t1.
        replica0.appendRecordsToLeader(genMemoryLogRecordsByObject(ANOTHER_DATA1), 1);
        assertThat(replica0.getLogTablet().getHighWatermark()).isEqualTo(20L);
        replicaManager.checkpointHighWatermarks();

        // verify the highWatermark for t1 and t2.
        highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(20L);
        highWatermark1 = highWatermarkFor(tableBucket1);
        assertThat(highWatermark1).isEqualTo(10L);
    }

    @Test
    void testHighWatermarkPersistenceThread() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        long highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(0L);

        // become leader.
        makeLogTableAsLeader(tableBucket.getBucket());

        // append record to replica and update high watermark to log end offset.
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(highWatermarkFor(tableBucket)).isEqualTo(10L));
    }

    @Test
    void testReplicaManagerShutDownAndCheckpointHighWatermarks() throws Exception {
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb1.getBucket());
        // append record to replica and update high watermark to log end offset.
        Replica replica = replicaManager.getReplicaOrException(tb1);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        // Wait highWatermark checkpoint thread to checkpoint highWatermark.
        retry(Duration.ofMinutes(1), () -> assertThat(highWatermarkFor(tb1)).isEqualTo(10L));

        // tb2 don't wait highWatermark checkpoint thread to checkpoint highWatermark.
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb2.getBucket());
        // append record to replica and update high watermark to log end offset.
        Replica replica2 = replicaManager.getReplicaOrException(tb2);
        replica2.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);

        replicaManager.shutdown();
        assertThat(highWatermarkFor(tb1)).isEqualTo(10L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);

        // 1. replicaManager shutdown after tb1/tb2 become leader.
        replicaManager = buildReplicaManager(new TestCoordinatorGateway());
        replicaManager.startup();
        assertThat(highWatermarkFor(tb1)).isEqualTo(10L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);
        makeLogTableAsLeader(tb1.getBucket());
        makeLogTableAsLeader(tb2.getBucket());
        replica = replicaManager.getReplicaOrException(tb1);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replicaManager.shutdown();
        assertThat(highWatermarkFor(tb1)).isEqualTo(20L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);

        // 2. replicaManager shutdown before tb1/tb2 become leader.
        replicaManager = buildReplicaManager(new TestCoordinatorGateway());
        replicaManager.startup();
        replicaManager.shutdown();
        assertThat(highWatermarkFor(tb1)).isEqualTo(20L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);
    }

    /**
     * Reproducer for https://github.com/apache/fluss/issues/5190: after a crash the local high
     * watermark is only the last checkpoint, so a follower without a leader must not truncate its
     * log to it, as it is the only replica left in ISR and will be elected leader again.
     */
    @Test
    void testFollowerWithoutLeaderKeepsLogAboveCheckpointedHighWatermark() throws Exception {
        // Only explicit checkpoints below, so the checkpoint content is deterministic.
        conf.set(ConfigOptions.LOG_REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL, Duration.ofHours(1));
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        List<Integer> onlyReplica = Collections.singletonList(TABLET_SERVER_ID);
        OffsetCheckpointFile checkpointFile =
                new OffsetCheckpointFile(new File(tempDir, HIGH_WATERMARK_CHECKPOINT_FILE_NAME));

        makeLogTableAsLeader(tb.getBucket());
        // The checkpoint thread runs once right after the first notifyLeaderAndIsr; wait for it
        // so that it cannot overwrite the stale checkpoint prepared below.
        waitUntil(
                () -> checkpointFile.read().containsKey(tb),
                Duration.ofMinutes(1),
                "initial high watermark checkpoint is not written");

        Replica replica = replicaManager.getReplicaOrException(tb);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        // All 20 rows are acknowledged, as the ISR is only this server.
        assertThat(replica.getLogTablet().getHighWatermark()).isEqualTo(20L);

        // Simulate a crash: shut down first so only one ReplicaManager ever writes the checkpoint
        // file, then put back a checkpoint taken before the last 10 rows, which is what a crash
        // between two checkpoints leaves behind.
        replicaManager.shutdown();
        checkpointFile.write(Collections.singletonMap(tb, 10L));
        replicaManager = buildReplicaManager(testCoordinatorGateway);
        replicaManager.startup();

        // The coordinator kept this server as the only ISR member with NO_LEADER, so the
        // restarted replica first becomes a follower without a leader.
        makeLeaderAndFollower(
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                onlyReplica,
                                new LeaderAndIsr(
                                        LeaderAndIsr.NO_LEADER,
                                        INITIAL_LEADER_EPOCH + 1,
                                        onlyReplica,
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH + 1),
                                TEST_ROUTING_BUCKET_COUNT,
                                0L)));
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        assertThat(logTablet.getHighWatermark()).isEqualTo(10L);
        assertThat(logTablet.localLogEndOffset()).isEqualTo(20L);

        // Then the same replica is elected leader and must still serve all acknowledged rows.
        makeLeaderAndFollower(
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                onlyReplica,
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        INITIAL_LEADER_EPOCH + 2,
                                        onlyReplica,
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH + 2),
                                TEST_ROUTING_BUCKET_COUNT,
                                0L)));
        assertThat(logTablet.localLogEndOffset()).isEqualTo(20L);
        assertThat(logTablet.getHighWatermark()).isEqualTo(20L);
    }

    private long highWatermarkFor(TableBucket tableBucket) throws Exception {
        return new OffsetCheckpointFile(
                        new File(
                                conf.getString(ConfigOptions.DATA_DIR),
                                HIGH_WATERMARK_CHECKPOINT_FILE_NAME))
                .read()
                .getOrDefault(tableBucket, 0L);
    }
}
